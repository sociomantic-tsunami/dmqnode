/*******************************************************************************

    A struct that constitutes the state of a queue disk channel.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.ChannelMetadata;

import dmqnode.storage.engine.overflow.RecordHeader;

import ocean.transition;

struct ChannelMetadata
{
    import dmqnode.storage.engine.overflow.Const;
    import Tracker = dmqnode.storage.engine.overflow.FirstOffsetTracker;

    import ocean.core.Enforce: enforce;
    import ocean.stdc.posix.sys.types: off_t;

    /***************************************************************************

        `FirstOffsetTracker` struct definition.

    ***************************************************************************/

    alias Tracker.FirstOffsetTracker!(typeof(*this)) FirstOffsetTracker;

    /***************************************************************************

        A number that identifies this channel. It must be greater than 0 and
        does not change as long as the channel exists.

    ***************************************************************************/

    uint id;

    /***************************************************************************

        The file position (offset) of the next record to pop from this channel
        or 0 if the channel is empty (records == 0).
        If the channel is not empty, 0 is a valid value but indicates that no
        record has been popped from the channel yet.

    ***************************************************************************/

    off_t first_offset = 0;

    /***************************************************************************

        The file position (offset) of the most recently pushed record in this
        channel or 0 if the channel is empty (records == 0).
        If the channel is not empty, 0 is a valid value but indicates
        that exactly one record is in the channel.

    ***************************************************************************/

    off_t last_offset = 0;

    /***************************************************************************

        The amount of payload data of all records in the channel that have not
        been popped yet.
        - The number of bytes in the disk overflow file occupied by the records
          in this channel that have not been popped yet is
          this.bytes + this.records * RecordHeader.sizeof.
        - The number of bytes occupied by records in this channel that have
          already been popped is not tracked and can only be retrieved by
          parsing the data file from the beginning.
        - This value is only for usage statistics and not required for
          operation. It is, however, used for sanity checks.

        If the channel is not empty, 0 is a valid value but indicates that all
        records in the channel are of size 0.

    ***************************************************************************/

    ulong bytes = 0;

    /***************************************************************************

        The number of records in the channel that have not been popped yet.
        - The number of records in this channel that have already been popped is
          not tracked and can only be retrieved by parsing the data file from
          the beginning.
        - This value is only for usage statistics and not required for
          operation. It is, however, used for sanity checks.

    ***************************************************************************/

    uint records = 0;

    /***************************************************************************

        The header of the most recently pushed record in this channel.
        When pushing the next record, the header of the last record must be
        updated. Caching its header saves a file seek & read operation.

    ***************************************************************************/

    RecordHeader last_header;

    /***************************************************************************

        The entry of this channel in the first offset tracker.
        This member is `null` if and only if this channel is not tracked, which
        is the case if and only if this channel is empty. Except for checking if
        this member is `null` or not, it should be used only in
        `FirstOffsetTracker`.

    ***************************************************************************/

    package Tracker.FirstOffsetTrackerEntry* tracker_entry;

    /***************************************************************************

        Updates this instance after pushing a record.

        Params:
            header         = the header of the pushed record
            new_rec_offset = the file offset of the pushed record
            data_length    = the length of the payload data of the pushed record
            first_offset_tracker = the first offset tracker

    ***************************************************************************/

    void updatePush ( RecordHeader header, off_t new_rec_offset, size_t data_length,
                      ref FirstOffsetTracker first_offset_tracker )
    {
        if (!this.records++)
        {
            /*
             * channel.records was 0 so we're pushing the first record
             * to a new channel and have to initialise channel.first_offset.
             */
            this.first_offset = new_rec_offset;
            first_offset_tracker.track(*this);
        }
        /*
         * Update channel.last_offset: The record we just pushed is now the
         * last record in the channel.
         */
        this.last_offset = new_rec_offset;
        this.last_header = header;

        this.bytes += data_length;
    }

    /***************************************************************************

        Updates this instance after popping a record.

        Params:
            next_offset    = the offset of the next record after the popped one
                             as reported in the record header
            data_length    = the length of the payload data of the popped record
            first_offset_tracker = the first offset tracker
            e              = exception to throw on inconsistent parameters

        In:
            this.records must not be 0, i.e. the channel must not be empty.

    ***************************************************************************/

    void updatePop ( off_t next_offset, size_t data_length,
                     ref FirstOffsetTracker first_offset_tracker,
                     lazy Exception e )
    in
    {
        assert(this.records);
    }
    body
    {
        enforce(e, this.bytes >= data_length, "pop: channel too short");
        this.bytes -= data_length;

        switch (--this.records)
        {
            case 0:
                /*
                 * Popped the last record in the channel: Verify that the
                 * channel length was equal to the size of the record, then
                 * reset_ the channel to be empty.
                 */
                enforce(e, !next_offset, "popped record points to a next one but the channel is now empty");
                enforce(e, !this.bytes, "pop: channel size mismatch");
                this.reset_();
                break;
            case 1:
                /*
                 * Popped the second last record in the channel, which must be
                 * the second most recently pushed, so header.next_offset of the
                 * popped record must refer to channel.last_offset, the most
                 * recently pushed record.
                 */
                enforce(e, this.first_offset + next_offset == this.last_offset,
                        "pop: offset mismatch of last record");
                goto default;
            default:
                enforce(e, next_offset,
                        "popped records appears to be the last but there are more records in the channel");
                /*
                 * Adjust channel.first_offset to the file position of the next
                 * record in the channel, which will be the next record to pop.
                 */
                this.first_offset += next_offset;
                first_offset_tracker.track(*this);
        }
    }


    /***************************************************************************

        Obtains the next channel in ascending order of `first_offset`. This
        channel is expected to contain records (i.e. not be empty).

        Returns:
            the next channel in ascending order of `first_offset` or `null` if
            this channel has the highest `first_offset` of all channels.

    ***************************************************************************/

    public typeof(this) next ( )
    {
        return FirstOffsetTracker.next(*this);
    }

    /***************************************************************************

        Validates channel except the channel ID. Used in the invariant and
        externally.
        This method must be static so that the invariant is not called upon
        entering, which will result in recursion.

        Params:
            channel = instance of this struct to validate
            check   = callback delegate:
                      - good: true if a test passed or false if it failed,
                      - msg:  error message if the test failed.

    ***************************************************************************/

    static void validate ( typeof(*this) channel, void delegate ( bool good, char[] msg ) check )
    {
        switch (channel.records)
        {
            case 0:
                check(!channel.first_offset, "non-zero first_offset with no records");
                check(!channel.last_offset, "non-zero last_offset with no records");
                check(!channel.bytes, "non-zero bytes with no records");
                return;
            case 1:
               check(channel.first_offset == channel.last_offset, "first_offset different from last with one record");
               break;
            default:
                check(channel.first_offset < channel.last_offset, "first_offset expected to be less than last");
                break;
        }

        check(channel.first_offset >= Const.datafile_id.length, "first_offset before end of data file ID");
    }

    /***************************************************************************

        Resets channel to be empty. Does not change the channel ID.

        This is a static method to prevent the invariant from failing upon
        entering this method if channel is not in a valid state. The invariant
        is executed after channel has been reset.

        Params:
            channel = channel metadata to reset

    ***************************************************************************/

    public static void reset ( ref typeof(*this) channel )
    {
        channel.reset_();
    }

    /**************************************************************************/

    invariant ( )
    {
        assert(this.id, "zero channel ID");

        if (this.records)
        {
            assert(this.last_header.channel == this.id, "wrong channel ID of last header");
            assert(this.tracker_entry !is null,
                   "not registered in the first offset tracker with records");
        }
        else
        {
            assert(this.last_header == this.last_header.init, "last header expected to be blank with no records");
            assert(this.tracker_entry is null,
                   "registered in the first offset tracker with no records");
        }

        assert(!this.last_header.next_offset, "last_header.next expected to be 0");

        this.validate(*this, (bool good, char[] msg)
                      {
                          assert(good, msg);
                      });
    }

    /***************************************************************************

        Resets this channel to be empty. Does not change the channel ID.

        This method must be private to prevent the invariant from being executed
        upon entry.

        Out:
            This instance is in a clean state.

    ***************************************************************************/

    private void reset_ ( )
    out
    {
        assert(&this); // invariant
    }
    body
    {
        FirstOffsetTracker.remove(*this);
        auto id = this.id;
        *this = (*this).init;
        this.id = id;
    }
}
