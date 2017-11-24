/*******************************************************************************

    The public channel access interface. The DiskOverflow.Channel subclass is
    instantiatable in the public.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.OverflowChannel;

import dmqnode.storage.engine.DiskOverflow;
import dmqnode.storage.engine.overflow.ChannelMetadata;
import dmqnode.storage.engine.overflow.RecordHeader;

import ocean.transition;

package class OverflowChannel: DiskOverflowInfo
{
    /***************************************************************************

        The channel name.

    ***************************************************************************/

    private istring name;

    /***************************************************************************

        The host of the disk queue; queue access methods of this instance
        forward the calls to the host.

    ***************************************************************************/

    private DiskOverflow host;

    /***************************************************************************

        Pointer to the channel metadata maintained in this.host. The referenced
        object may be modified by the host itself without this instance doing
        anything, or by another instance of this class that refers to the same
        channel.

    ***************************************************************************/

    private ChannelMetadata* metadata;

    /**************************************************************************/

    invariant ( )
    {
        assert(this.metadata);
    }

    /***************************************************************************

        Constructor. Obtains a handle for channel_name, creating the channel if
        it doesn't exists.

        Params:
            host         = the host of the disk queue
            channel_name = channel name

    ***********************************************************************/

    package this ( DiskOverflow host, istring channel_name )
    {
        this.name     = channel_name;
        this.host     = host;
        this.metadata = host.getChannel(channel_name);
    }

    /***************************************************************************

        Pushes a record to this channel.

        Params:
            data    = record data

        Throws:
            FileException on file I/O error or data corruption.

    ***************************************************************************/

    public void push ( void[] data )
    {
        this.host.push(*this.metadata, data);
    }

    /***************************************************************************

        Pops a record to this channel.

        Calls get_buffer with the record length n; get_buffer is expected to
        return an array of length n. Populates that buffer with the record data.
        Does not call get_buffer if the queue was empty.

        Params:
            get_buffer = callback delegate to obtain the destination buffer for
                         the record data

        Returns:
            true if a record was popped or false if the queue was empty.

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public bool pop ( void[] delegate ( size_t n ) get_buffer )
    {
        return this.host.pop(*this.metadata, get_buffer);
    }

    /***************************************************************************

        Resets the state of this channel to empty.

        If there are records in other channels, the record data of this channel
        remain untouched but are not referenced any more. If all other channels
        are empty or this is the only channel, the data and index file are
        truncated to zero size.

    ***************************************************************************/

    public void clear ( )
    {
        this.host.clearChannel(*this.metadata);
    }


    /***************************************************************************

        Renames this channel.

        Params:
            `new_name` = new channel name

    ***************************************************************************/

    public void rename ( istring new_name )
    {
        auto old_name = this.name;
        this.name = new_name;
        this.metadata = this.host.renameChannel(old_name, this.name);
    }

    /***************************************************************************

        Returns:
            the number of records in this channel.

    ***************************************************************************/

    public uint num_records ( )
    {
        return this.metadata.records;
    }

    /***************************************************************************

        Returns:
            the amount of payload bytes of all records in this channel.

    ***************************************************************************/

    public ulong num_bytes ( )
    {
        return this.metadata.bytes;
    }

    /***************************************************************************

        Returns:
            the total amount of bytes occupied by all records in this
            channel.

    ***************************************************************************/

    public ulong length ( )
    {
        return this.metadata.bytes + this.metadata.records * RecordHeader.sizeof;
    }

    /***************************************************************************

        Removes the channel referred to by `this_`. If the channel contains
        records they will be inaccessible, but their data will stay in the file
        until the file data are removed automatically when the file is reset or
        truncated.

        This function renders `this_` unusable; that is, its invariant will
        fail. `readd` makes `this_` usable again.

        Params:
            `this_` = the instance of this class referring to the channel that
                      should be removed.

    ***************************************************************************/

    public static void remove ( typeof(this) this_ )
    in
    {
        assert(this_);
    }
    body
    {
        this_.host.removeChannel(this_.name);
        this_.name = null;
        this_.metadata = null;
    }

    /***************************************************************************

        Adds channel `channel_name` and sets up `this_` to refer to that
        channel. `this_` is expected to have been released by a previous
        `remove` call.

        Params:
            `this_` = an instance of this class that does not currently refer to
                      any channel and should refer to channel `channel_name`

    ***************************************************************************/

    public static void readd ( typeof(this) this_, istring channel_name )
    in
    {
        assert(this_.name is null);
        assert(this_.metadata is null);
    }
    out
    {
        assert(this_);
    }
    body
    {
        this_.name     = channel_name;
        this_.metadata = this_.host.getChannel(channel_name);
    }
}
