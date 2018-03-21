/******************************************************************************

    Message queue with channels using file storage

    The queue operates on one file and uses queue channels. Pushing to any
    channel appends the pushed record to the file so all records of all
    channels are in the file in the order of pushing. Each record consists of
    a fixed size header of type RecordHeader, followed by the record payload.
    The record header contains
      - the byte length of the record payload,
      - the relative file position to the next record in the same channel
        (except for the last record in a channel) and
      - a channel ID number identifying the channel the record belongs to.

    For each channel the file position of the first (next to pop) and last (most
    recently pushed) record is tracked.
    Pushing
      - sets the position tracker of the last record in the channel to the end
        of the file and
      - adjusts the last (most recently pushed) record in the same channel to
        also point to the end of the file, which is the file position of the
        newly pushed record, then
      - appends the new record to the file.

    Popping reads the record at the file position of the "first" tracker and
    advances that tracker to the next record in the same channel as reported in
    the header of that record.

    The tracked positions, channel names, number of records and total number of
    record payload in each channel are written to an index file on shutdown to
    be restored on the next startup. The format of this file is ASCII text:
     - One line per channel.
     - Each line consists of

           {channel name} {records} {bytes} {first-position} {last-position}

       where {channel name} is a string and {records}, {bytes},
       {first-position}, {last-position} are decimal numbers. The tokens are
       whitespace separated. For example,

           MyChannel 3049 1540516 5040 9590357

       means, channel "MyChannel" has 3049 records with a total payload of
       1540516 bytes, the first record is at file position 5040 and the last at
       9590357.

    Channel names and channel IDs

    Each channel has a channel name and a channel ID. The channel ID is a
    positive integer value. The ID of the channel each record belongs to is
    stored in the header of the record in the data file when the record is
    pushed.
    The channel names are stored in the index file.
    When a new channel is created, the highest of all currently used channel IDs
    plus 1 is assigned to it.
    Starting with an empty disk overflow without any channel, this highest ID is
    0 so the channel that is created first has channel ID 1, the next one 2 and
    so on. At startup, if loading data stored on the previous shutdown, each
    channel name is read from the index file and the corresponding channel ID
    from the first record in the channel.

    Data validation and version check

    The first eight bytes of the data file are a magic ASCII string (not NUL-
    terminated) that includes a version number. It is checked on initialisation
    during startup.

    RecordHeader uses an 8-bit horizontal parity (that is, the exclusive OR of
    all bytes in the struct) as a basic protection should the tracker positions
    be wrong because of a program bug.

    Saving data on shutdown and loading them on startup

    On shutdown the states of the channels trackers must be written to the index
    file. For the data file the current operation (push, pop or clear) must be
    allowed to finish if shutdown is requested while performing an operation.
    Except for that the data file does not need any special shutdown handling.

    If an index and data file exists on startup then the startup sequence is:
     - Verify the data file ID (the magic string in the first eight bytes).
     - Parse the index file and create all channels that are mentioned in it.
     - For each channel read the first and last record of the channel from the
       data file assign their channel ID (both should have the same) to the
       channel.

    All steps involve sanity checks, and startup will fail if any of the checks
    fails.

    An up-to-date index file is of critical importance to restore the previous
    state on startup. Because of that the index file is periodically written to
    disk, the time interval can be configured and is 60 seconds by default.

    An outdated but not otherwise corrupted index file will cause the
    follwing issues at startup:
     - For a channel that has been pushed to in the mean time the "next" field
       the record at the "last" position according to the index file will not be
       0 (which indicates it is really the last record in the channel but point
       to another record. This can be fixed by following the chain of the
       following records in the data file until the really last record of the
       channel is found (whose "next" field in the header is 0).
     - Records that have been popped from a channel in the mean time will be in
       the queue again. There is no way of telling which records were already
       popped.
     - A channel that has been created in the mean time can only be recovered by
       parsing the whole data file and looking for chains of records that are
       not referenced by the index. However, such a chain could also be a
       deleted channel, and there is no way of telling which it is. The channel
       name is lost.
     - A channel that has been deleted in the mean time will be resurrected,
       possibly with outdated push and pop file positions.

    An outdated push position (i.e. the supposedly last record isn't really the
    last one) is the only of these four conditions that can be (and are)
    detected by sanity checks.

    I/O details and performance concerns

    Data file I/O involved in push and pop operation uses POSIX pread(),
    pwrite() and writev(). Of these functions only writev() uses the implicit
    file position, which is always at the end of the file. Writing pwrite()s a
    record header (<100 B), then writev()s a full record (header plus payload).
    When writing in a high frequency to one or more channels then the seeking
    distance for pwrite() is most of the time the length of one record. Reading
    pread()s one record from a low file position. When reading in a high
    frequency from one channel without writing in between then reading is purely
    sequential but involves skipping records that belong to other channels.

    Of each data chunk that is written to the file the first <100B (the record
    header) are later (mostly very soon) overwritten once. All data that were
    written to the file are read exactly once so reading could benefit from the
    "Don't Reuse" POSIX file advice. However, that file advice is not
    implemented in Linux (unless it was added recently), and testing with Linux
    3.0.0-32-generic did not yield any noticeable performance gain.

    Currently blocking I/O is used; if that proves to harm server operation,
    non-blocking epoll-multiplexed I/O will be used instad. Note that on Linux
    non-blocking file I/O is currently only partly implemented and depends on
    the file system; reading should work but writing may sometimes still block.
    At least for the queue node server sporadic stalls are not critical as long
    as the server can keep up with the throughput for channels that aren't
    overflown.

    Index file I/O uses C stdio streams to allow for text formatting and
    parsing.

    Certain POSIX I/O functions fail with EINTR if they were interrupted by a
    signal (more precisely, if a signal was raised before they read or wrote any
    byte of data). In this case these functions just need to be called again;
    this is taken care of. If a signal is raised while a C stdio stream function
    is executing, it is impossible to restore a valid state and ensure valid
    output data. Because valid output to the index file is very important all
    signals are blocked during index file I/O (except for a couple of fatal ones
    including SIGABRT and SIGSEGV). Signals that should be blocked during index
    file I/O include SIGINT and SIGTERM to prevent incomplete file output if the
    user initiates an application shutdown (for example by pressing Ctrl+C) in
    that moment.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.DiskOverflow;

/******************************************************************************/

interface DiskOverflowInfo
{
    /***************************************************************************

        Returns:
            the number of records in the queue.

    ***************************************************************************/

    uint  num_records ( );

    /***************************************************************************

        Returns:
            the total number of record data bytes in the queue.

    ***************************************************************************/

    ulong num_bytes ( );
}

/***************************************************************************

    Exceptions thrown in DiskOverflow.

***************************************************************************/

class DiskOverflowException: Exception {this ( ) {super(null);}}

/******************************************************************************/

class DiskOverflow: DiskOverflowInfo
{
    import dmqnode.storage.engine.overflow.ChannelMetadata;
    import dmqnode.storage.engine.overflow.Constants;
    import dmqnode.storage.engine.overflow.file.DataFile;
    import dmqnode.storage.engine.overflow.file.FileException;
    import dmqnode.storage.engine.overflow.file.HeadTruncationTestFile;
    import dmqnode.storage.engine.overflow.file.IndexFile;
    import dmqnode.storage.engine.overflow.FirstOffsetTracker;
    import dmqnode.storage.engine.overflow.RecordHeader;
    import dmqnode.storage.engine.OverflowChannel;

    import swarm.neo.protocol.socket.uio_const: iovec_const;

    import ocean.core.Enforce: enforce;
    import ocean.io.FilePath;
    import core.stdc.errno: errno;
    import core.sys.posix.sys.types: off_t;
    import core.stdc.stdio: SEEK_CUR, SEEK_END;
    import ocean.core.Verify;
    import ocean.transition;
    import ocean.util.log.Logger;

    /***************************************************************************

        The public interface for obtaining a channel handle, see the base class
        for methods.

    ***************************************************************************/

    public class Channel: OverflowChannel
    {
        /***********************************************************************

            Constructor. Obtains a handle for channel_name, creating the channel
            if it doesn't exists.

            Params:
                channel_name = channel name

        ***********************************************************************/

        public this ( istring channel_name )
        {
            super(this.outer, channel_name);
        }
    }

    /***************************************************************************

        The registry of the channels that are currently present. It serves two
        purposes:
            1. It allows for looking up a channel by channel name.
            2. The ChannelMetadata instances that track the state of each
               channel are stored here. All operations that manipulate a channel
               (pushing to, popping from, clearing a channel) read from and
               write to the value in this associative array that corresponds to
               the channel that is being manipulated.

        This registry contains all channels that have been created since
        startup including channels that were emptied in the mean time. It is
        populated when restoring the channels that were saved on the previous
        shutdown.

    ***************************************************************************/

    private ChannelMetadata[cstring] channels;

    /***************************************************************************

        Keeps track of the `first_offset` of the channels that contain records,
        allowing for iterating over the channels in ascending order of their
        `first_offset` value.

    ***************************************************************************/

    private ChannelMetadata.FirstOffsetTracker first_offset_tracker;

    /***************************************************************************

        The highest channel ID at present. This value plus 1 is used when
        creating a new channel. Must be greater than 0 if there are channels or
        0 otherwise.

    ***************************************************************************/

    private uint highest_channel_id = 0;

    /***************************************************************************

        The total number of records in all channels that have not been popped
        yet.
        - The number of records that have already been popped is not tracked and
          can only be retrieved by parsing the data file from the beginning.
        - This value is only for usage statistics and not required for
          operation. It is, however, used for sanity checks.

    ***************************************************************************/

    private uint records = 0;

    /***************************************************************************

        The total amount of payload data of all records in all channels that
        have not been popped yet.
        - The number of bytes in the disk overflow file occupied by all records
          in all channels that have not been popped yet is
          this.bytes + this.records * RecordHeader.sizeof.
        - The number of bytes occupied by records that have already been popped
          is not tracked and can only be retrieved by parsing the data file from
          the beginning.
        - This value is only for usage statistics and not required for
          operation. It is, however, used for sanity checks.

        If there are records, 0 is a valid value but indicates that all records
        are of size 0.

    ***************************************************************************/

    private ulong bytes = 0;

    /***************************************************************************

        `true` if the run-time test for truncating a file in the beginning
        succeeded in the constructor or false if it failed. Minimizing the data
        file size is supported only if this member is `true`.

    ***************************************************************************/

    debug (OvfMinimizeTest) public  bool data_file_size_mimimizing_supported;
    else                    private bool data_file_size_mimimizing_supported;

    /***************************************************************************

        Reusable exception.

    ***************************************************************************/

    private DiskOverflowException e;

    /**************************************************************************/

    invariant ( )
    {
        if (this.records)
        {
            assert(this.channels.length, "have records but no channels");
            assert(this.highest_channel_id, "zero highest_channel_id with records");
        }
        else
        {
            assert(!this.bytes, "expected zero bytes with records");

            if (this.channels.length)
            {
                assert(this.highest_channel_id, "zero highest_channel_id with channels");
            }
            else
            {
                assert(!this.highest_channel_id, "non-zero highest_channel_id without channels");
            }
        }

        debug (Full)
        {
            auto mthis = (cast(Unqual!(typeof(this)))this);

            uint records = 0;
            ulong bytes = 0;

            auto first_offsets = new off_t[this.channels.length];

            {
                size_t i = 0;
                foreach (ref channel; this.channels)
                {
                    assert(channel.id, "zero channel id");
                    assert(&channel); // call channel invariant
                    records += channel.records;
                    bytes += channel.bytes;

                    if (channel.bytes)
                        first_offsets[i++] = channel.first_offset;
                }
                first_offsets.length = i;
            }

            assert(records == this.records, "numbers of records mismatch");
            assert(bytes == this.bytes, "numbers of bytes mismatch");

            auto channel = mthis.first_offset_tracker.first;
            foreach (i, n; first_offsets.sort)
            {
                assert(channel !is null,
                       "less tracked channels than channels with records");
                assert(channel.first_offset == n,
                       "channel.first_offset mismatch");
                channel = channel.next;
            }
            assert(channel is null,
                   "more tracked channels than channels with records");

            delete first_offsets;
        }
    }

    /***************************************************************************

        Logger

    ***************************************************************************/

    private static Logger log;

    static this ( )
    {
        log = Log.lookup("diskoverflow");
    }

    /***************************************************************************

        The data file.

    ***************************************************************************/

    debug (OvfMinimizeTest) public  DataFile data;
    else                    private DataFile data;

    /***************************************************************************

        The index file.

    ***************************************************************************/

    private IndexFile index;

    /***************************************************************************

        Constructor.

        Params:
            dir = working directory

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public this ( cstring dir )
    {
        FilePath(dir).create();

        try
        {
            this.data_file_size_mimimizing_supported = false;
            scope testfile = new HeadTruncationTestFile(dir);
            this.data_file_size_mimimizing_supported = testfile.head_truncation_supported;

            if (this.data_file_size_mimimizing_supported)
                log.info(
                    "Minimizing the disk overflow data file size is supported."
                );
            else
                log.warn(
                    "Minimizing the disk overflow data file size is not " ~
                    "supported."
                );
        }
        catch (FileException e)
        {
            log.error("Error testing file truncation: {}", getMsg(e));
        }

        this.e     = new DiskOverflowException;
        this.data  = new DataFile(dir, Constants.datafile_name);
        this.index = new IndexFile(dir, Constants.indexfile_name);

        this.initChannels(this.verifyDataFileId());
    }

    /***************************************************************************

        Returns:
            the number of records in the queue.

    ***************************************************************************/

    public uint num_records ( )
    {
        return this.records;
    }

    /***************************************************************************

        Returns:
            the total number of record data bytes in the queue.

    ***************************************************************************/

    public ulong num_bytes ( )
    {
        return this.bytes;
    }

    /***************************************************************************

        Writes the queue index.

        This method is called on shutdown but may also be called at any time to
        save the current queue index in the file.

        Throws:
            IOException on file I/O error.

    ***************************************************************************/

    public void writeIndex ( )
    {
        if (this.records)
        {
            this.index.writeLines(
            ( void delegate ( cstring name, ChannelMetadata channel ) writeln )
            {
                foreach (name, channel; this.channels)
                {
                    if (channel.records)
                    {
                        writeln(name, channel);
                    }
                }
            });
        }
    }

    /***************************************************************************

        Pushes a record.

        Params:
            channel = the channel to write to (as obtained from getChannel())
            data    = record data

        Throws:
            FileException on file I/O error or data corruption.

    ***************************************************************************/

    package void push ( ref ChannelMetadata channel, in void[] data )
    in
    {
        assert(&channel);
    }
    out
    {
        assert(&channel);
    }
    body
    {
        off_t pos = this.getPushPosition();

        /*
         * If there are already records in the channel, we have to make the
         * last (most recently pushed) record point to the record we are about
         * to push so we update the header.next_offset field of the last to the
         * current file position, which will be the file position of the newly
         * pushed record.
         */

        if (channel.records)
        {
            this.updateLastHeader(pos, channel.last_offset, channel.last_header);
        }

        channel.updatePush(
            this.writeRecord(channel.id, data), pos, data.length,
            this.first_offset_tracker
        );

        this.bytes += data.length;
        this.records++;
    }


    /***************************************************************************

        Obtains the file position where a new record should be written. Writes
        the global data file ID string if the data file is empty.

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    private off_t getPushPosition ( )
    out (pos)
    {
        /*
         * Make sure the file position never interferes with the ID string at
         * the beginning of the data file.
         */
        assert(pos >= Constants.datafile_id.length);
    }
    body
    {
        /*
         * Pushing a record means appending it to the data file. The file
         * position is always at the end.
         * If there is no record in the file then the file should be empty so we
         * have to write the data file ID string before appending the first
         * record to the file. We're also going to need the file position where
         * we append the record.
         */
        off_t pos = this.data.seek(0, SEEK_CUR, "Unable to seek to tell the file size");

        if (this.records)
        {
            this.data.enforce(pos >= Constants.datafile_id.length, "File size less than length of ID string");
        }
        else
        {
            this.data.enforce(!pos, "File expected to be empty");
            this.data.write(Constants.datafile_id, "Unable to write the data file ID");
            pos = Constants.datafile_id.length;
        }

        return pos;
    }

    /***************************************************************************

        Updates the header of the record at file offset last_offset to refer to
        a record at file position pos.

        Params:
            pos         = the file position of the record to be referred to
            last_offset = the file position of the record to be adjusted
            last_header = the cached header of the record to be adjusted

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    private void updateLastHeader ( off_t pos, off_t last_offset, RecordHeader last_header )
    in
    {
        /*
         * channel.last_offset is the start position of the last record in
         * the file. Given that there are records in the channel, it must be
         * less than the current file position (the total length of every
         * record is at least the length of the record header).
         */
        assert(last_offset < pos);
    }
    body
    {
        /*
         * Update the header of the previously pushed record so that the
         * file position of that record plus header.next_offset is the file
         * position of the record we're pushing, which is pos.
         */
        last_header.next_offset = pos - last_offset;
        last_header.setParity();
        /*
         * Write the updated header back.
         */
        this.data.pwrite(
            this.dump(last_header), last_offset,
            "push: unable to update last record"
        );
    }

    /***************************************************************************

        Writes a record to the file at the current file position (as obtained
        via getPushPosition()).

        Params:
            channel_id = the ID of the channel the record belongs to
            data       = record data (payload)

        Returns:
            the record header.

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    private RecordHeader writeRecord ( uint channel_id, in void[] data )
    {
        /*
         * Set up the header for the new record, make a copy for the next push
         * where it will be updated and append the new record to the file.
         */
        RecordHeader header;
        header.channel = channel_id;
        header.length  = data.length;
        header.setParity();

        iovec_const[2] iov_buf;
        auto iov = IoVec(iov_buf);
        iov[0]   = this.dump(header);
        iov[1]   = data;

        this.data.writev(iov, "unable to write record");

        return header;
    }

    /***************************************************************************

        Pops a record.

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

    package bool pop ( ref ChannelMetadata channel, void[] delegate ( size_t n ) get_buffer )
    in
    {
        assert(&channel);
    }
    out
    {
        assert(&channel);
    }
    body
    {
        if (!channel.records)
        {
            return false;
        }

        assert(this.records >= channel.records);
        assert(this.bytes >= channel.bytes);

        /*
         * The next record to pop in that channel is at file position
         * channel.first_offset. Read the record header, then the record data.
         */
        RecordHeader header;
        off_t pos = this.readHeader(header, channel.first_offset);
        enforce(this.e, header.channel == channel.id, "mismatch of channel ID in record header read from file");
        void[] data = get_buffer(header.length);
        verify(data.length == header.length, "pop: array returned by get_buffer not of requested size");

        this.data.enforce(
            !this.data.pread(data, pos, "unable to read record data"),
            "Unexpected end of file reading record data."
        );

        channel.updatePop(header.next_offset, data.length, this.first_offset_tracker, this.e);

        assert(this.records); // should be consistent with the above assertion
        assert(this.bytes >= data.length); // channel.updatePop() should otherwise have thrown
        this.records--;
        this.bytes -= data.length;

        if (!this.records)
        {
            this.data.reset();
            this.index.reset();
        }

        return true;
    }

    /***************************************************************************

        Resets the queue to be empty and truncates and reinitialises the file.

    ***************************************************************************/

    public void clear ( )
    out
    {
        assert(!this.records);
        assert(!this.bytes);
    }
    body
    {
        foreach (ref channel; this.channels)
        {
            this.clearChannel(channel);
        }
    }

    /***************************************************************************

        Resets the state of channel to empty.

        If there are records in other channels, the data of the records in this
        channel remain untouched. If all other channels are empty or this is the
        only channel, the data and index file are deleted.

        Params:
            channel = channel to reset

    ***************************************************************************/

    package void clearChannel ( ref ChannelMetadata channel )
    in
    {
        assert(this);
        assert(&channel);
        assert(this.records >= channel.records, "channel records greater than total records");
        assert(this.bytes >= channel.bytes, "channel bytes greater than total bytes");
    }
    out
    {
        assert(this);
    }
    body
    {
        if (!channel.records) return;

        this.records -= channel.records;
        this.bytes -= channel.bytes;

        channel.reset(channel); // Looks odd but see ChannelMetadata.reset().

        if (!this.records)
        {
            this.data.reset();
            this.index.reset();
        }
    }

    /***************************************************************************

        Re-registers the channel metadata is currently registered under
        `old_name` under `new_name`. Does not change anything if
        `old_name == new_name`.

        Params:
            old_name = current channel name
            new_name = new channel name

        Returns:
            a pointer to the `ChannelMetadata` object for the channel, which may
            have been moved to a different location. If this method throws
            rather than returning then the object `ChannelMetadata` has not been
            moved.

        Throws:
            Exception if
                - `old_name` was not found or
                - another channel `new_name` already exists, i.e. `new_name` was
                  found and is different from `old_name`.

    ***************************************************************************/

    package ChannelMetadata* renameChannel ( cstring old_name, istring new_name )
    out (channel_metadata)
    {
        assert(channel_metadata);
    }
    body
    {
        ChannelMetadata* channel = old_name in this.channels;
        enforce(this.e, channel !is null, "Unable to rename a channel: " ~
                "Channel not found");

        if (ChannelMetadata* new_channel = new_name in this.channels)
        {
            enforce(this.e, new_channel is channel, "Unable to rename a " ~
                    "channel: A channel with the new name already exists");
            return new_channel;
        }

        log.info("Renaming channel {} '{}' -> '{}'", channel.id, old_name, new_name);

        this.channels[new_name] = *channel;
        this.channels.remove(old_name);
        return new_name in this.channels;
    }

    /***************************************************************************

        Removes the channel referred to by `channel_name`.

        Params:
            channel_name = the name of the channel to remove

        Throws:
            Exception if `channel_name` was not found.

    ***************************************************************************/

    package void removeChannel ( cstring channel_name )
    {
        ChannelMetadata* channel = channel_name in this.channels;
        enforce(this.e, channel !is null, "Unable to remove a channel: " ~
                "Channel not found");

        this.clearChannel(*channel);
        this.channels.remove(channel_name);

        if (!this.channels.length)
            this.highest_channel_id = 0;
    }

    /***************************************************************************

        Flushes write buffers, minimizes the data file size and writes the index
        file.

    ***************************************************************************/

    public void flush ( )
    {
        this.minimizeDataFileSize();
        this.writeIndex();
        this.data.flush();
    }

    /***************************************************************************

        Writes the index and closes the files or deletes them if the queue was
        empty.

        It is not possible to use this instance and all associated Channel
        objects after this method returned.

    ***************************************************************************/

    public void close ( )
    {
        /*
         * Catch all file I/O exceptions and log them so that they don't prevent
         * further shutdown actions from being done.
         */
        static void logFileException ( lazy void op, Logger log )
        {
            try op;
            catch (FileException e)
            {
                log.error("{} @{}:{}", e.msg, e.file, e.line);
            }
        }

        if (this.records)
        {
            foreach (name, channel; this.channels)
            {
                if (channel.records)
                {
                    log.info("Closing channel {} '{}': {} records/{} bytes.", channel.id, name, channel.records, channel.bytes);
                }
                else
                {
                    log.info("Closing channel {} '{}': Empty.", channel.id, name);
                }
            }

            log.info("Shutting down with {} records/{} bytes in total.", this.records, this.bytes);

            if (this.index.fd >= 0)
            {
                logFileException(this.writeIndex(), this.index.log);
            }

            logFileException(this.index.close(), this.index.log);
            logFileException(this.data.close(), this.data.log);

        }
        else
        {
            log.info("Shutting down, empty.");
            logFileException(this.index.remove(), this.index.log);
            logFileException(this.data.remove(), this.data.log);

        }
    }

    /***************************************************************************

        Serialises x in the trivial way. T should be a value type.

        Params:
            x = variable to serialise

        Returns:
            a raw data slice referencing x.

    ***************************************************************************/

    private static void[] dump ( T ) ( ref T x )
    {
        return (cast(void*)(&x))[0 .. x.sizeof];
    }

    /***************************************************************************

        Reads a record header from the data file starting at position pos
        and ensures its integrity (parity and channel ID > 0).

        Params:
            header = output of the header read from the data file
            pos    = data file position of the start of the header data

        Returns:
            the data file position right after the header.

        Throws:
            - FileException on file I/O error
            - DiskOverflow.Exception on end of file or invalid header data.

    ***************************************************************************/

    private off_t readHeader ( out RecordHeader header, off_t pos )
    {
        auto start = pos;

        this.data.enforce(
            !this.data.pread(this.dump(header), pos, "unable to read record header"),
            "Unexpected end of file reading record header."
        );

        enforce(this.e, !header.calcParity, "invalid record header (parity check failed)");
        enforce(this.e, header.channel, "record read from file has channel 0");
        enforce(this.e, header.channel > 0, "record read from file was already popped");
        enforce(this.e, !header.next_offset || start + header.next_offset >= header.length,
                        "\"next\" link in record read from file is within the record itself");

        return pos;
    }

    /***************************************************************************

        Gets a handle for channel_name, creating the channel if it doesn't
        exists.

        Params:
            channel_name = channel name

        Returns:
            a handle for channel_name (never null).

    ***************************************************************************/

    package ChannelMetadata* getChannel ( istring channel_name )
    out (channel)
    {
        assert(channel);
    }
    body
    {
        if (ChannelMetadata* channel = channel_name in this.channels)
        {
            log.info("Handing over existing channel {} '{}'.", channel.id, channel_name);
            return channel;
        }
        else
        {
            enforce(this.e, this.highest_channel_id < this.highest_channel_id.max,
                                 "Unable to add a new channel: ChannelMetadata IDs exhausted");

            this.channels[channel_name] = ChannelMetadata(++this.highest_channel_id);
            /*
             * Return a pointer to this.channels[channel_name].
             */
            ChannelMetadata* channel = channel_name in this.channels;
            log.info("Creating new channel {} '{}'", channel.id, channel_name);
            return channel;
        }
    }

    /***************************************************************************

        `foreach`-style iteration over the channel names.

    ***************************************************************************/

    public int iterateChannelNames ( int delegate ( ref cstring name ) dg )
    {
        foreach (name, metadata; this.channels)
        {
            if (int x = dg(name))
                return x;
        }

        return 0;
    }

    /***************************************************************************

        Verifies the data file ID.
        Assumes the file position is at the beginning; upon return it is at the
        end.

        Returns:
            the data file size.

        Throws:
            FileException on
             - file I/O error or, if the data file is not empty,
             - EOF while reading or
             - data file ID mismatch.

    ***************************************************************************/

    private ulong verifyDataFileId ( )
    in
    {
        assert(this.data);
    }
    body
    {
        char[Constants.datafile_id.length] datafile_id;

        if (auto rem = this.data.read(datafile_id, "unable to read the file ID"))
        {
            /*
             * rem is datafile_id.length minus the number of bytes read before
             * EOF so if rem == datafile_id.length then the file is empty,
             * otherwise EOF happened while reading.
             */
            this.data.enforce(rem == datafile_id.length, "Unexpected end of file reading file ID");
            return 0;
        }
        else
        {
            this.data.enforce(datafile_id == Constants.datafile_id, "File ID mismatch");
            return this.data.seek(0, SEEK_END, "unable to initially seek to the end of the file");
        }
    }

    /***************************************************************************

        Miminizes the size of the data file by truncating the beginning of the
        file, as follows:
        The data file starts with an 8-byte identifier string,
        `Constants.datafile_id`, and the records follow that string. After
        records have been popped, there may be a contiguous range of already
        popped records between the identifier string and the first record in the
        data file that hasn't been popped yet. If such a range exists then this
        method will
         - truncate the data file from the beginning, removing the largest
           integer multiple of `DataFile.head_truncation_chunk_size` (1 MiB)
           that is less than the size of the range and
         - fill the range remaining after the truncation with a dummy record
           with channel id = 0 and bytes of zero value as the record data,
         - adjust the `first_offset` of all channels to reference the new
           position of the next record to pop.

        For example, suppose there is a range of 2.5 MiB of already popped
        records between the data file ID and the first not yet popped record, so
        the file layout looks like this:

                        |<-- 2.5 MiB  -->|
            datafile_id popped_records... first_not_yet_popped_record

        Then this method will do the following:

        1. Truncate the data file from the beginning, removing 2 MiB, which is
           the largest integer multiple of 1 MiB that is less than 2.5 MiB,
           leaving `datafile_id.length` + 0.5 MiB of junk at the beginning of
           the file:

            |<- datafile_id.length ->|<- 0.5 MiB ->|
            junk                     junk           first_not_yet_popped_record

        2. Write the data file ID to the new beginning of the data file:

                        |<-- 0.5 MiB -->|
            datafile_id junk             first_not_yet_popped_record

        3. Fill the remaining junk with a dummy record:

                        |<-- 0.5 MiB -->|
            datafile_id dummy_record     first_not_yet_popped_record

        The essential file operations are done using Linux `fallocate()`
          - in `FALLOC_FL_COLLAPSE_RANGE` mode to truncate the data file from
            the beginning,
          - in `FALLOC_FL_ZERO_RANGE` mode to write bytes of zero value as dummy
            record data.

        This is supported only by Linux 3.15 or higher and only for certain file
        systems including ext4 and XFS. See the `fallocate()` manual page for
        details. The constructor of this class does a run-time test if
        `fallocate()` supports these modes, and this method does nothing if that
        test failed.

        Returns:
            the number of bytes removed from the data file, which is 0 if
            nothing was done.

    ***************************************************************************/

    private ulong minimizeDataFileSize ( )
    {
        if (!this.data_file_size_mimimizing_supported)
            return 0;

        auto channel = this.first_offset_tracker.first;
        if (channel is null)
            return 0;

        /*
         * Set bytes_cutoff to the offset of the first record. The data file
         * starts with datafile_id, followed by header and data of the first
         * record. Consequently the lowest offset is either datafile_id.length,
         * if the very first record hasn't been popped yet, or at least
         * datafile_id.length + RecordHeader.sizeof otherwise.
         */
        auto bytes_cutoff = cast(ulong)channel.first_offset;
        if (bytes_cutoff == Constants.datafile_id.length)
            return 0;

        assert(bytes_cutoff >= (Constants.datafile_id.length + RecordHeader.sizeof));

        /*
         * Subtract (datafile_id.length + RecordHeader.sizeof), the extra space
         * needed at the beginning if truncating the file.
         */
        bytes_cutoff -= (Constants.datafile_id.length + RecordHeader.sizeof);

        /*
         * Now bytes_cutoff is the maximum number of bytes that can be
         * truncated. Do the truncation, truncateHead() will round bytes_cutoff
         * down to an integer multiple of the truncation chunk size, cut that
         * many bytes off and return that amount.
         */
        bytes_cutoff = this.data.truncateHead(bytes_cutoff);

        if (!bytes_cutoff)
            return 0;

        /*
         * Trials show that fallocate(FALLOC_FL_COLLAPSE_RANGE) messes with the
         * file position in the following way: The data file position -- that
         * is, the return value of lseek(0, SEEK_CUR) and the offset used by
         * write()/writev() -- is always at the end, so it should decrease if
         * we truncate the file from the beginning. However, the current file
         * position does in fact _not_ change!  The manual doesn't say what the
         * intended effect of fallocate(FALLOC_FL_COLLAPSE_RANGE) for the
         * current file position is.
         * So we have to seek to the end of the data file in order to get to
         * the right file position.
         */
        this.data.seek(0, SEEK_END, "unable to seek to the end of the file " ~
                                    "after truncating");

        /*
         * Subtract the number of bytes removed from the first offset of all
         * tracked channels (i.e. channels with records).
         */
        this.first_offset_tracker.updateCutoff(bytes_cutoff);

        /*
         * Write the datafile_id string to the beginning of the file.
         */
        off_t pos = 0;

        this.data.pwrite(
            Constants.datafile_id, pos,
            "unable to write the file ID after data file truncation"
        );

        /*
         * Overwrite the remaining junk data between the datafile_id string and
         * the now first record with a spare record. The spare record has
         * channel 0, which is not used for real records, and its data are zero
         * bytes.
         */
        auto first_offset = this.first_offset_tracker.first.first_offset;

        RecordHeader header;
        header.length = first_offset - header.sizeof - pos;
        header.setParity();

        this.data.pwrite(
            this.dump(header), pos,
            "unable to write the spare record header"
        );

        this.data.zeroRange(pos, header.length);

        debug (Full) this.verifyMinimizedDataFile();

        return bytes_cutoff;
    }

    /***************************************************************************

        Verifies the beginning of the data file after `minimizeDataFileSize()`
        has removed a range from the data file (that is, it returned a non-zero
        value).

    ***************************************************************************/

    debug (Full) private void verifyMinimizedDataFile ( )
    {
        off_t pos = 0;

        /*
         * Check the data file id.
         */
        char[Constants.datafile_id.length] datafile_id;
        this.data.enforce(
            !this.data.pread(
                datafile_id, pos, "unable to read the data file id"
            ),
            "Unexpected end of file reading the data file id."
        );

        /*
         * Check the header of the dummy record.
         */
        RecordHeader dummy_record_header;
        this.data.enforce(
            !this.data.pread(
                this.dump(dummy_record_header), pos,
                "unable to read the dummy record header"
            ),
            "Unexpected end of file reading the dummy record header."
        );
        enforce(
            this.e, !dummy_record_header.calcParity,
            "invalid dummy record header (parity check failed)"
        );

        /*
         * Check the header of the first real record in the data file:
         *   - It should follow the dummy record data immediately.
         *   - Its channel id should match the first channel in the first offset
         *     tracker.
         */
        pos += dummy_record_header.length;

        auto first_channel = this.first_offset_tracker.first;
        assert(first_channel);
        enforce(
            this.e, pos == first_channel.first_offset,
            "The first record in the data file does not immediately follow " ~
            "the dummy record data"
        );

        RecordHeader first_record_header;
        this.readHeader(first_record_header, pos);
        enforce(
            this.e, first_record_header.channel == first_channel.id,
            "The channel id in the first record in the data file does not " ~
            "match the id of the channel with the lowest first offset."
        );
    }

    /***************************************************************************

        Reads the index file, creates the channels listed in it and verifies
        consistency between the channel states and the contents of the data
        file.

        This method may only be called on startup when there are currently no
        channels but the index and data file have already been opened.

        Params:
            filesize = data file size

        Throws:
            - FileException on file I/O error,
            - DiskOverflow.Exception if the index is contains errors or is not
              consistent with the size or content of the data file.

    ***************************************************************************/

    private void initChannels ( ulong filesize )
    in
    {
        assert(this.index, "initChannels(): Index file expected to be opened");
        assert(this.data, "initChannels(): Data file expected to be opened");
        assert(!this.channels.length, "initChannels(): Map of channels expected to be empty");
        assert(!this.records, "initChannels(): Expected no records");
        assert(!this.bytes, "initChannels(): Expected no bytes");
    }
    out
    {
        assert(this);
    }
    body
    {
        this.index.readLines((cstring channel_name, ChannelMetadata channel, uint nline)
        {
            this.index.enforce(filesize, "No data");
            this.index.enforce(!(channel_name in this.channels),
                "Duplicate channel name", "", this.index.name, nline);
            this.index.enforce(channel.last_offset < filesize,
                "Position of last record beyond end of data file", "", this.index.name, nline);

            /*
             * An empty channel would need a channel ID but
             * this.highest_channel_id is not yet known. Disallowing empty
             * channels in the index makes life a bit easier.
             */
            this.index.enforce(channel.records, "Empty channel", "", this.index.name, nline);

            /*
             * Retrieve the channel ID from the first (next to be popped)
             * record. readHeader() also validates the header, thus will throw
             * if channel.first_offset is wrong.
             */
            RecordHeader header;
            this.readHeader(header, channel.first_offset);
            channel.id = header.channel;

            /*
             * Read the header of the last (most recently pushed) record and
             * store it in channel.last_header for the next push.
             * readHeader() will again throw if channel.last_offset is wrong.
             * Check if
             *   1. the channel ID of this record matches and
             *   2. that this record is really the last one
             *      (channel.last_header.next is 0).
             */
            this.readHeader(channel.last_header, channel.last_offset);
            this.index.enforce(!channel.last_header.next_offset,
                "Last record in channel points to a next record",
                "", this.index.name, nline);
            this.index.enforce(channel.last_header.channel == channel.id,
                "Last record in channel has the wrong channel ID",
                "", this.index.name, nline);

            /*
             * Add the channel to the registry.
             */
            this.channels[idup(channel_name)] = channel;

            /*
             * Add the channel to the tracker, storing a pointer to the
             * associative array element.
             */
            {
                ChannelMetadata* channel_ptr = channel_name in this.channels;
                scope (failure)
                    this.channels.remove(channel_name);

                assert(channel_ptr !is null);

                this.index.enforce(
                    this.first_offset_tracker.track(*channel_ptr),
                    "Multiple channels have the same first offset",
                    "", this.index.name, nline
                );

                assert(channel_ptr); // invariant
            }

            this.records += channel.records;
            this.bytes += channel.bytes;

            if (this.highest_channel_id < channel.id)
            {
                this.highest_channel_id = channel.id;
            }

            log.info(
                "Opened channel {} '{}': {} records/{} bytes, first record at file position {}.",
                channel.id, channel_name, channel.records, channel.bytes, channel.first_offset
            );
        });

        this.channels.rehash;

        this.validateChannels(filesize);

        if (this.records)
        {
            log.info("Started with {} records/{} bytes in total.", this.records, this.bytes);
        }
        else
        {
            log.info("Started, empty.");
        }
    }

    /***************************************************************************

        Verifies that the values in this.channels contain unique channel IDs and
        first and last record positions.

        Params:
            filesize = the size of the data file

    ***************************************************************************/

    private void validateChannels ( ulong filesize )
    {
        auto num_channels = this.channels.length;

        scope channel_ids      = new typeof(ChannelMetadata.id)[num_channels],
              first_positions  = new off_t[num_channels],
              last_positions   = new off_t[num_channels];

        uint  num_records = 0;
        ulong num_bytes   = 0;

        uint i = 0;

        foreach (channel; this.channels)
        {
            assert(&channel);

            channel_ids[i]     = channel.id;
            first_positions[i] = channel.first_offset;
            last_positions[i]  = channel.last_offset;

            num_records += channel.records;
            num_bytes   += channel.bytes;
            i++;
        }

        /*
         * While we're at it, also verify that the numbers of records and bytes
         * add up.
         */
        assert(this.num_records == num_records);
        assert(this.num_bytes   == num_bytes);
        assert(filesize         >= num_bytes + num_records * RecordHeader.sizeof);

        channel_ids.sort;
        first_positions.sort;
        last_positions.sort;

        while (i > 1)
        {
            i--;
            this.index.enforce(channel_ids[i] > channel_ids[i - 1], "Duplicate channel ID");
            this.index.enforce(first_positions[i] > first_positions[i - 1], "Duplicate position of first record in channel");
            this.index.enforce(last_positions[i] > last_positions[i - 1], "Duplicate position of last record in channel");
        }
    }
}
