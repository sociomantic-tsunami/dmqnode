/*******************************************************************************

    The index file of the queue disk overflow.

    This is a text file where each line corresponds to one queue channel and
    contains the channel name as the first token, followed by the decimal values
    of the following ChannelMetadata fields in that order: records, bytes,
    first_offset, last_offset. Tokens are separated by whitespace.
    The numeric channel ID is not stored in the index file. After reading the
    index file it read from the first or last record in the data file.

    The index file is a text file for the sake of easy inspection. It should be
    written only by the IndexFile.writeLines() method.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.file.IndexFile;

import dmqnode.storage.engine.overflow.ChannelMetadata;
import dmqnode.storage.engine.overflow.file.PosixFile;

import ocean.transition;

class IndexFile: PosixFile
{
    import ocean.core.Enforce: enforceImpl;
    import core.sys.posix.signal: SIGABRT, SIGSEGV, SIGILL, SIGBUS;
    import core.sys.posix.stdio: fdopen;
    import core.stdc.stdio: FILE, EOF, fscanf, fprintf, feof, rewind, clearerr, fflush;
    import core.stdc.stdlib: free;
    import ocean.sys.SignalMask;
    import dmqnode.storage.model.StorageChannels: IChannel;

    /***************************************************************************

        Signals that should not be blocked because the program should be
        terminated immediately if one of these is raised.

    ***************************************************************************/

    public const signals_dontblock = [SIGABRT, SIGSEGV, SIGBUS, SIGILL];

    /***************************************************************************

        Signal set to block all signals except unblocked_signals while
        formatted file I/O functions are running, which cannot be restarted or
        recovered if interrupted by a signal.

    ***************************************************************************/

    private static SignalSet fmt_io_signal_blocker;

    static this ( )
    {
        this.fmt_io_signal_blocker = this.fmt_io_signal_blocker; // Pacify compiler
        this.fmt_io_signal_blocker.setAll();
        this.fmt_io_signal_blocker.remove(this.signals_dontblock);
    }

    /***************************************************************************

        The file as stdio FILE stream.

    ***************************************************************************/

    public FILE* stream;

    /**************************************************************************/

    invariant ( )
    {
        assert(this.stream);
    }

    /***************************************************************************

        Constructor.

        Params:
            dir  = working directory
            name = file name

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public this ( char[] dir, char[] name )
    {
        super(dir, name);
        this.stream = fdopen(this.fd, "w+".ptr);
        this.enforce(this.stream, "unable to fdopen");
    }

    /***************************************************************************

        Parses the index file and calls got_channel for each channel in the
        file. The channel_name and channel arguments passed to got_channel are
        validated: channel_name is a valid queue channel name, and channel is
        validated according to the criteria of its invariant.

        Params:
            got_channel = called for each channel with validated channel_name
                          and channel; nline is the line number

        Throws:
            FileException on file I/O error or bad index file content (parse
            error or values that would make the ChannelMetadata invariant fail).

    ***************************************************************************/

    public void readLines ( void delegate ( cstring channel_name,
                                            ChannelMetadata channel,
                                            uint nline ) got_channel )
    {
        rewind(this.stream);

        for (uint nline = 1;; nline++)
        {
            ChannelMetadata channel;
            char[] channel_name = null;

            scope (exit)
            {
                /*
                 * fscanf() allocates channel_name via malloc() on a match or
                 * leaves it untouched (null) on mismatch.
                 */
                if (channel_name) free(channel_name.ptr);
            }

            int n;
            this.fmt_io_signal_blocker.callBlocked(
                n = readLine(this.stream, channel_name, channel)
            );

            switch (n)
            {
                case 5:
                    enforceImpl(this.e,
                        validateSubscriberSeparator(channel_name),
                        "Invalid use of subscriber/channel '@' separator",
                        this.name, nline);
                    /*
                     * Validate channel by checking the same conditions as its
                     * invariant.
                     */
                    channel.validate(channel,
                        (bool good, char[] msg)
                        {
                            enforceImpl(this.e, good, msg, this.name, nline);
                        });

                    got_channel(channel_name, channel, nline);
                    break;

                case EOF:
                    this.enforce(feof(this.stream), "Error reading channel index",
                                 "feof", this.name, nline);
                    return;

                default:
                    this.enforce(!feof(this.stream), "Unexpected end of file",
                                 "feof", this.name, nline);
                    auto errmsg =
                    [
                        "Invalid channel name"[],
                        "Invalid number of records",
                        "Invalid number of bytes",
                        "Invalid position of first record",
                        "Invalid offset of last record"
                    ];
                    this.e.msg = errmsg[n];
                    this.e.file = this.name;
                    this.e.line = nline;
                    throw this.e;
            }
        }
    }

    /***************************************************************************

        Resets the index file to be empty, then writes lines to the index file.

        Calls iterate, which in turn should call writeln for each line that
        should be written to the index file. All signals except the ones in
        this.signals_dontblock are blocked while iterate is executing. Flushes
        the index file output after iterate has returned (not if it throws).

        Params:
            iterate = called once with a writeln delegate as argument; each call
                      of writeln writes one line to the index file

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public void writeLines ( void delegate ( void delegate ( cstring name, ChannelMetadata channel ) writeln ) iterate )
    {
        this.reset();

        this.fmt_io_signal_blocker.callBlocked({
            iterate((cstring name, ChannelMetadata channel)
            {
                int n = fprintf(this.stream, "%.*s %lu %llu %lld %lld\n".ptr,
                                name.length, name.ptr,
                                channel.records, channel.bytes,
                                channel.first_offset, channel.last_offset);

                this.enforce(n >= 0, "error writing index");
            });
            this.enforce(!fflush(this.stream), "error flushing index");
        }());
    }

    /***************************************************************************

        Resets the error indicator when the file is truncated to be empty.

    ***************************************************************************/

    override public void reset ( )
    {
        super.reset();
        clearerr(this.stream);
    }

    /***************************************************************************

        Reads one line from the index file and parses it, using `fscanf(3)` and
        expecting the following tokens, separated and possibly surrounded by
        white space in that order:

            - `channel_name`: a string, allowed characters are ASCII
                              alphanumeric, '_', '-' and '@'
            - `channel.records`: decimal `uint` value
            - `channel.bytes`: decimal `ulong` value
            - `channel.first_offset`: decimal `off_t` value
            - `channel.last_offset`: decimal `off_t` value

        If a valid channel name was read from `stream` then `channel_name`
        outputs a string, otherwise it outputs `null`. If it does output a
        string then the string buffer will have been allocated via `malloc` so
        the caller needs to deallocate it via `free(channel_name.ptr)`.

        Params:
            stream       = the input stream
            channel_name = channel name output, either `malloc`-allocated or
                           `null`
            channel      = channel metadata output

        Returns:
            - 5 on success (i.e. all five tokens were parsed successfully).
            - a value less than 5 if that number of tokens was successfully
              parsed before a token mismatch, end-of-file condition or I/O error
              occurred. Use `feof(stream)` and `ferror(stream)` to find the
              exact cause.
            - `EOF` if an end-of-file condition or I/O error occurred before the
              first non-whitespace character was read. Use `feof(stream)` and
              `ferror(stream)` to find the exact cause. End-of-file means that
              there was white space at the end of the file.

    ***************************************************************************/

    private static int readLine ( FILE* stream,
        out char[] channel_name, out ChannelMetadata channel )
    {
        char* channel_name_ptr;
        int name_start, name_end;
        /*
         * Special fscanf format tokens:
         *   - The leading ' ' skips leading white space.
         *   - %n stores the current position in the input string in the
         *     argument so that channel_name.length = name_end - name_start.
         *     Note that the Linux manpage mentions some confusion about whether
         *     this token increments the `fscanf` return value or not, referring
         *     to Corrigendum 1 of the C90 standard. However, both C99 and POSIX
         *     explicitly say %n does not increment the returned count so we
         *     rely on that here and say, fscanf returns 5 (not 7) on success.
         *     References:
         *     http://pubs.opengroup.org/onlinepubs/9699919799/functions/scanf.html
         *     http://www.open-std.org/jtc1/sc22/wg14/www/docs/n1124.pdf (p.287)
         *   - %m matches a string, stores it in a buffer allocated by malloc
         *     and stores a pointer to that buffer in the argument.
         *   - [_0-9a-zA-Z@-] makes %m match only strings that consist of the
         *     characters '_', '0'-'9', 'a'-'z', 'A'-'Z', '@' or '-',
         *     which ensures the string is a valid queue channel name.
         */
        int n = fscanf(stream, " %n%m[_0-9a-zA-Z@-]%n %lu %llu %lld %lld".ptr,
                       &name_start, &channel_name_ptr, &name_end,
                       &channel.records, &channel.bytes, &channel.first_offset,
                       &channel.last_offset);

        if (channel_name_ptr !is null)
            channel_name = channel_name_ptr[0 .. name_end - name_start];

        return n;
    }

    /***************************************************************************

        Validates the occurrences of the subscriber-channel separator '@' in
        `storage_name`: '@' may occur at most once and not as the first or last
        character.

        Params:
            storage_name = a storage name

        Returns:
            true if `storage_name` contains valid occurrences of '@' or false
            otherwise.

    ***************************************************************************/

    private static bool validateSubscriberSeparator ( cstring storage_name )
    {
        cstring subscriber_name;
        cstring channel_name =
            IChannel.splitSubscriberName(storage_name, subscriber_name);

        if (!channel_name.length) // storage_name[$ - 1] == '@', no other '@'
            return false;

        if (subscriber_name is null) // no '@' in storage_name
            return true;

        // It's subscriber_name@channel_name so check for a second '@' in
        // channel_name.
        IChannel.splitSubscriberName(channel_name, subscriber_name);
        return subscriber_name is null;
    }
}

version (UnitTest)
{
    import core.stdc.stdio;
    import core.stdc.stdlib;
    import ocean.core.Test;
    import ocean.sys.ErrnoException;
    extern (C) private FILE* fmemopen(void* buf, size_t size, Const!(char)* mode);

    /// Creates a `FILE` stream reading from `buf`.
    private FILE* fmemopen_read ( in void[] buf )
    {
        // Cast `const` away from `buf` because the "r" parameter specifies
        // read-only access to it.
        return fmemopen(cast(void*)buf.ptr, buf.length, "r".ptr);
    }
}

unittest
{
    // Parses line and calls check(), passing the input stream (for feof
    //  checking) and the readLine return and output values.
    static void checkLine ( cstring line, void delegate ( FILE* stream,
        int n, cstring channel_name, ChannelMetadata channel ) check )
    {
        FILE* stream = fmemopen_read(line);
        if (stream is null)
            throw (new ErrnoException).useGlobalErrno("fmemopen");

        scope (exit) fclose(stream);

        char[] channel_name = null;
        ChannelMetadata channel;
        int n = IndexFile.readLine(stream, channel_name, channel);
        scope (exit) if (channel_name) free(channel_name.ptr);
        check(stream, n, channel_name, channel);
    }

    checkLine(
        "hello_world4711 2 3 5 7",
        (FILE* stream, int n, cstring channel_name, ChannelMetadata channel)
        {
            test!("==")(n, 5);
            test!("==")(channel_name, "hello_world4711");
            test!("==")(channel.records, 2);
            test!("==")(channel.bytes, 3);
            test!("==")(channel.first_offset, 5);
            test!("==")(channel.last_offset, 7);
        }
    );

    checkLine(
        "hello_4711@world 11 13 17 19",
        (FILE* stream, int n, cstring channel_name, ChannelMetadata channel)
        {
            test!("==")(n, 5);
            test!("==")(channel_name, "hello_4711@world");
            test!("==")(channel.records, 11);
            test!("==")(channel.bytes, 13);
            test!("==")(channel.first_offset, 17);
            test!("==")(channel.last_offset, 19);
        }
    );

    checkLine(
        "hello.world_4711 11 13 17 19",
        (FILE* stream, int n, cstring channel_name, ChannelMetadata channel)
        {
            test!("!=")(n, 5);
        }
    );

    // Verify end-of-file is correctly reported if the line contains only
    // white space.
    checkLine(
        "   \t  ",
        (FILE* stream, int n, cstring channel_name, ChannelMetadata channel)
        {
            test!("==")(n, EOF);
            test(!!feof(stream));
        }
    );
}

unittest
{
    test(IndexFile.validateSubscriberSeparator("hello_world"));
    test(IndexFile.validateSubscriberSeparator("hello@world"));
    test(!IndexFile.validateSubscriberSeparator("hello@@world"));
    test(!IndexFile.validateSubscriberSeparator("hello_world@@"));
    test(IndexFile.validateSubscriberSeparator("@hello_world"));
    test(!IndexFile.validateSubscriberSeparator("@hello@world"));
    test(!IndexFile.validateSubscriberSeparator("hello_world@"));
    test(!IndexFile.validateSubscriberSeparator("hello@wor@ld"));
    test(!IndexFile.validateSubscriberSeparator("@"));
    test(!IndexFile.validateSubscriberSeparator("@@"));
}
