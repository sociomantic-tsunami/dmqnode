/*******************************************************************************

    A wrapper around POSIX file I/O functionality with convenience extensions.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.file.DataFile;

import dmqnode.storage.engine.overflow.file.PosixFile;

/*******************************************************************************

    Linux `fallocate()` with the modes relevant for `DataFile`. See the manual
    page for details. Note that `FALLOC_FL.COLLAPSE_RANGE` and
    `FALLOC_FL.ZERO_RANGE` are supported only on Linux 3.15 or later for certain
    file systems.

*******************************************************************************/

enum FALLOC_FL
{
    /// Default mode, unnamed in the C API
    ALLOCATE       = 0,
    /// FALLOC_FL_COLLAPSE_RANGE
    COLLAPSE_RANGE = 0x08,
    /// FALLOC_FL_ZERO_RANGE
    ZERO_RANGE     = 0x10
}

private extern (C) int fallocate(
    int fd, FALLOC_FL mode, DataFile.off_t offset, DataFile.off_t len
);

class DataFile: PosixFile
{
    import core.sys.posix.sys.types: off_t, ssize_t;
    import unistd = core.sys.posix.unistd: read, write, pread, pwrite;
    import uio = core.sys.posix.sys.uio: iovec, writev;
    import ocean.core.Verify;
    import ocean.meta.types.Qualifiers : cstring;

    /***************************************************************************

        The chunk size for file head truncation: `truncateHead()` removes
        integer multiples of this amount of bytes from the head of the file.
        The operating system only allows removing whole logical file system
        blocks so this value needs to be a multiple of the logical file system
        block size, which is usually a power of 2.
        If the logical file system block size is not a power of 2 then the run-
        time file truncation test using `HeadTruncationTestFile` will fail, and
        file head truncation will not be supported. (This is, however, much less
        likely than the OS or file system not supporting file head truncation.)

    ***************************************************************************/

    public static immutable head_truncation_chunk_size = 1 << 20;

    /***************************************************************************

        Constructor, opens or creates the file using `name` as the file name and
        `dir` as the file directory. `dir` is expected to exist.

        Params:
            name = the file name without directory path
            dir  = the directory for the file, expected to exist

        Throws:
            FileException on error creating or opening the file.

    ***************************************************************************/

    public this ( cstring dir, cstring name )
    {
        super(dir, name);
    }

    /***************************************************************************

        Reads or writes `data` from/to the file starting at position `pos`.
        `pos` is increased by the number of bytes read or written, which is
        `data.length` - the returned value.

        Params:
            data = source or destination buffer to read from or write to, resp.
            pos  = file position, increased by the number of bytes read/written
            errmsg = error message to use

        Returns:
            the number of bytes n in data that have not been transmitted because
            POSIX `pread` or `pwrite` returned 0. The remaining bytes are
            `data[$ - n .. $]` so n == 0 indicates that all bytes have been
            transmitted.

        Throws:
            FileException on error.

    ***************************************************************************/

    public size_t ptransmit ( bool output )
        ( IOVoid!(output)[] data, ref off_t pos, cstring errmsg,
          string file = __FILE__, long line = __LINE__ )
    out (n)
    {
        assert(n <= data.length);
    }
    do
    {
        verify(pos >= 0);

        for (auto left = data; left.length;)
        {
            static if (output)
                alias unistd.pwrite op;
            else
                alias unistd.pread op;

            if (ssize_t n = this.restartInterrupted(op(this.fd, data.ptr, data.length, pos)))
            {
                this.enforce(n > 0, errmsg, "", file, line);
                left = left[n .. $];
                pos += n;
            }
            else // end of file for pread(); pwrite() should
            {    // return 0 iff data.length is 0
                return left.length;
            }
        }

        return 0;
    }

    /// ditto
    alias ptransmit!(false) pread;
    /// ditto
    alias ptransmit!(true) pwrite;

    /***************************************************************************

        Reads or writes `data` from/to the file at the current position.

        Params:
            data = source or destination buffer to read from or write to, resp.
            errmsg = error message to use

        Returns:
            the number of bytes n in data that have not been transmitted because
            POSIX `read` or `write` returned 0. The remaining bytes are
            `data[$ - n .. $]` so n == 0 indicates that all bytes have been
            transmitted.

        Throws:
            FileException on error.

    ***************************************************************************/

    public size_t transmit ( bool output )
        ( IOVoid!(output)[] data, cstring errmsg,
          string file = __FILE__, long line = __LINE__ )
    out (n)
    {
        assert(n <= data.length);
    }
    do
    {
            static if (output)
                alias unistd.write op;
            else
                alias unistd.read op;

        for (auto left = data; left.length;)
        {
            if (ssize_t n = this.restartInterrupted(op(this.fd, left.ptr, left.length)))
            {
                this.enforce(n > 0, errmsg, "", file, line);
                left = left[n .. $];
            }
            else // end of file for read(); write() should
            {    // return 0 iff data.length is 0
                return left.length;
            }
        }

        return 0;
    }

    /// ditto
    alias transmit!(false) read;
    /// ditto
    alias transmit!(true) write;

    /***************************************************************************

        Writes `data` to the file at the current position.

        Params:
            data = vector of buffers to read from
            errmsg = error message to use

        Returns:
            the number of bytes n in data that have not been transmitted because
            POSIX `writev` returned 0. n == 0 indicates that all bytes have been
            transmitted. data is adjusted to reference only the remaining
            chunks.

        Throws:
            FileException if op returns a negative value and sets errno to a
            value different to EINTR.

    ***************************************************************************/

    public size_t writev ( ref IoVec data, cstring errmsg,
                           string file = __FILE__, long line = __LINE__ )
    {
        while (data.length)
        {
            if (ssize_t n = this.restartInterrupted(uio.writev(
                this.fd, cast(iovec*)data.chunks.ptr, cast(int)data.chunks.length
            )))
            {
                this.enforce(n > 0, errmsg, "", file, line);
                data.advance(n);
            }
            else // writev() should return 0 iff data.length is 0
            {
                return data.length;
            }
        }

        return 0;
    }

    /***************************************************************************

        Rounds `n` down to an integer multiple of `head_truncation_chunk_size`,
        and removes that many bytes from the beginning of the file.

        This is supported only by Linux 3.15 and later for certain file systems,
        see the description of the `FALLOC_FL_COLLAPSE_RANGE` mode in the
        `fallocate` manual for details. The `HeadTruncationTestFile` class in
        the same package provides a run-time test if this feature is supported.

        Params:
            n = the maximum number of bytes to remove from the beginning of the
                file

        Returns:
            the actual number of bytes removed from the file, i.e. `n` rounded
            down to an integer multiple of `head_truncation_chunk_size`.

        Throws:
            FileException on error.

    ***************************************************************************/

    public ulong truncateHead ( ulong n,
                                string file = __FILE__, long line = __LINE__ )
    {
        if (n < this.head_truncation_chunk_size)
            return 0;

        auto collapse_bytes = n / this.head_truncation_chunk_size;
        assert(collapse_bytes);
        collapse_bytes *= this.head_truncation_chunk_size;

        this.log.info("Data file head truncation: Removal of {} bytes " ~
                      "requested, removing {} bytes.", n, collapse_bytes);

        this.allocate(
            FALLOC_FL.COLLAPSE_RANGE, 0, collapse_bytes,
            "Unable to truncate the file from the beginning", file, line
        );

        return collapse_bytes;
    }

    /***************************************************************************

        Sets the `len` bytes in the file starting with `offset` to zero.

        This is supported only by Linux 3.15 and later for certain file systems,
        see the description of the `FALLOC_FL_ZERO_RANGE` mode in the
        `fallocate` manual for details. At the time of writing the manual
        implies that the requirements for `truncateHead()` also satisfy this
        feature.

        Params:
            start = the start offset of the bytes in the file to set to zero
            len   = the number of bytes in the file to set to zero

        Throws:
            FileException on error.

    ***************************************************************************/

    public void zeroRange ( off_t start, off_t len,
                            string file = __FILE__, long line = __LINE__ )
    {
        this.allocate(
                FALLOC_FL.ZERO_RANGE, start, len,
                "Unable to set a file range to zero", file, line
        );
    }

    /***************************************************************************

        Calls `fallocate()` with `this.fd`, restarting if interrupted and
        throwing on error.

        Params:
            mode   = `fallocate()` mode
            offset = file range start offset
            len    = file range length

        Throws:
            FileException if `fallocate()` indicates an error.

    ***************************************************************************/

    protected void allocate ( FALLOC_FL mode, off_t offset, off_t len,
                              cstring errmsg,
                              string file = __FILE__, long line = __LINE__ )
    {
        this.enforce(
            !this.restartInterrupted(.fallocate(this.fd, mode, offset, len)),
            errmsg, "fallocate", file, line
        );
    }
}

/*******************************************************************************

    Evaluates to `const(void)`, if using a D2 compiler and `output` is `true`,
    or `void` otherwise.

*******************************************************************************/

template IOVoid ( bool output )
{
    version (D_Version2)
        static if (output)
            mixin("alias const(void) IOVoid;");
        else
            alias void IOVoid;
    else
        alias void IOVoid;
}

/*******************************************************************************

    Vector aka. gather output helper; tracks the byte position if `writev()`
    didn't manage to transfer all data with one call.

*******************************************************************************/

struct IoVec
{
    import swarm.neo.protocol.socket.uio_const: iovec_const;
    import ocean.core.Verify;

    /***************************************************************************

        The vector of buffers. Pass to this.chunks.ptr and this.chunks.length to
        readv()/writev().

    ***************************************************************************/

    iovec_const[] chunks;

    /***************************************************************************

        The remaining number of bytes to transfer.

    ***************************************************************************/

    size_t length;

    /***************************************************************************

        Adjusts this.chunks and this.length after n bytes have been transferred
        by readv()/writev() so that this.chunks.ptr and this.chunks.length can
        be passed to the next call.

        Resets this instance if n == this.length, i.e. all data have been
        transferred at once. Does nothing if n is 0.

        Params:
            n = the number of bytes that have been transferred according to the
                return value of readv()/writev()

        Returns:
            the number of bytes remaining ( = this.length).

        In:
            n must be at most this.length.

    ***************************************************************************/

    size_t advance ( size_t n )
    {
        verify(n <= this.length);

        if (n)
        {
            if (n == this.length)
            {
                this.chunks = null;
            }
            else
            {
                size_t bytes = 0;

                foreach (i, ref chunk; this.chunks)
                {
                    bytes += chunk.iov_len;
                    if (bytes > n)
                    {
                        size_t d = bytes - n;
                        chunk.iov_base += chunk.iov_len - d;
                        chunk.iov_len  = d;
                        this.chunks = this.chunks[i .. $];
                        break;
                    }
                }
            }
            this.length -= n;
        }

        return this.length;
    }

    /***************************************************************************

        Returns this.chunks[i] as a D array.

    ***************************************************************************/

    const(void)[] opIndex ( size_t i )
    {
        with (this.chunks[i]) return iov_base[0 .. iov_len];
    }

    /***************************************************************************

        Sets this.chunks[i] to reference data.

    ***************************************************************************/

    const(void)[] opIndexAssign ( const(void)[] data, size_t i )
    {
        with (this.chunks[i])
        {
            this.length -= iov_len;
            this.length += data.length;
            iov_len      = data.length;
            iov_base     = data.ptr;
        }

        return data;
    }

    /**************************************************************************/

    import ocean.core.Test: test;

    unittest
    {
        iovec_const[6] iov_buf;

        const(void)[] a = "Die",
                      b = "Katze",
               c = "tritt",
               d = "die",
               e = "Treppe",
               f = "krumm";

        auto iov = typeof(this)(iov_buf);

        test(iov.chunks.length == iov_buf.length);
        iov[0] = a;
        iov[1] = b;
        iov[2] = c;
        iov[3] = d;
        iov[4] = e;
        iov[5] = f;
        test(iov.length == 27);

        iov.advance(1);
        test(iov.length == 26);
        test(iov.chunks.length == 6);

        test(iov[0] == a[1 .. $]);
        test(iov[1] == b);
        test(iov[2] == c);
        test(iov[3] == d);
        test(iov[4] == e);
        test(iov[5] == f);

        iov.advance(10);
        test(iov.length == 16);
        test(iov.chunks.length == 4);
        test(iov[0] == c[3 .. $]);
        test(iov[1] == d);
        test(iov[2] == e);
        test(iov[3] == f);

        iov.advance(2);
        test(iov.length == 14);
        test(iov.chunks.length == 3);
        test(iov[0] == d);
        test(iov[1] == e);
        test(iov[2] == f);

        iov.advance(14);
        test(!iov.chunks.length);
    }
}
