/*******************************************************************************

    Run-time test if truncating the head of a file is supported.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.file.HeadTruncationTestFile;

import dmqnode.storage.engine.overflow.file.DataFile;

/// Tests if truncating a file from the beginning is supported.
class HeadTruncationTestFile: DataFile
{
    import core.sys.posix.stdlib: mkstemp;
    import core.stdc.stdio: SEEK_END;
    import ocean.transition;

    /***************************************************************************

        `true` if truncating the head of a file is supported or `false`
        otherwise.

    ***************************************************************************/

    public bool head_truncation_supported;

    /***************************************************************************

        Creates a temporary file in directory `dir`, tries to truncate its
        head, then deletes it. `dir` is expected to exist.

        Do not call any method with this instance after the constructor has
        returned.

        Params:
            dir = the directory where the test should be done, expected to exist

        Throws:
            `FileException` on error creating or deleting the file.

    ***************************************************************************/

    public this ( cstring dir )
    {
        super(dir, "falloctest_XXXXXX");

        this.head_truncation_supported = false;

        try
        {
            this.allocate(
                FALLOC_FL.ALLOCATE, 0, this.head_truncation_chunk_size + 100,
                "Unable to allocate test file"
            );
            this.truncateHead(this.head_truncation_chunk_size);
            auto filesize = this.seek(
                0, SEEK_END, "Unable to seek to tell the test file size"
            );
            this.head_truncation_supported = (filesize == 100);
        }
        catch (FileException e)
            this.log.error(getMsg(e));

        this.remove();
    }

    /***************************************************************************

        Creates the temporary file.
        `path` is expected to be a file path template suitable for `mkstemp()`,
        and its content will be modified to contain the actual file path. See
        the `mkstemp` manual for details.

        Params:
            path = the file path template suitable for `mkstemp`

        Returns:
            the non-negative file descriptor on success or a negative value on
            error; on error `errno` is set appropriately.

    ***************************************************************************/

    override protected int open ( char* path )
    {
        return mkstemp(path);
    }
}
