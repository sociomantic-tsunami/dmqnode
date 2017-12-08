/*******************************************************************************

    A test for minimizing the disk overflow data file size.
    This test requires operating and file system support for `fallocate()` and
    its `FALLOC_FL_COLLAPSE_RANGE` and `FALLOC_FL_ZERO_RANGE` modes. These are
    supported from Linux 3.15 for the ext4 and XFS file systems. If this is not
    supported when the test is run, a warning is logged, and the test program
    returns `EXIT_SUCCESS`.
    This test creates files in the current working directory and deletes them on
    exit.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.ovfminimize.main;

import dmqnode.storage.engine.DiskOverflow;
import dmqnode.storage.engine.overflow.Constants;
import dmqnode.storage.engine.overflow.file.DataFile;
import dmqnode.storage.engine.overflow.RecordHeader;

import ocean.core.Test;
import ocean.io.Stdout: Stderr;
import core.sys.posix.stdlib: srand48, mrand48;
import core.sys.posix.sys.stat: stat_t, fstat;
import core.stdc.stdio: SEEK_CUR;
import core.stdc.stdlib: EXIT_SUCCESS, EXIT_FAILURE;
import ocean.transition: getMsg;
import ocean.util.log.Log;

/*******************************************************************************

    Runs the test but first checks if the required `fallocate()` modes are
    supported.

    Returns:
         - `EXIT_SUCCESS` if either
             - the test succeeded or
             - the required `fallocate()` modes are not supported so the test
               wasn't run (but a warning logged).
         - `EXIT_FAILURE` if the test failed.

*******************************************************************************/

version (UnitTest) {} else
int main ( )
{
    try
    {
        run();
    }
    catch (Exception e)
    {
        Log.lookup("test-ovfminimize").error(
            "{} @{}:{}", getMsg(e), e.file, e.line
        );
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

/*******************************************************************************

    The number of bytes of a record payload pushed into the disk overflow during
    the test: 2500 hexadecimal `uint` numbers.

*******************************************************************************/

private const record_data_length = 2500 * uint.sizeof * 2;

/*******************************************************************************

    Rename `DataFile.head_truncation_chunk_size`, which is 1 MiB to make
    calculations easier to read.

*******************************************************************************/

private alias DataFile.head_truncation_chunk_size MiB;
static assert(MiB == 1 << 20);

/*******************************************************************************

    Runs the test but first checks if the required `fallocate()` modes are
    supported.

    Throws:
        `Exception` if the test failed. Does not throw if required `fallocate()`
        modes are not supported, but logs a warning instead in that case.

*******************************************************************************/

void run ( )
{
    Log.config(Stderr);

    scope ovf = new DiskOverflow(".");

    scope (exit)
        ovf.close();

    if (!ovf.data_file_size_mimimizing_supported)
    {
        Log.lookup("test-ovfminimize").warn(
            "Unable run the disk overflow data file minimization test: " ~
            "Truncating a file from the beginning is not supported by this " ~
            "operating system version and/or file system."
        );
        return;
    }

    /*
     * Create some data to push into the queue: 2500 hexadecimal uint random
     * numbers. The random number generator is seeded with a constant so that
     * the sequence of numbers is always the same.
     */
    scope data = new char[record_data_length];
    srand48(12345);
    for (uint i = 0; i < data.length;)
    {
        auto n = cast(uint)mrand48();
        foreach (b; (cast(ubyte*)&n)[0 .. n.sizeof])
        {
            static hexdigits = "0123456789ABCDEF";
            data[i++] = hexdigits[b & 0xF];
            b >>= 4;
            data[i++] = hexdigits[b & 0xF];
        }
    }

    scope ch1 = ovf.new Channel("ch1"),
          ch2 = ovf.new Channel("ch2");

    /*
     * On success the disk overflow is empty and will delete the files upon
     * closing. Clear it on failure to not leave files.
     */
    scope (failure)
        ovf.clear();

    /*
     * Returns the size of the data file. Obtains it by seeking to the current
     * position of the file, which is always at the end, and verifies that
     * fstat() reports the same value.
     */
    ulong getDataFileSize ( )
    {
        auto size = ovf.data.seek(
            0, SEEK_CUR, "unable to seek to tell the data file size"
        );
        test!(">=")(size, 0);

        stat_t status;
        ovf.data.enforce(
            !fstat(ovf.data.fd, &status), "unable get the data file status"
        );
        test!("==")(status.st_size, size);
        return cast(ulong)size;
    }

    scope pop_result = new char[record_data_length];
    /*
     * Pops a record from channel and verifies it contains the expected data.
     */
    void pop ( DiskOverflow.Channel channel )
    {
        bool popped = channel.pop(
            delegate void[] (size_t n)
            {
                test!("==")(n, pop_result.length);
                return pop_result;
            }
        );
        test(popped);
        test!("==")(pop_result, data);
    }

    /*
     * Push 500 records to each channel and verify the data file size.
     * Number of records per channel: 0 => 500
     */
    test!("==")(getDataFileSize, 0);

    for (uint i = 0; i < 500; i++)
    {
        ch1.push(data);
        ch2.push(data);
    }

    const data_file_size_pushed = calcDataFileSize(500, 500);
    test!("==")(getDataFileSize, data_file_size_pushed);

    /*
     * Pop 300 records from each channel, and verify that the data file size
     * hasn't changed.
     * Number of records per channel: 500 => 200
     */
    for (uint i = 0; i < 300; i++)
    {
        pop(ch1);
        pop(ch2);
    }

    test!("==")(getDataFileSize(), data_file_size_pushed);

    /*
     * Now as much space as 300 records * 2 channels are free at the beginning
     * of the data file, which is at least 11 MiB but less than 12 MiB so 11 MiB
     * should be cut off when the data file size is minimized. Verify that
     * calculation, minimize the data file size, then verify that the file size
     * was reduced by 11 MiB. A gap of less than 1 MiB still remains, which
     * needs to be included in further data file size calculations.
     */
    const bytes_free = calcRecordSize(300, 300);
    static assert(data_file_size_pushed > bytes_free);
    static assert(bytes_free / MiB == 11);
    const gap_size = bytes_free % MiB;
    const data_file_size_miminized = data_file_size_pushed - 11 * MiB;

    ovf.flush();
    test!("==")(getDataFileSize(), data_file_size_miminized);

    /*
     * Push another 100 records to each channel, and verify the data file size.
     * It should be the size needed for 300 records plus the gap that was left
     * when the file size was minimized.
     * Number of records per channel: 200 => 300
     */
    for (uint i = 0; i < 100; i++)
    {
        ch1.push(data);
        ch2.push(data);
    }

    const data_file_size_pushed2 = calcDataFileSize(300, 300) + gap_size;
    test!("==")(getDataFileSize(), data_file_size_pushed2);

    /*
     * Pop 250 records from each channel, which includes 200 records pushed
     * before and 50 records after minimizing the file size. Pop the records in
     * reverse order this time to increase the chance of detecting bugs.
     * Number of records per channel: 300 => 50
     */
    for (uint i = 0; i < 250; i++)
    {
        pop(ch2);
        pop(ch1);
    }

    test!("==")(getDataFileSize(), data_file_size_pushed2);

    /*
     * Now as much space as 250 records * 2 channels plus the gap from
     * minimizing the file size are free at the beginning of the data file,
     * which is at least 10 MiB but less than 11 MiB so 10 MiB should be cut off
     * when the data file size is minimized. Verify that calculation, minimize
     * the data file size, then verify that the file size was reduced by 10 MiB.
     */
    ovf.flush();

    const bytes_free2 = calcRecordSize(250, 250) + gap_size;
    static assert(data_file_size_pushed2 > bytes_free2);
    static assert(bytes_free2 / MiB == 10);
    const data_file_size_miminized2 = data_file_size_pushed2 - 10 * MiB;

    test!("==")(getDataFileSize(), data_file_size_miminized2);

    /*
     * Pop the remaining 50 records to detect possible bugs that manifest
     * themselves when the channels are empty and the files are reset.
     * Number of records per channel: 50 => 0
     */
    for (uint i = 0; i < 50; i++)
    {
        pop(ch2);
        pop(ch1);
    }

    test!("==")(ch1.num_records, 0);
    test!("==")(ch2.num_records, 0);
    test!("==")(getDataFileSize(), 0);
}

/*******************************************************************************

    Calculates the expected size of a disk overflow data file that contains
    `n_records_per_channel` records in its channels where the size of payload
    data of each record is `record_data_length`, not taking a gap introduced by
    minimizing the file size into account.

    Params:
        n_records_per_channel = each parameter corresponds to a number of
                                records in a channel

    Returns:
        the expected size of the data file according to `n_records_per_channel`.

*******************************************************************************/

private size_t calcDataFileSize ( uint[] n_records_per_channel ... )
{
    return Constants.datafile_id.sizeof + calcRecordSize(n_records_per_channel);
}

/*******************************************************************************

    Calculates the number of bytes that `n_records_per_channel` records occupy
    in the disk overflow file.

    Params:
        n_records_per_channel = each parameter corresponds to a number of
                                records in a channel

    Returns:
        the number of bytes that `n_records_per_channel` records occupy in the
        disk overflow file.

*******************************************************************************/

private size_t calcRecordSize ( uint[] n_records_per_channel ... )
{
    size_t n_total = 0;
    foreach (n; n_records_per_channel)
        n_total += (RecordHeader.sizeof + record_data_length) * n;

    return n_total;
}
