/*******************************************************************************

    The header of each disk overflow record.

    The first RecordHeader.sizeof bytes of a disk overflow record are
    RecordHeader data, which are immediately followed by the record payload.

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.RecordHeader;

struct RecordHeader
{
    import ocean.stdc.posix.sys.types: off_t;

    /***************************************************************************

        The channel identifier enumerator, tells the channel to which this
        record belongs. Technically it is not needed but allows for integrity
        checking and recovering the index file if it is corrupted or lost.

        When pushing a new record to the file, its channel identifier is set to
        be at least 1. When popping, it is negated to allow for finding the
        first not yet popped record in the data file if the index file is lost.

    ***************************************************************************/

    uint channel = 0;

    /***************************************************************************

        The number of bytes between the start of the next record in the same
        channel and the start of this record.

        When writing this record, this value is 0. It is updated when writing
        the next record in the same channel.

        If this is not the last (most recently pushed) record in its channel,
        the value must be at least (*this).sizeof + this.length so it cannot be
        0 in that case.

    ***************************************************************************/

    off_t next_offset = 0;

    /***************************************************************************

        The 8-bit horizontal parity for checking data integrity after
        deserialising this instance.

    ***************************************************************************/

    private ubyte parity;

    /***************************************************************************

        The number of bytes of the record payload.

        In the data file the record payload data follow the data of this struct.
        This field must be at the end so that the data layout in the file
        matches the layout of a serialised array: The array length followed by
        the array data.

    ***************************************************************************/

    size_t length;

    static assert(
        typeof(*this).length.offsetof + typeof(*this).length.sizeof
        == typeof(*this).sizeof,
        typeof(*this).stringof ~ ".length should be at the end of the struct"
    );

    /***************************************************************************

        Calculates the parity of the serialised data of this instance except
        this.parity and sets this.parity to the resulting value. From this point
        on data integrity can be checked using this.calcParity().

        Returns:
            The parity result; this is now the value of this.parity.

    ***************************************************************************/

    ubyte setParity ( )
    {
        /*
         * Zero bytes don't change the result of the parity calculation
         * (x XOR 0 = x for all x) so to exclude this.parity from the
         * calculation of the parity of the data of this instance, set it to 0.
         */

        this.parity = 0;
        return this.parity = this.calcParity();
    }

    /***************************************************************************

        Calculates the parity of the serialised data of this instance including
        this.parity. Does not modify this.parity.

        If this.setParity() was executed before and the returned value is
        different from 0, the data of this instance have changed inbetween.

        Returns:
            The parity remainder of all data of this instance, including
            this.parity.

    ***************************************************************************/

    ubyte calcParity ( )
    {
        ulong parity = 0;

        foreach (x; this.tupleof)
        {
            parity ^= x;
        }

        parity ^= parity >> 32;
        parity ^= parity >> 16;
        parity ^= parity >> 8;

        return cast(ubyte)parity;
    }
}
