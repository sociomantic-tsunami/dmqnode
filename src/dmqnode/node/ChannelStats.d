/*******************************************************************************

    Definition of the per-channel statistics to log in addition to those
    automatically written by ChannelsNodeStats.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.node.ChannelStats;

/// ditto
struct ChannelStats
{
    import dmqnode.storage.Ring;

    /// The number of bytes stored in the memory queue.
    ulong bytes_memory;

    /// The number of bytes stored in the overflow.
    ulong bytes_overflow;

    /// The number of records stored in the memory queue.
    uint  records_memory;

    /// The number of records stored in the overflow.
    uint  records_overflow;

    /// The relative fullness of the memory queue in percent.
    ubyte percent_memory;

    /***************************************************************************

        Creates a new instance of this struct, populated with the corresponding
        stats counter values from `channel`.

        Params:
            channel = the channel to get statistics from

        Returns:
            an instance of this struct populated with the correspondent values
            in `channel`.

    ***************************************************************************/

    static typeof(this) set ( RingNode.Ring channel )
    {
        auto stats = typeof(this)(channel.memory_info.used_space,
                                   channel.overflow_info.num_bytes,
                                   cast(uint)channel.memory_info.length,
                                   channel.overflow_info.num_records,
                                   100);
        if (auto mem_capacity = channel.memory_info.total_space)
        {
            /*
             * channel.memory_info.total_space == 0 would be a memory queue
             * of zero capacity, which should be impossible in production.
             * Still it's a good idea to prevent a division by 0 as this may
             * happen in special test builds.
             */
            stats.percent_memory =
                cast(ubyte)((stats.bytes_memory * 100.) / mem_capacity);
        }

        return stats;
    }
}
