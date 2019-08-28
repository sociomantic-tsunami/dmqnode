/*******************************************************************************

    Tracks `ChannelMetadata` items in ascending order of their `first_offset`.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.FirstOffsetTracker;

/*******************************************************************************

    The struct of entries stored in `FirstOffsetTracker`. `ChannelMetadata`
    contains a pointer to it, and the struct points back to that
    `ChannelMetadata`. This struct is outside `FirstOffsetTracker` to avoid a
    template-recursive type definition.

*******************************************************************************/

struct FirstOffsetTrackerEntry
{
    import ocean.util.container.ebtree.c.eb64tree: eb64_node;

    /***************************************************************************

        This instance is stored in an ebtree so it starts with an ebtree node.

    ***************************************************************************/

    private eb64_node node;

    /***************************************************************************

        Pointer to the `ChannelMetadata` instance that points to this instance.
        This is a generic pointer to avoid a mutual import between
        `ChannelMetadata` and this module.

    ***************************************************************************/

    private void* channel;
}

/*******************************************************************************

    To be used with the `ChannelMetadata` struct from the `ChannelMetadata`
    module. This is a template only to avoid a mutual import between
    `ChannelMetadata` and this module.

*******************************************************************************/

struct FirstOffsetTracker ( ChannelMetadata )
{
    import ocean.core.Enforce: enforce;
    import ocean.stdc.posix.sys.types: off_t;
    import ocean.util.container.ebtree.c.eb64tree;
    import ocean.util.container.ebtree.c.ebtree;

    /***************************************************************************

        The ebtree root.

    ***************************************************************************/

    private eb_root ebroot = empty_unique_ebtroot;

    /***************************************************************************

        Tracks `channel`. This method needs to be called whenever
        `channel.first_offset` has changed.

        If no other channel that is currently tracked has the same
        `first_offset` value as `channel.first_offset` then `channel` is
        tracked, and the return value is `true`. Otherwise `channel` is not
        tracked, and the return value is `false`.

        Note that a pointer to `channel` will be stored so `channel` should be
        in a persistent memory location.

        When this method returns `channel.tracker_entry` is `null` if and only
        if the channel is tracked.

        Params:
            channel = the overflow channel to track; `channel.first_offset` must
                      not be negative

        Returns:
            `true` if  `channel` is tracked or `false` if not because
            `channel.first_offset` matches the `first_offset` of another
            currently tracked channel.

    ***************************************************************************/

    public bool track ( ref ChannelMetadata channel )
    in
    {
        assert(channel.first_offset >= 0);
    }
    out (updated)
    {
        if (updated)
        {
            assert(channel.tracker_entry !is null);
            assert(channel.tracker_entry.node.key ==
                   cast(ulong)channel.first_offset);
            assert(channel.tracker_entry.channel is &channel);
        }
        else
        {
            assert(channel.tracker_entry is null);
        }
    }
    body
    {
        if (channel.tracker_entry is null)
        {
            channel.tracker_entry = new FirstOffsetTrackerEntry;
        }
        else
        {
            eb64_delete(&channel.tracker_entry.node);
        }

        channel.tracker_entry.node.key = cast(ulong)channel.first_offset;
        channel.tracker_entry.channel = &channel;
        auto ret_ebnode = eb64_insert(&(&this).ebroot,
                                      &channel.tracker_entry.node);

        if (ret_ebnode is &channel.tracker_entry.node)
        {
            return true;
        }
        else
        {
            delete channel.tracker_entry; // sets it to null
            return false;
        }
    }

    /***************************************************************************

        Stops tracking `channel`.

        When this method returns `channel.tracker_entry` is `null`.

        Params:
            channel = the overflow channel to stop tracking

    ***************************************************************************/

    public static void remove ( ref ChannelMetadata channel )
    out
    {
        assert(channel.tracker_entry is null);
    }
    body
    {
        if (channel.tracker_entry !is null)
        {
            eb64_delete(&channel.tracker_entry.node);
            delete channel.tracker_entry;  // sets it to null
        }
    }

    /***************************************************************************

        Updates all currently tracked channels after `bytes_cutoff` bytes were
        removed from the beginning of the data file. Updating is done by
        subtracting `bytes_cutoff` from `first_offset` and `last_offset` for
        each channel.

        Params:
            bytes_cutoff = the number of bytes removed from the beginning of the
                           data file, expected to be at least the lowest
                           `first_offset` of all tracked channels

    ***************************************************************************/

    public void updateCutoff ( ulong bytes_cutoff )
    {
        if (!bytes_cutoff)
            return;

        /*
         * The loop contains two subtleties:
         * 1. We get the next ebtree node after the current one has been
         *    modified. This works because we iterate in ascending order and
         *    lower all ebtree node keys by the same amount so they stay in the
         *    same order.
         * 2. After changing the key of each ebtree node we assert it doesn't
         *    match the key of another node in the ebtree. This works because
         *    a) all keys are unique before changing them,
         *    b) we subtract the same amount from each key so unique keys stay
         *       unique and
         *    c) we iterate in ascending order and lower the keys so when each
         *       key is lowered, its new value cannot match any of the lower
         *       keys because they have been lowered already by the same amount.
         */
        for (
            auto ebnode = eb64_first(&(&this).ebroot);
            ebnode !is null;
            ebnode = eb64_next(ebnode)
        )
        {
            eb64_delete(ebnode);

            auto channel = nodeToChannel(ebnode);
            assert(channel); // invariant, ensures last_offset >= first_offset
            assert(channel.first_offset >= bytes_cutoff);

            channel.first_offset -= bytes_cutoff;
            channel.last_offset  -= bytes_cutoff;

            ebnode.key = channel.first_offset;

            // eb64_insert returns ebnode iff the ebtree doesn't contain any
            // other node with the same key.
            if (eb64_insert(&(&this).ebroot, ebnode) !is ebnode)
                assert(false);
        }
    }

    /***************************************************************************

        Returns:
            the channel with the least `first_offset` or `null` if no channel
            is currently tracked.

    ***************************************************************************/

    public ChannelMetadata* first ( )
    {
        return nodeToChannel(eb64_first(&(&this).ebroot));
    }

    /***************************************************************************

        Gets the channel with the least `first_offset` that is greater than
        `channel.first_offset`.

        Params:
            channel = the overflow channel to get the next for in the order of
                      `first_offset`. `channel` is expected to be tracked.

        Returns:
            the next channel or `null` if `channel` is the last channel.

    ***************************************************************************/

    public static ChannelMetadata* next ( ref ChannelMetadata channel )
    in
    {
        assert(&channel); // invariant
        assert(channel.tracker_entry !is null);
    }
    body
    {
        return nodeToChannel(eb64_next(&channel.tracker_entry.node));
    }

    /***************************************************************************

        Gets the `channel` member of an object returned by a libebtree library
        function.

        Params:
            ebnode = the object returned by a libebtree library function

        Returns:
            the `channel` member of `ebnode` or `null` if `ebnode` is `null`.

    ***************************************************************************/

    private static ChannelMetadata* nodeToChannel ( eb64_node* ebnode )
    out (channel)
    {
        if (channel !is null)
        {
            assert(cast(FirstOffsetTrackerEntry*)ebnode is
                   channel.tracker_entry);
            assert(cast(ulong)channel.first_offset ==
                   channel.tracker_entry.node.key);
        }
    }
    body
    {
        return (ebnode !is null)
            ? cast(ChannelMetadata*)
                (cast(FirstOffsetTrackerEntry*)ebnode).channel
            : null;
    }

    /***************************************************************************

        An empty unique ebtree root.

        The value is generated via CTFE.

    ***************************************************************************/

    private static enum eb_root empty_unique_ebtroot =
        function ( )
        {
            eb_root root;
            root.unique = true;
            return root;
        }();
}

/******************************************************************************/

version (UnitTest) import ocean.core.Test;

unittest
{
    static struct ChannelMetadata
    {
        long first_offset, last_offset;
        FirstOffsetTrackerEntry* tracker_entry;

        invariant ( )
        {
            test!("<=")((&this).first_offset, (&this).last_offset);
        }
    }

    FirstOffsetTracker!(ChannelMetadata) tracker;
    assert(tracker.first is null);

    // Add two entries
    ChannelMetadata ch1;
    ch1.first_offset = 8;
    ch1.last_offset  = 8;
    test(tracker.track(ch1));
    test!("!is")(ch1.tracker_entry, null);
    test!("is")(tracker.first, &ch1);
    test!("is")(tracker.next(ch1), null);

    ChannelMetadata ch2;
    ch2.first_offset = 6;
    ch2.last_offset  = 6;
    test(tracker.track(ch2));
    test!("!is")(ch2.tracker_entry, null);
    test!("is")(tracker.first, &ch2);
    test!("is")(tracker.next(ch2), &ch1);
    test!("is")(tracker.next(ch1), null);

    tracker.updateCutoff(3);
    test!("==")(ch1.first_offset, 5);
    test!("==")(ch2.first_offset, 3);
    test!("is")(tracker.first, &ch2);
    test!("is")(tracker.next(ch2), &ch1);
    test!("is")(tracker.next(ch1), null);

    // Update the highest offset to the lowest
    ch1.first_offset = 2;
    ch1.last_offset  = 2;
    test(tracker.track(ch1));
    test!("!is")(ch1.tracker_entry, null);
    test!("is")(tracker.first, &ch1);
    test!("is")(tracker.next(ch1), &ch2);
    test!("is")(tracker.next(ch2), null);

    // Attempt to add a duplicate
    {
        ChannelMetadata ch3;
        ch3.first_offset = 3;
        ch3.last_offset  = 3;
        test(!tracker.track(ch3));
    }

    // Attempt to track ch1 with the same first_offset as ch2
    assert(ch2.first_offset == 3);
    ch1.first_offset = ch2.first_offset;
    ch1.last_offset  = ch2.last_offset;
    test(!tracker.track(ch1));
    test!("is")(ch1.tracker_entry, null);

    // Re-add ch1
    ch1.first_offset = 2;
    ch1.last_offset  = 2;
    test(tracker.track(ch1));
    test!("!is")(ch1.tracker_entry, null);
    test!("is")(tracker.first, &ch1);
    test!("is")(tracker.next(ch1), &ch2);
    test!("is")(tracker.next(ch2), null);

    // Remove the entry with the lowest offset
    tracker.remove(ch1);
    test!("is")(ch1.tracker_entry, null);
    test!("is")(tracker.first, &ch2);
    test!("is")(tracker.next(ch2), null);

    // Remove the only still existing entry
    tracker.remove(ch2);
    test!("is")(tracker.first, null);
    test!("is")(ch2.tracker_entry, null);
}
