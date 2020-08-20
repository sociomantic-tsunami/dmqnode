/*******************************************************************************

    Channel size configuration. This is a list of channel id sizes, each
    associated with a channel prefix, plus a default size for channels with an
    id that doesn't start with any of the prefixes in the list.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.config.ChannelSizeConfig;

public struct ChannelSizeConfig
{
    import ocean.core.Enforce;
    import ocean.core.ExceptionDefinitions: IllegalArgumentException;
    import ocean.math.Math: min;
    import ocean.meta.types.Qualifiers : cstring, istring;

    /***************************************************************************

        List element.

    ***************************************************************************/

    public struct Channel
    {
        /***********************************************************************

            Channels whose ids start with this string should have the size
            `size_limit`.

        ***********************************************************************/

        public istring id_prefix;

        /***********************************************************************

            Channels whose ids start with `id_prefix` should have this size.

        ***********************************************************************/

        public ulong size_limit;
    }

    /***************************************************************************

        Channels whose ids do not start with the `id_prefix` of any element of
        `channels` should have this size.

    ***************************************************************************/

    public ulong default_size_limit;

    /***************************************************************************

        The list of configured channel sizes per channel id prefix.

    ***************************************************************************/

    public Channel[] channels;

    /***************************************************************************

        Specifies that all channels whose ids start with `id_prefix` should have
        the size `size_limit`. Enforces that `id_prefix` doesn't overlap with
        any other previously added channel id prefix.

        Params:
            id_prefix  = the channel id prefix `size_limit` is associated with
            size_limit = the channel size to use for channels with `id_prefix`

        Throws:
            `IllegalArgumentException` if `id_prefix` either overlaps with
            another previously added channel id prefix or is empty or `null`.

    ***************************************************************************/

    public void add ( istring id_prefix, ulong size_limit )
    {
        enforce!(IllegalArgumentException)(
            id_prefix.length,
            "Channel size configuration: Attempted to add an empty prefix"
        );

        foreach (channel; this.channels)
        {
            auto n = min(id_prefix.length, channel.id_prefix.length);
            enforce!(IllegalArgumentException)(
                id_prefix[0 .. n] != channel.id_prefix[0 .. n],
                "Channel size configuration: Conflicting channel id prefixes \""
                ~ id_prefix ~ "\" and \"" ~ channel.id_prefix ~ "\""
            );
        }

        this.channels ~= Channel(id_prefix, size_limit);
    }

    /***************************************************************************

        Obtains the channel size from a channel id. If `id` starts with a
        channel id prefix for which a certain size was specified with `add()`
        then this channel size is returned, otherwise `default_size_limit`.

        Params:
            id = the channel id

        Returns:
            the channel size for `id`.

    ***************************************************************************/

    public ulong getChannelSize ( cstring id )
    {
        if (id.length)
        {
            foreach (channel; this.channels)
            {
                if (id.length >= channel.id_prefix.length)
                {
                    if (id[0 .. channel.id_prefix.length] == channel.id_prefix)
                    {
                        return channel.size_limit;
                    }
                }
            }
        }

        return this.default_size_limit;
    }
}

version (unittest) import ocean.core.Test;

unittest
{
    ChannelSizeConfig c;

    c.default_size_limit = 4711;

    c.add("Die", 123);
    c.add("Katze", 456);

    testThrown!(c.IllegalArgumentException)(c.add("Katzeklo", 789));
    testThrown!(c.IllegalArgumentException)(c.add("", 44));
    testThrown!(c.IllegalArgumentException)(c.add(null, 45));

    test!("==")(c.getChannelSize("Die"), 123);
    test!("==")(c.getChannelSize("Katze"), 456);
    test!("==")(c.getChannelSize("Dieses"), 123);
    test!("==")(c.getChannelSize("Katzeklo"), 456);
    test!("==")(c.getChannelSize("Hundekuchen"), 4711);
    test!("==")(c.getChannelSize("Kat"), 4711);
    test!("==")(c.getChannelSize(""), 4711);
    test!("==")(c.getChannelSize(null), 4711);
}
