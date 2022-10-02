/*******************************************************************************

    Pop request implementation.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.neo.Pop;

import dmqnode.connection.neo.SharedResources;
import dmqnode.storage.model.StorageEngine;

import dmqproto.node.neo.request.Pop;
import dmqproto.node.neo.request.core.IRequestResources;

import dmqnode.util.Downcast;

/*******************************************************************************

    DMQ node implementation of the v1 Pop request protocol.

*******************************************************************************/

class PopImpl_v1 : PopProtocol_v1
{
    import dmqproto.common.RequestCodes : RequestCode;
    import ocean.core.TypeConvert : castFrom, downcast;
    import ocean.meta.types.Qualifiers : cstring, mstring;
    import swarm.neo.request.Command : Command;

    /// Request code and version (required by ConnectionHandler)
    static immutable Command command = Command(RequestCode.Pop, 1);

    /// Request name for stats tracking (required by ConnectionHandler)
    static immutable string name = "pop";

    /// Flag indicating whether timing stats should be generated for
    /// requests of this type
    static immutable bool timing = true;

    /// Flag indicating whether this request is scheduled for removal
    /// (if `true`, clients will be warned)
    static immutable bool scheduled_for_removal = false;

    /***************************************************************************

        Storage engine being popped from.

    ***************************************************************************/

    private StorageEngine storage_engine;

    /***************************************************************************

        Performs any logic needed to pop from the channel of the given name.

        Params:
            resources = request resources
            channel_name = channel to pop from
            subscribed = `true` if the return value is `false` because the
                channel has subscribers so it is not possible to pop from it

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( IRequestResources resources,
        cstring channel_name, out bool subscribed )
    out (ok)
    {
        assert(!(ok && subscribed));
    }
    do
    {
        if (auto channel =
            downcastAssert!(SharedResources.RequestResources)(resources)
            .storage_channels.getCreate(channel_name))
        {
            this.storage_engine = channel.storage_unless_subscribed;
            subscribed = this.storage_engine is null;
            return !subscribed;
        }
        else
        {
            return false;
        }
    }

    /***************************************************************************

        Pop the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    override protected bool getNextValue ( ref void[] value )
    {
        auto mstring_value = castFrom!(void[]*).to!(mstring*)(&value);
        this.storage_engine.pop(*mstring_value);

        return value.length > 0;
    }
}
