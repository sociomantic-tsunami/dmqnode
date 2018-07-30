/*******************************************************************************

    Push request implementation.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.neo.Push;

import dmqnode.connection.neo.SharedResources;
import dmqproto.node.neo.request.Push;
import dmqproto.node.neo.request.core.IRequestResources;

import dmqnode.util.Downcast;
import ocean.transition;

/*******************************************************************************

    DMQ node implementation of the v3 Push request protocol.

*******************************************************************************/

class PushImpl_v3 : PushProtocol_v3
{
    import ocean.core.TypeConvert : castFrom;

    /***************************************************************************

        Ensures that requested channels exist / can be created and can be
        written to.

        Params:
            r = request resources
            channel_names = list of channel names to check

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    override protected bool prepareChannels ( IRequestResources r,
        in cstring[] channel_names )
    {
        auto resources = downcastAssert!(SharedResources.RequestResources)(r);
        foreach ( channel; channel_names )
        {
            if ( !resources.storage_channels.getCreate(channel) )
                return false;
        }

        return true;
    }

    /***************************************************************************

        Push a record to the specified storage channel.

        Params:
            resources = request resources
            channel_name = channel to push to
            value = record value to push

        Returns:
            true if the record was pushed to the channel, false if it failed

    ***************************************************************************/

    override protected bool pushToStorage ( IRequestResources resources,
        cstring channel_name, in void[] value )
    {
        if ( auto storage_channel =
            downcastAssert!(SharedResources.RequestResources)(resources)
            .storage_channels.getCreate(channel_name) )
        {
            foreach (subscriber; storage_channel)
                subscriber.push(castFrom!(Const!(void)[]).to!(cstring)(value));
            return true;
        }

        return false;
    }
}
