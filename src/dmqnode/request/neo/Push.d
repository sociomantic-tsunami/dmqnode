/*******************************************************************************

    Push request implementation.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.neo.Push;

import dmqnode.connection.neo.SharedResources;

import dmqproto.node.neo.request.Push;

import ocean.core.TypeConvert : downcast;
import ocean.transition;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Consume command as specified by
                      the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

public void handle ( Object shared_resources, RequestOnConn connection,
    Command.Version cmdver, Const!(void)[] msg_payload )
{
    auto dmq_shared_resources = downcast!(SharedResources)(shared_resources);
    assert(dmq_shared_resources);

    switch ( cmdver )
    {
        case 2:
            scope rq_resources = dmq_shared_resources.new RequestResources;
            scope rq = new PushImpl_v2(rq_resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(GlobalStatusCode.RequestVersionNotSupported);
                }
            );
            break;
    }
}

/*******************************************************************************

    DMQ node implementation of the v0 Push request protocol.

*******************************************************************************/

private scope class PushImpl_v2 : PushProtocol_v2
{
    import ocean.core.TypeConvert : castFrom, downcast;

    private SharedResources.RequestResources resources;

    public this ( SharedResources.RequestResources resources )
    {
        super(resources);

        this.resources = resources;
    }

    /***************************************************************************

        Ensures that requested channels exist / can be created and can be
        written to.

        Params:
            channel_names = list of channel names to check

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    override protected bool prepareChannels ( in cstring[] channel_names )
    {
        foreach ( channel; channel_names )
        {
            if ( !this.resources.storage_channels.getCreate(channel) )
                return false;
        }

        return true;
    }

    override protected bool pushToStorage ( cstring channel_name,
        in void[] value )
    {
        if ( auto storage_channel =
            this.resources.storage_channels.getCreate(channel_name) )
        {
            foreach (subscriber; storage_channel)
                subscriber.push(castFrom!(Const!(void)[]).to!(cstring)(value));
            return true;
        }

        return false;
    }
}
