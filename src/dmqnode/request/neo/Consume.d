/*******************************************************************************

    Consume request implementation.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.neo.Consume;

import dmqnode.request.neo.Consume_v2;

import dmqproto.node.neo.request.Consume;

import dmqnode.connection.neo.SharedResources;
import dmqnode.storage.model.StorageEngine;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.core.TypeConvert : castFrom, downcast;

import ocean.transition;

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
        case 1:
            scope rq_resources = dmq_shared_resources.new RequestResources;
            scope rq = new ConsumeImpl_v1(rq_resources);
            rq.handle(connection, msg_payload);
            break;

        case 2:
            scope rq_resources = dmq_shared_resources.new RequestResources;
            scope rq = new ConsumeImpl_v2(rq_resources);
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

    DMQ node implementation of the v0 Consume request protocol.

*******************************************************************************/

public scope class ConsumeImpl_v1 : ConsumeProtocol_v1, StorageEngine.IConsumer
{
    private SharedResources.RequestResources resources;

    /***************************************************************************

        Storage engine being consumed from.

    ***************************************************************************/

    private StorageEngine storage_engine;

    /***************************************************************************

        Constructor.

        Params:
            resources = shared resource acquirer

    ***************************************************************************/

    public this ( SharedResources.RequestResources resources )
    {
        super(resources);

        this.resources = resources;
    }

    /***************************************************************************

        Performs any logic needed to start consuming from the channel of the
        given name.

        Params:
            channel_name = channel to consume from

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        if (auto channel = this.resources.storage_channels.getCreate(channel_name))
        {
            this.storage_engine = channel.subscribe("");

            if ( this.storage_engine !is null )
            {
                this.storage_engine.registerConsumer(this);
                return true;
            }
        }

        return false;
    }

    /***************************************************************************

        Performs any logic needed to stop consuming from the channel of the
        given name.

        Params:
            channel_name = channel to stop consuming from

    ***************************************************************************/

    override protected void stopConsumingChannel ( cstring channel_name )
    {
        this.storage_engine.unregisterConsumer(this);
    }

    /***************************************************************************

        Retrieve the next value from the channel, if available.

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

    /***************************************************************************

        StorageEngine.IConsumer method, called when new data arrives or the
        channel is deleted.

        Params:
            code = trigger event code

    ***************************************************************************/

    override public void trigger ( Code code )
    {
        with ( Code ) switch ( code )
        {
            case DataReady:
                this.dataReady();
                break;
            case Finish:
                this.channelRemoved();
                break;
            default:
                break;
        }
    }
}
