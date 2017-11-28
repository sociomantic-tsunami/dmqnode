/*******************************************************************************

    Consume request implementation.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.neo.Consume_v2;

import dmqproto.node.neo.request.Consume;

import dmqnode.connection.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import dmqnode.storage.model.StorageEngine;
import ocean.core.TypeConvert : castFrom, downcast;

import ocean.transition;

/*******************************************************************************

    DMQ node implementation of the v2 Consume request protocol.

*******************************************************************************/

public scope class ConsumeImpl_v2 : ConsumeProtocol_v2, StorageEngine.IConsumer
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

        Performs any logic needed to subscribe to and start consuming from the
        channel of the given name.

        Params:
            channel_name = channel to consume from
            subscriber_name = the identifying name of the subscriber

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name,
                                             cstring subscriber_name )
    {
        if (auto channel = this.resources.storage_channels.getCreate(channel_name))
        {
            this.storage_engine = channel.subscribe(idup(subscriber_name));

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
