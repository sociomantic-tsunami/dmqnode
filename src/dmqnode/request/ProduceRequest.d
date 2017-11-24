/*******************************************************************************

    Produce request class.

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.ProduceRequest;


import dmqnode.request.model.IDmqRequestResources;
import dmqnode.storage.model.StorageEngine;

import Protocol = dmqproto.node.request.Produce;

import swarm.common.request.helper.LoopCeder;



/*******************************************************************************

    Produce request

*******************************************************************************/

public scope class ProduceRequest : Protocol.Produce
{
    import dmqnode.storage.model.StorageChannels: IChannel;

    /***************************************************************************

        Set upon starting valid Produce request, reused when pushing records
        for that request (so that it won't be fetched for each record)

    ***************************************************************************/

    private IChannel storage_channel;

    /***************************************************************************

        Shared resource acquirer

    ***************************************************************************/

    private IDmqRequestResources resources;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
            IDmqRequestResources resources )
    {
        super(reader, writer, resources);
        this.resources = resources;
        this.storage_channel = null;
    }

    /***************************************************************************

        Ensures that requested channel exists or can be created

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with Produce request

    ***************************************************************************/

    override protected bool prepareChannel ( char[] channel_name )
    {
        this.storage_channel = this.resources.storage_channels.getCreate(
            *this.resources.channel_buffer);
        return this.storage_channel !is null;
    }

    /***************************************************************************

        Pushes a received record to the queue.

        Params:
            channel_name = name of channel to push to
            value = record value to push

    ***************************************************************************/

    override protected void pushRecord ( char[] channel_name, char[] value )
    {
        assert (this.storage_channel !is null);
        foreach (subscriber; this.storage_channel)
            subscriber.push(value);
        this.resources.loop_ceder.handleCeding();
    }
}
