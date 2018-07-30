/*******************************************************************************

    ProduceMulti request class.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.ProduceMultiRequest;


import dmqnode.request.model.IDmqRequestResources;

import Protocol = dmqproto.node.request.ProduceMulti;

import swarm.common.request.helper.LoopCeder;

/*******************************************************************************

    ProduceMulti request

*******************************************************************************/

public class ProduceMultiRequest : Protocol.ProduceMulti
{
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
    }

    /***************************************************************************

        Pushes a received record to one or more queues. To be overriden by
        an actual implementors of dmqnode protocol.

        Params:
            channel_names = names of channels to push to
            value = record value to push

    ***************************************************************************/

    override protected void pushRecord ( in cstring[] channel_names, cstring value )
    {
        foreach ( channel; channel_names )
        {
            if ( auto storage_channel =
                 this.resources.storage_channels.getCreate(channel) )
            {
                foreach (subscriber; storage_channel)
                    subscriber.push(value);
            }
        }

        this.resources.loop_ceder.handleCeding();
    }
}
