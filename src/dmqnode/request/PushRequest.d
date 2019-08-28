/*******************************************************************************

    Push request class.

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.PushRequest;


import dmqnode.request.model.IDmqRequestResources;
import dmqnode.storage.model.StorageEngine;

import Protocol = dmqproto.node.request.Push;

import ocean.core.Verify;
import ocean.meta.types.Qualifiers;

/*******************************************************************************

    Push request

*******************************************************************************/

public scope class PushRequest : Protocol.Push
{
    import dmqnode.storage.model.StorageChannels: IChannel;

    /***************************************************************************

        Channel storage cache, to avoid re-fetching it from different methods

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
    }

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.storage_channel = this.resources.storage_channels.getCreate(
            channel_name);
        return this.storage_channel !is null;
    }

    /***************************************************************************

        Push the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

    ***************************************************************************/

    override protected void pushValue ( cstring channel_name, in void[] value )
    {
        verify(this.storage_channel !is null);
        // legacy char[] values :(
        foreach (subscriber; this.storage_channel)
            subscriber.push(cast(char[]) value);
    }
}
