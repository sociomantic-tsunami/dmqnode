/*******************************************************************************

    Pop request class.

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.PopRequest;


import dmqnode.request.model.IDmqRequestResources;
import dmqnode.storage.model.StorageEngine;

import Protocol = dmqproto.node.request.Pop;

/*******************************************************************************

    Pop request

*******************************************************************************/

public scope class PopRequest : Protocol.Pop
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

        Pops the last value from the channel. If returning `true` then
        `getNextValue()` will return that value.

        Params:
            channel_name = name of channel to pop from

        Return:
            `true` if popped so it is possible to proceed with this request or
            `false` if popping from the channel was not possible.

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        if (auto channel = channel_name in this.resources.storage_channels)
        {
            if (auto storage = channel.storage_unless_subscribed)
            {
                storage.pop(*this.resources.value_buffer);
                return true;
            }
        }

        (*this.resources.value_buffer).length = 0;
        return false;
    }

    /***************************************************************************

        Returns the popped value, expected to be called only if
        `prepareChannel()` returned true.

        Params:
            channel_name = unused, passed to `prepareChannel()`

        Returns:
            popped value, empty array if channel is empty

    ***************************************************************************/

    override protected void[] getNextValue ( cstring channel_name )
    {
        return *this.resources.value_buffer;
    }
}
