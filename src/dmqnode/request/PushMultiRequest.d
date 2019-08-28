/*******************************************************************************

    PushMulti request class.

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.PushMultiRequest;


import dmqnode.request.model.IDmqRequestResources;

import Protocol = dmqproto.node.request.PushMulti;

import ocean.core.Verify;
import ocean.meta.types.Qualifiers;

/*******************************************************************************

    PushMulti request

*******************************************************************************/

public scope class PushMultiRequest : Protocol.PushMulti
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

        Ensure that requested channels exist / can be created and can be written
        to.

        Params:
            channel_name = list of channel names to checl

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    override protected bool prepareChannels ( in cstring[] channel_names )
    {
        foreach (channel; channel_names)
        {
            if (!this.resources.storage_channels.getCreate(channel))
                return false;
        }

        return true;
    }

    /***************************************************************************

        PushMulti the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

        Returns:
            "true" if writing the value was possible
            "false" if there wasn't enough space

    ***************************************************************************/

    override protected void pushValue ( cstring channel_name, in void[] value )
    {
        auto channel = this.resources.storage_channels.getCreate(channel_name);
        verify(channel !is null); // already verified in this.prepareChannels
        foreach (subscriber; channel)
            subscriber.push(cast(char[]) value);
    }
}
