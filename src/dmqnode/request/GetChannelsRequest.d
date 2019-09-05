/*******************************************************************************

    GetChannels request class.

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.GetChannelsRequest;


import dmqnode.request.model.IDmqRequestResources;

import Protocol = dmqproto.node.request.GetChannels;


/*******************************************************************************

    GetChannels request

*******************************************************************************/

public scope class GetChannelsRequest : Protocol.GetChannels
{
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
    }
}
