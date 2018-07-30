/*******************************************************************************

    GetNumConnections request class.

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.GetNumConnectionsRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqnode.request.model.IDmqRequestResources;

import Protocol = dmqproto.node.request.GetNumConnections;

/*******************************************************************************

    GetNumConnections request

*******************************************************************************/

public scope class GetNumConnectionsRequest : Protocol.GetNumConnections
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
