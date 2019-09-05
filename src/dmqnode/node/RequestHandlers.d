/*******************************************************************************

    Table of request handlers by command.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.node.RequestHandlers;

import dmqnode.request.neo.Consume;
import dmqnode.request.neo.Pop;
import dmqnode.request.neo.Push;

import swarm.neo.node.ConnectionHandler;
import swarm.neo.request.Command;

import dmqproto.common.RequestCodes;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.RequestMap request_handlers;

static this ( )
{
    request_handlers.addHandler!ConsumeImpl_v4;
    request_handlers.addHandler!PushImpl_v3;
    request_handlers.addHandler!PopImpl_v1;
}
