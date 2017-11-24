/*******************************************************************************

    Table of request handlers by command.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.node.RequestHandlers;

import Consume = dmqnode.request.neo.Consume;
import Pop     = dmqnode.request.neo.Pop;
import Push    = dmqnode.request.neo.Push;

import swarm.neo.node.ConnectionHandler;
import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.CmdHandlers request_handlers;

static this ( )
{
    request_handlers.add(DmqConst.Command.E.Consume, "consume", &Consume.handle, false);
    request_handlers.add(DmqConst.Command.E.Push, "push", &Push.handle);
    request_handlers.add(DmqConst.Command.E.Pop, "pop", &Pop.handle);
}
