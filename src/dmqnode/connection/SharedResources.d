/*******************************************************************************

    DMQ shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.connection.SharedResources;



/*******************************************************************************

    Imports

    Imports which are required by the ConnectionResources struct, below,
    are imported publicly, as they are also needed in
    dmqnode.request.model.IDmqRequestResources (which imports this
    module). This is done to simplify the process of modifying the fields of
    ConnectionResources --  forgetting to import something into both
    modules is a common source of very confusing compile errors.

*******************************************************************************/

import swarm.common.connection.ISharedResources;

public import swarm.common.request.helper.LoopCeder;

public import swarm.protocol.StringListReader;

public import ocean.io.select.client.FiberSelectEvent;



/*******************************************************************************

    Struct whose fields define the set of shared resources which can be acquired
    by a request. Each request can acquire a single instance of each field.

*******************************************************************************/

public struct ConnectionResources
{
    char[] channel_buffer;
    bool[] channel_flags_buffer;
    char[] value_buffer;
    char[][] channel_list_buffer;
    FiberSelectEvent event;
    StringListReader string_list_reader;
    LoopCeder loop_ceder;
}



/*******************************************************************************

    Mix in a class called SharedResources which contains a free list for each of
    the fields of DmqConnectionResources. The free lists are used by
    individual requests to acquire and relinquish resources required for
    handling.

*******************************************************************************/

mixin SharedResources_T!(ConnectionResources);

