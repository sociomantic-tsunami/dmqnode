/*******************************************************************************

    Interface and base scope class containing getter methods to acquire
    resources needed by a DMQ node request. Multiple calls to the same getter
    only result in the acquiring of a single resource of that type, so that the
    same resource is used over the life time of a request. When a request
    resource instance goes out of scope all required resources are automatically
    relinquished.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.model.IDmqRequestResources;


import dmqnode.connection.SharedResources;
import dmqnode.node.IDmqNodeInfo;
import dmqnode.storage.model.StorageChannels;

import dmqproto.node.request.model.DmqCommand;

import swarm.common.request.model.IRequestResources;

/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (dmqnode.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding a couple of
    DMQ-specific getters.

*******************************************************************************/

public interface IDmqRequestResources : IRequestResources, DmqCommand.Resources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .StringListReader StringListReader;
    alias .LoopCeder LoopCeder;
    alias .StorageChannels StorageChannels;
    alias .IDmqNodeInfo IDmqNodeInfo;


    /***************************************************************************

        Storage channels getter.

    ***************************************************************************/

    StorageChannels storage_channels ( );


    /***************************************************************************

        Node info getter.

    ***************************************************************************/

    IDmqNodeInfo node_info ( );
}



/*******************************************************************************

    Mix in a scope class called RequestResources which implements
    IRequestResources. Note that this class does not implement the additional
    methods required by IDmqRequestResources -- this is done in
     dmqnode.connection.ConnectionHandler.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);
