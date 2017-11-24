/*******************************************************************************

    Information interface for the Distributed Message Queue Node

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.node.IDmqNodeInfo;


import dmqnode.storage.Ring;

import swarm.node.model.IChannelsNodeInfo;

interface IDmqNodeInfo : IChannelsNodeInfo
{
    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public ulong channelSizeLimit ( );

    /***************************************************************************

        'foreach' iteration over the channels.

    ***************************************************************************/

    public int opApply ( int delegate ( ref RingNode.Ring channel ) dg );
}
