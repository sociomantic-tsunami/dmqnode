/*******************************************************************************

    Distributed Message Queue Node Implementation

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.node.DmqNode;


import dmqnode.config.ChannelSizeConfig;
import dmqnode.config.ServerConfig;
import dmqnode.connection.ConnectionHandler;
import Neo = dmqnode.connection.neo.SharedResources;
import dmqnode.connection.SharedResources;
import dmqnode.node.IDmqNodeInfo;
import dmqnode.node.RequestHandlers;
import dmqnode.storage.model.StorageChannels;
import dmqnode.storage.model.StorageEngine;
import dmqnode.storage.Ring;

import dmqnode.util.Downcast;

import swarm.neo.authentication.HmacDef: Key;
import swarm.node.model.NeoChannelsNode : ChannelsNodeBase;
import swarm.node.storage.model.IStorageEngineInfo;
import dmqproto.client.legacy.DmqConst;

import ocean.io.select.EpollSelectDispatcher;
import ocean.meta.types.Qualifiers;



/*******************************************************************************

    DmqNode

*******************************************************************************/

public class DmqNode
    : ChannelsNodeBase!(IChannel, ConnectionHandler), IDmqNodeInfo
{
    /***************************************************************************

        Shared resources.

    ***************************************************************************/

    private Neo.SharedResources shared_resources;

    /***************************************************************************

        Constructor

        Params:
            config = server configuration
            channel_size_config = channel size configuration
            client_credentials = the client authentication keys by client names
            epoll = epoll select dispatcher to be used internally
            no_delay = toggle Nagle's algorithm (true = disabled, false =
                enabled) on the connection sockets

    ***************************************************************************/

    public this ( ServerConfig server_config,
                  ChannelSizeConfig channel_size_config,
                  EpollSelectDispatcher epoll, bool no_delay )
    {
        auto ringnode = new RingNode(server_config.data_dir, this,
                                     server_config.size_limit,
                                     channel_size_config);

        this.shared_resources = new Neo.SharedResources(ringnode);

        // Classic connection handler settings
        auto conn_setup_params = new ConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = ringnode;
        conn_setup_params.shared_resources = new SharedResources;

        // Neo node / connection handler settings
        Options options;
        options.epoll = epoll;
        options.requests = request_handlers;
        options.shared_resources = this.shared_resources;
        options.unix_socket_path = idup(server_config.unix_socket_path());
        options.credentials_filename = "etc/credentials";

        super(DmqConst.NodeItem(server_config.address(), server_config.port()),
            server_config.neoport(), ringnode, conn_setup_params, options,
            server_config.backlog);

        auto command = DmqConst.Command();
        static immutable cmd_codes =
        [
            command.E.Push, command.E.PushMulti,
            command.E.Produce, command.E.ProduceMulti,
            command.E.Pop, command.E.Consume
        ];
        foreach (cmd_code; cmd_codes)
            this.request_stats.init(command[cmd_code]);
    }


    /***************************************************************************

        Returns:
            information interface to this node

    ***************************************************************************/

    public IDmqNodeInfo node_info ( )
    {
        return this;
    }


    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public ulong channelSizeLimit ( )
    {
        return downcastAssert!(StorageChannels)(this.channels).channelSizeLimit;
    }


    /***************************************************************************

        Returns:
            maximum number of bytes per channel

    ***************************************************************************/

    public void writeDiskOverflowIndex ( )
    {
        downcastAssert!(StorageChannels)(this.channels).writeDiskOverflowIndex;
    }


    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    override protected cstring id ( )
    {
        return typeof(this).stringof;
    }

    /***************************************************************************

        'foreach' iteration over the storage i.e. channel subscribers.

    ***************************************************************************/

    public int opApply ( scope int delegate ( ref RingNode.Ring channel ) dg )
    {
        return super.opApply((ref IStorageEngineInfo channel)
        {
            return downcastAssert!(RingNode.Channel)(channel).opApply(
                (ref StorageEngine storage_)
                {
                    auto storage = downcastAssert!(RingNode.Ring)(storage_);
                    return dg(storage);
                }
            );
        });
    }

    /**************************************************************************

        Makes the super class create record action counters.

        Returns:
            the identifier for the record action counters to create.

     **************************************************************************/

    override protected string[] record_action_counter_ids ( )
    {
        return ["pushed", "popped"];
    }

    /***************************************************************************

        Calls `callback` with a `RequestResources` object whose scope is limited
        to the run-time of `callback`.

        Params:
            callback = a callback to call with a `RequestResources` object

    ***************************************************************************/

    override protected void getResourceAcquirer (
        scope void delegate ( Object request_resources ) callback )
    {
        scope request_resources = this.shared_resources.new RequestResources;
        callback(request_resources);
    }
}
