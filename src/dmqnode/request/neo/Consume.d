/*******************************************************************************

    Consume request implementation.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.request.neo.Consume;


import dmqproto.node.neo.request.Consume;

import dmqproto.node.neo.request.core.IRequestResources;
import dmqnode.connection.neo.SharedResources;
import dmqnode.storage.model.StorageEngine;

import ocean.core.TypeConvert : castFrom;
import dmqnode.util.Downcast;


/*******************************************************************************

    DMQ node implementation of the v4 Consume request protocol.

*******************************************************************************/

class ConsumeImpl_v4 : ConsumeProtocol_v4, StorageEngine.IConsumer
{
    import dmqproto.common.RequestCodes : RequestCode;
    import ocean.meta.types.Qualifiers : cstring, mstring;
    import swarm.neo.request.Command : Command;

    /// Request code and version (required by ConnectionHandler)
    static immutable Command command = Command(RequestCode.Consume, 4);

    /// Request name for stats tracking (required by ConnectionHandler)
    static immutable string name = "consume";

    /// Flag indicating whether timing stats should be generated for
    /// requests of this type
    static immutable bool timing = false;

    /// Flag indicating whether this request is scheduled for removal
    /// (if `true`, clients will be warned)
    static immutable bool scheduled_for_removal = false;

    /***************************************************************************

        Storage engine being consumed from.

    ***************************************************************************/

    private StorageEngine storage_engine;

    /***************************************************************************

        Performs any logic needed to subscribe to and start consuming from the
        channel of the given name.

        Params:
            resources = request resources
            channel_name = channel to consume from
            subscriber_name = the identifying name of the subscriber

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( IRequestResources resources,
                                             cstring channel_name,
                                             cstring subscriber_name )
    {
        if (auto channel =
            downcastAssert!(SharedResources.RequestResources)(resources)
            .storage_channels.getCreate(channel_name))
        {
            this.storage_engine = channel.subscribe(idup(subscriber_name));

            if ( this.storage_engine !is null )
            {
                this.storage_engine.registerConsumer(this);
                return true;
            }
        }

        return false;
    }

    /***************************************************************************

        Performs any logic needed to stop consuming from the channel of the
        given name.

        Params:
            channel_name = channel to stop consuming from

    ***************************************************************************/

    override protected void stopConsumingChannel ( cstring channel_name )
    {
        this.storage_engine.unregisterConsumer(this);
    }

    /***************************************************************************

        Retrieve the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    override protected bool getNextValue ( ref void[] value )
    {
        auto mstring_value = castFrom!(void[]*).to!(mstring*)(&value);
        this.storage_engine.pop(*mstring_value);

        return value.length > 0;
    }

    /***************************************************************************

        StorageEngine.IConsumer method, called when new data arrives or the
        channel is deleted.

        Params:
            code = trigger event code

    ***************************************************************************/

    override public void trigger ( Code code )
    {
        with ( Code ) switch ( code )
        {
            case DataReady:
                this.dataReady();
                break;

            case Flush:
                this.flushBatch();
                break;

            case Finish:
                this.channelRemoved();
                break;
            default:
                break;
        }
    }
}
