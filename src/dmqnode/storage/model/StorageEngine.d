/*******************************************************************************

    Queue Storage engine abstract base class

    The StorageEngine abstract class is the base class for the storage engines
    used in the Queue Node.

    The queue storage engine extends the base storage engine with the
    following features:
        * Methods to push & pop data.
        * A set of consumers -- clients waiting to read data from the channel.
        * A method to register a new consumer with the channel.

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.model.StorageEngine;


import ocean.core.Array;
import ocean.io.FilePath;
import ocean.io.Path : normalize, PathParser;
import ocean.sys.Environment;
import ocean.transition;

import swarm.node.storage.listeners.Listeners;
import swarm.node.storage.model.IStorageEngine;

public abstract class StorageEngine : IStorageEngine
{
    /***************************************************************************

        Set of consumers waiting for data on this storage channel. When data
        arrives the next consumer in the set is notified (round robin). When a
        flush / finish signal for the channel is received, all registered
        consumers are notified.

    ***************************************************************************/

    protected static class Consumers : IListeners!()
    {
        public alias Listener.Code ListenerCode;

        override protected void trigger_ ( Listener.Code code )
        {
            switch ( code )
            {
                case code.DataReady:
                    auto listener = this.listeners.next();
                    if ( listener )
                    {
                        listener.trigger(code);
                    }
                    break;

                case code.Flush:
                    super.trigger_(code);
                    break;
                case code.Finish:
                    super.trigger_(code);
                    break;

                default:
                    assert(false);
            }
        }
    }

    protected Consumers consumers;


    /***************************************************************************

        Alias Listener -> IConsumer

    ***************************************************************************/

    public alias Consumers.Listener IConsumer;


    /***************************************************************************

        Constructor

        Params:
            id    = identifier string for this instance

     **************************************************************************/

    protected this ( cstring id )
    {
        super(id);

        this.consumers = new Consumers;
    }


    /***************************************************************************

        Pushes a record into queue, notifying any waiting consumers that data is
        ready.

        Params:
            value = record value

        Returns:
            true if the record was pushed

     **************************************************************************/

    public void push ( cstring value )
    {
        this.push_(value);
        this.consumers.trigger(Consumers.ListenerCode.DataReady);
    }


    /***************************************************************************

        Reset method, called when the storage engine is returned to the pool in
        IStorageChannels. Sends the Finish trigger to all registered consumers,
        which will cause the requests to end (as the channel being consumed is
        now gone).

    ***************************************************************************/

    public override void reset ( )
    {
        this.consumers.trigger(Consumers.ListenerCode.Finish);
    }


    /***************************************************************************

        Flushes sending data buffers of consumer connections.

    ***************************************************************************/

    public override void flush ( )
    {
        this.consumers.trigger(Consumers.ListenerCode.Flush);
    }


    /***************************************************************************

        Attempts to push a record into queue.

        Params:
            value = record value

        Returns:
            true if the record was pushed

     **************************************************************************/

    abstract protected void push_ ( cstring value );


    /***************************************************************************

        Pops a record from queue.

        Params:
            value = record value

        Returns:
            this instance

     **************************************************************************/

    abstract public typeof(this) pop ( ref char[] value );

    /***************************************************************************

        Registers a consumer with the channel. The dataReady() method of the
        given consumer may be called when data is put to the channel.

        Params:
            consumer = consumer to notify when data is ready

    ***************************************************************************/

    public void registerConsumer ( IConsumer consumer )
    {
        this.consumers.register(consumer);
    }


    /***************************************************************************

        Unregisters a consumer from the channel.

        Params:
            consumer = consumer to stop notifying when data is ready

    ***************************************************************************/

    public void unregisterConsumer ( IConsumer consumer )
    {
        this.consumers.unregister(consumer);
    }

    /***************************************************************************

        Sets the subscriber name for this instance.

    ***************************************************************************/

    abstract public void rename ( cstring channel_name );

    /***************************************************************************

        Returns the storage identifier which will start with '@' if the
        subscriber name is empty (i.e. the subscriber used by Consume
        requests prior to v2 and the default for Consume v2).

        This method is necessary because `id()` needs to strip a leading '@'
        because it is used for logging.

        Returns:
            the storage identifier.

    ***************************************************************************/

    abstract public cstring storage_name ( );
}
