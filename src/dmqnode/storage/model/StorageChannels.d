/*******************************************************************************

    Queue Storage Channels manager interface

    Extends the core storage channels base class with the following features:
        * A method to get the number of bytes a record will take when stored in
          the storage engine (including any required headers, etc).
        * A per-channel size limit, in addition to the global node size limit.

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.model.StorageChannels;


import swarm.node.storage.model.IStorageEngine;

import ocean.transition;

/*******************************************************************************

    Abstract definition of a channel that can have subscribers. Initially the
    channel has one storage engine and no subscribers. The first subscriber uses
    that storage engine, for each subsequent subscriber a new empty storage
    engine is created.

*******************************************************************************/

abstract class IChannel: IStorageEngine
{
    import dmqnode.storage.model.StorageEngine;
    import core.stdc.string: memchr;
    import ocean.core.Enforce;

    /***************************************************************************

        The initial storage engine, active unless
          - `this.is_reset` is true or
          - there is a subscriber for this channel.

    ***************************************************************************/

    private StorageEngine initial_storage;

    /***************************************************************************

        The storage engines of the subscribers per subscriber name, active if
        there are subscribers for this channel.

    ***************************************************************************/

    private StorageEngine[istring] subscribers;

    /***************************************************************************

        `true` if this channel doesn't contain a storage engine, which is the
        case while the constructor runs and after `reset` has been called.

        Set to `true` by `reset` and to `false` on the next use. All public
        methods which obtain a storage engine check this flag and create one if
        it is `true`. The preferred of to handling thiswould be to override
        `initialise`; however, that causes the following problems:
        - It complicates passing a storage engine to the constructor on startup;
          `initialise` would need to know whether it is called from the
          constructor or not.
        - It causes unnecessary channel renaming if a consume request creates a
          channel: First an initial storage engine would be created with the
          plain channel name only to be immediately renamed to
          `subscriber@channel`.

        This flag is initially `true` so that the invariant won't fail in the
        constructor because the super constructor calls a public method.

    ***************************************************************************/

    private bool is_reset = true;

    /***************************************************************************

        Make sure there is
          - either an initial storage engine or subscribers if `this.is_reset`
            is false or
          - neither an initial storage engine nor subscribers if `this.is_reset`
            is true.

    ***************************************************************************/

    invariant ( )
    {
        if (this.is_reset)
        {
            assert(this.initial_storage is null);
            assert(!this.subscribers.length);
        }
        else
            if (this.initial_storage is null)
                assert(this.subscribers.length);
            else
                assert(!this.subscribers.length);
    }

    /***************************************************************************

        Creates a channel with one subscriber, if `storage.storage_name`
        contains a subscriber name, or no subscriber otherwise.

        Params:
            storage = the subscriber or initial storage according to
                      `storage.storage_name`

    ***************************************************************************/

    protected this ( StorageEngine storage )
    {
        cstring subscriber_name;

        super(splitSubscriberName(storage.storage_name, subscriber_name));
        // Setting the initial storage and this.is_reset must be done after the
        // super constructor has returned or the invariant will fail.

        if (subscriber_name is null)
            this.initial_storage = storage;
        else
            // subscriber_name may be a non-null empty string for the default
            // Consume subscriber.
            this.subscribers[idup(subscriber_name)] = storage;

        this.is_reset = false;
    }

    /***************************************************************************

        Splits a storage name into the channel ID and the subscriber name.

         If the `storage_name` contains a subscriber name then `subscriber_name`
         outputs it, otherwise `subscriber_name` outputs `null`. If
          `storage_name` starts with the subscriber name limiter character '@'
          then `subscriber_name` outputs `storage_name[0 .. 0]`, an empty
          non-`null` string.

        Params:
            storage_name    = the storage name, may or may not contain a
                              subscriber name
            subscriber_name = output of the subscriber name

        Returns:
            the channel ID

    ***************************************************************************/

    public static cstring splitSubscriberName (
        cstring storage_name, out cstring subscriber_name
    )
    {
        if (auto sep_ptr = memchr(storage_name.ptr, '@', storage_name.length))
        {
            auto sep = sep_ptr - storage_name.ptr;
            subscriber_name = storage_name[0 .. sep];
            return storage_name[sep + 1 .. $];
        }
        else
        {
            return storage_name;
        }
    }

    /***************************************************************************

        Looks up the storage engine for `subscriber_name`. If `subscriber_name`
        was not found
         - and this is the first subscriber to this channel then the initial
           storage engine is returned, which may contain records.
         - and there are other subscribers to this channel then a new empty
           storage engine is created and returned.

        Params:
            subscriber_name = the name of the subscriber

        Returns:
            the storage engine for the subscriber.

    ***************************************************************************/

    public StorageEngine subscribe ( istring subscriber_name )
    {
        StorageEngine subscriber;

        istring storage_name ( ) {return subscriber_name ~ "@" ~ this.id_;}

        if (this.subscribers.length)
        {
            // Return the subscriber storage engine if existing or create a
            // new one otherwise, which will be assigned to a subscriber at the
            // end of this method.
            if (auto subscriber_ptr = subscriber_name in this.subscribers)
                return *subscriber_ptr;
            else
                subscriber = this.newStorageEngine(storage_name);
        }
        else if (this.is_reset)
        {
            // No initial storage or subscribers (invariant-verified), create a
            // new subscriber storage engine, which will be assigned to a
            // subscriber at the end of this method.
            subscriber = this.newStorageEngine(storage_name);
            this.is_reset = false;
        }
        else
        {
            // No subscribers so there is an initial storage engine: Remove the
            // initial storage and rename it to a subscriber storage, it will be
            // assigned to a subscriber at the end of this method.
            subscriber = this.initial_storage;
            this.initial_storage = null;
            subscriber.rename(storage_name);
        }

        assert(subscriber);
        this.subscribers[subscriber_name] = subscriber;
        return subscriber;
    }

    /***************************************************************************

        Adds a new subscriber unless a subscriber with the same name already
        exists.

        This method is meant to be called on startup only; the following
        restraints apply:

          - The channel id in `storage_name` must match the id of this channel.
          - `storage_name` must contain a subscriber name. The subscriber name
            may be an empty string i.e. `storage_name[0] == '@'`.
          - This channel must have been created with a subscriber.

        Params:
            storage_name = the storage name

        Returns:
            `null` if a subscriber with the same name was found, otherwise the
            newly created storage engine that has been registered for the
            subscriber name.

        Throws:
            `StartupException` if
              - this channel has no subscribers or
              - the channel id in `storage_name` does not match the name of this
                channel or
              - `storage_name` does not contain a subscriber name.

    ***************************************************************************/

    public StorageEngine addSubscriber ( cstring storage_name )
    in
    {
        assert(!this.is_reset);
    }
    body
    {
        enforce!(AddSubscriberException)(
            this.initial_storage is null,
            "Cannot add \"" ~ storage_name ~ "\": Channel \"" ~ this.id_ ~ "\""~
            " has no subscribers"
        );

        cstring subscriber_name;
        enforce!(AddSubscriberException)(
            splitSubscriberName(storage_name, subscriber_name) == this.id_,
            "Channel name in \"" ~ storage_name ~
            "\" does not match \"" ~ this.id_ ~ "\""
        );
        enforce!(AddSubscriberException)(
            subscriber_name !is null,
            "Cannot add \"" ~ storage_name ~ "\" as a subscriber: " ~
            "No subscriber name"
        );
        if (!(subscriber_name in this.subscribers))
        {
            auto subscriber = this.newStorageEngine(storage_name);
            this.subscribers[idup(subscriber_name)] = subscriber;
            return subscriber;
        }
        else
            return null;
    }

    /***************************************************************************

        `foreach` iteration over all storage engines in this channel.

    ***************************************************************************/

    public int opApply ( int delegate ( ref StorageEngine subscriber ) dg )
    {
        if (this.initial_storage is null)
        {
            if (this.is_reset)
            {
                // This instance has been recycled in the object pool, make it
                // an empty channel without subscribers.
                this.initial_storage = this.newStorageEngine(this.id_);
                this.is_reset = false;
            }
            else
            {
                foreach (ref subscriber; this.subscribers)
                {
                    if (int x = dg(subscriber))
                        return x;
                }
                return 0;
            }
        }

        return dg(this.initial_storage);
    }

    /***************************************************************************

        Returns
          - the storage engine for this channel if there are no subscribers or
          - `null` if there are subscribers.

    ***************************************************************************/

    public StorageEngine storage_unless_subscribed ( )
    {
        if (this.is_reset)
        {
            // This instance has been recycled in the object pool, make it an
            // empty channel without subscribers.
            this.initial_storage = this.newStorageEngine(this.id_);
            this.is_reset = false;
            return this.initial_storage;
        }
        else
            return this.subscribers.length? null : this.initial_storage;
    }

    /***************************************************************************

        Returns:
            the total number of records stored in this channel.

    ***************************************************************************/

    public ulong num_records ( )
    {
        ulong n = 0;
        if (!this.is_reset)
            foreach (subscriber; this)
                n += subscriber.num_records;
        return n;
    }

    /***************************************************************************

        Returns:
            the total number of bytes stored in this channel.

    ***************************************************************************/

    public ulong num_bytes ( )
    {
        ulong n = 0;
        if (!this.is_reset)
            foreach (subscriber; this)
                n += subscriber.num_bytes;
        return n;
    }

    /***************************************************************************

        Clears all storage engines in this channel.

        Returns:
            this instance.

    ***************************************************************************/

    override public typeof(this) clear ( )
    {
        if (!this.is_reset)
            foreach (subscriber; this)
                subscriber.clear();
        return this;
    }

    /***************************************************************************

        Closes all storage engines in this channel.

        Returns:
            this instance.

    ***************************************************************************/

    override public typeof(this) close ( )
    {
        if (!this.is_reset)
            foreach (subscriber; this)
                subscriber.close();
        return this;
    }


    /***************************************************************************

        Reset method, called when this channel is returned to the pool in
        `IStorageChannels`. Resets all storage engines. Recycles all storage
        engines but one, which is stored in `this.initial_storage`. Removes all
        elements from `this.subscribers`.

    ***************************************************************************/

    public override void reset ( )
    out
    {
        assert(this.is_reset);
    }
    body
    {
        assert(!this.is_reset);

        if (this.initial_storage !is null)
        {
            this.recycleStorageEngine(this.initial_storage);
            this.initial_storage = null;
        }
        else
        {
            foreach (ref storage; this.subscribers)
            {
                this.recycleStorageEngine(storage);
                storage = null;
            }

            version (D_Version2)
                this.subscribers.clear;
            else
                this.subscribers = null;
        }

        this.is_reset = true;
    }


    /***************************************************************************

        Flushes write buffers of stream connections.

    ***************************************************************************/

    override public void flush()
    {
        if (!this.is_reset)
            foreach (subscriber; this)
                subscriber.flush();
    }

    /***************************************************************************

        Creates a new storage engine for this channel. It is safe to assume that
        this method is called only once with the same `storage_name`.

        Params:
            storage_name = the storage name

        Returns:
            a new storage engine.

    ***************************************************************************/

    abstract protected StorageEngine newStorageEngine ( cstring storage_name );


    /***************************************************************************

        Recycles `storage`, which is an object previously returned by
        `newStorageEngine` or passed to the constructor.

        Params:
            storage = the storage engine object to recycle

    ***************************************************************************/

    abstract protected void recycleStorageEngine ( StorageEngine storage );

    /// Thrown by `addSubscriber`.
    static class AddSubscriberException: Exception
    {
        this ( istring msg, istring file = __FILE__, int line = __LINE__ )
        {
            super(msg, file, line);
        }
    }
}

/*******************************************************************************

    StorageChannels base class

*******************************************************************************/

import swarm.node.storage.model.IStorageChannels;

public abstract class StorageChannels :
    IStorageChannelsTemplate!(IChannel)
{
    /***************************************************************************

        Calculates the size (in bytes) an item would take if it were pushed
        to the queue.

        Params:
            len = length of data item

        Returns:
            bytes that data will claim in the queue

    ***************************************************************************/

    abstract protected size_t pushSize ( size_t additional_size );


    /***************************************************************************

        Constructor

        Params:
            size_limit = maximum number of bytes allowed in the node

    ***************************************************************************/

    public this ( ulong size_limit )
    {
        super(size_limit);
    }


    /***************************************************************************

        Returns:
            the default size limit per channel in bytes.

    ***************************************************************************/

    abstract public ulong channelSizeLimit ( );

    /***************************************************************************

        Writes disk overflow index.

    ***************************************************************************/

    abstract public void writeDiskOverflowIndex ( );
}

version (UnitTest)
{
    import ocean.core.Test;
    import dmqnode.util.Downcast;
}

// Test for IChannel except splitSubscriberName (separate test below).

unittest
{
    static class Storage: IChannel.StorageEngine
    {
        bool flushed, cleared, closed, recycled;
        uint records, bytes;

        this ( char[] id ) { super(id); }

        override void rename ( cstring ch ) { this.initialise(ch); }
        override cstring storage_name ( ) { return this.id; }
        override void flush ( ) { this.flushed = true; }

        override typeof(this) clear ( )
        {
            this.cleared = true;
            return this;
        }

        override typeof(this) close ( )
        {
            this.closed = true;
            return this;
        }

        ulong num_records ( ) { return this.records; }
        ulong num_bytes ( ) { return this.bytes; }

        override void push_ ( char[] value ) { }
        override typeof(this) pop ( ref char[] value ) { return this; }
    }

    static class Channel: IChannel
    {
        this ( StorageEngine storage ) { super(storage); }

        override StorageEngine newStorageEngine ( cstring storage_name )
        {
            return new Storage(storage_name);
        }

        override void recycleStorageEngine ( StorageEngine storage )
        {
            (downcastAssert!(Storage)(storage)).recycled = true;
        }
    }

    // Test IChannel except addStorage, initialise with subscriberless storage.
    {
        scope storage = new Storage("ch");
        scope channel = new Channel(storage);
        test!("is")(channel.storage_unless_subscribed, storage);
        foreach (s; channel) test!("is")(s, storage);

        test!("is")(channel.subscribe("max"), storage);
        test!("==")(storage.id, "max@ch");
        test!("is")(channel.storage_unless_subscribed, cast(Object)null);
        foreach (s; channel) test!("is")(s, storage);

        auto storage2 = downcastAssert!(Storage)(channel.subscribe("moritz"));
        test!("!is")(storage2, storage);
        test!("==")(storage2.id, "moritz@ch");

        storage.records = 4700;
        storage2.records = 11;
        test!("==")(channel.num_records, 4711);

        storage.bytes = 123000;
        storage2.bytes = 456;
        test!("==")(channel.num_bytes, 123456);

        channel.flush();
        test(storage.flushed);
        test(storage2.flushed);

        channel.clear();
        test(storage.cleared);
        test(storage2.cleared);

        channel.close();
        test(storage.closed);
        test(storage2.closed);

        channel.reset();
        test(storage.recycled);
        test(storage2.recycled);

        auto storage3 = downcastAssert!(Storage)(channel.storage_unless_subscribed);
        test!("!is")(storage3, cast(Object)null);
        test!("==")(storage3.id, "ch");

        channel.reset();
        test(storage3.recycled);
        foreach (s; channel)
        {
            test!("!is")(s, cast(Object)null);
            test!("==")(s.id, "ch");
            test!("is")(channel.storage_unless_subscribed, s);
        }
    }

    // Initialise with subscriber.
    {
        scope storage = new Storage("fritz@ch");
        scope channel = new Channel(storage);
        test!("is")(channel.storage_unless_subscribed, cast(Object)null);
        test!("is")(channel.subscribe("fritz"), storage);
    }

    // Initialise with subscriber "".
    {
        scope storage = new Storage("@ch");
        scope channel = new Channel(storage);
        test!("is")(channel.storage_unless_subscribed, cast(Object)null);
        test!("is")(channel.subscribe(""), storage);
    }

    // Test IChannel.addStorage: Initialise with subscriberless storage, then
    // add subscriber storage. The subscriberless storage should be changed to
    // subscriber "".
    {
        scope storage = new Storage("@ch");
        scope channel = new Channel(storage);
        auto storage2 = channel.addSubscriber("fritz@ch");
        test!("!is")(storage2, cast(Object)null);
        test!("==")(storage2.id, "fritz@ch");
        test!("is")(channel.storage_unless_subscribed, cast(Object)null);
        test!("is")(channel.subscribe(""), storage);
        test!("is")(channel.subscribe("fritz"), storage2);
    }

    // Test IChannel.addStorage: Initialise with subscriberless storage, then
    // add subscriber storage. The subscriberless storage should be changed to
    // subscriber "".
    {
        scope storage = new Storage("@ch");
        scope channel = new Channel(storage);
        auto storage2 = channel.addSubscriber("fritz@ch");
        test!("!is")(storage2, cast(Object)null);
        test!("==")(storage2.id, "fritz@ch");
        test!("is")(channel.storage_unless_subscribed, cast(Object)null);
        test!("is")(channel.subscribe(""), storage);
        test!("is")(channel.subscribe("fritz"), storage2);
    }
}

// Test for IChannel.splitSubscriberName.

unittest
{
    // Named test, splits storage_name into subscriber_name and channel_name and
    // verifies the result. subscriber_name should be null if and only if a
    // null subscriber name is expected; that is, if storage_name does not
    // contain '@'. If and only if storage_name starts with '@' then an empty
    // non-null subscriber name is expected.
    static void check ( istring test_name, cstring storage_name,
                        cstring subscriber_name, cstring channel_name )
    {
        auto test = new NamedTest(test_name);
        cstring result_subscriber_name;
        test.test!("==")(
            IChannel.splitSubscriberName(storage_name, result_subscriber_name),
            channel_name
        );
        test.test!("==")(result_subscriber_name, subscriber_name);
        test.test!("==")(result_subscriber_name is null, subscriber_name is null);
    }

    check("subscriber@channel", "hello@world", "hello", "world");
    check("@channel", "@world", "", "world");
    check("plain channel", "hello_world", null, "hello_world");
    check("subscriber@", "hello@", "hello", "");

    // Also test empty channel names, which are in fact invalid, still
    // splitSubscriberName should handle them properly.
    check("only '@'", "@", "", "");
    check("empty storage name", "", null, "");
    check("null storage name", "", null, "");
}
