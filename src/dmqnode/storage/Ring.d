/*******************************************************************************

    Ring queue Storage engine

    copyright:
        Copyright (c) 2010-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.Ring;


import dmqnode.config.ChannelSizeConfig;
import dmqnode.storage.engine.DiskOverflow;
import dmqnode.storage.model.StorageChannels;
import dmqnode.storage.model.StorageEngine;
// This import is not ordered, but there is a circular dependency
// (`IDmqNodeInfo` imports this module) which triggers
// forward references error when running on the CI.
import dmqnode.node.IDmqNodeInfo;
import dmqnode.util.Downcast;

import dmqproto.client.legacy.DmqConst;

import ocean.core.Enforce: enforce;
import ocean.io.device.File;
import ocean.io.FilePath;
import ocean.io.Path : normalize, PathParser;
import ocean.sys.Environment;
import ocean.util.container.mem.MemManager;
import ocean.util.container.queue.FlexibleRingQueue;
import ocean.util.container.queue.model.IQueueInfo;
import ocean.util.log.Log;
import core.stdc.ctype;
import ocean.transition;


/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dmqnode.storage.Ring");
}



/*******************************************************************************

    Ring node storage channels class, acts as a container for a set of ring
    queue channels.

*******************************************************************************/

public class RingNode : StorageChannels
{
    /***************************************************************************

        Dump file name suffix

    ***************************************************************************/

    const istring DumpFileSuffix = ".rq";


    /***************************************************************************

        Ring queue storage engine class.

    ***************************************************************************/

    private class Ring : StorageEngine
    {
        /***********************************************************************

            Memory queue status information

        ***********************************************************************/

        public IQueueInfo memory_info;

        /***********************************************************************

            Disk overflow status information

        ***********************************************************************/

        public DiskOverflowInfo overflow_info;

        /***********************************************************************

            RingQueue instance

        ***********************************************************************/

        private FlexibleByteRingQueue queue;

        /***********************************************************************

            Disk overflow channel

        ***********************************************************************/

        private DiskOverflow.Channel overflow;

        /***********************************************************************

            Filename

        ***********************************************************************/

        private istring filename;


        /***********************************************************************

            Re-used FilePath instance.

        ***********************************************************************/

        private FilePath file_path;

        /***********************************************************************

            For the temporary console log only.

            Counters for the number of records and bytes of payload pushed
            to and popped from this channel.

        ***********************************************************************/

        public uint records_pushed = 0,
                    records_popped = 0;

        public ulong bytes_pushed = 0,
                     bytes_popped = 0;

        /***********************************************************************

            Logger

        ***********************************************************************/

        private Logger log;

        /***********************************************************************

            Constructor. Creates the ring queue. Also attempts to deserialize
            its contents from the specified path if the RingNode is being
            constructed (the loading of a dumped channel does not happen once
            the node has started up -- only at initialisation).

            `storage_name == channel_id ~ '@' ~ subscriber_name` is used as the
            real channel name in this class, for the memory queue dump file path
            and as the disk overflow channel name.

            Params:
                storage_name = the name of this storage

        ***********************************************************************/

        public this ( cstring storage_name )
        in
        {
            assert(!this.outer.shutting_down,
                   "Attempted to create storage '" ~ storage_name ~
                   "' during shutdown");
        }
        body
        {
            // The super constructor calls this.initialise, which uses
            // this.file_path.
            this.file_path = new FilePath;

            super(storage_name);

            cstring subscriber_name;

            this.queue = new FlexibleByteRingQueue(
                noScanMallocMemManager,
                this.outer.channel_size_config.getChannelSize(
                    Channel.splitSubscriberName(storage_name, subscriber_name)
                )
            );

            this.memory_info = this.queue;
        }

        /***********************************************************************

            Creates an overflow channel and the memory dump file path.

            Called from the super constructor and when this instance is taken
            from the object pool and assigned to a channel again.

            Params:
                storage_name = the name of this storage, see constructor

        ***********************************************************************/

        override public void initialise ( cstring storage_name )
        {
            super.initialise(storage_name);
            // From this point this.id == storage_name.

            this.log = Log.lookup("storage:" ~ this.storage_name);

            this.filename = FilePath.join(
                this.outer.data_dir,
                cast(istring)(this.storage_name ~ this.outer.DumpFileSuffix)
            );
            this.file_path.set(this.filename);

            if (this.overflow is null)
            {
                this.overflow = this.outer.overflow.new Channel(idup(this.storage_name));
                this.overflow_info = this.overflow;
            }
            else
            {
                this.overflow.readd(this.overflow, idup(this.storage_name));
            }
        }

        /***********************************************************************

            Removes the overflow channel.

            Called after the channel has been removed when this instance is put
            in the object pool.

        ***********************************************************************/

        override public void reset ( )
        {
            super.reset();
            this.overflow.remove(this.overflow);
        }

        /***********************************************************************

            Changes the name of this storage.

            Params:
                storage_name = the new storage name

        ***********************************************************************/

        override public void rename ( cstring storage_name )
        {
            super.initialise(storage_name);
            this.log = Log.lookup("storage:" ~ this.storage_name);
            this.overflow.rename(idup(storage_name));
            this.filename = FilePath.join(
                this.outer.data_dir,
                cast(istring)(this.storage_name ~ this.outer.DumpFileSuffix)
            );
            this.file_path.set(this.filename);
        }

        /***********************************************************************

            Looks for and loads a saved dump of the channel's contents.

        ***********************************************************************/

        private void loadDumpedChannel ( )
        {
            auto filepath = this.file_path.toString();

            if ( this.file_path.exists() )
            {
                this.log.info("Loading file \"{}\"", filepath);
                scope file = new File(filepath, File.ReadExisting);
                scope ( exit ) file.close();

                this.queue.load(file);
            }
            else
            {
                this.log.error("File \"{}\" not found", filepath);
            }
        }


        /***********************************************************************

            Pushes a record into queue.

            Params:
                value = record value

        ***********************************************************************/

        override protected void push_ ( char[] value )
        {
            this.outer.dmqnode.record_action_counters.increment("pushed", value.length);

            // For the temporary console log only:
            this.records_pushed++;
            this.bytes_pushed += value.length;

            if (!this.queue.push(cast(ubyte[])value))
            {
                this.overflow.push(value);
            }
        }


        /***********************************************************************

            Pops a record from queue.

            Params:
                value = record value

            Returns:
                this instance

        ***********************************************************************/

        override public typeof(this) pop ( ref char[] value )
        {
            void[] allocValue ( size_t n )
            {
                value.length = n;
                return value;
            }

            if (void[] item = this.queue.pop())
            {
                 allocValue(item.length)[] = item[];
                 this.outer.dmqnode.record_action_counters.increment("popped", value.length);

                 // For the temporary console log only:
                 this.records_popped++;
                 this.bytes_popped += value.length;
            }
            else if (this.overflow.pop(&allocValue))
            {
                 this.outer.dmqnode.record_action_counters.increment("popped", value.length);

                 // For the temporary console log only:
                 this.records_popped++;
                 this.bytes_popped += value.length;
            }
            else
            {
                value.length = 0;
            }

            return this;
        }


        /***********************************************************************

            Removes all records from the queue.

            Returns:
                this instance

        ***********************************************************************/

        override public typeof(this) clear ( )
        {
            this.queue.clear;
            this.overflow.clear;

            return this;
        }


        /***********************************************************************

            Closes the queue.

            Returns:
                this instance

        ***********************************************************************/

        override public typeof(this) close ( )
        {
            if ( this.file_path.exists )
            {
                this.log.warn("Closing -- will {} existing dump file \"{}\"",
                         this.queue.length? "overwrite" : "delete",
                         this.file_path.toString());
            }
            else
            {
                if ( this.queue.length )
                {
                    this.log.info("Closing -- saving in file \"{}\"",
                        this.file_path.toString());
                }
                else
                {
                    this.log.info("Closing -- storage is empty, not saving");
                }
            }

            if ( this.queue.length )
            {
                scope file = new File(this.file_path.toString(), File.WriteCreate);
                scope ( exit ) file.close();

                this.queue.save(file);
            }

            return this;
        }

        /***********************************************************************

            Returns the storage identifier, stripping the leading '@' if the
            subscriber name is empty (i.e. the subscriber used by Consume
            requests prior to v2 and the default for Consume v2).

            The storage identifier returned by this method is used
              - by the public API, the stats log for example
              - internally for the name of the dmq dump file.

            Returns:
                the storage identifier without a leading '@'.

        ***********************************************************************/

        override public cstring id ( )
        {
            auto idstr = super.id();
            if (idstr.length)
                if (idstr[0] == '@')
                    return idstr[1 .. $];
            return idstr;
        }

        /***********************************************************************

            Returns the storage identifier which will start with '@' if the
            subscriber name is empty (i.e. the subscriber used by Consume
            requests prior to v2 and the default for Consume v2).

            Returns:
                the storage identifier.

        ***********************************************************************/

        override public cstring storage_name ( )
        {
            return super.id();
        }

        /***********************************************************************

            Deletes the channel dump file.

        ***********************************************************************/

        public void deleteDumpFile ( )
        {
            this.file_path.remove();
        }

        /***********************************************************************

            Returns:
                number of records stored

        ***********************************************************************/

        public ulong num_records ( )
        {
            return this.queue.length + this.overflow.num_records;
        }


        /***********************************************************************

            Returns:
                number of records stored

        ***********************************************************************/

        public ulong num_bytes ( )
        {
            return this.queue.used_space + this.overflow.num_bytes;
        }


        /***********************************************************************

           Tells the queue capacity in bytes.

           Returns:
               the queue capacity in bytes.

        ***********************************************************************/

        public ulong capacity_bytes ( )
        {
            return queue.total_space;
        }
    }

    /***************************************************************************

        Channel implementation, needs to be a nested class here because it needs
        to create `Ring` instances.

    ***************************************************************************/

    class Channel: IChannel
    {
        /***********************************************************************

            Creates a channel with no subscriber.

            Params:
                storage = the initial storage for the channel

        ***********************************************************************/

        protected this ( StorageEngine storage )
        {
            super(storage);
        }

        /***********************************************************************

            Creates a new storage engine for this channel.

            Params:
                storage_name = the storage name

            Returns:
                a new storage engine.

        ***********************************************************************/

        override protected Ring newStorageEngine ( cstring storage_name )
        {
            return this.outer.newStorageEngine(storage_name);
        }

        /***********************************************************************

            Recycles `storage`, which is an object previously returned by
            `newStorageEngine` or passed to the constructor.

            Params:
                storage = the storage engine object to recycle

        ***********************************************************************/

        override protected void recycleStorageEngine ( StorageEngine storage )
        {
            auto ring = cast(Ring)storage;
            assert(ring);
            this.outer.storage_pool.recycle(ring);
        }
    }

    import ocean.util.container.pool.ObjectPool;

    /***************************************************************************

        Pool of storage engines.

    ***************************************************************************/

    private ObjectPool!(Ring) storage_pool;


    /***************************************************************************

        Disk overflow.

    ***************************************************************************/

    private DiskOverflow overflow;


    /***************************************************************************

        Data directory where dump files are stored.

    ***************************************************************************/

    private Immut!(char[]) data_dir;


    /***************************************************************************

        Channel size configuration.

    ***************************************************************************/

    private ChannelSizeConfig channel_size_config;


    /***************************************************************************

        Delegate which is called when a record is pushed or popped. Note that a
        failed push (e.g. queue full) will call this delegate, whereas a failed
        pop (e.g. queue empty) will not.

    ***************************************************************************/

    private IDmqNodeInfo dmqnode;


    /***************************************************************************

        This is a parameter passed from `createChannelOnStartup` to `create_()`.

    ***************************************************************************/

    private Ring storage_for_create;


    /***************************************************************************

        Flag indicating whether the node is currently shut down to prevent
        creating a new channel if a Produce request sends a record during
        shutdown.

    ***************************************************************************/

    private bool shutting_down = false;


    /***************************************************************************

        Constructor. If the specified data directory exists, it is scanned for
        dumped queue channels, which are loaded. Otherwise the data directory is
        created.

        Params:
            data_dir = data directory for dumped queue channels
            dmqnode = the hosting node for push/pop counting
            size_limit = maximum number of bytes allowed in the node
            channel_size_config = channel size configuration

    ***************************************************************************/

    public this ( istring data_dir, IDmqNodeInfo dmqnode, ulong size_limit,
                  ChannelSizeConfig channel_size_config )
    in
    {
        assert(dmqnode);
    }
    body
    {
        super(size_limit);

        this.channel_size_config = channel_size_config;

        this.dmqnode = dmqnode;

        this.data_dir = data_dir;

        scope path = new FilePath;
        this.setWorkingPath(path, this.data_dir);

        this.overflow = new DiskOverflow(data_dir);

        this.storage_pool = new typeof(storage_pool);

        if ( path.exists() )
        {
            this.loadDumpedChannels(path);
        }
        else
        {
            this.createWorkingDir(path);
        }
    }

    /***************************************************************************

        Writes disk overflow index.

    ***************************************************************************/

    override public void writeDiskOverflowIndex ( )
    {
        this.overflow.flush();
    }


    /***************************************************************************

        Returns:
            the default size limit per channel in bytes

    ***************************************************************************/

    override public ulong channelSizeLimit ( )
    {
        return this.channel_size_config.default_size_limit;
    }


    /***************************************************************************

        Creates a new channel with the given name.

        Params:
            id = identifier string for the new channel

        Returns:
            a new channel.

        Throws:
            Exception if the node is shutting down.

    ***************************************************************************/

    override protected Channel create_ ( cstring id )
    {
        enforce(!this.shutting_down,
            cast(istring)("Cannot create channel '" ~ id ~
            "' while shutting down"));

        // During startup this.storage_for_create contains the storage to
        // use for this channel, which is set by this.createChannelOnStartup
        // before calling super.getCreate, which calls this method.
        return this.new Channel(
            (this.storage_for_create is null)
                ? this.newStorageEngine(id) // normal operation
                : this.storage_for_create   // channel creation on startup
        );
    }


    /***********************************************************************

        Creates a new storage engine.

        Params:
            storage_name = the storage name

        Returns:
            a new storage engine.

    ***********************************************************************/

    private Ring newStorageEngine ( cstring storage_name )
    {
        Ring new_storage = null;

        auto storage = this.storage_pool.get(
            new_storage = this.new Ring(storage_name)
        );

        if (!new_storage)
            storage.initialise(storage_name);

        return storage;
    }



    /***************************************************************************

        Calculates the size (in bytes) an item would take if it were pushed
        to the queue.

        Params:
            len = length of data item

        Returns:
            bytes that data will claim in the queue

    ***************************************************************************/

    override protected size_t pushSize ( size_t additional_size )
    {
        return FlexibleByteRingQueue.pushSize(additional_size);
    }


    /***************************************************************************

        Returns:
             string identifying the type of the storage engine

    ***************************************************************************/

    override public cstring type ( )
    {
        return Ring.stringof;
    }


    /***************************************************************************

        Shuts the disk overflow down and prevents creating a new channel from
        this point.

    ***************************************************************************/

    protected override void shutdown_ ( )
    {
        this.shutting_down = true;
        this.overflow.close();
    }


    /***************************************************************************

        Creates a FilePath instance set to the absolute path of dir, if dir is
        not null, or to the current working directory of the environment
        otherwise.

        Params:
            path = FilePath instance to set
            dir = directory string; null indicates that the current working
                  directory of the environment should be used

    ***************************************************************************/

    private void setWorkingPath ( FilePath path, cstring dir )
    {
        if ( dir )
        {
            path.set(dir);

            if ( !path.isAbsolute() )
            {
                path.prepend(Environment.cwd());
            }
        }
        else
        {
            path.set(Environment.cwd());
        }
    }

    /***************************************************************************

        Searches dir for files with DumpFileSuffix suffix and retrieves the file
        names without suffices as storage engine identifiers.

        Params:
            dir = directory to search for database file objects

    ***************************************************************************/

    private void loadDumpedChannels ( FilePath path )
    {
        scope filename = new FilePath;

        auto log = Log.lookup("scan-dumpfiles");
        log.info("Using directory \"{}\".", path.toString);

       {
            foreach ( info; path )
            {
                if ( !info.folder )
                {
                    switch (filename.set(info.name).suffix())
                    {
                        case DumpFileSuffix:
                            if (validateDumpFileName(filename.name))
                            {
                                log.info("Found \"{}\".", filename.file);
                                this.createChannelFromDumpFile(filename.name);
                            }
                            else
                                log.error("Ignoring file \"{}\" " ~
                                    "(invalid name).",
                                    info.name);
                            break;

                        case this.overflow.Const.datafile_suffix,
                             this.overflow.Const.indexfile_suffix:
                            break;

                        default:
                            log.warn("Ignoring file \"{}\" " ~
                                    "(unknow suffix).", info.name);
                    }
                }
                else
                {
                    log.warn("Ignoring subdirectory \"{}\".", info.name);
                }
            }

            /*
             * Create all channels that are present in the disk overflow but didn't
             * have a memory dump file because their memory queue was empty. We
             * iterate over all channels in the disk overflow here; `getCreate()`
             * will do nothing for channels that already exist because a dump file
             * was found for them.
             */
            this.overflow.iterateChannelNames(
                (ref cstring storage_name)
                {
                    this.createChannelFromDiskOverflow(storage_name);
                    return 0;
                }
            );
       }

        // Delete the dump files after successful deserialisation.

        foreach (channel; this)
        {
            foreach (storage_; channel)
            {
                auto storage = cast(Ring)storage_;
                assert(storage);
                downcastAssert!(Ring)(storage).deleteDumpFile();
            }
        }
    }

    /***************************************************************************

        Creates a channel, potentially with a subscriber, or adds a subscriber
        to an  existing channel, depending on storage_name`; finally loads the
        queue dump file. The subscriber/channel combination is expected to not
        already exist.

        Params:
            storage_name = the storage name, determines the channel id and
                           subscriber name

        Throws:
            `StartupException` if the channel/subscriber combination already
            exists or a sanity check for `storage_name` fails.

    ***************************************************************************/

    private void createChannelFromDumpFile ( cstring storage_name )
    {
        auto log = Log.lookup("scan-dumpfiles");
        Ring storage;

        cstring subscriber_name;
        cstring channel_id = Channel.splitSubscriberName(
            storage_name, subscriber_name
        );

        if (auto channel_ = channel_id in this)
        {
            auto channel = downcastAssert!(Channel)(*channel_);
            if (auto storage_ = channel.addSubscriber(storage_name))
            {
                storage = downcastAssert!(Ring)(storage_);
                log.info("Added storage \"{}\" to channel \"{}\".",
                         storage.storage_name, channel_id);
            }
            else
                throw new Channel.AddSubscriberException(
                    cast(istring)
                    ("Duplicate storage name \"" ~ storage_name ~ '"')
                );
        }
        else
        {
            log.info("Creating channel \"{}\" with storage \"{}\".",
                     channel_id, storage_name);
            storage = this.newStorageEngine(storage_name);
            this.storage_for_create = storage;
            scope (exit) this.storage_for_create = null;
            this.getCreate(channel_id);
        }

        assert(storage);
        storage.loadDumpedChannel();
    }

    /***************************************************************************

        Creates a channel, potentially with a subscriber, or adds a subscriber
        to an  existing channel, depending on storage_name`. Does nothing if the
        subscriber/channel combination already exists.

        Params:
            storage_name = the storage name, determines the channel id and
                           subscriber name

    ***************************************************************************/

    private void createChannelFromDiskOverflow ( cstring storage_name )
    {
        auto log = Log.lookup("scan-diskoverflow");

        cstring subscriber_name;
        cstring channel_id = Channel.splitSubscriberName(
            storage_name, subscriber_name
        );

        if (auto channel_ = channel_id in this)
        {
            auto channel = downcastAssert!(Channel)(*channel_);
            if (subscriber_name !is null)
            {
                if (auto storage = channel.addSubscriber(storage_name))
                {
                    // This happens only if there is a disk overflow channel but
                    // no memory dump file for this subscriber. With a memory
                    // dump file, which is likely to be there, the subscriber
                    // already exists so channel.addSubscriberOnStartup returns
                    // null.
                    log.info("Added storage \"{}\" to channel \"{}\".",
                        storage.storage_name, channel_id);
                }
            }
            else
                enforce!(Channel.AddSubscriberException)(
                    channel.storage_unless_subscribed !is null,
                    cast(istring)("Found disk overflow channel \"" ~
                    storage_name ~ "\", but the channel has a subscriber")
                );
        }
        else
        {
            log.info("Creating channel \"{}\" with storage \"{}\".",
                channel_id, storage_name);
            this.storage_for_create = this.newStorageEngine(storage_name);
            scope (exit) this.storage_for_create = null;
            this.getCreate(channel_id);
        }
    }

    /***************************************************************************

        Creates data directory.

        Params:
            dir = directory to initialize; set to null to use the
                current working directory

    ***************************************************************************/

    private void createWorkingDir ( FilePath path )
    {
        try
        {
            path.createFolder();
        }
        catch (Exception e)
        {
            e.msg = typeof(this).stringof ~ ": Failed creating directory: " ~ e.msg;

            throw e;
        }
    }


    /***************************************************************************

        Validates the name of a queue dump file: Only ASCII alphanumeric
        characters, '-', '_' and '@' are allowed. '@' may appear only once and
        not as the first or last character.

        Params:
            filename = queue dump file name without the ".rq" extension

        Returns:
            true if `filename` is valid or false otherwise.

    ***************************************************************************/

    private static bool validateDumpFileName ( cstring filename )
    {
        if (!filename.length)
            return false;

        bool have_subscriber_separator = false;

        foreach (i, c; filename)
        {
            if (!isalnum(c))
            {
                switch (c)
                {
                    case '_', '-':
                        break;

                    case '@':
                        if ((i < (filename.length - 1)) &&
                            !have_subscriber_separator)
                        {
                            have_subscriber_separator = true;
                            break;
                        }
                        else
                            return false;

                    default:
                        return false;
                }
            }
        }

        return true;
    }
}

version (UnitTest) import ocean.core.Test;

unittest
{
    test(RingNode.validateDumpFileName("helloworld"));
    test(RingNode.validateDumpFileName("hello_world"));
    test(RingNode.validateDumpFileName("hello@world"));
    test(!RingNode.validateDumpFileName("hello.world"));
    test(!RingNode.validateDumpFileName("hello:world"));
    test(!RingNode.validateDumpFileName("hello@wor@ld"));
    test(RingNode.validateDumpFileName("@world"));
    test(!RingNode.validateDumpFileName("@hello@world"));
    test(RingNode.validateDumpFileName("_world"));
    test(!RingNode.validateDumpFileName("hello@"));
    test(!RingNode.validateDumpFileName("@"));
    test(!RingNode.validateDumpFileName(""));
}
