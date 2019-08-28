/*******************************************************************************

    Distributed Message Queue Node Server

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.main;

import Version;

import dmqnode.config.ChannelSizeConfig;
import dmqnode.config.OverflowConfig;
import dmqnode.config.PerformanceConfig;
import dmqnode.config.ServerConfig;
import dmqnode.config.StatsConfig;
import dmqnode.node.ChannelStats;
import dmqnode.node.DmqNode;
import dmqnode.storage.Ring;

import swarm.node.model.ISwarmConnectionHandlerInfo;
import dmqproto.client.legacy.DmqConst;

import swarm.util.node.log.Stats;

import ocean.core.ExceptionDefinitions : OutOfMemoryException;
import ocean.core.MessageFiber;
import ocean.io.select.client.model.ISelectClient;
import ocean.io.select.EpollSelectDispatcher;
import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;
import ocean.io.select.selector.EpollException;
import core.sys.posix.signal: SIGINT, SIGTERM, SIGQUIT;
import ocean.sys.CpuAffinity;
import ocean.util.app.DaemonApp;
import ConfigReader = ocean.util.config.ConfigFiller;
import ocean.util.log.Logger;
import ocean.transition;

import ocean.transition;

/*******************************************************************************

    Setup the logger for this module

*******************************************************************************/

static Logger log;
static this ( )
{
    log = Log.lookup("dmqnode.main");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts the DMQ node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

version (UnitTest) {} else
private int main ( istring[] cl_args )
{
    auto app = new DmqNodeServer;
    return app.main(cl_args);
}



/*******************************************************************************

    DMQ Node Server

*******************************************************************************/

public class DmqNodeServer : DaemonApp
{
    import swarm.neo.authentication.Credentials;

    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        DMQ node instance

     **************************************************************************/

    private DmqNode node;


    /***************************************************************************

        Logger for the node and basic per-channel stats. The additional
        per-channel stats in ChannelStats are logged separately.

    ***************************************************************************/

    private ChannelsNodeStats dmq_stats;


    /***************************************************************************

        Instances of each config class to be read.

    ***************************************************************************/

    private ServerConfig server_config;
    private PerformanceConfig performance_config;
    private StatsConfig stats_config;
    private OverflowConfig overflow_config;
    private ChannelSizeConfig channel_size_config;


    /***************************************************************************

        The signal codes that request a shutdown.

    ***************************************************************************/

    private static immutable shutdown_signals = [SIGINT, SIGTERM, SIGQUIT];

    /***************************************************************************

         Constructor

    ***************************************************************************/

    public this ( )
    {
        static immutable app_name = "dmqnode";
        static immutable app_desc = "dmqnode: distributed message queue server node.";

        DaemonApp.OptionalSettings settings;
        settings.signals = shutdown_signals.dup;
        this.epoll = new EpollSelectDispatcher;

        super(app_name, app_desc, version_info, settings);
    }


    /***************************************************************************

        Get values from the configuration file.

    ***************************************************************************/

    public override void processConfig ( IApplication app, ConfigParser config )
    {
        ConfigReader.fill("Stats", this.stats_config, config);
        ConfigReader.fill("Server", this.server_config, config);
        ConfigReader.fill("Performance", this.performance_config, config);
        ConfigReader.fill("Overflow", this.overflow_config, config);

        this.channel_size_config.default_size_limit = this.server_config.channel_size_limit();

        foreach (key; config.iterateCategory("ChannelSizeById"))
        {
            this.channel_size_config.add(key, config.getStrict!(ulong)("ChannelSizeById", key));
        }
    }

    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    override protected int run ( Arguments args, ConfigParser config )
    {
        auto cpu = server_config.cpu();

        if (cpu >= 0)
        {
            CpuAffinity.set(cast(uint)cpu);
        }

        this.startEventHandling(this.epoll);

        this.node = new DmqNode(this.server_config, this.channel_size_config,
            epoll, this.performance_config.no_delay);

        this.node.error_callback = &this.nodeError;
        this.node.connection_limit = this.server_config.connection_limit;

        this.dmq_stats = new ChannelsNodeStats(this.node, this.stats_ext.stats_log);

        // This needs to be done after `startEventHandling` has been called
        // because `startEventHandling` creates `this.timer_ext`.
        this.timer_ext.register(&this.flushNode,
            this.performance_config.write_flush_ms / 1000.0);
        this.timer_ext.register(&this.flushNode,
            this.overflow_config.write_index_ms / 1000.0);

        this.node.register(this.epoll);
        this.epoll.eventLoop();
        return 0;
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the error.log file, and optionally to the console (if the
        Log/console_echo_errors config parameter is true).

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred

    ***************************************************************************/

    private void nodeError ( Exception exception, IAdvancedSelectClient.Event event_info,
        ISwarmConnectionHandlerInfo.IConnectionHandlerInfo conn )
    {
        if ( cast(MessageFiber.KilledException)exception ||
             cast(IOWarning)exception ||
             cast(EpollException)exception )
        {
            // Don't log these exception types, which only occur on the normal
            // disconnection of a client.
        }
        else if ( cast(OutOfMemoryException)exception )
        {
            log.error("OutOfMemoryException caught in eventLoop");
        }
        else
        {
            log.error("Exception caught in eventLoop: '{}' @ {}:{}",
                    getMsg(exception), exception.file, exception.line);
        }
    }


     /***************************************************************************

        Override default DaemonApp arguments parsing, specifying that --config
        is required.

        Params:
            app = application instance
            args = arguments parser instance

    ***************************************************************************/

    override public void setupArgs ( IApplication app, Arguments args )
    {
        super.setupArgs(app, args);
        args("config").required;
    }

    /***************************************************************************

        Signal handler.

        Firstly unregisters all periodics. (Any periodics which are about to
        fire in epoll will still fire, but the setting of the 'terminating' flag
        will stop them from doing anything.)

        Secondly calls the node's shutdown method. This unregisters the select
        listener (stopping any more requests from being processed), then shuts
        down the storage channels.

        Finally shuts down epoll. This will result in the run() method, above,
        returning.

        Params:
            siginfo = info struct about signal which fired

    ***************************************************************************/

    override public void onSignal ( int signal )
    {
        foreach (s; shutdown_signals)
        {
            if (signal == s)
            {
                this.node.stopListener(this.epoll);
                this.node.shutdown;

                this.epoll.shutdown;
                break;
            }
        }
    }


    /**************************************************************************

        Writes the stats to the log.

    ***************************************************************************/

    override protected void onStatsTimer ( )
    {
        this.reportSystemStats();
        this.dmq_stats.log();

        auto stats_log = this.stats_ext.stats_log;

        foreach (channel; this.node)
        {
            stats_log.addObject!("channel")(channel.id, ChannelStats.set(channel));
        }

        stats_log.add(Log.stats);
        stats_log.flush();
        this.node.resetCounters();
    }

    /***************************************************************************

        TimerExt callback to send pending output data.

        Returns:
            always true to stay registered with TimerExt

    ***************************************************************************/

    private bool flushNode ( )
    {
        this.node.flush();
        return true;
    }

    /***************************************************************************

        TimerExt callback to write the disk overflow index to disk.

        Returns:
            always true to stay registered with TimerExt

    ***************************************************************************/

    private bool writeDiskOverflowIndex ( )
    {
        this.node.writeDiskOverflowIndex();
        return true;
    }
}
