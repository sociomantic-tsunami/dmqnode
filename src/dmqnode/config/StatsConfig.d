/*******************************************************************************

    Stats config class for use with ocean.util.config.ClassFiller.

    copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.config.StatsConfig;


import ocean.util.log.Stats;

/*******************************************************************************

    Stats logging config values

*******************************************************************************/

public class StatsConfig: StatsLog.Config
{
    bool console_stats_enabled = false;
}
