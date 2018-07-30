/*******************************************************************************

    Disk overflow config class for use with ocean.util.config.ClassFiller.

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.config.OverflowConfig;



/*******************************************************************************

    Overflow config values

*******************************************************************************/

public class OverflowConfig
{
    uint write_index_ms = 60_000;
}

