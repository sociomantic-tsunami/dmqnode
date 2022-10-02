/*******************************************************************************

    Server config class for use with ocean.util.config.ClassFiller.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.config.ServerConfig;


import ConfigReader = ocean.util.config.ConfigFiller;


/*******************************************************************************

    Server config values

*******************************************************************************/

public class ServerConfig
{
    ConfigReader.Required!(char[]) address;

    ConfigReader.Required!(ushort) port;
    ConfigReader.Required!(ushort) neoport;

    // CPU index counting from 0; negative: use any CPU
    ConfigReader.Min!(int, -1) cpu;

    ulong size_limit = 0; // 0 := no global size limit

    ConfigReader.Required!(ConfigReader.Min!(ulong, 1)) channel_size_limit;

    string data_dir = "data";

    uint connection_limit = 5000;

    uint backlog = 2048;

    ConfigReader.Required!(char[]) unix_socket_path;
}
