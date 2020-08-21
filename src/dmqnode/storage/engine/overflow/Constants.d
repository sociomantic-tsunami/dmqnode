/*******************************************************************************

    Constant definitions.

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.Constants;

struct Constants
{
    import ocean.meta.types.Qualifiers : istring;

    /***************************************************************************

        File names and suffices.

    ***************************************************************************/

    enum istring
        datafile_suffix  = ".dat",
        datafile_name    = "overflow" ~ datafile_suffix,
        indexfile_suffix = ".csv",
        indexfile_name   = "ofchannels" ~ indexfile_suffix;

    /***************************************************************************

        A magic string at the beginning of the data file. It may be used as a
        data file version tag.

    ***************************************************************************/

    static immutable(char[8]) datafile_id = "QDSKOF01";
}
