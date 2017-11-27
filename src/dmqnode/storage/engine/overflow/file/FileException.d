/*******************************************************************************

    A thin wrapper around basic POSIX file functionality with convenience
    extensions.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.storage.engine.overflow.file.FileException;

import ocean.sys.ErrnoException;

class FileException: ErrnoException
{
    import ocean.stdc.string: memmove;
    import ocean.transition;

    /***************************************************************************

        The name of the file where a failed operation resulted in throwing this
        instance.

    ***************************************************************************/

    public Immut!(char[]) filename;

    /***************************************************************************

        Constructor.

        Params:
            filename = the name of the file where a failed operation resulted in
                       throwing this instance.

    ***************************************************************************/

    public this ( istring filename )
    {
        this.filename = filename;
    }

    /**************************************************************************

        Calls super.set() to render the error message, then prepends
        this.filename ~ " - " to it.

        Params:
            err_num = error number with same value set as in errno
            name = extern function name that is expected to set errno, optional

        Returns:
            this

     **************************************************************************/

    override public typeof(this) set ( int err_num, istring name,
                                       istring file = __FILE__, int line = __LINE__ )
    {
        super.set(err_num, name, file, line);

        if (this.filename.length)
        {
            this.append(" - ").append(this.filename);
        }

        return this;
    }
}
