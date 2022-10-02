/*******************************************************************************

    Test runner for loading memory dump and disk overflow files.

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.loadfiles.main;

import integrationtest.loadfiles.cases.LoadFiles;
import turtle.runner.Runner;
import ocean.util.log.Logger;

/*******************************************************************************

    Test runner which spawns a real DMQ node to run tests on.

*******************************************************************************/

private class LoadFilesTestRunner : TurtleRunnerTask!(TestedAppKind.Daemon)
{
    /***************************************************************************

        Copies the DMQ node's configuration and data files to the sandbox before
        starting the node.

    ***************************************************************************/

    override public CopyFileEntry[] copyFiles ( )
    {
        return [
            CopyFileEntry("integrationtest/loadfiles/etc/config.ini", "etc/config.ini"),
            CopyFileEntry("integrationtest/loadfiles/etc/credentials", "etc/credentials"),
            CopyFileEntry("integrationtest/loadfiles/data/", "./")
        ];
    }

    public this()
    {
        this.test_package = "integrationtest.loadfiles.cases";
    }

    /***************************************************************************

        Print only warnings and errors on the console, not trace and info
        messages.

    ***************************************************************************/

    override public void prepare ( )
    {
        auto app_log = Log.lookup(this.config.name);
        app_log.level(Level.Warn);
    }

    /***************************************************************************

        No arguments but add small startup delay to let DMQ node initialize
        listening socket.

    ***************************************************************************/

    override protected void configureTestedApplication ( out double delay,
        out string[] args, out string[string] env )
    {
        delay = 1.0;
        args  = null;
        env   = null;
        args = ["--config=etc/config.ini"];
    }
}

/*******************************************************************************

    Main function. Forwards arguments to test runner.

*******************************************************************************/

version (UnitTest) {} else
int main ( string[] args )
{
    return (new TurtleRunner!(LoadFilesTestRunner)("dmqnode",
        "integrationtest.loadfiles.cases")).main(args);
}
