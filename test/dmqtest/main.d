/*******************************************************************************

    DMQ node test runner

    Imports the DMQ test from dmqproto and runs it on the real DMQ node.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.main;


import dmqtest.TestRunner;
import turtle.runner.Runner;

import ocean.transition;

/*******************************************************************************

    Test runner which spawns a real DMQ node to run tests on.

*******************************************************************************/

private class RealDmqTestRunner : DmqTestRunner
{
    /***************************************************************************

        Copies the DMQ node's config file to the sandbox before starting the
        node.

    ***************************************************************************/

    override public CopyFileEntry[] copyFiles ( )
    {
        return [
            CopyFileEntry("test/dmqtest/etc/config.ini", "etc/config.ini"),
            CopyFileEntry("test/dmqtest/etc/credentials", "etc/credentials")
        ];
    }

    /***************************************************************************

        Override the super class' method to specify the dhtnode's required
        arguments.

    ***************************************************************************/

    override protected void configureTestedApplication ( out double delay,
        out istring[] args, out istring[istring] env )
    {
        super.configureTestedApplication(delay, args, env);
        args = ["--config=etc/config.ini"];
    }
}

/*******************************************************************************

    Main function. Forwards arguments to test runner.

*******************************************************************************/

int main ( istring[] args )
{
    return (new TurtleRunner!(RealDmqTestRunner)("dmqnode", "dmqtest.cases"))
        .main(args);
}
