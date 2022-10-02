/*******************************************************************************

    A test case that verifies the content of the DMQ node which has loaded the
    prepared queue dump and disk overflow files.

    The test uses the following queue dump files and disk overflow channels
    where each dump file and each overflow channel contains one record:

    Channel     Dump file(s)        Overflow channel(s)
    ch1         `@ch1.rq`           `@ch1`
                `sub1@ch1.rq`       `sub1@ch1`
                `sub2@ch1.rq`       `sub2@ch1`
    ch2         `ch2.rq`            `ch2`
    ch3         `@ch3.rq`
                                    `sub1@ch3`
    ch4         `ch4`
    ch5                             `@ch5`

    This results in the following DMQ node channels and subscribers:
      - Channel "ch1": Subscribers "", "sub1", "sub2"; two records each.
      - Channel "ch2": No subscriber, two records.
      - Channel "ch3": Subscribers "", "sub1", one record each.
      - Channel "ch4": No subscriber, one record.
      - Channel "ch5": Subscriber "", one record.

    The records in each channel are composed from the channel name as
    `"Hello " ~ channel_name ~ "!"`.

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.loadfiles.cases.LoadFiles;

import turtle.TestCase;

/// ditto
class LoadFiles: TestCase
{
    import integrationtest.loadfiles.cases.checker.CheckedRequests;
    import dmqproto.client.DmqClient;
    import swarm.neo.authentication.HmacDef: Key;
    import ocean.task.Scheduler;
    import ocean.task.Task;
    import Test = ocean.core.Test;
    import ocean.core.TypeConvert : assumeUnique;
    import ocean.meta.types.Qualifiers : cstring, mstring;
    import ocean.util.log.Logger;

    /***************************************************************************

        DMQ client used for the test.

    ***************************************************************************/

    DmqClient dmq;

    /***************************************************************************

        Constructor; connects to the DMQ node.

    ***************************************************************************/

    this ( )
    {
        auto connect = new Connect;
        this.dmq = new DmqClient(
            theScheduler.epoll, "test", Key.init.content, &connect.notifier
        );

        dmq.neo.addNode("127.0.0.1", 11001);
        connect.connect(dmq);
    }

    /***************************************************************************

        Runs the test.

    ***************************************************************************/

    override void run ( )
    {
        // For each channel with subscribers start a Consume request for each
        // subscriber.
        Consume[] consumes =
        [
            new Consume(this.dmq, "ch1", "",     2, "Hello ch1!"),
            new Consume(this.dmq, "ch1", "sub1", 2, "Hello ch1!"),
            new Consume(this.dmq, "ch1", "sub2", 2, "Hello ch1!"),
            new Consume(this.dmq, "ch3", "",     1, "Hello ch3!"),
            new Consume(this.dmq, "ch3", "sub1", 1, "Hello ch3!"),
            new Consume(this.dmq, "ch5", "",     1, "Hello ch5!")
        ];

        // Start as many Pop requests as records are expected in each channel
        // subscribers.
        Pop[] pops =
        [
            new Pop(this.dmq, "ch2",     2, "Hello ch2!"),
            new Pop(this.dmq, "ch4",     1, "Hello ch4!")
        ];

        foreach (consume; consumes)
            while (!consume.finished)
                Task.getThis.suspend();

        foreach (pop; pops)
            while (!pop.finished)
                Task.getThis.suspend();

        // Check for unexpected notifications for any of the requests.
        foreach (consume; consumes)
            consume.checkNotification();
        foreach (pop; pops)
            pop.checkNotification();

        // Stop the Consume requests.
        foreach (consume; consumes)
            consume.stop();

        // Start another pop requests for each channel without subscribers to
        // see if the channel is now empty.
        PopEmpty[] pop_emptys =
        [
            new PopEmpty(this.dmq, "ch2"),
            new PopEmpty(this.dmq, "ch4")
        ];

        foreach (pop_empty; pop_emptys)
            while (!pop_empty.finished)
                Task.getThis.suspend();

        foreach (consume; consumes)
            while (!consume.stopped)
                Task.getThis.suspend();

        this.dmq.neo.shutdown();

        // Verify that the channels we popped from are now empty and there were
        // no unexpected notifications.
        foreach (pop_empty; pop_emptys)
            pop_empty.checkNotification();

        // Verify the number and contents of the records received via Consume.
        foreach (consume; consumes)
            consume.checkRecords();

        // Verify the number and contents of the records received via Pop.
        foreach (pop; pops)
            pop.checkRecords();
    }

    /***************************************************************************

        Establishes a connection with each node.

    ***************************************************************************/

    struct Connect
    {
        import ocean.task.Task;

        /***********************************************************************

            The task to suspend until connections to all nodes have been
            established.

        ***********************************************************************/

        Task task;

        /***********************************************************************

            The message of the last unexpected (i.e. not `connected`)
            notification.

        ***********************************************************************/

        string errmsg;

        /***********************************************************************

            Connects `dmq` to the node, suspending the current task until
            connections to all nodes have been established. `dmq` needs to have
            been constructed using the `notifier` method of this instance.

            Params:
                dmq = the DMQ client to connect to the node using
                      `this.notifier`.

        ***********************************************************************/

        void connect ( DmqClient dmq )
        {
            this.task = Task.getThis();
            assert(this.task);

            scope stats = dmq.neo.new Stats;
            do
            {
                this.task.suspend();
                if (this.errmsg.length)
                    throw new Exception(this.errmsg, __FILE__, __LINE__);
            }
            while (stats.num_connected_nodes < stats.num_registered_nodes);
        }

        /***********************************************************************

            Connection notifier passed to the client as a delegate.

            Params:
                info = notification information

        ***********************************************************************/

        void notifier ( DmqClient.Neo.ConnNotification info )
        {
            with (info) switch (active)
            {
                case active.connected:
                    Log.lookup("loadfiles")
                        .info("Connection established (on {}:{})",
                        connected.node_addr.address_bytes,
                        connected.node_addr.port);
                    break;

                case active.error_while_connecting:
                    mstring errmsg;
                    error_while_connecting.toString(
                        (cstring chunk) {errmsg ~= chunk;}
                    );
                    this.errmsg = assumeUnique(errmsg);
                    break;

                default: assert(false);
            }

            if (this.task.suspended)
                this.task.resume();
        }
    }
}
