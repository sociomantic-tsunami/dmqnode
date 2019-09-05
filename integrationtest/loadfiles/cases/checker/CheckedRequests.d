/*******************************************************************************

    Helper classes to run Consume or Pop DMQ requests, receiving all records for
    a subscriber (Consume) or in a channel (Pop) and comparing the number of the
    received records and their contents with expected values.

    copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.loadfiles.cases.checker.CheckedRequests;

import dmqproto.client.DmqClient;
import ocean.task.Task;
import ocean.core.Traits;
import ocean.core.TypeConvert : assumeUnique;
import ocean.meta.types.Qualifiers : Const, cstring, Immut, istring;

/*******************************************************************************

    Base class for a Consume or Pop request expecting to receive a certain
    number of records, all with the same content. This class contains a facility
    to check if the received records match the expected number and content.

*******************************************************************************/

abstract class RecordChecker
{
    import Test = ocean.core.Test;

    /***************************************************************************

        Set to `true` when `n_expected_records` have been received.

    ***************************************************************************/

    public bool finished = false;

    /***************************************************************************

        The number of expected records.

    ***************************************************************************/

    public uint n_expected_records;

    /***************************************************************************

        The expected content of the records.

    ***************************************************************************/

    public istring expected_record_content;

    /***************************************************************************

        Named test used in `check`.

    ***************************************************************************/

    private istring testname;

    /***************************************************************************

        The received records, populated by the subclass.

    ***************************************************************************/

    private Immut!(char[])[] received_records;

    /***************************************************************************

        The task that should be resumed when `n_expected_records` have been
        received.

    ***************************************************************************/

    protected Task waiting_task;

    /***************************************************************************

        Reports and checks for unexpected notifications.

    ***************************************************************************/

    protected UnexpectedNotification unex_notification;

    /***************************************************************************

        Constructor.

        Params:
            testname = the name of the named test used in `check`
            n_expected_records = the expected number of records
            expected_record_content = the expected record content

    ***************************************************************************/

    protected this ( istring testname, uint n_expected_records,
                     istring expected_record_content )
    {
        this.waiting_task = Task.getThis();
        assert(this.waiting_task);
        this.n_expected_records = n_expected_records;
        this.expected_record_content = expected_record_content;
        this.testname = testname;
        this.unex_notification = new UnexpectedNotification(testname);
    }

    /***************************************************************************

        Runs a named test check if the received records match the expected
        number and content.

    ***************************************************************************/

    public void checkRecords ( )
    {
        auto ntest = new Test.NamedTest(this.testname);
        ntest.test!("==")(this.n_expected_records, this.received_records.length);
        foreach (record; this.received_records)
            ntest.test!("==")(record, this.expected_record_content);
    }

    /***************************************************************************

        Throws an exception if an unexpected notification has been reported.

    ***************************************************************************/

    public void checkNotification ( istring file = __FILE__, int line = __LINE__ )
    {
        this.unex_notification.check(file, line);
    }

    /***************************************************************************

        Appends `record` to the list of received records. Sets `this.finished`
        to `true` and resumes the waiting task if the number of expected records
        has been reached.

        Params:
            record = a received record

    ***************************************************************************/

    protected void received ( in void[] record )
    {
        this.received_records ~= idup(cast(cstring)record);
        if (this.received_records.length == this.n_expected_records)
        {
            this.finished = true;
            if (this.waiting_task.suspended)
                this.waiting_task.resume();
        }
    }
}

/*******************************************************************************

    Helper class to report an unexpected request notification.

*******************************************************************************/

class UnexpectedNotification
{
    import ocean.core.SmartUnion;

    /***************************************************************************

        The name of the test, for the error message.

    ***************************************************************************/

    private istring testname;

    /***************************************************************************

        The error message, set via `report` and used in `check`.

    ***************************************************************************/

    private istring msg;

    /***************************************************************************

        Constructor.

        Params:
            testname = the name of the test, for the error message

    ***************************************************************************/

    public this ( istring testname )
    {
        this.testname = testname;
    }

    /***************************************************************************

        Throws an exception if an unexpected notification has been reported.

    ***************************************************************************/

    public void check ( istring file = __FILE__, int line = __LINE__ )
    {
        if (this.msg.length)
            throw new Exception(this.msg, file, line);
    }

    /***************************************************************************

        Reports an unexpected notification. Calling `check` will throw an
        exception with the notification message.

        Params:
            notification = the unexpected notification to report

    ***************************************************************************/

    public void report (N : SmartUnion!(U), U) ( N notification )
    {
        foreach (i, Field; typeof(U.tupleof))
        {
            if (notification.active ==
                mixin("notification.active." ~ FieldName!(i, U)))
            {
                this.setMsg(FieldName!(i, U),
                    &mixin("notification." ~ FieldName!(i, U)).toString);
            }
        }
    }

    /***************************************************************************

        Sets `this.msg` to say that an unexpected notification has happened,
        mentioning `type` and appending the output of `to_string`.

        Params:
            type = the notification type
            to_string = `&toString` of the active notification

    ***************************************************************************/

    private void setMsg ( cstring type,
        scope void delegate ( void delegate ( cstring chunk ) sink ) to_string )
    {
        auto msg = this.testname;
        msg ~= " - Unexpected notification \"";
        msg ~= type;
        msg ~= "\". ";
        to_string((cstring chunk) {msg ~= chunk;});
        this.msg = assumeUnique(msg);
    }
}

/*******************************************************************************

    Runs a Consume request where a certain number of records with a certain
    content is expected to be received.

*******************************************************************************/

class Consume: RecordChecker
{
    /***************************************************************************

        Set to `true` when receiving a `stopped` notification.

    ***************************************************************************/

    public bool stopped = false;

    /***************************************************************************

        The request ID, used by `stop`.

    ***************************************************************************/

    public ulong id;

    /***************************************************************************

        The DMQ client, used by `stop`.

    ***************************************************************************/

    private DmqClient dmq;

    /***************************************************************************

        Constructor, starts the Consume request. The caller needs to
         - suspend the current task while the `finished` (base class) member is
           `false`,
         - then call `checkNotification` to check if an error has been reported,
         - then call `checkRecords` to check the received records.

        Params:
            dmq = the DMQ client to use
            channel = the channel to consume from
            subscriber = the channel subscriber
            n_expected_records = the expected number of records for the
                subscriber in the channel
            expected_record_content = the expected record content, for `check`

    ***************************************************************************/

    public this ( DmqClient dmq, cstring channel, cstring subscriber,
        uint n_expected_records, istring expected_record_content )
    {
        super(cast(istring)("consume " ~ subscriber ~ "@" ~ channel),
            n_expected_records, expected_record_content);
        this.dmq = dmq;
        this.id = dmq.neo.consume(
            channel, &this.notifier, dmq.neo.Subscriber(subscriber)
        );
    }

    /***************************************************************************

        Stops the request. The caller needs to suspend the running task while
        the `stopped` (base class) member is `false`.

    ***************************************************************************/

    public void stop ( )
    {
        this.dmq.neo.control(this.id,
            (DmqClient.Neo.Consume.IController controller)
            {
                if (controller.stop())
                {
                    this.stopped = true;
                    if (this.waiting_task.suspended)
                        this.waiting_task.resume();
                }
            }
        );
    }

    /***************************************************************************

        Consume notifier.
        If a receiving a record, passes it to the super class, or reports an
        unexpected notification.

    ***************************************************************************/

    private void notifier ( DmqClient.Neo.Consume.Notification info,
                            Const!(DmqClient.Neo.Consume.Args) args )
    {
        with (info) switch (active)
        {
            case active.stopped:
                break;

            case active.received:
                this.received(info.received.value);
                break;

            default:
                this.unex_notification.report(info);
        }
    }
}

/*******************************************************************************

    Runs `n` Pop requests where `n` is the number of records expected to be in
    the channel, and these records are expected to contain a certain string.

********************************************************************************/

class Pop: RecordChecker
{
    /***************************************************************************

        The last notification information that is not `received`.

    ***************************************************************************/

    DmqClient.Neo.Pop.Notification info;

    /***************************************************************************

        Constructor, starts `n_expected_records` Pop requests. The caller needs
        to
         - suspend the current task while the `finished` (base class) member is
           `false`,
         - then call `checkNotification` to check if an error has been reported,
         - then call `checkRecords` to check the received records.

        Params:
            dmq = the DMQ client to use
            channel = the channel to pop from
            n_expected_records = the number of Pop requests to start, which is
                the expected number of records in the channel
            expected_record_content = the expected record content, for `check`

    ***************************************************************************/

    public this ( DmqClient dmq, cstring channel, uint n_expected_records,
        istring expected_record_content )
    {
        super(cast(istring)("pop " ~ channel), n_expected_records,
            expected_record_content);
        for (uint i = 0; i < n_expected_records; i++)
            dmq.neo.pop(channel, &this.notifier);
    }

    /***************************************************************************

        Pop notifier.
        If receiving a record, passes it to the super class, otherwise reports
        an unexpected notification.

    ***************************************************************************/

    private void notifier ( DmqClient.Neo.Pop.Notification info,
                            Const!(DmqClient.Neo.Pop.Args) args )
    {
        with (info) switch (active)
        {
            case active.received:
                this.received(info.received.value);
                break;

            default:
                this.unex_notification.report(info);
        }
    }
}

/*******************************************************************************

    Runs a Pop requests, expecting the notification that the channel is empty.

*******************************************************************************/

class PopEmpty
{
    /***************************************************************************

        Set to `true` when receiving the "channel empty" notification.

    ***************************************************************************/

    public bool finished;

    /***************************************************************************

        The task that should be resumed when the "channel empty" notification
        has been received.

    ***************************************************************************/

    private Task waiting_task;

    /***************************************************************************

        The message of the last unexpected notification. Only "channel empty"
        notifications are expected.

    ***************************************************************************/

    private UnexpectedNotification unex_notification;

    /***************************************************************************

        Constructor, starts one Pop requests. The caller needs to
         - suspend the current task while the `finished` (base class) member is
           `false`,
         - then check if `info` contains a notification, which it does if the
           channel wasn't empty or an error or other unexpected notification has
           been reported.

        Params:
            dmq = the DMQ client to use
            channel = the channel to pop from

    ***************************************************************************/

    public this ( DmqClient dmq, istring channel )
    {
        this.waiting_task = Task.getThis();
        assert(this.waiting_task);
        this.unex_notification =
            new UnexpectedNotification("pop from empty channel " ~ channel);
        dmq.neo.pop(channel, &this.notifier);
    }

    /***************************************************************************

        Pop notifier. Expects a "channel empty" notification and reports any
        other notification as unexpected.

    ***************************************************************************/

    private void notifier ( DmqClient.Neo.Pop.Notification info,
                            Const!(DmqClient.Neo.Pop.Args) args )
    {
        with (info) switch (active)
        {
            case active.empty:
                break;

            default:
                this.unex_notification.report(info);
        }

        this.finished = true;
        if (this.waiting_task.suspended)
            this.waiting_task.resume();
    }

    /***************************************************************************

        Throws an exception if an unexpected, i.e. not "channel empty"
        notification has been reported.

    ***************************************************************************/

    public void checkNotification ( istring file = __FILE__, int line = __LINE__ )
    {
        this.unex_notification.check(file, line);
    }
}
