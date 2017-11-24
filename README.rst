Description
===========

The dmq node is a server which handles requests from the dmq client defined
in swarm (``dmqproto.client.DmqClient``). One or more nodes make up a complete
dmq, though only the client has this knowledge -- individual nodes know
nothing of each others' existence.

Data in the dmq node is stored in memory, in fixed-sized, pre-allocated
buffers, one per data channel.

Deployment
==========

Upstart
-------

The dmq node is configured to use upstart and will start automatically upon
server reboot. The upstart scripts are located in ``deploy/upstart/dmq.conf``.

Manually
--------

To manually start the dmq node on a server, run ``sudo service dmq start``.
This will start the screen session and the application. If the application/
screen session are already running, you'll need to shut them down first before
restarting.

Screen
------

The dmqnode node runs as dmqnode user in a screen session.

Processes
---------

There should be a directory in ``/srv/dmqnode/dmqnode-n`` for each instance of
the dmqnode node, like ``/srv/dmqnode/dmqnode-1``. Each directory should contain a
``dmqnode`` binary.

Design
======

The structure of the dmq node's code is based very closely around the
structure of the ``core.node`` package of swarm.

The basic components are:

Select Listener
  The ``swarm.node.model.Node : NodeBase`` class, which forms the
  foundation of all swarm nodes, owns an instance of
  ``ocean.net.server.SelectListener : SelectListener``. This provides the basic
  functionality of a server; that is, a listening socket which will accept
  incoming client connections. Each client connection is assigned to a
  connection handler instance from a pool.

Connection Handler Pool
  The select listener manages a pool of connection handlers (derived from
  ``swarm.node.connection.ConnectionHandler : ConnectionHandlerTemplate``.
  Each is associated with an incoming socket connection from a client. The
  connection handler reads a request code from the socket and then passes the
  request on to a request handler instance, which is constructed at scope (i.e.
  only exists for the lifetime of the request).

Request Handlers
  A handler class exists for each type of request which the node can handle.
  These are derived from ``swarm.node.request.model.IRequest : IRequest``.
  The request handler performs all communication with the client which is
  required by the protocol for the given request. This usually involves
  interacting with the node's storage channels.

Storage Channels
  The ``swarm.node.storage.model.IStorageChannels : IStorageChannelsTemplate``
  class provides the base for a set of storage channels, where each channel is
  conceived as storing a different type of data in the system. The individual
  storage channels are derived from
  ``swarm.node.storage.model.IStorageEngine : IStorageEngine``.

Monitoring
==========

Resource Usage
--------------

A dmq node process typically uses non-negligible amount of a CPU usage (depending on
traffic), and a large chunk of RAM -- the config file defines the amount of
memory which is allocated for each channel stored, so the memory usage should be
in the region of <num channels * channel size>.

Checking Everything's OK
------------------------

Console Output
..............

The dmq node displays some basic statistics on the console: its memory usage,
the number of open connections and handled records, the number of records and
bytes stored, and the fullness (as a percentage) of each channel.

Log Files
.........

The dmq node writes two log files:

``root.log``
  Notification of errors when handling requests.

``stats.log``
  Statistics about the number of records and bytes stored (globally and per
  channel), the number of bytes sent and received over the network, and the
  number of open connections and records handled.

Possible Problems
-----------------

Crash
.....

If a dmq node crashes, it can simply be restarted.

Design
======

See section on overall design of the swarm nodes.

Data Flow
=========

Dmq nodes do not access any other data stores.
