/*******************************************************************************

    DMQ shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqnode.connection.neo.SharedResources;

import ocean.transition;

/*******************************************************************************

    Resources owned by the node which are needed by the request handlers.

*******************************************************************************/

public final class SharedResources
{
    import dmqnode.storage.model.StorageChannels;

    import dmqproto.node.neo.request.core.IRequestResources;

    import ocean.util.container.pool.FreeList;

    /***************************************************************************

        Pool of buffers to store record values in. (We store ubyte[] buffers
        internally, as a workaround for ambiguities in ocean.core.Buffer because
        void[][] can be implicitly cast to void[].)

    ***************************************************************************/

    private FreeList!(ubyte[]) value_buffers;

    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;

    /***************************************************************************

        Constructor.

        Params:
            storage_channels = storage channels which the requests are operating
                on

    ***************************************************************************/

    public this ( StorageChannels storage_channels )
    {
        this.storage_channels = storage_channels;

        this.value_buffers = new FreeList!(ubyte[]);
    }

    /***************************************************************************

        Scope class which may be newed inside request handlers to get access to
        the shared pools of resources. Any acquired resources are relinquished
        in the destructor.

        The class should always be newed as scope, but cannot be declared as
        such because the request handler classes need to store a reference to it
        as a member, which is disallowed for scope instances.

    ***************************************************************************/

    public /*scope*/ class RequestResources : IRequestResources
    {
        /***********************************************************************

            Buffer of acquired buffers. The getVoidBuffer() method may be called
            multiple times by the request handler to acquire multiple buffers.
            But this class may not allocate, so we cannot simple have a void[][]
            as a member and append acquired buffers to it (doing so would
            allocate). Thus, in order to avoid any heap allocations in this
            class, we also acquire the buffer (in which the set of acquired
            buffers are stored) from the pool maintained by the outer instance.

            This field will be null, if the main buffer has not been acquired.

        ***********************************************************************/

        private void[][] acquired_values_buffer;

        /***********************************************************************

            Destructor. Relinquishes any acquired resources back to the shared
            resource pools.

        ***********************************************************************/

        ~this ( )
        {
            if ( this.acquired_values_buffer )
            {
                foreach ( ref buffer; this.acquired_values_buffer )
                    this.outer.value_buffers.recycle(cast(ubyte[])buffer);

                this.outer.value_buffers.recycle(
                    cast(ubyte[])this.acquired_values_buffer);
            }
        }

        /***********************************************************************

            Returns:
                the node's storage channels

        ***********************************************************************/

        public StorageChannels storage_channels ( )
        {
            return this.outer.storage_channels;
        }

        /***********************************************************************

            Returns:
                a pointer to a new chunk of memory (a void[]) to use during the
                request's lifetime

        ***********************************************************************/

        override public void[]* getVoidBuffer ( )
        {
            void[] newBuffer ( size_t len )
            {
                auto buffer =
                    this.outer.value_buffers.get(cast(ubyte[])new void[len]);
                buffer.length = 0;
                enableStomping(buffer);

                return buffer;
            }

            // Acquire main buffer, if not already done
            if ( this.acquired_values_buffer is null )
            {
                this.acquired_values_buffer =
                    cast(void[][])newBuffer((void[]).sizeof * 4);
            }

            // Acquire and re-initialise new buffer to return to the user. Store
            // it in the array of acquired buffers
            this.acquired_values_buffer ~= newBuffer(16);

            return &this.acquired_values_buffer[$-1];
        }
    }
}
