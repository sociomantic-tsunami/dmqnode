#!/bin/sh

# Exits with an error message
error_exit()
{
    msg="$1"
    code="$2"
    echo "$msg" 1>&2
    exit "$code"
}

APP="dmqnode"

if [ "$1" = "configure" ]; then
    adduser --system --group --no-create-home ${APP}

    # Check that deployment directory exists
    test -d /srv/dmqnode/dmqnode-* || error_exit "/srv/dmqnode/dmqnode-* directories missing" 1

    # Create directory structure if missing and ensure proper permissions.
    for FOLDER in $(find /srv/dmqnode -type d -name "dmqnode-[0-9]*")
    do
        install --owner=${APP} --group=${APP} -d $FOLDER/data
        install --owner=${APP} --group=${APP} -d $FOLDER/etc
        # Only the user should be able to write to the log directory,
        # otherwise logrotate will complain...
        install --owner=${APP} --group=${APP} --mode="u=rwx,g=rx,o=rx" -d $FOLDER/log
    done
fi
