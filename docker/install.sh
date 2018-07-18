#!/bin/sh
set -eu

# Install dependencies

apt update
apt install -y libebtree6

# Prepare folder structure and install dmqnode

mkdir -p /srv/dmqnode/dmqnode-0/log
apt install -y /packages/*.deb
