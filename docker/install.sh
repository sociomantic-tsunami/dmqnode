#!/bin/sh
set -eu

# Install dependencies

apt update
apt install -y libebtree6

# Prepare folder structure and install dmqnode

mkdir /etc/dmqnode
mkdir -p /srv/dmqnode/dmqnode-0/log
ln -s /etc/dmqnode /srv/dmqnode/dmqnode-0/etc

apt install -y /packages/*.deb
