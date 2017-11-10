#!/bin/sh -e
exec 2>&1
exec /opt/bendo/bin/bendo -config-file /opt/bendo/config
