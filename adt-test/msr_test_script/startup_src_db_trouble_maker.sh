#!/bin/bash

set -o xtrace

echo ifdown
sudo /sbin/ifdown eth0

sleep 5

echo my-down
sudo /etc/init.d/mysql restart

sleep 5

echo ifup
sudo /sbin/ifup eth0

echo Done
