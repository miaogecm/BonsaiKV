#!/bin/bash

mkfs.ext4 /dev/pmem0

mount -t ext4 -o dax /dev/pmem0 /mnt/ext4

mkdir /mnt/ext4/node0

ulimit -c unlimited

echo "$PWD/./index/core" > /proc/sys/kernel/core_pattern
