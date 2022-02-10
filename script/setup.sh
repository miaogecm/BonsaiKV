#!/bin/bash

mkfs.ext4 /dev/pmem0

mount -t ext4 -o dax /dev/pmem0 /mnt/ext3

mkdir /mnt/ext3/node0

ulimit -c unlimited

echo "$PWD/../index/core" > /proc/sys/kernel/core_pattern
