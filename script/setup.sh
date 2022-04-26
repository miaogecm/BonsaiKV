#!/bin/bash

mkfs.ext4 /dev/pmem0
mount -t ext4 -o dax /dev/pmem0 /mnt/ext4/dimm0

mkfs.ext4 /dev/pmem1
mount -t ext4 -o dax /dev/pmem1 /mnt/ext4/dimm1

mkfs.ext4 /dev/pmem2
mount -t ext4 -o dax /dev/pmem2 /mnt/ext4/dimm2

mkfs.ext4 /dev/pmem3
mount -t ext4 -o dax /dev/pmem3 /mnt/ext4/dimm3

mkfs.ext4 /dev/pmem4
mount -t ext4 -o dax /dev/pmem4 /mnt/ext4/dimm4

mkfs.ext4 /dev/pmem5
mount -t ext4 -o dax /dev/pmem5 /mnt/ext4/dimm5

mkdir /mnt/ext4/node0

ulimit -c unlimited

echo "$PWD/../index/masstree/core" > /proc/sys/kernel/core_pattern
