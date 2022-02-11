#!/bin/bash

sudo rm -f ./core*

sudo rm -rf /mnt/ext3/*

sudo umount /dev/pmem0

sudo mkfs.ext4 /dev/pmem0

sudo mount -t ext4 -o dax /dev/pmem0 /mnt/ext3

sudo mkdir /mnt/ext3/node0