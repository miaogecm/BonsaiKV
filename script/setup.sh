#!/bin/bash

mkfs.ext4 /dev/pmem0

mount -t ext4 -o dax /dev/pmem0 /mnt/ext4
