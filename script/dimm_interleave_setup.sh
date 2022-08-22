#!/bin/bash

umount /mnt/ext4/*
dmsetup remove numa-interleaved-set -f
dmsetup remove interleaved-set0 -f
dmsetup remove interleaved-set1 -f

echo -e "0 $(( `blockdev --getsz /dev/pmem0` * 6 )) striped 6 8 /dev/pmem0 0 /dev/pmem1 0 /dev/pmem2 0 /dev/pmem3 0 /dev/pmem4 0 /dev/pmem5 0" | sudo dmsetup create --force interleaved-set0
echo -e "0 $(( `blockdev --getsz /dev/pmem6` * 6 )) striped 6 8 /dev/pmem6 0 /dev/pmem7 0 /dev/pmem8 0 /dev/pmem9 0 /dev/pmem10 0 /dev/pmem11 0" | sudo dmsetup create --force interleaved-set1

mkfs.ext4 /dev/mapper/interleaved-set0
mkfs.ext4 /dev/mapper/interleaved-set1

mount -t ext4 -o dax /dev/mapper/interleaved-set0 /mnt/ext4/dimm0
mount -t ext4 -o dax /dev/mapper/interleaved-set1 /mnt/ext4/dimm1

chown -R sdp /mnt/ext4/*
chgrp -R sdp /mnt/ext4/*
