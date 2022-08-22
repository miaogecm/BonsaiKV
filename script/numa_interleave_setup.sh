#!/bin/bash

./dimm_interleave_setup.sh

umount /mnt/ext4/*

echo -e "0 $(( `blockdev --getsz /dev/mapper/interleaved-set0` * 2 )) striped 2 2048 /dev/mapper/interleaved-set0 0 /dev/mapper/interleaved-set1 0" | sudo dmsetup create --force numa-interleaved-set

mkfs.ext4 /dev/mapper/numa-interleaved-set

mount -t ext4 -o dax /dev/mapper/numa-interleaved-set /mnt/ext4/dimm0

chown -R sdp /mnt/ext4/*
chgrp -R sdp /mnt/ext4/*
