#!/bin/bash

umount /mnt/ext4/*

NR_DIMM=12
PMEMS=(/dev/pmem0 /dev/pmem1 /dev/pmem2 /dev/pmem3 /dev/pmem4 /dev/pmem5 /dev/pmem6 /dev/pmem7 /dev/pmem8 /dev/pmem9 /dev/pmem10 /dev/pmem11)

for i in $(seq 0 $((NR_DIMM - 1))); do
  mkdir -p /mnt/ext4/dimm$i
  mkfs.ext4 ${PMEMS[$i]}
  mount -t ext4 -o dax ${PMEMS[$i]} /mnt/ext4/dimm$i
done

chown -R sdp /mnt/ext4/*
chgrp -R sdp /mnt/ext4/*
