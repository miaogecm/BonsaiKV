#!/bin/bash

NR_DIMM=12
DIMM_SIZE=2G

for i in $(seq 0 $((NR_DIMM - 1))); do
  mkdir -p /mnt/ext4/dimm$i
  mount -t tmpfs -o size=$DIMM_SIZE tmpfs /mnt/ext4/dimm$i
done

ulimit -c unlimited

echo "$PWD/../index/masstree/core" > /proc/sys/kernel/core_pattern
