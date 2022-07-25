#!/bin/bash

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   exit 1
fi

for dimm in 0 1 2 3
do
  mount -t ext4 -o dax /dev/pmem${dimm} /mnt/ext4/dimm${dimm}
  truncate -s 10485760 /mnt/ext4/dimm${dimm}/nvm_perf_test
done

chown -R deadpool /mnt/ext4
chgrp -R deadpool /mnt/ext4
