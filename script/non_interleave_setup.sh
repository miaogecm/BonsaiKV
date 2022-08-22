#!/bin/bash

umount /mnt/ext4/*
dmsetup remove numa-interleaved-set -f
dmsetup remove interleaved-set0 -f
dmsetup remove interleaved-set1 -f

./pmem_setup.sh

chown -R sdp /mnt/ext4/*
chgrp -R sdp /mnt/ext4/*
