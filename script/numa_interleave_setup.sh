#!/bin/bash

./dimm_interleave_setup.sh

echo -e "0 $(( `blockdev --getsz /dev/mapper/interleaved-set0` * 2 )) striped 2 2048 /dev/mapper/interleaved-set0 0 /dev/mapper/interleaved-set1 0" | sudo dmsetup create --force numa-interleaved-set
