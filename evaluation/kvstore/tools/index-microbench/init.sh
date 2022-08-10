#!/bin/bash

mkdir -p ./YCSB/generated
mount -t tmpfs -o size=60G tmpfs ./YCSB/generated

mkdir -p ./workloads
mount -t ext4 /dev/sdb ./workloads
