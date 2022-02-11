#!/bin/bash

sudo rm -f ./core*

sudo rm -rf /home/gky/Desktop/ext4/*

sudo mkdir /home/gky/Desktop/ext4

sudo umount /dev/pmem0

sudo mkfs.ext4 /dev/pmem0

sudo mount -t ext4 -o dax /dev/pmem0 /home/gky/Desktop/ext4

sudo chmod 775 /home/gky/Desktop/ext4

sudo chown gky.gky /home/gky/Desktop/ext4