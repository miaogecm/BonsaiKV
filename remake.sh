#!/bin/bash

cd kv
sudo ./clean.sh
cd ../src
make clean
make
cd ../kv
make clean
make
