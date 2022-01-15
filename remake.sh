#!/bin/bash

cd index
sudo ./clean.sh
cd ../src
make clean
make
cd ../index
make clean
make