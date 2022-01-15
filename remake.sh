#!/bin/bash

cd index
./clean.sh
cd ../src
make clean
make
cd ../index
make clean
make