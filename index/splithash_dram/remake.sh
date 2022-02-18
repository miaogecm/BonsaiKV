#!/bin/bash

sudo ../clean.sh

pushd ../../src
make clean
make

popd
make clean
make
