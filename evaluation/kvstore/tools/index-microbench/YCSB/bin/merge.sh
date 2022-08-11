#!/bin/bash

echo Merging all workload shards...
cat generated/shard-* > generated/workload
rm -rf generated/shard-*
