#!/bin/bash

CONFIGS=(
# "name        workload  load_cnt op_cnt  distrib str_key"
  "readc_index workloadc 500000   5000000 uniform 0",
  "readc_numa  workloadc 500000   5000000 zipfian 1",
  "readd_index workloadd 500000   5000000 uniform 0",
  "readd_numa  workloadd 500000   5000000 zipfian 1",
  "scan        workloade 5000000  500000  uniform 1"   # range: 100
)
THREADS=(1 16 32 48 64 80 96)

for config in "${CONFIGS[@]}"; do
  name=$(echo $config | awk "{ print $1 }")
  workload=$(echo $config | awk "{ print $2 }")
  load_cnt=$(echo $config | awk "{ print $3 }")
  op_cnt=$(echo $config | awk "{ print $4 }")
  distrib=$(echo $config | awk "{ print $5 }")
  str_key=$(echo $config | awk "{ print $6 }")
  for thread in "${THREADS}"; do

  done
done
