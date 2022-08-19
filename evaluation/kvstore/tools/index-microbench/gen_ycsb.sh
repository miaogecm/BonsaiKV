#!/bin/bash

CONFIGS=(
# "name        workload  load_cnt op_cnt  distrib str_key"
  "a_int       workloada 5000000  5000000 uniform 0"    # also: load_int, load_str, c_int
  "a_str       workloada 240000   240000  uniform 1"
  "c_str       workloadc 500000   5000000 zipfian 1"
  "d_int       workloadd 5000000  5000000 uniform 0"
  "d_str       workloadd 500000   5000000 zipfian 1"
  "e_int       workloade 5000000  500000  uniform 0"
  "e_str       workloade 5000000  500000  uniform 1"    # range: 100
)
THREADS=(1 6 12 18 24 30 36 42 48)

for config in "${CONFIGS[@]}"; do
  name=$(echo $config | awk "{ print(\$1) }")
  workload=$(echo $config | awk "{ print(\$2) }")
  thread_load_cnt=$(echo $config | awk "{ print(\$3) }")
  thread_op_cnt=$(echo $config | awk "{ print(\$4) }")
  distrib=$(echo $config | awk "{ print(\$5) }")
  str_key=$(echo $config | awk "{ print(\$6) }")
  for thread in "${THREADS[@]}"; do
    load_cnt=$((thread_load_cnt * thread))
    op_cnt=$((thread_op_cnt * thread))
    dir=workloads/$name/$thread
    mkdir -p $dir
    ./gen_workload.sh $workload $load_cnt $op_cnt $distrib
    ../../compress $dir/load workloads/workload_load $str_key
    ../../compress $dir/op workloads/workload_op $str_key
  done
done

