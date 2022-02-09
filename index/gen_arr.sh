#!/bin/bash

export index_microbench_dir='/home/gky/Desktop/index-microbench-master'
export load_file='load_monoint_workloada'
export txn_file='txn_monoint_workloada'

count=10000000
id=1
num=4

cd ${index_microbench_dir}
rm -rf ./workloads
mkdir workloads

cd -
rm -rf ./data
mkdir data
g++ -g gen_array.cpp -o gen_array.out
cp gen_workload.py ${index_microbench_dir}

while(( $id <= $num ))
do 
    cd ${index_microbench_dir}
    python ${index_microbench_dir}/gen_workload.py ${index_microbench_dir}/workload_config.inp

    cd -
    if [ $id == 1 ]
    then
        ./gen_array.out ${index_microbench_dir}/workloads/${load_file} ./data/load.h 0 $count 
    fi
    ./gen_array.out ${index_microbench_dir}/workloads/${txn_file} ./data/op.h 1 $count $id $num
    let "id++"
done