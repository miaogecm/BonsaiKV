#!/bin/bash

export index_microbench_dir='./'

gcc -g gen_data.c -o gen_data.out ${env}

cd ${index_microbench_dir}

python2 ${index_microbench_dir}/gen_workload_str.py ${index_microbench_dir}/workload_config.inp

suffix=${key_type}'_workload'${workload_type}

load_file='load_'${suffix}
txn_file='txn_'${suffix}

echo "load:${load_file}, txn:${txn_file}"

load_out=${STR_K}${STR_V}${SAMPLE}${UNIFORM}'load_'${suffix}'.h'
op_out=${STR_K}${STR_V}${SAMPLE}${UNIFORM}'op_'${suffix}'.h'

./gen_data.out ${index_microbench_dir}/workloads/${load_file} ./data/${load_out} 0 $count 
./gen_data.out ${index_microbench_dir}/workloads/${txn_file} ./data/${op_out} 1 $count

lib_name=${SAMPLE}${UNIFORM}'libkvdata_'${suffix}'.so'

rm kvdata.c
touch kvdata.c
echo '#include "./data/'${load_out}'"'>> ./kvdata.c
echo '#include "./data/'${op_out}'"'>> ./kvdata.c

gcc -w -shared -o ./data_lib/${lib_name} ./kvdata.c -std=c99

echo "----------done----------"