#!/bin/bash

# export index_microbench_dir='/home/miaogecm/Documents/Projects/index-microbench-master'
export index_microbench_dir='/home/gky/Desktop/index-microbench-master'

#long_key="-DLONG_KEY"

count=1000000

cd ${index_microbench_dir}
rm -rf ./workloads
mkdir workloads

cd -
rm -rf ./data
mkdir data
g++ -g gen_data.cpp ${long_key} -o gen_data.out
cp gen_workload.py ${index_microbench_dir}
cp workload_config.inp ${index_microbench_dir}

cd ${index_microbench_dir}
python ${index_microbench_dir}/gen_workload.py ${index_microbench_dir}/workload_config.inp

export load_file=$(ls ./workloads | grep load_)
export txn_file=$(ls ./workloads | grep txn_)
echo "load:${load_file}, txn:${txn_file}"

cd -

./gen_data.out ${index_microbench_dir}/workloads/${load_file} ./data/load.h 0 $count 
./gen_data.out ${index_microbench_dir}/workloads/${txn_file} ./data/op.h 1 $count

gcc -shared -o data/libkvdata.so kvdata.c -std=c99
sudo ln -sf $(pwd)/data/libkvdata.so /usr/lib/libkvdata.so

echo "// BEGIN kvdata header" > data/kvdata.h
if [ "$long_key" == "-DLONG_KEY" ]
then
    echo "extern char load_arr[${count}][2][100];" >> data/kvdata.h
    echo "extern char op_arr[${count}][3][100];" >> data/kvdata.h
else
    echo "extern uint64_t load_arr[${count}][2];" >> data/kvdata.h
    echo "extern uint64_t op_arr[${count}][3];" >> data/kvdata.h
fi
echo "// END kvdata header" >> data/kvdata.h\

rm ./data/load.h
rm ./data/op.h
