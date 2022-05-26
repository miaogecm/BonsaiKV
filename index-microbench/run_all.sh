#!/bin/bash

echo start

sudo ln -sf /home/deadpool/Desktop/kv_test/data_lib/libkvdata_monoint_workloada.so /usr/lib/libkvdata.so

# cd /home/deadpool/Desktop/kv_test/DPTree-code/build
# cmake ..
# make -j 10
# echo done1

cd /home/deadpool/Desktop/kv_test/FAST_FAIR/concurrent_pmdk
make -j 10
echo done2

# cd /home/deadpool/Desktop/kv_test/pactree/build
# cmake ..
# make -j 10
# echo done3

# cd /home/deadpool/Desktop/kv_test/viper/build
# cmake-3.23 ..
# make -j 10
# echo done4

# sleep 5

cd /home/deadpool/Desktop/kv_test/index-microbench
dptree="sudo /home/deadpool/Desktop/kv_test/DPTree-code/build/concur_dptree"
fast_fair="/home/deadpool/Desktop/kv_test/FAST_FAIR/concurrent_pmdk/btree_concurrent"
pactree="/home/deadpool/Desktop/kv_test/pactree/build/example/pactree-example"
viper="/home/deadpool/Desktop/kv_test/viper/build/playground"

lib_dir="/home/deadpool/Desktop/kv_test/data_lib/"

# SAMPLE="sample_"
for UNIFORM in "" u_; do
	for KEY_TYPE in monoint randint; do
		for WORKLOAD_TYPE in a b c d e; do
			lib_name=${SAMPLE}${UNIFORM}'libkvdata_'${KEY_TYPE}'_workload'${WORKLOAD_TYPE}'.so'
			sudo ln -sf ${lib_dir}${lib_name} /usr/lib/libkvdata.so

			echo EXP OUTPUT: $UNIFORM, $KEY_TYPE, workload$WORKLOAD_TYPE
			for NUM_THREAD in 1 8 16 24 32 40 48; do

				# rm -f /mnt/ext4/node0/* 
				# rm -rf /mnt/ext4/node1/*
				# sleep 5
				# $dptree $NUM_THREAD
				
				rm -rf /mnt/ext4/node0/*
				rm -rf /mnt/ext4/node1/*
				sleep 5
				$fast_fair $NUM_THREAD

				# rm -rf /mnt/ext4/node0/*
				# rm -rf /mnt/ext4/node1/*
				# sleep 5
				# $pactree $NUM_THREAD

				# rm -rf /mnt/ext4/node0/*
				# rm -rf /mnt/ext4/node1/*
				# sleep 5
				# $viper $NUM_THREAD
			done
		done
	done
done
echo --------------------------done--------------------------
