#!/bin/bash

mkdir ./data
mkdir ./data_lib

rm ./workloads/*
rm ./data/*
rm ./data_lib/*

export SAMPLE="sample_"

# export UNIFORM="u_"

# export STR_K="strk_"
# export K_LEN="16_"

export STR_V="no"
#export V_LEN="16_"

# export env="-DSTR_KEY -DKEY_LEN=16 -DSTR_VAL -DVAL_LEN=16"
#export env="-DSTR_VAL -DVAL_LEN=16"
export env="-DSTR_KEY -DKEY_LEN=24"

# count=36000000
export count=48000000

# for KEY_TYPE in monoint randint; do
# 	for WORKLOAD_TYPE in a b c d e; do
#     	echo workload${WORKLOAD_TYPE} > workload_config.inp
#     	echo ${KEY_TYPE} >> workload_config.inp
		
# 		echo ----------workload${WORKLOAD_TYPE} ${KEY_TYPE}----------
# 		export key_type=$KEY_TYPE
# 		export workload_type=$WORKLOAD_TYPE
# 		./__gen_data.sh
#   	done
# done 

for KEY_TYPE in randint; do
	for WORKLOAD_TYPE in a; do
    	echo workload${WORKLOAD_TYPE} > workload_config.inp
    	echo ${KEY_TYPE} >> workload_config.inp
		
		echo ----------workload${WORKLOAD_TYPE} ${KEY_TYPE}----------
		export key_type=$KEY_TYPE
		export workload_type=$WORKLOAD_TYPE
		./__gen_data.sh
  	done
done 

echo --------------------------done--------------------------
