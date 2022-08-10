SPEC=$1
NR_THREAD=48

cd ./YCSB

mkdir -p ./generated

./bin/ycsb.sh load basic -P $SPEC -s -threads $NR_THREAD
./bin/merge.sh
mv ./generated/workload ../workloads/workload_load

./bin/ycsb.sh run basic -P $SPEC -s -threads $NR_THREAD
./bin/merge.sh
mv ./generated/workload ../workloads/workload_op
