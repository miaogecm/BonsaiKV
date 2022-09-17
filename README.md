## BonsaiKV: Towards Fast, Scalable, and Persistent Key-Value Stores with Tiered, Heterogeneous Memory System

### 1. Overview

This repository contains the source code, setup utilities, and test scripts of *BonsaiKV*: a fast, scalable, and persistent key-value store on heterogeneous memory system. 

### 2. Getting Started

#### 2.1 Prerequisites

**Hardware:** Intel Optane DC Persistent Memory with **Non-Interleaved AppDirect** Mode

**Operating System:** Ubuntu 18.04+

**Kernel Version:** Linux Kernel 5.4.0-125-generic

**Libraries:** libpmemobj, libnuma, libjemalloc

#### 2.2 Configure and Build BonsaiKV

1. Clone BonsaiKV Repo:
   + `git clone https://github.com/xxxxxx/BonsaiKV.git`

2. Configure BonsaiKV:

   + Modify `./include/config.h` to change hardware configurations and BonsaiKV functionalities. Important configurations:

     + **NUM_CPU:**  Total CPU number
     + **NUM_DIMM:** Total number of NVMM DIMMs
     + **NUM_SOCKET:** Total NUMA node number
     + **NUM_USER_THREAD:** Total user thread number
     + **STR_KEY:** Enable string key or not
     + **STR_VAL:** Enable string value or not
     + **VAL_LEN:** String value length

     Advanced options:

     + **LOG_REGION_SIZE:** Log region size (default: 68GB)
     + **DATA_REGION_SIZE:** Data region size (default: 51GB)
     + **CPU_VAL_POOL_SIZE:** Number of string value slots for each CPU.
     + **ENABLE_PNODE_REPLICA:** Enable NUMA-aware data migration. Recommend to enable this for skewed workloads.

3. Build BonsaiKV

   + `cd ./src`
   + `make -j`

   You can find `libbonsai.so` in `./src`.

#### 2.3 (Optional) Change DRAM Index

By default, BonsaiKV uses Masstree as its DRAM index. You can plug other indexes as you desire.

1. Modify `./src/Makefile` to link to your desired index library, or just copy its source code to `src` and `include`.
2. Register `index_init`, `index_destroy`, `index_insert`, `index_remove`, `index_lowerbound` in `src/adapter.c`. 
3. Rebuild BonsaiKV.

### 3. Run the Toy Example

The `example` directory contains a toy example which demonstrates how to run BonsaiKV. To run it:

1. Setup the environment
   + `cd ./scripts`
   + `./setup.sh`

2. Build the toy example
   + `cd ./example`
   + `make -j`

3. Run the toy example: `./example`

### 4. Testing BonsaiKV with YCSB

#### 4.1 Generate Workloads

You can generate YCSB workload traces as follows:

1. `cd ./test/benchmark_ycsb/tools/index-microbench`
2. Modify `gen_ycsb.sh` to change workload configurations.
3. `./gen_ycsb.sh`

Note that these scripts are only compatible with our modified YCSB in `./test/benchmark_ycsb/tools/index-microbench`. We remove some outputs and add multi-thread support in YCSB to accelerate benchmark running.

#### 4.2 Configure and Build the Benchmark Driver

1. Modify `./test/benchmark_ycsb/include/config.h` to change hardware configurations and benchmark options. Important options:
   + **NUM_CPU:** Total CPU number
   + **NUM_THREADS:** Total worker thread number
   + **YCSB_KVLIB_PATH:** Key-value store library file path
   + **YCSB_WORKLOAD_NAME:** The workload to run (see all workloads in `./test/benchmark_ycsb/workloads`)
   + **YCSB_IS_STRING_KEY:** Use string key or not.
   + **YCSB_VAL_LEN:** Value length.
2. Build the Benchmark Driver
   + `cd ./test/benchmark_ycsb`
   + `make -j`

   Now you will find `kvstore` in `./test/benchmark_ycsb`.

#### 4.3 Run YCSB Benchmark

1. Setup the environment
   + `cd ./scripts`
   + `./setup.sh`

2. Run the YCSB Benchmark:
   + `cd ./test/benchmark/ycsb`
   + `./kvstore`
