## BonsaiKV: Towards Fast, Scalable, and Persistent Key-Value Stores with Tiered, Heterogeneous Memory System

This repository contains the source code, setup utilities, and test scripts of *BonsaiKV*: a fast, scalable, and persistent key-value store on heterogeneous memory system. 

## Overview

BonsaiKV is a *versatile* key-value store built for tiered, heterogeneous memory system that fulfills efficient indexing, fast persistence, and high scalability simultaneously.

## Getting Started with BonsaiKV

### Prerequisites

**Hardware requirements:** Intel Optane DC Persistent Memory in **Non-Interleaved AppDirect** Mode

**OS version:** Ubuntu 18.04

**Kernel version:** Linux Kernel 5.4.0-125-generic

**Libraries:** libpmemobj, libnuma, libjemalloc

### Configuring and Building BonsaiKV

1. Checkout BonsaiKV
   + `git clone https://github.com/xxxxxx/BonsaiKV.git`

2. Configure BonsaiKV

   + Modify `./include/config.h` to change hardware configurations and BonsaiKV functionalities. Important options:

     + **NUM_CPU:** CPU core number
     + **NUM_DIMM:** Number of NVDIMMs installed on the server
     + **NUM_SOCKET:** NUMA node number
     + **NUM_USER_THREAD:** Number of user threads
     + **STR_KEY:** Enable string key or not
     + **STR_VAL:** Enable string value or not
     + **VAL_LEN:** String value length

     Advanced options:

     + **LOG_REGION_SIZE:** Log region size (default: 68GB)
     + **DATA_REGION_SIZE:** Data region size (default: 51GB)
     + **CPU_VAL_POOL_SIZE:** Number of string value slots for each CPU.
     + **ENABLE_PNODE_REPLICA:** Enable NUMA-Aware data migration. Recommend to enable this option for skewed workloads.

3. Build BonsaiKV

   + `cd ./src`
   + `make -j`

   Now you will find `libbonsai.so` in `./src`.

### (Optional) Changing Index for BonsaiKV

BonsaiKV uses Masstree as its index by default. But you can plug other indexes as you desire as long as they have `init`/`destroy`/`put`/`del`/`lower_bound` interfaces.

1. Modify `./src/Makefile` to link to your desired index library, or just copy its source code to `src` and `include` directly.
2. Modify `index_init`, `index_destroy`, `index_insert`, `index_remove`, `index_lowerbound` in `src/adapter.c`. You can call interfaces of your desired index directly in these functions.
3. Rebuild BonsaiKV.

### Run the Toy Example

The `example` directory contains a toy example which demonstrates the usage of BonsaiKV library. To run it:

1. Setup the environment
   + `cd ./scripts`
   + `./setup.sh`

2. Build the toy example
   + `cd ./example`
   + `make -j`

3. Run the toy example: `./example`

## Testing BonsaiKV

### YCSB Benchmark

#### Generating Workloads

You can generate YCSB workloads as follows:

1. `cd ./test/benchmark_ycsb/tools/index-microbench`
2. Modify `gen_ycsb.sh` to change workload settings.
3. `./gen_ycsb.sh`

Note that these scripts are only compatible with our modified YCSB in `./test/benchmark_ycsb/tools/index-microbench`. We remove some outputs and add multi-thread support to speed up benchmark generation.

#### Configuring and Building the Benchmark Driver

1. Modify `./test/benchmark_ycsb/include/config.h` to change hardware configurations and benchmark options. Important options:
   + **NUM_CPU:** CPU core number
   + **NUM_THREADS:** Worker thread number
   + **YCSB_KVLIB_PATH:** Key-value store library path
   + **YCSB_WORKLOAD_NAME:** The workload to run (see all workloads in `./test/benchmark_ycsb/workloads`)
   + **YCSB_IS_STRING_KEY:** Use string key or not.
   + **YCSB_VAL_LEN:** Value length.
2. Build the Benchmark Driver
   + `cd ./test/benchmark_ycsb`
   + `make -j`

   Now you will find `kvstore` in `./test/benchmark_ycsb`.

#### Run YCSB Benchmark

1. Setup the environment
   + `cd ./scripts`
   + `./setup.sh`

2. Run the YCSB Benchmark:
   + `cd ./test/benchmark/ycsb`
   + `./kvstore`
