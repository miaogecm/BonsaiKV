# BonsaiKV: Towards Fast, Scalable, and Persistent Key-Value Stores with Tiered, Heterogeneous Memory System

## Introduction



## Build BonsaiKV

### Prerequisites

+ *Intel Optane DCPMM* in **Non-interleaved** AppDirect mode. *Fsdax/devdax* namespaces.

### Configuration

+ Modify `./include/config.h` to change platform-dependent parameters and BonsaiKV functionalities.
+ Modify `./src/region.c` to setup per-NVDIMM regions for logs, pnodes, and string values.

### Build

```
cd src
make -j
```

Now you can see `libbonsai.so` under `./src` directory.

### Change Index



## Benchmark KVStores

We implement a generic KVStore benchmarking framework under `./evaluation/kvstore`. You can use it to benchmark BonsaiKV or other KVStores under `./evaluation/kvstore/lib`.

### Generate YCSB workloads

+ Modify `./evaluation/kvstore/tools/index-microbench/gen_ycsb.sh` to change workload settings.
+ Run `./evaluation/kvstore/tools/index-microbench/gen_ycsb.sh`. Workloads are generated under `workloads` directory.

### Configuration

+ Modify `./evaluation/include/config.h` to change core number, thread number, KVStore library path, and benchmark settings.

### Build

```
cd evaluation/kvstore
make -j
```

### Run

```
cd evaluation/kvstore
./kvstore
```

