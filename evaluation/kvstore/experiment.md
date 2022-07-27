### Bonsai实验设计

### 1. Experiment Setup

#### 1.1 对比KVS

**DRAM-NVMM key-value store:** ListDB, Viper

**NVM-optimized key-value store:** SLM-DB, NoveLSM, Pmem-RocksDB

**Persistent index structure:** PACTree, FAST-FAIR, DPTree

*NoveLSM and SLM-DB were designed to use NVMM as an intermediate layer on top of the block device file system, but our experiments store all SSTables in NVMM formatted with EXT4-DAX for a fair comparison.*

#### 1.2 Benchmark

Yahoo Cloud Serving Benchmark: YCSB

OLTP Benchmark: TPC-C

Telecommunication Application Transaction Processing: TATP

### 2. Overview

+ Evaluating four core technique performance
+ Evaluating three data-intensive benchmarks
+ Performance interference evaluation

### 3. Experiment

#### 3.1 Evaluating indexing technique

+ **collabrative**: comparing with DRAM-only indexing (performance & DRAM consumption)
  + FlatStore is not open-source. Stop checkpointing to obtain a FlatStore-like DRAM-only index.
  + Use YCSB Workload C (Read-only), 24B K + 8B V, uniform distribution.


#### 3.2 Evaluating persistence technique

+ baseline
+ baseline+log batch
+ baseline+log batch+volatile LCB
+ baseline+log batch+volatile LCB+thread throttling
+ log checkpoint time threshold study
+ Use YCSB Load, 24B K + 8B V, uniform distribution.

#### 3.3 Evaluating scalability technique

+ different data strip size: 256B-4KB

  throughput

+ bandwidth utilization

+ Use YCSB Custom Workload (100% Scan), 24B K + 8B V, uniform distribution

+ remote p-memory read reduction and throughput
+ data validity threshold study (throughput, hit rate)
+ MESI

#### 3.4 Read-intensive benchmark

YCSB-B op, YCSB-C op, YCSB-D op

+ Load 480Mops, Read 4800Mops
+ Key: 24B Val: 8B/256B
+ Zipfian distribution

| KVStore                          | Throughput (1/2/4/8/16/24/32/40/48) |
| -------------------------------- | ----------------------------------- |
| DPTree (not support string KV)   |                                     |
| FastFair (not support string KV) |                                     |
| ListDB                           |                                     |
| NoveLSM                          |                                     |
| PACTree (not support string V)   |                                     |
| pmem-rocksdb                     |                                     |
| SLM-DB                           |                                     |
| Viper                            |                                     |
| Bonsai                           |                                     |

Highlight techniques:

+ 8B Val (indexing and lookup dominates): Collabrative indexing (compared to FastFair, PACTree, NoveLSM, SLM-DB, ListDB) and OCC
+ 256B Val (value read dominates): NUMA-Aware read (compared to all)
+ compacted inode design: utilize modern CPU adjacent cacheline prefetch functionality

#### 3.5 Write-intensive benchmark

YCSB-A load+op

+ Load 4800Mops, Read 4800Mops
+ Key: 8B Val: 8B
+ Uniform distribution

Load (100% Write):

| KVStore      | Throughput (1/2/4/8/16/24/32/40/48) |
| ------------ | ----------------------------------- |
| DPTree       |                                     |
| FastFair     |                                     |
| ListDB       |                                     |
| NoveLSM      |                                     |
| PACTree      |                                     |
| pmem-rocksdb |                                     |
| SLM-DB       |                                     |
| Viper        |                                     |
| Bonsai       |                                     |

Op (50% Write, 50% Read)

| KVStore      | Throughput (1/2/4/8/16/24/32/40/48) |
| ------------ | ----------------------------------- |
| DPTree       |                                     |
| FastFair     |                                     |
| ListDB       |                                     |
| NoveLSM      |                                     |
| PACTree      |                                     |
| pmem-rocksdb |                                     |
| SLM-DB       |                                     |
| Viper        |                                     |
| Bonsai       |                                     |

TPC-C, TATP

| KVStore      | Throughput (1/2/4/8/16/24/32/40/48) |
| ------------ | ----------------------------------- |
| DPTree       |                                     |
| FastFair     |                                     |
| ListDB       |                                     |
| NoveLSM      |                                     |
| PACTree      |                                     |
| pmem-rocksdb |                                     |
| SLM-DB       |                                     |
| Viper        |                                     |
| Bonsai       |                                     |

Highlight techniques:

+ Batching + Throttling
+ Wait-free pnode lookup (mixed R/W)
+ NUMA-Aware write

#### 3.6 Scan-intensive benchmark

YCSB-E op

+ Load 4800Mops, Read 480Mops
+ Key: 24B Val: 8B Range: 100
+ Uniform distribution

| KVStore      | Throughput (1/2/4/8/16/24/32/40/48) |
| ------------ | ----------------------------------- |
| DPTree       |                                     |
| FastFair     |                                     |
| ListDB       |                                     |
| NoveLSM      |                                     |
| PACTree      |                                     |
| pmem-rocksdb |                                     |
| SLM-DB       |                                     |
| Viper        |                                     |
| Bonsai       |                                     |
