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

#### 3.1 Evaluating unified collaborative index performance

+ **collabrative**: comparing with DRAM-only indexing (performance & DRAM consumption)
  + FlatStore is not open-source. Stop checkpointing to obtain a FlatStore-like DRAM-only index.
  + Use YCSB Workload C (Read-only), 24B K + 8B V, uniform distribution.


#### 3.2 Evaluating decoupled log-structure framework

+ baseline
+ baseline+log batch
+ baseline+log batch+volatile LCB
+ baseline+log batch+volatile LCB+thread throttling
+ log checkpoint time threshold study
+ Use YCSB Load, 24B K + 8B V, uniform distribution.

#### 3.3 Evaluating CLP-based data stripping

+ different data strip size: 256B-4KB

  throughput

+ bandwidth utilization

+ Use YCSB Custom Workload (100% Scan), 24B K + 8B V, uniform distribution

#### 3.4 Evaluating data coherence protocol

+ remote p-memory read reduction and throughput
+ data validity threshold study (throughput, hit rate)
+ MESI

#### 3.5 Read-intensive benchmark

YCSB-B, YCSB-C, YCSB-D

#### 3.6 Write-intensive benchmark

YCSB-A, TPC-C, TATP

#### 3.7 Scan-intensive benchmark

YCSB-E

#### 3.8 Performance interference study

log batch size: 256B-4KB





