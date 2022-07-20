### Bonsai实验设计

### 1. Setup

#### 1.1 对比KVS

**DRAM-NVMM key-value store:** ListDB, Viper

**NVM-optimized key-value store:** SLM-DB, NoveLSM, Pmem-RocksDB

**Persistent index structure:** PACTree, FAST-FAIR, DPTree

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

+ comparing with DRAM-only indexing
+ comparing memory consumption

#### 3.2 Evaluating decoupled log-structure framework

+ baseline
+ baseline+log batch
+ baseline+log batch+volatile LCB
+ baseline+log batch+volatile LCB+thread throttling
+ log checkpoint time threshold

#### 3.3 Evaluating CLP-based data stripping

+ different data strip size: 256B-4KB
+ optane prefetch vs. cache line prefetch

#### 3.4 Evaluating data coherence protocol

+ remote memory write reduction
+ data validity threshold impact

#### 3.5 Read-intensive benchmark

YCSB-B, YCSB-C, YCSB-D

#### 3.6 Write-intensive benchmark

YCSB-A, TPC-C, TATP

#### 3.7 Scan-intensive benchmark

YCSB-E

#### 3.8 Performance interference study

LCB size: 1KB-4KB





