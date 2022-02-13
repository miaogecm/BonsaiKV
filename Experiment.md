### Bonsai实验设置

#### Benchmark

YCSB: load, workloada, workloadb, workloadc, workloadd, workloade

#### Bonsai vs. NVM Index Structures

1. B+ Tree

FastFair[1], FP-tree[2]

Bonsai + B+ tree

Bonsai + Masstree[6]

Bonsai + PALM[13]

2. Hash Table

LevelHash[3], CCEH[4]

Bonsai + Hash table

Bonsai + CLHT[7]

3. Radix Tree

WORT[5]

Bonsai + Radix tree

Bonsai + ART[8]

#### Bonsai vs. DRAM Index Structure Conversion Techniques

Bonsai, TIPS[9], PRONTO[10], NVTraverse[11], RECIPE[12]

converse DRAM **B+ Tree, Hash Table** into NVRAM

### References:
[1] Endurable Transient Inconsistency in Byte-addressable Persistent B+-tree. *FAST'18*.
[2] FPTree: A Hybrid SCM-DRAM Persistent and Concurrent B-Tree for Storage Class Memory. *SIGMOD'16*
[3] Write-Optimized and High-Performance Hashing Index Scheme for Persistent Memory. *OSDI'18*
[4] Write-Optimized Dynamic Hashing for Persistent Memory. *FAST'19*
[5] WORT: Write Optimal Radix Tree for Persistent Memory Storage Systems. *FAST'17*
[6] Cache Craftiness for Fast Multicore Key-Value Storage. *EuroSys'12*
[7] Asynchronized Concurrency: The Secret to Scaling Concurrent Search Data Structures. *ASPLOS'15*
[8] The ART of Practical Synchronization. *International Workshop on Data Management on New Hardware 2016*
[9] TIPS: Making Volatile Index Structures Persistent with DRAM-NVMM Tiering. *Usenix ATC'21*
[10] Pronto: Easy and Fast Persistence for Volatile Data Structures. *ASPLOS'20*
[11] NVTraverse: In NVRAM Data Structures, the Destination is More Important than the Journey. *PLDI'20*
[12] RECIPE: Converting Concurrent DRAM Indexes to PersistentMemory Indexes. *SOSP'19*
[13] PALM: Parallel Architecture-Friendly Latch-Free Modifications to B+ Trees on Many-Core Processors. *VLDB'11*