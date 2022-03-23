## Bonsai: a High-performance Scalable Persistent Key-Value Store on DRAM-NVM Systems with Multi-Layer Storage

### Introduction

Bonsai is a transparent, scalable, NUMA-aware persistent data store for DRAM index structures. It consists of three layers: *index layer*, *log layer*, and *data layer* to separate data indexing from persistent storage. The index layer provides transparent data persistence support for legacy in-DRAM data structures. The log layer utilizes epoch-based batch log persistence technique to support low-cost data insert/remove. The data layer provides NUMA-aware data management as well as support fast range query.

### How to run Bansai

prepare the environment: `./script/setup.sh`

collect YCSB workload traces: `./index/gen_data.sh`

compile bonsai and a toy key-value store: `./index/remake.sh`

run a toy kv-store: `./index/kv`

