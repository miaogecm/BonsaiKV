## Bonsai: A Fast, Scalable, Persistent Key-Value Store on DRAM-NVM Systems

### Introduction

Bonsai is a fast, scalable, persistent data store for DRAM-NVM systems. It consists of three layers: *index layer*, *log layer*, and *data layer*. The index layer provides unified collaborated indexing. The log layer support decouple log-structured framework. The data layer offers scalable data management.

### How to run Bansai

prepare the environment: `./script/setup.sh`

collect YCSB workload traces: `./index/gen_data.sh`

compile bonsai and a toy key-value store: `./index/remake.sh`

run a toy kv-store: `./index/kv`

