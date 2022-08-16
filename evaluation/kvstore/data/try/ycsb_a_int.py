#!/bin/python3

# YCSB A Op
# preload size: 10M per thread
# write size: 5M per thread
# read size:  5M per thread
# key: 8B
# val: 8B
# distribution: uniform
# fsdax + dm-stripe (for NUMA-oblivious designs)

M = 1000000
MAX = 240 * M
SIZE = 10 * M
THREADS = [1, 8, 16, 24, 32, 40, 48]
LATENCY = {
    # thread num: 1/8/16/24/32/40/48

    # bottleneck:
    # Non log-structured: write amplification +
    # Log-structured: contention(BW) + metadata cacheline thrashing(perf L3 cache miss)

    # Nbg:Nfg = 1:2, at least 1 Nbg
    # dm-stripe 2M-Interleave
    # bottleneck: bloom filter array maintenance, WAL metadata cacheline thrashing, NUMA Awareness
    'dptree':   [MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # dm-stripe 2M-Interleave
    # bottleneck: large SMO overhead, node metadata cacheline thrashing
    'fastfair': [MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    # modify key shard calculation function to atoi(data)
    # bottleneck:
    #   (1) insert: memtable skiplist too high (perf reports high cache-miss rate when lockfree_skiplist::find_position)
    #   (2) read:
    #          1. memtable/L0/L1 skiplist too high
    #          2. L0 compaction causes many small random reads and writes to NVM, very slow, underutilizing PM
    #             bandwidth (only 3276.98/24000 B/s).
    #          3. Slow L0 compaction leads to many L0 tables, which hurts read performance. Cache is only cache.
    'listdb':   [MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # 1 worker per node (default setting)
    'pactree':  [MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [4.324, 12.485, 13.787, 14.562, 18.696, 19.377, 20.499],

    # dm-stripe 4K-Interleave
    # bottleneck: VPage metadata cacheline thrashing, segment lock overhead, NUMA Awareness
    'viper':    [3.807, 8.265, 9.072, 10.905, 14.581, 16.522, 19.565],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [3.282, 9.888, 10.678, 11.398, 13.951, 14.284, 15.108]
}

import numpy as np
import matplotlib.pyplot as plt
import csv

THROUGHPUT = {}
for kvstore, latencies in LATENCY.items():
    THROUGHPUT[kvstore] = list(map(lambda x: SIZE * THREADS[x[0]] / x[1] / M, enumerate(latencies)))

xs = THREADS

plt.plot(xs, THROUGHPUT['dptree'], markerfacecolor='none', marker='s', markersize=8, linestyle='-', linewidth=2, label='DPTree')
plt.plot(xs, THROUGHPUT['fastfair'], markerfacecolor='none', marker='^', markersize=8, linestyle='-', linewidth=2, label='FastFair')
plt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB')
plt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACMAN')
plt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='Viper')
plt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='<', markersize=8, linestyle='-', linewidth=2, label='PACTree')
plt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai')

font = {'size': '20','fontname': 'Times New Roman'}
plt.xlabel("Thread Number", font)
plt.ylabel("Throughput (M ops/s)", font)

font2 = {'size': '15','fontname': 'Times New Roman'}
ax = plt.gca()
ax.set_xticks(xs)
ax.set_xticklabels(xs, fontdict=font2)

ytick=[0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
