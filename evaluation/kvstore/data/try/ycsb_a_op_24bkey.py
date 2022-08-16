#!/bin/python3

# YCSB A Op
# preload size: 5M per thread
# write size: 2.5M per thread
# read size:  2.5M per thread
# key: 24B
# val: 8B
# distribution: uniform
# memory limit: 24GiB
# fsdax + dm-stripe (for NUMA-oblivious designs)

M = 1000000
MAX = 240 * M
SIZE = 5 * M
THREADS = [1, 16, 32, 48, 64, 80, 96]
LATENCY = {
    # thread num: 1/16/32/48/64/80/96

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
    'listdb':   [8.4, 24.9, 27.6, 35.5, 45.2, 116.8, 122.5],

    # 1 worker per node (default setting)
    #'pactree':  [MAX, MAX, 29.120, 57.424, 89.129, 106.461, 107.901],
    'pactree':  [14.933, 22.522, 28.926, 34.844, 89.129, 106.461, 107.901],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    #'pacman':   [7.824, 14.654, 18.419, 15.541, 34.757, MAX, MAX],
    'pacman':   [7.804, 11.310, 12.144, 12.631, MAX, MAX, MAX],

    # dm-stripe 4K-Interleave
    # bottleneck: VPage metadata cacheline thrashing, segment lock overhead, NUMA Awareness
    'viper':    [7.557, 7.514, 7.691, 8.681, 11.715, 15.046, 22.213],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    #'bonsai':   [5.429, 11.376, 12.628, 14.224, 18.402, 20.988, 22.851]
    'bonsai':   [6.546, 9.836,  11.289, 11.864, 18.402, 20.988, 22.851]
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
plt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='<', markersize=8, linestyle='-', linewidth=2, label='PACTree')
plt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai')

font = {'size': '20','fontname': 'Times New Roman'}
plt.xlabel("Thread Number", font)
plt.ylabel("Throughput (M ops/s)", font)

font2 = {'size': '15','fontname': 'Times New Roman'}
ax = plt.gca()
ax.set_xticks(xs)
ax.set_xticklabels(xs, fontdict=font2)

ytick=[0,2,4,6,8,10,12,14,16,18,20,22,24,26]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
