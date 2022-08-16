#!/bin/python3

# YCSB-D
# load size: 5M per thread
# read size: 5M per thread
# key: 8B
# val: 8B
# distribution: uniform
# fsdax + dm-stripe (for NUMA-oblivious designs)
# measure: throughput(thread)

M = 1000000
MAX = 240 * M
SIZE = 5.0 * M
THREADS = [1, 8, 16, 24, 32, 40, 48]
LATENCY = {
    # thread num: 1/8/16/24/32/40/48

    # bottleneck:
    # Non log-structured: write amplification +
    # Log-structured: contention(BW) + metadata cacheline thrashing(perf L3 cache miss)

    # Nbg:Nfg = 1:2, at least 1 Nbg
    # dm-stripe 2M-Interleave
    # bottleneck: bloom filter array maintenance, WAL metadata cacheline thrashing, NUMA Awareness
    'dptree':   [2.818, 8.043, 20.090, 31.996, 44.304, 52.824, 65.354],

    # dm-stripe 2M-Interleave
    # bottleneck: large SMO overhead, node metadata cacheline thrashing
    'fastfair': [5.083, 8.654, 10.994, 15.638, 26.681, 36.599, 46.999],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    # bottleneck: skiplist too high (perf reports high cache-miss rate when lockfree_skiplist::find_position)
    'listdb':   [6.159, 10.293, 11.202, 12.699, 14.107, 17.873, 20.764],

    # 1 worker per node (default setting)
    'pactree':  [5.157, 9.146, 11.928, 19.241, 26.778, 35.905, 42.072],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [2.156, 4.324, 4.640, 4.969, 8.090, 22.547, 48.724],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [1.577, 3.152, 3.597, 3.434, 3.868, 4.093, 5.669]
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

ytick=[0,2,4,6,8,10,12,14,16,18,20,22,24,26,28]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
