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
THREADS = [1, 6, 12, 18, 24, 30, 36, 42, 48]
LATENCY = {
    # thread num: 1/6/12/18/24/30/36/42/48

    # bottleneck:
    # Non log-structured: write amplification +
    # Log-structured: contention(BW) + metadata cacheline thrashing(perf L3 cache miss)

    # Nbg:Nfg = 1:2, at least 1 Nbg
    # dm-stripe 2M-Interleave
    # bottleneck: bloom filter array maintenance, WAL metadata cacheline thrashing, NUMA Awareness
    'dptree':   [4.536, 4.914, 5.330, 5.921, 6.430, 6.211, 6.451, 6.761, 7.155],

    # dm-stripe 2M-Interleave
    # bottleneck: large SMO overhead, node metadata cacheline thrashing
    'fastfair': [6.074, 9.063, 10.099, 10.781, 11.377, 12.032, 12.498, 13.419, 13.656],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    # bottleneck: skiplist too high (perf reports high cache-miss rate when lockfree_skiplist::find_position)
    'listdb':   [4.916, 7.676, 10.313, 13.569, 16.915, 21.238, 24.474, 32.453, 38.197],

    # 1 worker per node (default setting)
    'pactree':  [6.622, 9.947, 10.338, 10.269, 10.717, 11.807, 11.061, 12.128, 13.600],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [4.391, 7.957, 8.604, 9.158, 9.536, 11.590, 11.972, 12.364, 12.691],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [4.199, 5.523, 5.899, 6.267, 6.396, 7.334, 7.571, 7.808, 7.750]
}

import numpy as np
import matplotlib.pyplot as plt
import csv

THROUGHPUT = {}
for kvstore, latencies in LATENCY.items():
    THROUGHPUT[kvstore] = list(map(lambda x: SIZE * THREADS[x[0]] / x[1] / M, enumerate(latencies)))

MAPPING = {}
HEADER = ['thread']
for kvstore, latencies in THROUGHPUT.items():
    HEADER.append(kvstore)
    for thread, latency in zip(THREADS, latencies):
        if thread not in MAPPING:
            MAPPING[thread] = []
        MAPPING[thread].append(latency)

print(','.join(HEADER))
for thread, latencies in MAPPING.items():
    print(','.join(map(lambda x: str(round(x, 2)), [thread] + latencies)))

xs = THREADS

plt.plot(xs, THROUGHPUT['dptree'], markerfacecolor='none', marker='s', markersize=10, linestyle='-', linewidth=2, label='DPTree')
plt.plot(xs, THROUGHPUT['fastfair'], markerfacecolor='none', marker='^', markersize=10, linestyle='-', linewidth=2, label='FastFair')
plt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=10, linestyle='-', linewidth=2, label='ListDB')
plt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=10, linestyle='-', linewidth=2, label='PACMAN')
plt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='<', markersize=10, linestyle='-', linewidth=2, label='PACTree')
plt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=10, linestyle='-', linewidth=2, label='Bonsai')

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
