#!/bin/python3

# YCSB-A
# load size: 5M per thread
# write size: 2.5M per thread
# read size: 2.5M per thread
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

    # dm-stripe 2M-Interleave
    # bottleneck: large SMO overhead, node metadata cacheline thrashing
    'fastfair': [7.134, 9.799, 11.056, 12.000, 13.202, 21.624, 24.842, 31.675, 45.375],

    # Nbg:Nfg = 1:2, at least 1 Nbg
    # dm-stripe 2M-Interleave
    # bottleneck: bloom filter array maintenance, WAL metadata cacheline thrashing, NUMA Awareness
    'dptree':   [4.596, 10.472, 18.069, 25.214, 31.945, 37.688, 43.657, 50.189, 57.191],

    # 1 worker per node (default setting)
    'pactree':  [9.098, 9.586, 11.796, 12.733, 13.479, 24.368, 32.008, 31.117, 35.446],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [5.698, 7.327, 7.977, 8.516, 8.960, 12.496, 12.899, 13.325, 12.101],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    # bottleneck: skiplist too high (perf reports high cache-miss rate when lockfree_skiplist::find_position)
    'listdb':   [7.599, 12.307, 17.301, 23.195, 31.949, 35.298, 46.982, 61.836, 78.329],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [3.277, 4.442, 4.915, 5.134, 5.576, 6.556, 7.180, 7.138, 7.289]
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

ytick=[0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
