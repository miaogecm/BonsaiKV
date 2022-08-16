#!/bin/python3

# YCSB Load
# preload size: 5M per thread
# load size: 2.5M per thread
# key: 8B
# val: 8B
# distribution: uniform
# fsdax + dm-stripe (for NUMA-oblivious designs)
# measure: throughput(thread)

M = 1000000
MAX = 240 * M
SIZE = 2.5 * M
THREADS = [1, 6, 12, 18, 24, 30, 36, 42, 48]
LATENCY = {
    # thread num: 1/6/12/18/24/30/36/42/48

    # bottleneck:
    # Non log-structured: write amplification +
    # Log-structured: contention(BW) + metadata cacheline thrashing(perf L3 cache miss)

    # Nbg:Nfg = 1:2, at least 1 Nbg
    # dm-stripe 2M-Interleave
    # bottleneck: bloom filter array maintenance, WAL metadata cacheline thrashing, NUMA Awareness
    'dptree':   [2.087, 5.993, 9.980, 16.920, 24.916, 35.597, 40.868, 48.728, 54.497],

    # dm-stripe 2M-Interleave
    # bottleneck: large SMO overhead, node metadata cacheline thrashing
    'fastfair': [4.363, 5.831, 6.580, 7.284, 8.301, 13.605, 16.243, 22.317, 27.696],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    # bottleneck: skiplist too high (perf reports high cache-miss rate when lockfree_skiplist::find_position)
    'listdb':   [4.420, 6.863, 7.978, 9.604, 10.736, 10.651, 12.219, 13.062, 16.199],

    # 1 worker per node (default setting)
    'pactree':  [5.250, 6.526, 8.151, 8.448, 9.492, 14.051, 20.949, 23.543, 35.515],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [2.691, 3.558, 3.982, 4.297, 4.482, 5.729, 5.916, 6.059, 7.035],

    # dm-stripe 2M-Interleave
    # bottleneck: VPage metadata cacheline thrashing, segment lock overhead, NUMA Awareness
    'viper':    [3.121, 3.096, 3.420, 3.266, 3.575, 4.912, 5.824, 5.791, 6.045],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [1.362, 1.904, 2.127, 2.386, 2.325, 2.715, 2.854, 3.007, 2.946],
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

plt.plot(xs, THROUGHPUT['dptree'], markerfacecolor='none', marker='s', markersize=12, linestyle='-', linewidth=2, label='DPTree')
plt.plot(xs, THROUGHPUT['fastfair'], markerfacecolor='none', marker='^', markersize=12, linestyle='-', linewidth=2, label='FastFair')
plt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=12, linestyle='-', linewidth=2, label='ListDB')
plt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=12, linestyle='-', linewidth=2, label='PACMAN')
plt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='<', markersize=12, linestyle='-', linewidth=2, label='PACTree')
plt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='o', markersize=12, linestyle='-', linewidth=2, label='Viper')
plt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=12, linestyle='-', linewidth=2, label='Bonsai')

font = {'size': '20','fontname': 'Times New Roman'}
plt.xlabel("Thread Number", font)
plt.ylabel("Throughput (M ops/s)", font)

font2 = {'size': '15','fontname': 'Times New Roman'}
ax = plt.gca()
ax.set_xticks(xs)
ax.set_xticklabels(xs, fontdict=font2)

ytick=[0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
