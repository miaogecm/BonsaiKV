#!/bin/python3

# YCSB-D
# load size: 0.5M per thread
# read size: 5M per thread
# key: 24B
# val: 1KB
# distribution: zipfian
# fsdax + dm-stripe (for NUMA-oblivious designs)
# measure: throughput(thread)

M = 1000000
MAX = 240 * M
SIZE = 5.0 * M
THREADS = [1, 6, 12, 18, 24, 30, 36, 42, 48]
LATENCY = {
    # thread num: 1/6/12/18/24/30/36/42/48

    # dm-stripe 4K-Interleave
    # bottleneck: VPage metadata cacheline thrashing, segment lock overhead, NUMA Awareness
    'fastfair': [6.787, 6.438, 7.409, 8.098, 9.332, 18.668, 27.384, 43.369, 53.127],

    'dptree':   [6.149, 8.572, 9.146, 12.167, 14.962, 16.419, 18.892, 18.829, 29.819],

    'pactree':  [6.002, 7.778, 9.794, 10.984, 13.402, 18.749, 18.894, 21.873, 29.202],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [5.373, 6.010, 6.126, 6.843, 6.982, 11.333, 16.852, 27.485, 31.597],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    'listdb':   [8.365, 16.976, 19.052, 20.605, 22.252, 42.342, 52.483, 64.742, 63.975],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [4.692, 6.339, 6.442, 6.478, 6.515, 6.484, 6.617, 7.544, 8.313],
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

plt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB')
plt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACMAN')
plt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACTree-ptr')
plt.plot(xs, THROUGHPUT['dptree'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='DPTree-ptr')
plt.plot(xs, THROUGHPUT['fastfair'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='FAST-FAIR-ptr')
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
