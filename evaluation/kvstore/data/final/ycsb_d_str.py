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
THREADS = [1, 8, 16, 24, 32, 40, 48]
LATENCY = {
    # thread num: 1/8/16/24/32/40/48

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    'listdb':   [6.159, 10.293, 11.202, 12.699, 14.107, 17.873, 20.764],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [2.156, 4.324, 4.640, 4.969, 8.090, 22.547, 48.724],

    # dm-stripe 4K-Interleave
    # bottleneck: VPage metadata cacheline thrashing, segment lock overhead, NUMA Awareness
    'viper':    [3.393, 5.005, 4.984, 6.044, 6.980, 7.737, 10.433],

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

plt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB')
plt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACMAN')
plt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='Viper')
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
