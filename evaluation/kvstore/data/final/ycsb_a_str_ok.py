#!/bin/python3

# YCSB-A
# load size: 240K per thread
# write size: 120K per thread
# read size: 120K per thread
# key: 24B
# val: 16KB
# distribution: uniform
# fsdax + dm-stripe (for NUMA-oblivious designs)
# Enable snoopy mode for AD
# measure: throughput(thread)

M = 1000000
K = 1000
MAX = 5760 * K
SIZE = 240 * K
THREADS = [1, 6, 12, 18, 24, 30, 36, 42, 48]
LATENCY = {
    # thread num: 1/6/12/18/24/30/36/42/48

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    #'listdb':   [MAX, MAX, MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [1.767, 2.250, 3.441, 5.159, 6.898, 23.629, 28.833, 38.268, 43.489],

    # dm-stripe 2M-Interleave
    #'viper':    [3.202, MAX, MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [2.421, 2.637, 3.124, 3.935, 4.895, 3.948, 4.119, 4.560, 5.122]
}

import numpy as np
import matplotlib.pyplot as plt
import csv

THROUGHPUT = {}
for kvstore, latencies in LATENCY.items():
    THROUGHPUT[kvstore] = list(map(lambda x: SIZE * THREADS[x[0]] / x[1] / K, enumerate(latencies)))

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

#plt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB')
plt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACMAN')
#plt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='Viper')
plt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai')

font = {'size': '20','fontname': 'Times New Roman'}
plt.xlabel("Thread Number", font)
plt.ylabel("Throughput (K ops/s)", font)

font2 = {'size': '15','fontname': 'Times New Roman'}
ax = plt.gca()
ax.set_xticks(xs)
ax.set_xticklabels(xs, fontdict=font2)

ytick=[0, 200, 400, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
