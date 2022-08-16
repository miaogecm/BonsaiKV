#!/bin/python3

# YCSB Load
# preload size: 240K per thread
# load size: 120K per thread
# key: 24B
# val: 16KB
# distribution: uniform
# fsdax + dm-stripe (for NUMA-oblivious designs)
# measure: throughput/PMMWBW(thread)

# *** VIPER: SIGBUS ***

M = 1000000
K = 1000
MAX = 5760 * K
SIZE = 120 * K
THREADS = [1, 6, 12, 18, 24, 30, 36, 42, 48]
LATENCY = {
    # thread num: 1/6/12/18/24/30/36/42/48

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    # flush too late
    'listdb':   [2.446, 8.288, 15.971, 23.253, 31.075, 21.984, 21.839, 24.722, 29.558],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [0.835, 1.473, 3.210, 4.787, 6.402, 27.57, 33.30, 38.808, 44.256],

    # dm-stripe 2M-Interleave
    'viper':    [1.616, 2.230, 4.979, 7.304, 9.936, MAX, MAX, MAX, MAX],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [1.730, 1.755, 1.821, 2.540, 3.374, 2.650, 2.608, 2.990, 3.473],
}
PMMWBW = {
    # thread num: 1/8/16/24/32/40/48

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    'listdb':   [4368.55, 4937.32, 5090.55, 5117.40, 5042.62, 9606.24, 10490.72, 9447.24, 9747.96],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [2395.68, 8054.37, 7395.74, 7377.98, 7108.59, 3350.36, 3321.75, 3272.71, 3241.03],

    # dm-stripe 2M-Interleave
    'viper':    [2022.25, 6672.89, 6442.92, 6204.28, 6031.45, 0, 0, 0, 0],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    # disabled pflush workers
    'bonsai':   [1138.62, 6818.75, 13050.39, 13863.94, 14026.37, 21108.42, 24191.92, 24477.78, 24700.26],
}

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec
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

MAPPING = {}
HEADER = ['thread']
for kvstore, latencies in PMMWBW.items():
    HEADER.append(kvstore)
    for thread, latency in zip(THREADS, latencies):
        if thread not in MAPPING:
            MAPPING[thread] = []
        MAPPING[thread].append(latency)

print(','.join(HEADER))
for thread, bws in MAPPING.items():
    print(','.join(map(lambda x: str(round(x, 2)), [thread] + bws)))

xs = THREADS

gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1])

throughplt = plt.subplot(gs[0])
throughplt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB', color='blue')
throughplt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACMAN', color='orange')
throughplt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='Viper', color='aqua')
throughplt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai', color='brown')

bwplt = plt.subplot(gs[1], sharex=throughplt)
bwplt.plot(xs, PMMWBW['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='--', linewidth=2, label='ListDB', color='blue')
bwplt.plot(xs, PMMWBW['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='--', linewidth=2, label='PACMAN', color='orange')
bwplt.plot(xs, PMMWBW['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='--', linewidth=2, label='Viper', color='aqua')
bwplt.plot(xs, PMMWBW['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='--', linewidth=2, color='brown')

font = {'size': '18','fontname': 'Times New Roman'}
font2 = {'size': '15','fontname': 'Times New Roman'}

throughplt.set_xlabel("Thread Number", font)
throughplt.set_xticks(xs)
throughplt.set_xticklabels(xs, fontdict=font2)

throughplt.set_ylabel("Throughput (K ops/s)", font)
ytick=[0, 1000, 2000, 3000, 4000, 5000, 6000]
#throughplt.set_yticks(ytick)
#throughplt.set_yticklabels(ytick, fontdict=font2)

bwplt.set_ylabel("PMM Write BW (B/s)", font)
ytick=[0, 1000, 2000, 3000, 4000, 5000, 6000]
#bwplt.set_yticks(ytick)
#bwplt.set_yticklabels(ytick, fontdict=font2)

throughplt.legend(loc = 2, ncol=4, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
plt.subplots_adjust(hspace=.0)
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
