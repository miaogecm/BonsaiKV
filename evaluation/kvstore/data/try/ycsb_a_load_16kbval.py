#!/bin/python3

# YCSB-A Load
# preload size: 240K per thread
# load size: 120K per thread
# key: 8B
# val: 16KB
# distribution: uniform
# memory limit: unlimited
# fsdax + dm-stripe (for NUMA-oblivious designs)

M = 1000000
K = 1000
MAX = 5760 * K
SIZE = 120 * K
THREADS = [1, 16, 32, 48, 64, 80, 96]
LATENCY = {
    # thread num: 1/16/32/48/64/80/96

    # dm-stripe 4K-Interleave
    'viper-ntstore': [1.428, 4.824, 12.24, 22.56, 31.128, 41.028, 62.916],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    # BW contention, persistent (using memcpy+clwb+sfence, cache eviction), no thread-to-DIMM distribution
    'listdb':   [8.399, 20.184, 32.867, 46.145, 58.836, 78.108, 94.836],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    # <32 thread: LOG_BATCHING reduces clwb
    # BW contention, persistent (using memcpy+clwb+sfence, cache eviction), NUMA unaware, no thread-to-DIMM distribution
    'pacman':   [1.44, 3.516, 6.792, 25.332, 49.008, 87.911, 92.892],

    # dm-stripe 4K-Interleave
    # BW contention, persistent (using memcpy+clwb+sfence, cache eviction), NUMA unaware
    'viper':    [1.524, 7.008, 31.692, 52.884, 69.132, 89.568, 118.188],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [1.695, 3.978, 7.899, 7.974, 8.103, 8.125, 8.060]
}
PMMWBW = {
    # thread num: 1/16/32/48/64/80/96

    # dm-stripe 4K-Interleave
    'viper-ntstore': [1687.44, 8942.85, 7628.93, 6413.72, 6598.52, 5512.35, 5225.13],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    'listdb':   [1665.41, 6817.48, 6948.86, 6895.34, 6939.43, 7009.66, 6780.27],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [2015.57, 13624.17, 15730.59, 5979.76, 4257.99, 3381.25, 3738.73],

    # dm-stripe 4K-Interleave
    'viper':    [1644.38, 2963.48, 3038.43, 2715.46, 2713.59, 2538.42, 2222.62],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    # disabled pflush workers
    'bonsai':   [1147.85, 7524.23, 8240.46, 16094.66, 16055.59, 23135.46, 24433.48]
}

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec
import csv

THROUGHPUT = {}
for kvstore, latencies in LATENCY.items():
    THROUGHPUT[kvstore] = list(map(lambda x: SIZE * THREADS[x[0]] / x[1] / M, enumerate(latencies)))

xs = THREADS

gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1])

throughplt = plt.subplot(gs[0])
throughplt.plot(xs, THROUGHPUT['viper-ntstore'], markerfacecolor='none', marker='^', markersize=8, linestyle='-', linewidth=2, label='Viper-NT', color='green')
throughplt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB', color='blue')
throughplt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACMAN', color='orange')
throughplt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='Viper', color='aqua')
throughplt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai', color='brown')

bwplt = plt.subplot(gs[1], sharex=throughplt)
bwplt.plot(xs, PMMWBW['viper-ntstore'], markerfacecolor='none', marker='^', markersize=8, linestyle='--', linewidth=2, label='Viper-NT', color='green')
bwplt.plot(xs, PMMWBW['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='--', linewidth=2, label='ListDB', color='blue')
bwplt.plot(xs, PMMWBW['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='--', linewidth=2, label='PACMAN', color='orange')
bwplt.plot(xs, PMMWBW['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='--', linewidth=2, label='Viper', color='aqua')
bwplt.plot(xs, PMMWBW['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='--', linewidth=2, color='brown')

font = {'size': '18','fontname': 'Times New Roman'}
font2 = {'size': '15','fontname': 'Times New Roman'}

throughplt.set_xlabel("Thread Number", font)
throughplt.set_xticks(xs)
throughplt.set_xticklabels(xs, fontdict=font2)

throughplt.set_ylabel("Throughput (M ops/s)", font)
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
