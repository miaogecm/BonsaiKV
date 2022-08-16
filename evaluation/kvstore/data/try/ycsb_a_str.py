#!/bin/python3

# YCSB A Op
# preload size: 240K per thread
# write size: 120K per thread
# read size: 120K per thread
# key: 24B
# val: 16KB
# distribution: uniform
# fsdax + dm-stripe (for NUMA-oblivious designs)

M = 1000000
K = 1000
MAX = 5760 * K
SIZE = 240 * K
THREADS = [1, 8, 16, 24, 32, 40, 48]
LATENCY = {
    # thread num: 1/8/16/24/32/40/48

    # bottleneck:
    # Non log-structured: write amplification +
    # Log-structured: contention(BW) + metadata cacheline thrashing(perf L3 cache miss)

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
    'listdb':   [MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [1.757, 2.475, 3.713, 6.166, 36.985, 61.586, 61.586],

    # dm-stripe 4K-Interleave
    # bottleneck: VPage metadata cacheline thrashing, segment lock overhead, NUMA Awareness
    'viper':    [MAX, MAX, MAX, MAX, MAX, MAX, MAX],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [MAX, MAX, 3.810, 5.310, 32.965, 32.965, 32.965],
}
PMMWBW = {
    # thread num: 1/8/16/24/32/40/48

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 1 Nbg
    'listdb':   [1, 1, 1, 1, 1, 1, 1],

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [1116.49, 6414.75, 8595.85, 7739.43, 1999.53, 3561.09, 1],

    # dm-stripe 4K-Interleave
    'viper':    [1, 1, 1, 1, 1, 1, 1],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    # disabled pflush workers
    'bonsai':   [1, 1, 6170.54, 9036.94, 1, 1, 1],
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
