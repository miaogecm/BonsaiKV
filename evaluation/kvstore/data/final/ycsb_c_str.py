#!/bin/python3

# YCSB-C
# load size: 0.5M per thread
# read size: 5M per thread
# key: 24B
# val: 1KB
# distribution: zipfian
# fsdax + dm-stripe (for NUMA-oblivious designs)
# measure: throughput/remotePMAccess(thread)

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
REMOTEACCESS = {
    # thread num: 1/8/16/24/32/40/48

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
throughplt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB', color='blue')
throughplt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='PACMAN', color='orange')
throughplt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='Viper', color='aqua')
throughplt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai', color='brown')

accplt = plt.subplot(gs[1], sharex=throughplt)
accplt.plot(xs, REMOTEACCESS['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='--', linewidth=2, label='ListDB', color='blue')
accplt.plot(xs, REMOTEACCESS['pacman'], markerfacecolor='none', marker='D', markersize=8, linestyle='--', linewidth=2, label='PACMAN', color='orange')
accplt.plot(xs, REMOTEACCESS['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='--', linewidth=2, label='Viper', color='aqua')
accplt.plot(xs, REMOTEACCESS['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='--', linewidth=2, color='brown')

font = {'size': '18','fontname': 'Times New Roman'}
font2 = {'size': '15','fontname': 'Times New Roman'}

throughplt.set_xlabel("Thread Number", font)
throughplt.set_xticks(xs)
throughplt.set_xticklabels(xs, fontdict=font2)

throughplt.set_ylabel("Throughput (M ops/s)", font)
ytick=[0, 1000, 2000, 3000, 4000, 5000, 6000]
#throughplt.set_yticks(ytick)
#throughplt.set_yticklabels(ytick, fontdict=font2)

accplt.set_ylabel("Remote PM Access (lines)", font)
ytick=[0, 1000, 2000, 3000, 4000, 5000, 6000]
#accplt.set_yticks(ytick)
#accplt.set_yticklabels(ytick, fontdict=font2)

throughplt.legend(loc = 2, ncol=4, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
plt.subplots_adjust(hspace=.0)
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
