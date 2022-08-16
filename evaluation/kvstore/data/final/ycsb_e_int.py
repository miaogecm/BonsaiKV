#!/bin/python3

# YCSB-E
# load size: 5M per thread
# scan size: 0.5M per thread
# key: 8B
# val: 8B
# distribution: uniform
# fsdax + dm-stripe (for NUMA-oblivious designs)
# measure: throughput/DIMMRBW(thread)

M = 1000000
MAX = 240 * M
SIZE = 0.5 * M
THREADS = [1, 8, 16, 24, 32, 40, 48]
LATENCY = {
    # thread num: 1/8/16/24/32/40/48

    'dptree':   [2.818, 8.043, 20.090, 31.996, 44.304, 52.824, 65.354],

    'fastfair': [5.083, 8.654, 10.994, 15.638, 26.681, 36.599, 46.999],

    'pacman':   [2.156, 4.324, 4.640, 4.969, 8.090, 22.547, 48.724],

    # 1 worker per node (default setting)
    'pactree':  [3.393, 5.005, 4.984, 6.044, 6.980, 7.737, 10.433],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [1.577, 3.152, 3.597, 3.434, 3.868, 4.093, 5.669]
}
DIMMRBW = {
    # thread num: 1/8/16/24/32/40/48

    # 1 worker per node (default setting)
    'pactree':  [2000, 2000, 2000, 2000, 2000, 2000, 2000],

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
throughplt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='PACTree', color='aqua')
throughplt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai', color='brown')

rbwplt = plt.subplot(gs[1], sharex=throughplt)
rbwplt.plot(xs, DIMMRBW['pactree'], markerfacecolor='none', marker='o', markersize=8, linestyle='--', linewidth=2, label='PACTree', color='aqua')
rbwplt.plot(xs, DIMMRBW['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='--', linewidth=2, color='brown')

font = {'size': '18','fontname': 'Times New Roman'}
font2 = {'size': '15','fontname': 'Times New Roman'}

throughplt.set_xlabel("Thread Number", font)
throughplt.set_xticks(xs)
throughplt.set_xticklabels(xs, fontdict=font2)

throughplt.set_ylabel("Throughput (M ops/s)", font)
ytick=[0, 1000, 2000, 3000, 4000, 5000, 6000]
#throughplt.set_yticks(ytick)
#throughplt.set_yticklabels(ytick, fontdict=font2)

rbwplt.set_ylabel("DIMM Read BW (B/s)", font)
ytick=[0, 1000, 2000, 3000, 4000, 5000, 6000]
#rbwplt.set_yticks(ytick)
#rbwplt.set_yticklabels(ytick, fontdict=font2)

throughplt.legend(loc = 2, ncol=4, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
plt.subplots_adjust(hspace=.0)
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
