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
THREADS = [1, 6, 12, 18, 24, 30, 36, 42, 48]
LATENCY = {
    # thread num: 1/6/12/18/24/30/36/42/48

    'dptree':   [1.914, 2.222, 2.499, 3.112, 3.903, 25.401, 31.508, 37.077, 42.786],

    'fastfair': [24.754, 29.406, 28.550, 29.932, 32.693, 59.221, 90.554, 119.231, 145.943],

    'pacman':   [10.868, 13.169, 14.189, 15.201, 15.248, 33.186, 34.338, 35.470, 36.912],

    # 1 worker per node (default setting)
    'pactree':  [3.140, 3.473, 3.696, 3.925, 4.254, 7.595, 12.812, 16.329, 21.449],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [2.030, 2.178, 2.249, 2.469, 2.521, 5.135, 8.744, 14.289, 17.53]
}
DIMMRBW = {
    # thread num: 1/6/12/18/24/30/36/42/48

    # 1 worker per node (default setting)
    'pactree':  [0, 0, 0, 0, 0, 0, 0, 0, 0],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    # disabled pflush workers
    'bonsai':   [0, 0, 0, 0, 0, 0, 0, 0, 0],
}

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec
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

gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1])

throughplt = plt.subplot(gs[0])
throughplt.plot(xs, THROUGHPUT['dptree'], markerfacecolor='none', marker='o', markersize=12, linestyle='-', linewidth=2, label='DPTree', color='orange')
throughplt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='o', markersize=12, linestyle='-', linewidth=2, label='PACTree', color='aqua')
throughplt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=12, linestyle='-', linewidth=2, label='Bonsai', color='brown')
throughplt.plot(xs, THROUGHPUT['fastfair'], markerfacecolor='none', marker='.', markersize=12, linestyle='-', linewidth=2, label='FAST & FAIR', color='green')
throughplt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='o', markersize=12, linestyle='-', linewidth=2, label='PACMAN', color='blue')

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
