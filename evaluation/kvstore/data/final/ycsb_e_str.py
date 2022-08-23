#!/bin/python3

# YCSB-E
# load size: 5M per thread
# scan size: 0.5M per thread
# key: 24B
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

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [43.383, 26.230, 27.081, 27.631, 28.128, 37.564, 47.226, 39.862, 46.993],

    # 1 worker per node (default setting)
    'pactree':  [4.846, 5.073, 5.251, 5.640, 6.240, 26.108, 30.642, 35.776, 37.658],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    'bonsai':   [3.174, 3.652, 3.859, 4.130, 4.378, 11.997, 17.085, 23.558, 28.324]
}
DIMMRBW = {
    # thread num: 1/6/12/18/24/30/36/42/48

    # dm-stripe 2M-Interleave
    # LOG_BATCHING enabled, simulates FlatStore log batching (batch size: 512B)
    'pacman':   [0, 0, 0, 0, 0, 0, 0, 0, 0],

    # 1 worker per node (default setting)
    'pactree':  [0, 0, 0, 0, 0, 0, 0, 0, 0],

    # Nbg:Nfg = 1:4, at least 2 Nbg
    # disabled pflush workers
    'bonsai':   [0, 0, 0, 0, 0, 0, 0, 0, 0]
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
throughplt.plot(xs, THROUGHPUT['pacman'], markerfacecolor='none', marker='.', markersize=8, linestyle='-', linewidth=2, label='PACMAN', color='orange')
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
