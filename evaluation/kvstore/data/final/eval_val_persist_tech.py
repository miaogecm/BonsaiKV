#!/bin/python3

# Evaluate value persist technique
# YCSB Load
# load size: 240K per thread
# string key/value (24B key + 16KB value)
# thread: 1, 6, 12, 18, 24
# staging size: 256B, 512B, 1KB
# target: one DIMM
# measure: throughput, pmmwbw

M = 1000000
K = 1000
MAX = 5760 * K
SIZE = 240 * K
THREADS = [1, 6, 12, 18, 24]
LATENCY = {
    # thread num: 1/6/12/18/24
    '256B':    [3.524, 10.114, 20.158, 30.481, 41.293],
    '512B':    [2.052, 10.358, 20.311, 33.478, 59.334],
    '1024B':   [2.088, 10.170, 26.532, 50.726, 76.653],
    'wo':      [2.258, 22.857, 50.196, 81.558, 115.827],
}
PMMWBW = {
    # thread num: 1/6/12/18/24
    '256B':    [1119.35, 2349.79, 2343.50, 2338.85, 2300.41],
    '512B':    [1872.70, 2353.37, 2332.33, 2008.32, 1606.29],
    '1024B':   [1890.77, 2337.84, 1803.92, 1400.27, 1230.34],
    'wo':      [1867.89, 1048.54, 943.43, 871.23, 803.20],
}

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec
import csv

THROUGHPUT = {}
for ssize, latencies in LATENCY.items():
    THROUGHPUT[ssize] = list(map(lambda x: SIZE * THREADS[x[0]] / x[1] / K, enumerate(latencies)))

MAPPING = {}
HEADER = ['thread']
for ssize, latencies in THROUGHPUT.items():
    HEADER.append(ssize)
    for thread, latency in zip(THREADS, latencies):
        if thread not in MAPPING:
            MAPPING[thread] = []
        MAPPING[thread].append(latency)

print(','.join(HEADER))
for thread, latencies in MAPPING.items():
    print(','.join(map(lambda x: str(round(x, 2)), [thread] + latencies)))

MAPPING = {}
HEADER = ['thread']
for ssize, latencies in PMMWBW.items():
    HEADER.append(ssize)
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
throughplt.plot(xs, THROUGHPUT['256B'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='256B', color='blue')
throughplt.plot(xs, THROUGHPUT['512B'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='512B', color='orange')
throughplt.plot(xs, THROUGHPUT['1024B'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='1024B', color='aqua')
throughplt.plot(xs, THROUGHPUT['wo'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='wo', color='green')

bwplt = plt.subplot(gs[1], sharex=throughplt)
bwplt.plot(xs, PMMWBW['256B'], markerfacecolor='none', marker='x', markersize=8, linestyle='--', linewidth=2, label='256B', color='blue')
bwplt.plot(xs, PMMWBW['512B'], markerfacecolor='none', marker='D', markersize=8, linestyle='--', linewidth=2, label='512B', color='orange')
bwplt.plot(xs, PMMWBW['1024B'], markerfacecolor='none', marker='o', markersize=8, linestyle='--', linewidth=2, label='1024B', color='aqua')
bwplt.plot(xs, PMMWBW['wo'], markerfacecolor='none', marker='o', markersize=8, linestyle='--', linewidth=2, label='wo', color='green')

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
