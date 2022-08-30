#!/bin/python3

# Evaluate indexing technique
# Use YCSB Workload C (Read-only), 8BKV, uniform distribution
# 30*5000000 per thread
# threads: 1/6/12/18/24

import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np

GiB = 1024 * 1024 * 1024
M = 1000000
SIZE_PER_THREAD = 2.5 * M

THREADS = [1, 6, 12, 18, 24]
LATENCY = [
    # Bonsai-DI  +offload  +upload
    [2.395, 2.595, 1.871],
    [2.813, 3.671, 2.631],
    [2.982, 4.080, 2.875],
    [3.086, 4.362, 3.185],
    [3.210, 4.637, 3.277],
]
DRAMUSAGE = [
    # Bonsai-DI  +offload  +upload
    [160760248, 4729064, 14313056],
    [964733008, 32186400, 88956024],
    [1929270376, 94223432, 211161328],
    [2893829600, 246296568, 460087400],
    [3859207616, 297880568, 567411640],
]

THROUGHPUT = [list(map(lambda x: SIZE_PER_THREAD * thread / x / M, latencies)) for thread, latencies in zip(THREADS, LATENCY)]
for thread, (di, plusoff, plusup) in zip(THREADS, THROUGHPUT):
    print('{},{},{},{}'.format(thread, di, plusoff, plusup))

DRAMUSAGE = 30 * np.array(DRAMUSAGE) / GiB
for thread, (di, plusoff, plusup) in zip(THREADS, DRAMUSAGE):
    print('{},{},{},{}'.format(thread, di, plusoff, plusup))

x = np.array(THREADS)

w = 1

gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1])

throughplt = plt.subplot(gs[0])
p=throughplt.bar(x-w, [t[0] for t in THROUGHPUT], width=w, color='b', align='center')
q=throughplt.bar(x, [t[1] for t in THROUGHPUT], width=w, color='g', align='center')
r=throughplt.bar(x+w, [t[2] for t in THROUGHPUT], width=w, color='r', align='center')
throughplt.set_xticks(x)
throughplt.set_ylabel('Throughput (Mop/s)')
throughplt.set_xlabel('Thread Number')
throughplt.legend((p, q, r), ('Bonsai-DI', '+offload', '+upload'))

bwplt = plt.subplot(gs[1], sharex=throughplt)
bwplt.bar(x-w, [t[0] for t in DRAMUSAGE], width=w, color='b', align='center')
bwplt.bar(x, [t[1] for t in DRAMUSAGE], width=w, color='g', align='center')
bwplt.bar(x+w, [t[2] for t in DRAMUSAGE], width=w, color='r', align='center')
bwplt.set_xticks(x)
bwplt.set_ylabel('DRAM Usage (GiB)')

plt.subplots_adjust(hspace=.0)
plt.show()
