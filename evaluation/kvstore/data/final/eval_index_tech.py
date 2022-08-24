#!/bin/python3

# Evaluate indexing technique
# Use YCSB Workload C (Read-only), 24BK + 8KV, uniform distribution
# threads: 1/12/24/36/48

import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy

SIZE_PER_THREAD = 10

THREADS = [1, 12, 24, 36, 48]
LATENCY = [
    # Bonsai-DI  +offload  +upload
    [1, 2, 3],
    [4, 5, 6],
    [7, 8, 9],
    [10, 11, 12],
    [13, 14, 15],
]
DRAMUSAGE = [
    # Bonsai-DI  +offload  +upload
    [1, 2, 3],
    [4, 5, 6],
    [7, 8, 9],
    [10, 11, 12],
    [13, 14, 15],
]

THROUGHPUT = [list(map(lambda x: SIZE_PER_THREAD * thread / x, latencies)) for thread, latencies in zip(THREADS, LATENCY)]
print(THROUGHPUT)

x = numpy.array(THREADS)
y = THROUGHPUT

w = 3

gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1])

throughplt = plt.subplot(gs[0])
p=throughplt.bar(x-w, [t[0] for t in y], width=w, color='b', align='center')
q=throughplt.bar(x, [t[1] for t in y], width=w, color='g', align='center')
r=throughplt.bar(x+w, [t[2] for t in y], width=w, color='r', align='center')
throughplt.set_xticks(x)
throughplt.set_ylabel('Throughput (Mop/s)')
throughplt.set_xlabel('Thread Number')
throughplt.legend((p, q, r), ('Bonsai-DI', '+offload', '+upload'))

bwplt = plt.subplot(gs[1], sharex=throughplt)
bwplt.bar(x-w, [t[0] for t in y], width=w, color='b', align='center')
bwplt.bar(x, [t[1] for t in y], width=w, color='g', align='center')
bwplt.bar(x+w, [t[2] for t in y], width=w, color='r', align='center')
bwplt.set_xticks(x)
bwplt.set_ylabel('DRAM Usage (KB)')

plt.subplots_adjust(hspace=.0)
plt.show()
