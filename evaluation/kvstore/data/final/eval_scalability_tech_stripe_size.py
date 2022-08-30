#!/bin/python3

# Evaluate indexing technique
# Use YCSB Workload E (Scan-most), 24BK + 8BV, uniform distribution
# 6 threads
# stripe size: 256/512/768/1024/1280

import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np

GiB = 1024 * 1024 * 1024
M = 1000000
SIZE_PER_THREAD = 5.0 * M

STRIPE_SIZE = [256, 512, 768, 1024, 1280]
LATENCY = [3.783, 3.230, 3.587, 3.866, 4.229]

THROUGHPUT = SIZE_PER_THREAD / np.array(LATENCY) / M

for ssize, throughput in zip(STRIPE_SIZE, THROUGHPUT):
    print('{},{}'.format(ssize, throughput))

x = np.array(STRIPE_SIZE)

w = 64

throughplt = plt.subplot()
q=throughplt.bar(x, THROUGHPUT, width=w, color='g', align='center')
throughplt.set_xticks(x)
throughplt.set_ylabel('Throughput (Mop/s)')
throughplt.set_xlabel('Thread Number')

plt.subplots_adjust(hspace=.0)
plt.show()
