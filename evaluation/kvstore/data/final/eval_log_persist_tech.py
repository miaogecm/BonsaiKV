#!/bin/python3

# Evaluate log persist technique
# preload size: 5M per thread
# update size: 5M per thread
# thread: 12 fg + 6 bg (on single socket)
# 8B KV
# comment out index
# packing size: 0B 256B 512B 1024B 2048B 4096B
# throughput / throughput with gc

import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy

M = 1000000

PACKING_SIZE = [0, 256, 512, 1024, 2048, 4096]
THROUGHPUT = numpy.array([
    # throughput, throughput with gc for each packing size
    [2435043.04, 1393967.82],
    [9451573.12, 6020238.98],
    [10349996.48, 8027338.70],
    [10988867.17, 9783683.75],
    [12097121.73, 11482270.22],
    [12535130.43, 12465580.58],
])

# normalize throughput
THROUGHPUT = THROUGHPUT / THROUGHPUT.max()

for ps, (t, tgc) in zip(PACKING_SIZE, THROUGHPUT):
    print('{},{},{}'.format(ps, t, tgc))

x = numpy.array([0, 1, 2, 3, 4, 5])
y = THROUGHPUT

w = 0.4

throughplt = plt.subplot()
p=throughplt.bar(x-w/2, [t[0] for t in y], width=w, color='b', align='center')
q=throughplt.bar(x+w/2, [t[1] for t in y], width=w, color='g', align='center')
throughplt.set_xticks(x, PACKING_SIZE)
throughplt.set_ylabel('Normalized throughput (Mop/s)')
throughplt.set_xlabel('Packing size (B)')
throughplt.legend((p, q), ('Throughput', 'Throughput with GC'))

plt.show()
