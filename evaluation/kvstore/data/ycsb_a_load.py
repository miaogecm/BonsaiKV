#!/bin/python3

# YCSB-A Load
# size: 480M key: 8B val: 8B distribution: uniform

M = 1000000
SIZE = 480 * M
THREADS = [1, 8, 16, 24, 32, 40, 48]
LATENCY = {
    # thread num: 1/8/16/24/32/40/48

    # Nbg:Nfg = 1:2, at least 2 Nbg
    'dptree':   [403.7, 185.066, 150.930, 139.017, 130.152, 127.745, 127.129],

    'fastfair': [1457.413, 201.052, 107.171, 81.114, 70.346, 71.346, 87.691],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 2 Nbg
    'listdb':   [1271.163, 175.711, 96.981, 72.754, 61.885, 58.532, 56.878],

    'novelsm':  [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    # 1 worker per node
    'pactree':  [907.30, 210.892, 119.683, 88.091, 73.175, 74.227, 68.063],

    'procksdb': [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    'slmdb':    [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    'viper':    [805.80, 91.209, 49.217, 38.965, 37.387, 30.221, 40.993],

    # Nbg:Nfg = 1:2, at least 2 Nbg
    'bonsai':   [215.8, 60.62, 37.806, 25.398, 20.525, 22.674, 24.024]
}

import numpy as np
import matplotlib.pyplot as plt
import csv

THROUGHPUT = {}
for kvstore, latencies in LATENCY.items():
    THROUGHPUT[kvstore] = list(map(lambda x: SIZE / x / M, latencies))

print ('dptree:')
print (THROUGHPUT['dptree'])
print ('fastfair1:')
print (THROUGHPUT['fastfair'])
print ('listdb:')
print (THROUGHPUT['listdb'])
print ('novelsm:')
print (THROUGHPUT['novelsm'])
print ('pactree:')
print (THROUGHPUT['pactree'])
print ('procksdb:')
print (THROUGHPUT['procksdb'])
print ('slmdb:')
print (THROUGHPUT['slmdb'])
print ('viper:')
print (THROUGHPUT['viper'])
print ('bonsai:')
print (THROUGHPUT['bonsai'])

xs = THREADS

plt.plot(xs, THROUGHPUT['dptree'], markerfacecolor='none', marker='s', markersize=8, linestyle='-', linewidth=2, label='DPTree')
plt.plot(xs, THROUGHPUT['fastfair'], markerfacecolor='none', marker='^', markersize=8, linestyle='-', linewidth=2, label='FastFair')
plt.plot(xs, THROUGHPUT['listdb'], markerfacecolor='none', marker='x', markersize=8, linestyle='-', linewidth=2, label='ListDB')
plt.plot(xs, THROUGHPUT['novelsm'], markerfacecolor='none', marker='D', markersize=8, linestyle='-', linewidth=2, label='NoveLSM')
plt.plot(xs, THROUGHPUT['pactree'], markerfacecolor='none', marker='<', markersize=8, linestyle='-', linewidth=2, label='PACTree')
plt.plot(xs, THROUGHPUT['procksdb'], markerfacecolor='none', marker='>', markersize=8, linestyle='-', linewidth=2, label='PMEM-RocksDB')
plt.plot(xs, THROUGHPUT['slmdb'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='SLM-DB')
plt.plot(xs, THROUGHPUT['viper'], markerfacecolor='none', marker='o', markersize=8, linestyle='-', linewidth=2, label='Viper')
plt.plot(xs, THROUGHPUT['bonsai'], markerfacecolor='none', marker='*', markersize=8, linestyle='-', linewidth=2, label='Bonsai')

font = {'size': '20','fontname': 'Times New Roman'}
plt.xlabel("Thread Number", font)
plt.ylabel("Throughput (M ops/s)", font)

font2 = {'size': '15','fontname': 'Times New Roman'}
ax = plt.gca()
ax.set_xticks(xs)
ax.set_xticklabels(xs, fontdict=font2)

ytick=[0,2,4,6,8,10,12,14,16,18,20,22,24]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
