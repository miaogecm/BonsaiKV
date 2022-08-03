#!/bin/python3

# YCSB-A Load
# size: 480M key: 8B val: 256B distribution: uniform

M = 1000000
SIZE = 480 * M
THREADS = [1, 8, 16, 24, 32, 40, 48]
LATENCY = {
    # thread num: 1/8/16/24/32/40/48

    # bottleneck:
    # Non log-structured: write amplification +
    # Log-structured: contention(BW) + metadata cacheline thrashing(perf L3 cache miss)

    # Nbg:Nfg = 1:2, at least 2 Nbg
    # dm-stripe 2M-Interleave
    # bottleneck: bloom filter array maintenance, WAL metadata cacheline thrashing, NUMA Awareness
    'dptree':   [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    # dm-stripe 2M-Interleave
    'fastfair': [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    # 1GB memtable, max number=4
    # Enabled 1GB lookup cache (979 MB hash-based, 45 MB second chance)
    # Nbg:Nfg = 1:2, at least 2 Nbg
    'listdb':   [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    'novelsm':  [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    # 1 worker per node
    'pactree':  [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    'procksdb': [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    'slmdb':    [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE],

    # dm-stripe 4K-Interleave
    # bottleneck: NVM contention
    'viper':    [959.27, 210.77, 164.45, 214.58, 212.96, 216.16, 211.55],

    # Nbg:Nfg = 1:2, at least 2 Nbg
    'bonsai':   [SIZE, SIZE, SIZE, SIZE, SIZE, SIZE, SIZE]
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

ytick=[0,2,4]
ax.set_yticks(ytick)
ax.set_yticklabels(ytick, fontdict=font2)

plt.legend(loc = 2, ncol=3, frameon=False, mode="expand", prop={'size': 14, 'family': 'Times New Roman'},handletextpad=0.2)

fig = plt.gcf()
fig.set_figwidth(5)
fig.set_figheight(4)
fig.tight_layout()
#fig.savefig('/home/miaogecm/Desktop/MWCM.pdf', dpi=1000)
plt.show()
