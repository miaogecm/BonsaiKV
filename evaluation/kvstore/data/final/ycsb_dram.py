#!/bin/python3

# YCSB DRAM overhead
# load size: 5M per thread
# key: 8B
# val: 8B
# measure: DRAM consumption

from prettytable import PrettyTable

KB2GB = 1024 * 1024
M = 1000000
MAX = 240 * M
SIZE = 2.5 * M
THREADS = [1, 24, 48]
DRAM = {
    # thread num: 1/24/48

    'dptree':   [8282, 147266, 365506],

    'fastfair': [0, 0, 0],

    'pactree':  [0, 0, 0],

    'listdb':   [1048576, 1048576, 1048576],

    'pacman':   [157031, 3768757, 7537514],

    'viper':   [216516, 5196392, 11103700],

    'bonsai':   [23088, 554112, 1108224]
}

t = PrettyTable(['KV', '1', '24', '48'])
for k, (t1, t2, t3) in DRAM.items():
    t.add_row([k, round(t1 / KB2GB, 2), round(t2 / KB2GB, 2), round(t3 / KB2GB, 2)])

print(t)
