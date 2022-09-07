#!/bin/python3
import numpy
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

N = 5000000
S = 2
B = 4
MISS_RATE = 0.2

PS = numpy.array((0, 33, 66, 100))

PROTOCOLS = (
    #                       local_dram_read,       local_dram_write,                  remote_dram_write,         local_nvm_write,
    ('WI',   -5, '#E69F00', lambda p: (1 - p) * N, lambda p: MISS_RATE * (1 - p) * N, lambda p: (S - 1) * p * N, lambda p: 0,                 ),
    ('NR',    0, '#56B4E9', lambda p: 0,           lambda p: 0,                       lambda p: 0,               lambda p: (S - 1) * B * p * N),
    ('WoSI', +5, '#009E73', lambda p: (1 - p) * N, lambda p: MISS_RATE * (1 - p) * N, lambda p: 0,               lambda p: 0,                 )
)

for name, off, color, calc_local_dram_read, calc_local_dram_write, calc_remote_dram_write, calc_local_nvm_write in PROTOCOLS:
    print('{}:'.format(name))
    for p in PS:
        print('{},{},{},{},{}'.format(p, calc_local_dram_read(p / 100), calc_local_dram_write(p / 100), calc_remote_dram_write(p / 100), calc_local_nvm_write(p / 100)))

for name, off, color, calc_local_dram_read, calc_local_dram_write, calc_remote_dram_write, calc_local_nvm_write in PROTOCOLS:
    local_dram_read = calc_local_dram_read(PS / 100)
    local_dram_write = calc_local_dram_write(PS / 100)
    remote_dram_write = calc_remote_dram_write(PS / 100)
    local_nvm_write = calc_local_nvm_write(PS / 100)

    plt.bar(PS + off, local_dram_read, width=5, label=name, color=color, hatch='///')
    plt.bar(PS + off, local_dram_write, width=5, label=name, bottom=local_dram_read, color=color, hatch='\\\\\\')
    plt.bar(PS + off, remote_dram_write, width=5, label=name, bottom=(local_dram_write + local_dram_read), color=color, hatch='ooo')
    plt.bar(PS + off, local_nvm_write, width=5, label=name, bottom=(remote_dram_write + local_dram_write + local_dram_read), color=color, hatch='xxx')

plt.xticks(PS)
plt.ylabel('protocol overhead (lines)')
plt.xlabel('update ratio (%)')

b = mpatches.Patch(color='#E69F00', label='WI')
c = mpatches.Patch(color='#56B4E9', label='NR')
d = mpatches.Patch(color='#009E73', label='WoSI')
p = mpatches.Patch(facecolor='white', hatch='///', label='Local DRAM Read')
q = mpatches.Patch(facecolor='white', hatch='\\\\\\', label='Local DRAM Write')
r = mpatches.Patch(facecolor='white', hatch='ooo', label='Remote DRAM Write')
s = mpatches.Patch(facecolor='white', hatch='xxx', label='Local NVM Write')
plt.legend(handles=[b, c, d, p, q, r, s], loc=2)

plt.show()
