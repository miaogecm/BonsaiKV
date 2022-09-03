#!/bin/python3
import numpy
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

N = 5000000
S = 2
B = 4

PS = numpy.array((0, 33, 66, 100))

PROTOCOLS = (
    ('MESI', -5, '#E69F00', lambda p: (1 - p) * N, lambda p: (S - 1) * p * N, lambda p: 0,                   lambda p: 0),
    ('SMR',   0, '#56B4E9', lambda p: 0,           lambda p: 0,               lambda p: (S - 1) * B * p * N, lambda p: 0),
    ('WoSI', +5, '#009E73', lambda p: (1 - p) * N, lambda p: 0,               lambda p: 0,                   lambda p: 0)
)

for name, off, color, calc_local_dram, calc_remote_dram, calc_local_nvm, calc_remote_nvm in PROTOCOLS:
    local_dram = calc_local_dram(PS / 100)
    remote_dram = calc_remote_dram(PS / 100)
    local_nvm = calc_local_nvm(PS / 100)
    remote_nvm = calc_remote_nvm(PS / 100)

    plt.bar(PS + off, local_dram, width=5, label=name, color=color, hatch='///')
    plt.bar(PS + off, remote_dram, width=5, label=name, bottom=local_dram, color=color, hatch='\\\\\\')
    plt.bar(PS + off, local_nvm, width=5, label=name, bottom=(remote_dram + local_dram), color=color, hatch='ooo')
    plt.bar(PS + off, remote_nvm, width=5, label=name, bottom=(local_nvm + remote_dram + local_dram), color=color, hatch='xxx')

plt.xticks(PS)
plt.ylabel('protocol overhead (lines)')
plt.xlabel('update ratio (%)')

b = mpatches.Patch(color='#E69F00', label='MESI')
c = mpatches.Patch(color='#56B4E9', label='SMR')
d = mpatches.Patch(color='#009E73', label='WoSI')
p = mpatches.Patch(facecolor='white', hatch='///', label='Local DRAM Access')
q = mpatches.Patch(facecolor='white', hatch='\\\\\\', label='Remote DRAM Access')
r = mpatches.Patch(facecolor='white', hatch='ooo', label='Local NVM Access')
s = mpatches.Patch(facecolor='white', hatch='xxx', label='Remote NVM Access')
plt.legend(handles=[b, c, d, p, q, r, s], loc=2)

plt.show()
