#!/usr/bin/python3

import matplotlib.pyplot as plt

data_files = ('data',)

for data_file in data_files:
    with open(data_file, 'r') as f:
        names = f.readline().split()
        x = range(len(names))

        while True:
            data = f.readline()
            if not data:
                break
            data = data.split()

        plt.plot(x, list(map(float, data[1:])), marker='o', linestyle='-', label=data[0])

plt.legend()
plt.xticks(x, names, rotation=45)
plt.xlabel('n_worker')
plt.ylabel('latency')
plt.show()
