#!/usr/bin/python3


import matplotlib.pyplot as plt 


BASE_PATH = '/home/gky/Desktop/index-microbench-master/workloads'
LOAD_PATH = BASE_PATH + '/load_randint_workloada'
OP_PATH   = BASE_PATH + '/txn_randint_workloada'


def load_data():
    data = []
    with open(LOAD_PATH, 'r') as f:
        for op in f.readlines():
            cmd, k, v = op.split()
            data.append(k)
    return sorted(data, key=int)


def load_map(data):
    return { item: i for i, item in enumerate(data) }


def load_access(mp):
    access = []
    with open(OP_PATH, 'r') as f:
        for op in f.readlines():
            cmd, k = op.split()[:2]
            access.append(mp[k])
    return access


def main():
    data = load_data()
    mp = load_map(data)
    access = load_access(mp)
    plt.hist(access)
    #plt.plot(access)
    plt.show()

    
if __name__ == '__main__':
    main()
