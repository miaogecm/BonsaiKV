# Run

### 配置

+ 每个numa节点的DATA region和LOG region：region.h。**需要保证LOG region大小是NUM_CPU_PER_SOCKET的倍数。**
+ mptable内存池大小：shim.h。
+ benchmark线程数量：bench.c。

### 挂载

```
sudo mount -t ext4 -o dax /dev/pmem0 /mnt/ext4/node0
sudo mount -t ext4 -o dax /dev/pmem2 /mnt/ext4/node1
sudo chown -R deadpool /mnt/ext4
sudo chgrp -R deadpool /mnt/ext4
```

