# Mapping table设计

## 简介

在Bonsai中，Index Layer和Mapping table共同构成了一个在DRAM中的索引结构，将key映射到一个Log或一个PNode中。作为这个索引结构的“叶子节点”，减小Mapping table本身的开销十分重要。

一个Mapping table主要支持查询和插入操作，并且当里面的项数达到一定阈值，会触发分裂操作。为了减少查询开销，新的Mapping table采用一个Key-value Array和一个Permutation Array作为其内部结构，并使用乐观并发控制避免查询时锁的获取和竞争的开销。此外，如果使用传统的“创建并移动”的方法实现分裂操作，会影响插入和查询的性能。因此提出fast split技术，实现开销可忽略不计的分裂操作。

## 数据结构

一个Mapping table可以存储14个键值对。包含一个Master table（MT）和一个Slave table（ST）。MT中存储8Byte的Permutation array，还有一个8Byte的maxk，表示该MT的上界。以及前7个键值对。ST中存储剩下的7个键值对。MT和ST各占两个缓存行。MT中包含一个指向ST的指针。

Permutation array采用类似于MassTree[1]中的组织方式。将64bit分为16个4bit，第一个4bit存储键值对的数量，剩余的15个4bit每个存储一个Key-value Array中的下标。

## 设计

首先，我们规定Mapping table的前7个键值对存储在MT中，后8个键值对存储在ST中。

### 插入操作

首先对节点加spinlock，写者间是互斥的。然后检查key是否小于上界，如果否，则插入失败，返回。

对于半满以内的节点，插入操作和传统的插入操作一样。

+ 扫描Permutation array，找到一个空位（必然在MT中）。
+ 在该空位写入键值。键值应写入
+ 修改Permutation array，并通过一个8B原子写，使插入操作原子地对读者可见。

而如果节点达到半满以上，首先将key与MT的最大key比较。如果大于，则仍然和传统的插入操作一样，只不过是插在ST中。

唯一区别是节点达到半满以上，且key小于MT的最大key时。此时需要把MT的最大key移动到ST中，然后再插入。插入时直接在MT的Key-value Array中复用原先最大key的位置即可。总的来说：

+ a. 把MT中的最大key复制到ST中的一个空位。

+ b. 8B原子写，更新Permutation array。

+ c. 将key和value覆盖掉MT中最大key原本占据的位置。

这样存在一个并发问题。如果在b和c之间出现读者，因为它读取到的是旧的Permutation array，而键值对已经被覆盖为新的数据，导致不一致。因此，b、c必须原子地发生。如果体系结构支持，可以使用一个DCAS指令来实现。而X86并不支持不连续双字的CAS，因此，解决方案如下：

+ a. 把MT中的最大key复制到ST中的一个空位。
+ b. 将原来key设置成HAZARD_TAG。

+ c. 8B原子写，更新Permutation array。

+ d. 将key和value覆盖掉MT中最大key原本占据的位置。先写value再原子地写key。

第一步是读者在读取完毕之后，再读一次Permutation array中的MT最大值下标，然后和最开始拿到的Permutation array中MT最大值下标比较，不一致则重试，以确保所有和写者并发的读者要么在b状态之前就已经全部完成，要么就从b状态之后开始。在读取的过程中，如果遇到key为HAZARD_TAG，则重新读该key，直到其不再是HAZARD_TAG。

### 分裂操作

Fast split技术用于实现低开销的分裂操作，其主要思想是在插入时，通过可以忽略不计的开销，维护大部分分裂所需要的信息。

当ST中也没有空位时，触发分裂。分裂操作如下：

+ a. 首先创建两个Slave table，称为ST1和ST2。
+ b. 将ST的slave指针指向ST2。并将原来MT的Permutation array的右半字复制到ST2的Permutation array的前半字。
+ c. 一个原子写，更新MT的上界，为ST中的最小值。
+ d. 一个原子写，将MT的右半字清空。
+ e. 将MT的slave指针调整到指向ST1

然后解锁MT，并将ST插入索引（此时，ST成为一个MT）。在解锁之前，上述操作都是一些小的指针调整，开销很小。对读者而言，在c之前相当于没分裂，一切和原来一样。而跨越c的读操作则有两种情况，如果是针对前半部分key的读，则可以顺利进行成功。如果是针对后半部分key的读，且未查到，才会阻塞。而插入操作，在解锁之后，如果是落在前半部分区间则可以顺利进行，而后半部分的话，则会在插入索引操作的可线性化点之前不断重试，在可线性化点之后顺利插入。

### 查询操作

+ a. 首先，从索引层查询到MT。
+ b. 获取Permutation array
+ c. 进行二分查找。查找过程中，如果遇到HAZARD_TAG，则循环重新读该key。
+ d. 查找结束后，检查Permutation array中的最大值下标是否一致，不一致则goto b（说明发生了往左半边的插入）
+ e. 如果找到，则返回成功。
+ f. 如果未找到，检查key是否小于上界，如果是，则返回未找到。否则goto a（说明发生了分裂）

## 优势

+ 分裂操作自身开销小，复杂度低。如果是传统的分裂操作，在进行时需要读取4个cacheline，然后写入2个cacheline。
+ 分裂操作对读者的影响少。首先，对于左半边的key，读取完全可以并行，不需要任何重试。对于右半边的key，等待的时间仅仅取决与把ST插入索引的时间。而MassTree中，分裂操作使用version方法来保证和读操作并发时的一致性，一旦分裂开始，读者需要一直重试，直到分裂结束。
+ 分裂操作对插入的阻塞时间短。在MassTree中，一个节点分裂后，要等分裂全部完成（包含移动key、插入索引层）之后，才能插入这两个节点中。而Fast split在几个指针调整之后，左半边的key立刻就可以插入了，而右半边的key等ST插入索引后，也可以继续插入了。

