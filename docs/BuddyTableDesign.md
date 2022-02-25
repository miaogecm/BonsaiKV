# Buddy Table Design

## 数据结构

```C
typedef uint32_t mptable_off_t;

struct mptable {
	/* Header - 5 words */
	unsigned int seq;
	union {
		// MT
		struct {
        	spinlock_t lock;
        	uint64_t pa[2];
        	pkey_t max;
            unsigned int pa_seq;
            struct mptable *slave;
        };
        // ST
        struct {
            struct mptable *next;
        	//struct pnode *pnode;
        };
    };
    /* Key signatures - 2 words */
    uint8_t signatures[12];
    /* Key-Value pairs - 24 words */
    kvpair_t entries[12];
};
```

每对buddy table通过Index Layer索引到。Index Layer的Value格式：

```c
struct buddy_table_off {
	mptable_off_t mt, st_hint;
}
```

对应于

```C
struct buddy_table {
    struct mptable *mt, *st_hint;
}
```

## Operations

### Prefetch

Buddy Table将会在访问Master和Slave Table之前将它们全部（或只取各自的第一个Cacheline，根据实验结果调整）预取到Cache当中。好处在于，原本访问是通过master->slave访问slave，也就是要先等master的miss，然后等slave的miss，现在利用MLP隐藏了一次miss。

因此，在Index Layer的Value中，除了存储真正的MT之外，还存储一个st_hint。大部分情况下，st_hint都对应了MT的ST。然而在某些并发的情况下，不一定成立，但难得几次Prefetch错误影响不大。

### Lookup

+ a. 首先，从索引层查询到Buddy Table。并进行Prefetch。
+ b. 获取Permutation array，注意检查pa_seq，中间变了的话要重试。
+ c. 线性查找（先从signature里面筛选一次再去entries里面找）。
+ d. 查找结束后，检查entry_seq，变了说明中间有Entry被覆盖了，重试。
+ e. 如果找到，则返回成功。
+ f. 如果未找到，检查key是否小于上界，如果是，则返回未找到。否则沿着next指针往后找到第一个上界大于等于自己的。

### Upsert

和之前基本一样。有两个区别：

+ 先写entry再改permutation array。

+ 如果是覆盖（也就是插入的地方本来已经有值在里面的时候），需要加seq。

### Remove

+ 只改Permutation array，不要把Entry置空！
+ 当节点中的最后一个entry被删除，必须按照下列顺序：
  + 首先从Index Layer取到前驱，然后将其加锁，并确认是否仍然是前驱。如果不是则重试（说明前驱正在分裂）。
  + 然后把前驱的max，改为和自身一样。
  + 将前驱的后继指针，指向自身的next。
  + 将自身放入Retire List列表。
  + Epoch Based Reclamation，防止有读者仍然在读一个已经retire掉的node

### Split

和之前基本一样，但是在后面加一个update，更新master的ST Hint。便于后面Prefetch。设置pnode的时候要用CAS指令。

## 与PNode协调

+ ~~每个Mptable对应一个PNode，称为Persistener。表示该Mptable内的数据由该PNode负责持久化。在ST中可以访问到每个Mptable对应的PNode。~~
+ ~~每个PNode对应多个Mptable，每个PNode里面存储指向最左Mptable的指针。~~

Range Update！

