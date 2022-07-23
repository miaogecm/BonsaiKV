CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

TARGETS = masstree

IDX_SRCS 	= $(PWD)/src/index.c 
TREE_SRCS   = $(PWD)/src/masstree.c
BENCH_SRCS	= $(PWD)/../bench/bench.c

all: $(TARGETS)

masstree: index.o tree.o bench.o $(LIB_DIR)/libbonsai.so $(COMPRESSOR_DIR)/libdecompressor.so
	$(CC) $^ $(LDFLAGS) -o $@

index.o: $(IDX_SRCS)
	$(CC) -c $(CFLAGS) $^ $(LDFLAGS) -o $@

tree.o: $(TREE_SRCS)
	$(CC) -c $(CFLAGS) $^ $(LDFLAGS) -o $@
	
bench.o: $(BENCH_SRCS)
	$(CC) -c $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	$(Q)rm -f $(TARGETS)
	$(Q)rm -f bench.o index.o tree.o
