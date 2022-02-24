CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

TARGETS = worehole

IDX_SRCS 	= $(PWD)/src/index.c 
BENCH_SRCS	= $(PWD)/../bench/bench.c

LDFLAGS += -lwh 

all: $(TARGETS)

worehole: index.o bench.o $(LIB_DIR)/libbonsai.a
	$(CC) $^ $(LDFLAGS) -o $@

index.o: $(IDX_SRCS)
	$(CC) -c $(CFLAGS) $^ $(LDFLAGS) -o $@
	
bench.o: $(BENCH_SRCS)
	$(CC) -c $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	$(Q)rm -f $(TARGETS)
	$(Q)rm -f bench.o index.o
