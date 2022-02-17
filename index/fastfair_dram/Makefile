CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

TARGETS = fastfair_dram

FF_SRCS 	= $(PWD)/src/ff.cpp
BENCH_SRCS	= $(PWD)/../bench/bench.c

all: $(TARGETS)

fastfair_dram: ff.o bench.o $(LIB_DIR)/libbonsai.a
	$(CXX) $^ $(LDFLAGS) -o $@

ff.o: $(FF_SRCS)
	$(CXX) -c $(CFLAGS) -std=c++11 $^ $(LDFLAGS) -o $@

bench.o: $(BENCH_SRCS)
	$(CC) -c $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	$(Q)rm -f $(TARGETS)
	$(Q)rm -f bench.o ff.o
