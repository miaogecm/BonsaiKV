CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

TARGETS = splithash_dram

SRCS = $(PWD)/src/hash_set.c $(PWD)/src/hp.c $(PWD)/src/link_list.c $(PWD)/../bench/bench.c $(LIB_DIR)/libbonsai.a

all: $(TARGETS)

splithash_dram: $(SRCS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	$(Q)rm -f $(TARGETS)
