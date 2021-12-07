CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

CFLAGS += -I$(INC_DIR)
CFLAGS += -march=native -mtune=native -O3
CFLAGS += -g -ggdb3
CFLAGS += -Wall -Wextra -Wshadow -Wdeclaration-after-statement
CFLAGS += -Wno-attributes -Wno-unused-parameter -Wno-unused-value
CFLAGS += -Wno-unused-function

LDFLAGS += -pthread -lpmem -lpmemobj -lnuma

SRCS = $(PWD)/kv.c $(LIB_DIR)/libbonsai.a 

TARGETS = kv

all: $(TARGETS)

kv: $(SRCS)
	$(CC) $(CFLAGS) $(SRCS) $(LDFLAGS) -o $@

clean:
	$(Q)rm -f $(TARGETS)
