CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

CFLAGS += -I$(INC_DIR)
CFLAGS += -march=native -mtune=native -O3
CFLAGS += -g -ggdb3
CFLAGS += -Wall -Wextra -Wshadow -Wdeclaration-after-statement
CFLAGS += -Wno-attributes -Wno-unused-parameter -Wno-unused-value
CFLAGS += -Wno-unused-function
CFLAGS += ${THRDS} ${MAXN}
CFLAGS += ${LOCAL}

LDFLAGS += -pthread -lpmem -lpmemobj -lnuma -lkvdata 
LDFLAGS += -lrt -lm -lpthread -lstdc++

SRCS = $(PWD)/fast_fair.c $(PWD)/bench.c $(LIB_DIR)/libbonsai.a

TARGETS = kv

all: $(TARGETS)

kv: $(SRCS) ff.o
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

ff.o: ffwrapper.cpp
	g++ $(CFLAGS) -c $^ -std=c++11 -o $@

clean:
	$(Q)rm -f $(TARGETS)
	$(Q)rm -f ff.o
