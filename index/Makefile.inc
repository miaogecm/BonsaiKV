CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

CFLAGS += -I$(INC_DIR) -I$(CUR_DIR)/bench
CFLAGS += -march=native -mtune=native -O3
CFLAGS += -g -ggdb3
CFLAGS += -Wall -Wextra -Wshadow -Wdeclaration-after-statement
CFLAGS += -Wno-attributes -Wno-unused-parameter -Wno-unused-value
CFLAGS += -Wno-unused-function
CFLAGS += ${THRDS} ${MAXN}
CFLAGS += ${LOCAL}

LDFLAGS += -pthread -lpmem -lpmemobj -lnuma -lkvdata
LDFLAGS += -lrt -lm -lpthread -lstdc++