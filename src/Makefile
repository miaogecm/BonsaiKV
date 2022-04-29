CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

# - nvm configuration
CFLAGS += -DTS_NVM_IS_PMDK

CFLAGS += -std=gnu99
CFLAGS += -I$(INC_DIR)
CFLAGS += -march=native -mtune=native -O0
CFLAGS += -g -ggdb3
CFLAGS += -Wall -Wextra -Wshadow -Wdeclaration-after-statement
CFLAGS += -Wno-attributes -Wno-unused-parameter -Wno-unused-value
CFLAGS += -Wno-unused-function
CFLAGS += ${LOCAL}
CFLAGS += -D PNODE_FP

LDFLAGS += -pthread -lpmem -lpmemobj -lnuma 

DEPS_DIR  := $(CUR_DIR)/.deps
DEPCFLAGS = -MD -MF $(DEPS_DIR)/$*.d -MP

SRC_FILES = $(wildcard *.c)
OBJS_DIR  = $(CUR_DIR)/.objs
OBJ_FILES = $(addprefix $(OBJS_DIR)/, $(SRC_FILES:.c=.o))

all: libbonsai.a

$(OBJS_DIR)/%.o: %.c $(DEPS_DIR) $(OBJS_DIR)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -c -o $@ $<

$(OBJS_DIR):
	$(Q)mkdir -p $(OBJS_DIR)

$(DEPS_DIR):
	$(Q)mkdir -p $(DEPS_DIR)

$(INC_DIR)/numa_config.h:
	$(Q)$(TOOLS_DIR)/topology.py > $@

libbonsai.a: $(OBJ_FILES)
	$(Q)/bin/rm -f $@
	$(Q)$(AR) cru $@ $^

clean:
	$(Q)rm -f libbonsai.a
	$(Q)rm -rf $(CUR_DIR)/.deps*
	$(Q)rm -rf $(CUR_DIR)/.objs*
	$(Q)echo > $(CUR_DIR)/log
	#$(Q)rm -f $(INC_DIR)/numa_config.h
