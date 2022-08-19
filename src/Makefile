CUR_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

include $(CUR_DIR)/../Makefile.inc

#SANITIZER_FLAG = -fsanitize=address

# - nvm configuration
FLAGS += -DTS_NVM_IS_PMDK

FLAGS += -I$(INC_DIR)
FLAGS += -march=native -mtune=native -mcmodel=medium -O3
FLAGS += -shared -fPIC
FLAGS += -g -ggdb3
FLAGS += -Wall -Wextra -Wshadow -Wdeclaration-after-statement
FLAGS += -Wno-attributes -Wno-unused-parameter -Wno-unused-value -Wno-strict-aliasing
FLAGS += -Wno-unused-function
FLAGS += ${LOCAL}
FLAGS += $(SANITIZER_FLAG)

CFLAGS += $(FLAGS)
CFLAGS += -std=gnu99

CXXFLAGS += $(FLAGS)
CXXFLAGS += -std=gnu++14

LDFLAGS += -shared -lpmem -lpmemobj -lnuma -ljemalloc
LDFLAGS += -lrt -lm -lpthread -lstdc++
LDFLAGS += $(SANITIZER_FLAG)

C_DEPS_DIR  := $(CUR_DIR)/.c.deps
C_DEPFLAGS = -MD -MF $(C_DEPS_DIR)/$*.d -MP

CXX_DEPS_DIR  := $(CUR_DIR)/.cxx.deps
CXX_DEPFLAGS = -MD -MF $(CXX_DEPS_DIR)/$*.d -MP

C_SRC_FILES = $(wildcard *.c)
C_OBJS_DIR  = $(CUR_DIR)/.c.objs
C_OBJ_FILES = $(addprefix $(C_OBJS_DIR)/, $(C_SRC_FILES:.c=.o))

CXX_SRC_FILES = $(wildcard *.cc)
CXX_OBJS_DIR  = $(CUR_DIR)/.cxx.objs
CXX_OBJ_FILES = $(addprefix $(CXX_OBJS_DIR)/, $(CXX_SRC_FILES:.cc=.o))

all: libbonsai.so

$(C_OBJS_DIR)/%.o: %.c $(C_DEPS_DIR) $(C_OBJS_DIR)
	$(CC) $(CFLAGS) $(C_DEPFLAGS) -c -o $@ $<

$(CXX_OBJS_DIR)/%.o: %.cc $(CXX_DEPS_DIR) $(CXX_OBJS_DIR)
	$(CXX) $(CXXFLAGS) $(CXX_DEPFLAGS) -c -o $@ $<

$(C_OBJS_DIR):
	$(Q)mkdir -p $(C_OBJS_DIR)

$(CXX_OBJS_DIR):
	$(Q)mkdir -p $(CXX_OBJS_DIR)

$(C_DEPS_DIR):
	$(Q)mkdir -p $(C_DEPS_DIR)

$(CXX_DEPS_DIR):
	$(Q)mkdir -p $(CXX_DEPS_DIR)

libbonsai.so: $(C_OBJ_FILES) $(CXX_OBJ_FILES)
	$(Q)/bin/rm -f $@
	$(CC) $^ $(LDFLAGS) -o $@

clean:
	$(Q)rm -f libbonsai.so
	$(Q)rm -rf $(CUR_DIR)/.c.deps*
	$(Q)rm -rf $(CUR_DIR)/.cxx.deps*
	$(Q)rm -rf $(CUR_DIR)/.c.objs*
	$(Q)rm -rf $(CUR_DIR)/.cxx.objs*
	$(Q)echo > $(CUR_DIR)/log
