CC = gcc
CXX = g++
AR = ar
RM = rm

PROJ_DIR 	:= $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
SRC_DIR		:= $(PROJ_DIR)
C_OBJS_DIR 	:= $(PROJ_DIR)/.c.objs
C_SRC_FILES = $(wildcard $(SRC_DIR)/*.c)
C_OBJ_FILES = $(addprefix $(C_OBJS_DIR)/, $(patsubst %.c, %.o, $(notdir $(C_SRC_FILES))))
CXX_OBJS_DIR 	:= $(PROJ_DIR)/.cxx.objs
CXX_SRC_FILES = $(wildcard $(SRC_DIR)/*.cc)
CXX_OBJ_FILES = $(addprefix $(CXX_OBJS_DIR)/, $(patsubst %.cc, %.o, $(notdir $(CXX_SRC_FILES))))

FLAGS += -I$(INC_DIR)
FLAGS += -g3
FLAGS += -O3
FLAGS += -Wall
FLAGS += -fno-aggressive-loop-optimizations -Wno-unused-variable -Wno-unused-but-set-variable

CFLAGS += $(FLAGS)
CFLAGS += -std=gnu99

CXXFLAGS += $(FLAGS)
CXXFLAGS += -std=gnu++14

LDFLAGS += -L../src
LDFLAGS += -lpthread -ldl -ljemalloc -lstdc++ -lbonsai

all: example

$(C_OBJS_DIR)/%.o: $(SRC_DIR)/%.c $(C_OBJS_DIR)
	$(CC) $< $(CFLAGS) -c -o $@

$(CXX_OBJS_DIR)/%.o: $(SRC_DIR)/%.cc $(CXX_OBJS_DIR)
	$(CXX) $< $(CXXFLAGS) -c -o $@

$(C_OBJS_DIR):
	$(Q)mkdir -p $(C_OBJS_DIR)

$(CXX_OBJS_DIR):
	$(Q)mkdir -p $(CXX_OBJS_DIR)

example: $(C_OBJ_FILES) $(CXX_OBJ_FILES)
	$(Q)$(RM) -f $@
	$(CC) $^ $(LDFLAGS) -o $@

clean:
	$(Q)rm -f example
	$(Q)rm -rf $(C_OBJS_DIR)
	$(Q)rm -rf $(CXX_OBJS_DIR)
