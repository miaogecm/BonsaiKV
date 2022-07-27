CC = gcc
AR = ar
RM = rm

PROJ_DIR 	:= $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
INC_DIR 	:= $(PROJ_DIR)/include
SRC_DIR		:= $(PROJ_DIR)/src
OBJS_DIR 	:= $(PROJ_DIR)/.objs
SRC_FILES = $(wildcard $(SRC_DIR)/*.c)
OBJ_FILES = $(addprefix $(OBJS_DIR)/, $(patsubst %.c, %.o, $(notdir $(SRC_FILES))))

CFLAGS += -I$(INC_DIR)
CFLAGS += -g3
CFLAGS += -O3
CFLAGS += -Wall
CFLAGS += -fno-aggressive-loop-optimizations -Wno-unused-variable -Wno-unused-but-set-variable

LDFLAGS += -lpthread -ldl

all: kvstore compress

$(OBJS_DIR)/%.o: $(SRC_DIR)/%.c $(OBJS_DIR)
	$(CC) $< $(CFLAGS) -c -o $@

$(OBJS_DIR):
	$(Q)mkdir -p $(OBJS_DIR)

kvstore: $(OBJ_FILES)
	$(Q)$(RM) -f $@
	$(CC) $(CFLAGS) -o $@ $^

compress: compressor/compressor.c
	$(CC) $(CFLAGS) -o $@ $^

clean:
	$(Q)rm -f kvstore
	$(Q)rm -f compress
	$(Q)rm -rf $(OBJS_DIR)
