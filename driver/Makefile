CC = gcc
AR = ar
RM = rm

PROJ_DIR 	:= $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
INC_DIR 	:= $(PROJ_DIR)/include
SRC_DIR		:= $(PROJ_DIR)/src
OBJS_DIR 	:= $(PROJ_DIR)/.objs
SMP_DIR		:= $(PROJ_DIR)/sample
SRC_FILES = $(wildcard $(SRC_DIR)/*.c)
OBJ_FILES = $(addprefix $(OBJS_DIR)/, $(patsubst %.c, %.o, $(notdir $(SRC_FILES))))

CFLAGS += -I$(INC_DIR)
CFLAGS += -g3
CFLAGS += -O3
CFLAGS += -Wall

LDFLAGS += -lpthread

all: sample.out

sample.out: $(SMP_DIR)/main.c libtpcc.a
	#sudo ln -sf $(PROJ_DIR)/libtpcc.a /usr/lib/libtpcc.a
	$(CC) $< $(CFLAGS) -ltpcc $(LDFLAGS) -o $@

$(OBJS_DIR)/%.o: $(SRC_DIR)/%.c $(OBJS_DIR)
	$(CC) $< $(CFLAGS) -c -o $@

$(OBJS_DIR):
	$(Q)mkdir -p $(OBJS_DIR)

libtpcc.a: $(OBJ_FILES)
	$(Q)$(RM) -f $@
	$(Q)$(AR) cru $@ $^

clean:
	$(Q)rm -f libtpcc.a
	$(Q)rm -rf $(OBJS_DIR)
	