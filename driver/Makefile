CC = gcc
AR = ar
RM = rm

PROJ_DIR 	:= $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
INC_DIR 	:= $(PROJ_DIR)/include
SRC_DIR		:= $(PROJ_DIR)/src
OBJS_DIR 	:= $(PROJ_DIR)/.objs
SMP_DIR		:= $(PROJ_DIR)/sample

CFLAGS += -I$(INC_DIR)
CFLAGS += -g3
CFLAGS += -Wall
CFLAGS += -O2

LDFLAGS += 

all: libtpcc.a sample

sample: $(SMP_DIR)/main.c libtpcc.a
	sudo ln -sf $(PROJ_DIR)/libtpcc.a /usr/lib/libtpcc.a
	$(CC) $< $(CFLAGS) -ltpcc -o $@

$(OBJS_DIR)/%.o: $(SRC_DIR)/%.c $(OBJS_DIR)
	$(CC) $< $(CFLAGS) -c -o $@

$(OBJS_DIR):
	$(Q)mkdir -p $(OBJS_DIR)

libtpcc.a: $(OBJS_DIR)/*.o
	$(Q)$(RM) -f $@
	$(Q)$(AR) cru $@ $^

clean:
	$(Q)rm -f libbonsai.a
	$(Q)rm -rf $(OBJS_DIR)
	