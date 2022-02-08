SRC_DIR = $(PWD)

SRCS = $(SRC_DIR)/list_sort.c $(SRC_DIR)/sort.c

TARGETS = sort

all: $(TARGETS)

CC = gcc

CFLAGS = -g -O3

LIBS = -pthread

sort: $(SRCS) Makefile
	$(CC) $(CFLAGS) $(SRCS) -o $@ $(LIBS)

clean:
	rm -f $(TARGETS)
