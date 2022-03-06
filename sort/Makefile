SRC_DIR = $(PWD)

SRCS = $(SRC_DIR)/list_sort.c $(SRC_DIR)/fastsort.c

TARGETS = sort

all: $(TARGETS)

CC = gcc

CFLAGS = -g -O3 -std=gnu99

LIBS = -pthread

sort: $(SRCS) Makefile
	$(CC) $(CFLAGS) $(SRCS) -o $@ $(LIBS)

clean:
	rm -f $(TARGETS)
