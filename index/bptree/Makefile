CC = gcc
CFLAGS = -g -O3
LDFLAG = -lpthread -lkvdata 

all: test

test: test.c bptree.c
	$(CC) $(CFLAGS) $^ $(LDFLAG) -o $@

clean:
	rm test