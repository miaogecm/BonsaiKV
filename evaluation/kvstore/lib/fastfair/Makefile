.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -pthread -lpmemobj
INCLUDES=-I./include
CFLAGS=-O0 -std=c++11 -g

output = libfastfair.so

all: libfastfair.so

libfastfair.so: adapter.cpp btree.h
	g++ $(CFLAGS) -o $@ $^ $(LIBS) -DCONCURRENT

clean: 
	rm $(output)
