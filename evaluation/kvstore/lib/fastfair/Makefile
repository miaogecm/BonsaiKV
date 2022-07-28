.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -pthread -lpmemobj
INCLUDES=-I./include
CFLAGS=-O3 -std=c++11 -g

output = libfastfair.so

all: libfastfair.so

libfastfair.so: adapter.cpp btree.h
	g++ -shared -fPIC $(CFLAGS) -o $@ $^ $(LIBS) -DCONCURRENT

clean: 
	rm $(output)
