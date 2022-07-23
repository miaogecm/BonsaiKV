all: compressor libdecompressor.so

compressor: compressor.c
	gcc -o $@ $^

libdecompressor.so: decompressor.c
	gcc -shared -o $@ $^

.PHONY: clean
clean:
	rm compressor
	rm libdecompressor.so
