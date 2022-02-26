#ifndef LONG_KEY_H
#define LONG_KEY_H

#define KEY_MAX_LEN		32
struct long_key {
	char key[KEY_MAX_LEN];
	uint16_t len;	
};

#endif