// (C) 2013 by Troy Deck; see LICENSE.txt for details
#include "skiplist.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdio.h>

static inline k_cmp(sl_entry * entry, void* key, int len) {
    if (entry->k_len != len) {
        if (entry->k_len > len) 
            return 1;
        return -1;
    }
    return memcmp(entry->key, key, len);
}

int seeded = 0;

void sl_free_entry(sl_entry * entry);

// TODO have void functions (especially sl_set) return error codes and do proper
//      checking after allocations.

// Returns a random number in the range [1, max] following the geometric 
// distribution.
int grand (int max) {
    int result = 1;

    while (result < max && (random() > RAND_MAX / 2)) {
        ++ result;
    }

    return result;
}

// Returns a sentinel node representing the head node of a new skip list.
// Also seeds the random number generator the first time it is called.
sl_entry * sl_init() {
    // Seed the random number generator if we haven't yet
    if (!seeded) {
        srand((unsigned int) time(NULL));
        seeded = 1;
    }

    // Construct and return the head sentinel
    sl_entry * head = calloc(1, sizeof(sl_entry)); // Calloc will zero out next
    if (!head) return NULL; // Out-of-memory check
    head->height = MAX_SKIPLIST_HEIGHT;

    return head;
}

// Frees all nodes in the skiplist
void sl_destroy(sl_entry * head) {
    sl_entry * current_entry = head;
    sl_entry * next_entry = NULL;
    while (current_entry) {
        next_entry = current_entry->next[0];
        sl_free_entry(current_entry);
        current_entry = next_entry;
    }
}

// Searches for an entry by key in the skip list, and returns a copy of
// the associated value, or NULL if the key was not found.
char * sl_get(sl_entry * head, char * key, size_t len) {
    sl_entry * curr = head;
    int level = head->height - 1;
    
    // Find the position where the key is expected
    while (curr != NULL && level >= 0) {
        if (curr->next[level] == NULL) {
            -- level;
        } else {
            int cmp = k_cmp(curr->next[level], key, len);
            if (cmp == 0) { // Found a match
                return curr->next[level]->value; 
            } else if (cmp > 0) { // Drop down a level 
                -- level;
            } else { // Keep going at this level
                curr = curr->next[level];
            }
        }
    }
    // Didn't find it
    return NULL;
}

// Inserts copies of a key, value pair into the skip list,
// replacing the value associated with the key if it is already
// in the list.
void sl_set(sl_entry * head, char * key, char * value, size_t len, size_t v_len) {
    sl_entry * prev[MAX_SKIPLIST_HEIGHT];
    sl_entry * curr = head;
    int level = head->height - 1;

    // Find the position where the key is expected
    while (curr != NULL && level >= 0) {
        prev[level] = curr;
        if (curr->next[level] == NULL) {
            -- level;
        } else {
            int cmp = k_cmp(curr->next[level], key, len);
            if (cmp == 0) { // Found a match, replace the old value
                // free(curr->next[level]->value);
                // curr->next[level]->value = strdup(value);
                memcpy(curr->next[level]->value, value, v_len);
                return;
            } else if (cmp > 0) { // Drop down a level 
                -- level;
            } else { // Keep going at this level
                curr = curr->next[level];
            }
        }
    }

    // Didn't find it, we need to insert a new entry
    sl_entry * new_entry = malloc(sizeof(sl_entry));
    new_entry->height = grand(head->height);
    // new_entry->key = strdup(key);
    // new_entry->value = strdup(value);
    new_entry->key = (char*) malloc(len);
    new_entry->value = (char*) malloc(v_len);
    memcpy(new_entry->key, key, len);
    new_entry->k_len = len;
    memcpy(new_entry->value, value, v_len);
    int i;
    // Null out pointers above height
    for (i = MAX_SKIPLIST_HEIGHT - 1; i > new_entry->height; -- i) { 
        new_entry->next[i] = NULL;
    }
    // Tie in other pointers
    for (i = new_entry->height - 1; i >= 0; -- i) {
        new_entry->next[i] = prev[i]->next[i];
        prev[i]->next[i] = new_entry;
    }
}

// Frees the memory allocated for a skiplist entry.
void sl_free_entry(sl_entry * entry) {
    free(entry->key);
    entry->key = NULL;
    free(entry->value);
    entry->value = NULL;

    free(entry);
    entry = NULL;
}

// Removes a key, value association from the skip list.
void sl_unset(sl_entry * head, char * key, size_t len) {
    sl_entry * prev[MAX_SKIPLIST_HEIGHT];
    sl_entry * curr = head;
    int level = head->height - 1;

    // Find the list node just before the condemned node at every
    // level of the chain
    int cmp = 1;
    while (curr != NULL && level >= 0) {
        prev[level] = curr;
        if (curr->next[level] == NULL) {
            -- level;
        } else {
            cmp = k_cmp(curr->next[level], key, len);
            if (cmp >= 0) { // Drop down a level 
                -- level;
            } else { // Keep going at this level
                curr = curr->next[level];
            }
        }
    }

    // We found the match we want, and it's in the next pointer
    if (curr && !cmp) { 
        sl_entry * condemned = curr->next[0];
        // Remove the condemned node from the chain
        int i;
        for (i = condemned->height - 1; i >= 0; -- i) {
          prev[i]->next[i] = condemned->next[i];
        }
        // Free it
        sl_free_entry(condemned);
        condemned = NULL;
    }
}

int sl_scan(sl_entry * head, char * low, char * high, size_t* k_arr, size_t* v_arr, size_t len) {
    sl_entry * curr = head;
    int level = head->height - 1;
    int cnt = 0;

    // Find the position where the key is expected
    while (curr != NULL && level >= 1) {
        if (curr->next[level] == NULL) {
            -- level;
        } else {
            int cmp = k_cmp(curr->next[level], low, len);
            if (cmp > 0) { // Drop down a level 
                -- level;
            } else { // Keep going at this level
                curr = curr->next[level];
            }
        }
    }

    curr = curr->next[0];
    
    while(curr != NULL) {
        int cmp = k_cmp(curr, high, len);
        if (cmp > 0) {
            return cnt;
        }
        k_arr[cnt] = (size_t) curr->key;
        v_arr[cnt] = (size_t) curr->value;
        cnt++;
        curr = curr->next[0];
    }

    return cnt;
}
