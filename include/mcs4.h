#ifndef MCS4_H
#define MCS4_H

typedef struct {
    union {
        struct {
            uint16_t tid;
            uint16_t slot;
        };
        uint32_t val;
    };
} mcs4_t;

void mcs4_register_thread();
void mcs4_init(mcs4_t *lock);
void mcs4_lock(mcs4_t *lock);
void mcs4_unlock(mcs4_t *lock);

#endif //MCS4_H
