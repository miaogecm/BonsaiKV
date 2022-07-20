#ifndef SCHEMA_TATP_H
#define SCHEMA_TATP_H

#include <bits/stdint-uintn.h>
#include <string.h>
#include <stdint-gcc.h>

enum {
    E_Subscriber = 1,
    E_Access_Info,
    E_Special_Facility,
    E_Call_Forwarding
};

static int numSubscriberValue = 33;
static int subscriberValueSizeArr[33] = {15 * sizeof(char),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint8_t),
                                       sizeof(uint32_t),
                                       sizeof(uint32_t)
};
struct Subscriber_k {
    int id;
    int col;
    uint64_t s_id;
}__attribute__((packed));
struct Subscriber_v {
    char sub_nbr[15];
    uint8_t bit_1;
    uint8_t bit_2;
    uint8_t bit_3;
    uint8_t bit_4;
    uint8_t bit_5;
    uint8_t bit_6;
    uint8_t bit_7;
    uint8_t bit_8;
    uint8_t bit_9;
    uint8_t bit_10;
    uint8_t hex_1;
    uint8_t hex_2;
    uint8_t hex_3;
    uint8_t hex_4;
    uint8_t hex_5;
    uint8_t hex_6;
    uint8_t hex_7;
    uint8_t hex_8;
    uint8_t hex_9;
    uint8_t hex_10;
    uint8_t byte2_1;
    uint8_t byte2_2;
    uint8_t byte2_3;
    uint8_t byte2_4;
    uint8_t byte2_5;
    uint8_t byte2_6;
    uint8_t byte2_7;
    uint8_t byte2_8;
    uint8_t byte2_9;
    uint8_t byte2_10;
    uint32_t msc_location;
    uint32_t vlr_location;
}__attribute__((packed));
struct Subscriber {
    struct Subscriber_k k;
    struct Subscriber_v v;
}__attribute__((packed));
enum {
    e_Subscriber_sub_nbr = 1,
    e_Subscriber_bit_1,
    e_Subscriber_bit_2,
    e_Subscriber_bit_3,
    e_Subscriber_bit_4,
    e_Subscriber_bit_5,
    e_Subscriber_bit_6,
    e_Subscriber_bit_7,
    e_Subscriber_bit_8,
    e_Subscriber_bit_9,
    e_Subscriber_bit_10,
    e_Subscriber_hex_1,
    e_Subscriber_hex_2,
    e_Subscriber_hex_3,
    e_Subscriber_hex_4,
    e_Subscriber_hex_5,
    e_Subscriber_hex_6,
    e_Subscriber_hex_7,
    e_Subscriber_hex_8,
    e_Subscriber_hex_9,
    e_Subscriber_hex_10,
    e_Subscriber_byte2_1,
    e_Subscriber_byte2_2,
    e_Subscriber_byte2_3,
    e_Subscriber_byte2_4,
    e_Subscriber_byte2_5,
    e_Subscriber_byte2_6,
    e_Subscriber_byte2_7,
    e_Subscriber_byte2_8,
    e_Subscriber_byte2_9,
    e_Subscriber_byte2_10,
    e_Subscriber_msc_location,
    e_Subscriber_vlr_location
};

static inline void setSubscriberKey(struct Subscriber_k *k, uint64_t s_id) {
    k->id = E_Subscriber;
    k->s_id = s_id;
}

static inline void
setSubscriberValue(struct Subscriber_v *v, char *sub_nbr, uint8_t bit_1, uint8_t bit_2, uint8_t bit_3, uint8_t bit_4,
                   uint8_t bit_5, uint8_t bit_6, uint8_t bit_7, uint8_t bit_8, uint8_t bit_9, uint8_t bit_10,
                   uint8_t hex_1, uint8_t hex_2, uint8_t hex_3, uint8_t hex_4, uint8_t hex_5, uint8_t hex_6,
                   uint8_t hex_7, uint8_t hex_8, uint8_t hex_9, uint8_t hex_10, uint8_t byte2_1, uint8_t byte2_2,
                   uint8_t byte2_3, uint8_t byte2_4, uint8_t byte2_5, uint8_t byte2_6, uint8_t byte2_7, uint8_t byte2_8,
                   uint8_t byte2_9, uint8_t byte2_10, uint32_t msc_location, uint32_t vlr_location) {
    memcpy(v->sub_nbr, sub_nbr, 15);
    v->bit_1 = bit_1;
    v->bit_2 = bit_2;
    v->bit_3 = bit_3;
    v->bit_4 = bit_4;
    v->bit_5 = bit_5;
    v->bit_6 = bit_6;
    v->bit_7 = bit_7;
    v->bit_8 = bit_8;
    v->bit_9 = bit_9;
    v->bit_10 = bit_10;
    v->hex_1 = hex_1;
    v->hex_2 = hex_2;
    v->hex_3 = hex_3;
    v->hex_4 = hex_4;
    v->hex_5 = hex_5;
    v->hex_6 = hex_6;
    v->hex_7 = hex_7;
    v->hex_8 = hex_8;
    v->hex_9 = hex_9;
    v->hex_10 = hex_10;
    v->byte2_1 = byte2_1;
    v->byte2_2 = byte2_2;
    v->byte2_3 = byte2_3;
    v->byte2_4 = byte2_4;
    v->byte2_5 = byte2_5;
    v->byte2_6 = byte2_6;
    v->byte2_7 = byte2_7;
    v->byte2_8 = byte2_8;
    v->byte2_9 = byte2_9;
    v->byte2_10 = byte2_10;
    v->msc_location = msc_location;
    v->vlr_location = vlr_location;
}

static int numAccessInfoValue = 4;
static int accessInfoValueSizeArr[] = { sizeof(uint8_t), sizeof(uint8_t), sizeof(char) * 3, sizeof(char) * 5};
struct Access_Info_k {
    int id;
    int col;
    uint64_t s_id;
    uint8_t ai_type;
}__attribute__((packed));
struct Access_Info_v {
    uint8_t data1;
    uint8_t data2;
    char data3[3];
    char data4[5];
}__attribute__((packed));
struct Access_Info {
    struct Access_Info_k k;
    struct Access_Info_v v;
}__attribute__((packed));
enum {
    e_Access_Info_data1 = 1,
    e_Access_Info_data2,
    e_Access_Info_data3,
    e_Access_Info_data4
};

static inline void setAccessInfoKey(struct Access_Info_k *k, uint64_t s_id, uint8_t ai_type) {
    k->id = E_Access_Info;
    k->s_id = s_id;
    k->ai_type = ai_type;
}

static inline void
setAccessInfoValue(struct Access_Info_v *v, uint8_t data1, uint8_t data2, char *data3, char *data4) {
    v->data1 = data1;
    v->data2 = data2;
    memcpy(v->data3, data3, 3);
    memcpy(v->data4, data4, 5);
}

static int numSpecialFacilityValue = 4;
static int specialFacilityValueSizeArr[] = { sizeof(uint8_t), sizeof(uint8_t), sizeof(uint8_t), sizeof(char) * 5};
struct Special_Facility_k {
    int id;
    int col;
    uint64_t s_id;
    uint8_t sf_type;
}__attribute__((packed));
struct Special_Facility_v {
    uint8_t is_active;
    uint8_t error_cntrl;
    uint8_t data_a;
    char data_b[5];
}__attribute__((packed));
struct Special_Facility {
    struct Special_Facility_k k;
    struct Special_Facility_v v;
}__attribute__((packed));
enum {
    e_Special_Facility_is_active = 1,
    e_Special_Facility_error_cntrl,
    e_Special_Facility_data_a,
    e_Special_Facility_data_b
};

static inline void setSpecialFacilityKey(struct Special_Facility_k *k, uint64_t s_id, uint8_t sf_type) {
    k->id = E_Special_Facility;
    k->s_id = s_id;
    k->sf_type = sf_type;
}

static inline void
setSpecialFacilityValue(struct Special_Facility_v *v, uint8_t is_active, uint8_t error_cntrl, uint8_t data_a,
                        char *data_b) {
    v->is_active = is_active;
    v->error_cntrl = error_cntrl;
    v->data_a = data_a;
    memcpy(v->data_b, data_b, 5);
}

static int numCallForwardValue = 2;
static int callForwardValueSizeArr[] = { sizeof(int), sizeof(char) * 15};
struct Call_Forwarding_k {
    int id;
    int col;
    uint64_t s_id;
    uint8_t sf_type;
    int start_time;
}__attribute__((packed));
struct Call_Forwarding_v {
    int end_time;
    char numberx[15];
}__attribute__((packed));
struct Call_Forwarding {
    struct Call_Forwarding_k k;
    struct Call_Forwarding_v v;
}__attribute__((packed));
enum {
    e_Call_Forwarding_end_time = 1,
    e_Call_Forwarding_numberx
};

static inline void
setCallForwardingKey(struct Call_Forwarding_k *k, uint64_t s_id, uint8_t sf_type, int start_time) {
    k->id = E_Call_Forwarding;
    k->s_id = s_id;
    k->sf_type = sf_type;
    k->start_time = start_time;
}

static inline void setCallForwardingValue(struct Call_Forwarding_v *v, int end_time, char *numberx) {
    v->end_time = end_time;
    memcpy(v->numberx, numberx, 15);
}

#endif