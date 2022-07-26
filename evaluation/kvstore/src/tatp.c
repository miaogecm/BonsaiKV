/*
 * Implementation of seven transactions in TATP benchmark.
 */
 
#include "schema_tatp.h"
#include "stdio.h"
#include "rand.h"
#include "db.h"
#include "schema_tpcc.h"
#include "stdint-gcc.h"
#include "string.h"
#include "limit.h"
#include "config.h"
#include "loader.h"
#include "runner.h"

static int num_work;

static char allThePhoneNumber[NUM_SUBSCRIBER][15];

void tatp_loadSubscriber(struct kvstore *kvstore, void *tcontext, int start, int end) {
    struct Subscriber s;
//    char sub_nbr[15];
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
    uint64_t i;
    for (i = start + 1; i <= end; i++) {
        setSubscriberKey(&s.k, i);
        get_rand_int_str(allThePhoneNumber[i], 15, 16);
        bit_1 = get_rand(0, 255) & 1;
        bit_2 = get_rand(0, 255) & 1;
        bit_3 = get_rand(0, 255) & 1;
        bit_4 = get_rand(0, 255) & 1;
        bit_5 = get_rand(0, 255) & 1;
        bit_6 = get_rand(0, 255) & 1;
        bit_7 = get_rand(0, 255) & 1;
        bit_8 = get_rand(0, 255) & 1;
        bit_9 = get_rand(0, 255) & 1;
        bit_10 = get_rand(0, 255) & 1;
        hex_1 = get_rand(0, 15);
        hex_2 = get_rand(0, 15);
        hex_3 = get_rand(0, 15);
        hex_4 = get_rand(0, 15);
        hex_5 = get_rand(0, 15);
        hex_6 = get_rand(0, 15);
        hex_7 = get_rand(0, 15);
        hex_8 = get_rand(0, 15);
        hex_9 = get_rand(0, 15);
        hex_10 = get_rand(0, 15);
        byte2_1 = get_rand(0, 255);
        byte2_2 = get_rand(0, 255);
        byte2_3 = get_rand(0, 255);
        byte2_4 = get_rand(0, 255);
        byte2_5 = get_rand(0, 255);
        byte2_6 = get_rand(0, 255);
        byte2_7 = get_rand(0, 255);
        byte2_8 = get_rand(0, 255);
        byte2_9 = get_rand(0, 255);
        byte2_10 = get_rand(0, 255);
        msc_location = get_rand(1, UINT32_MAX);
        vlr_location = get_rand(1, UINT32_MAX);
        setSubscriberValue(&s.v, allThePhoneNumber[i], bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7, bit_8, bit_9,
                           bit_10, hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7, hex_8, hex_9, hex_10, byte2_1,
                           byte2_2, byte2_3, byte2_4, byte2_5, byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,
                           msc_location, vlr_location);
        db_insert(Subscriber, s.k, s.v, numSubscriberValue, subscriberValueSizeArr);
    }
}

void tatp_loadAccessInfo(struct kvstore *kvstore, void *tcontext, int start, int end) {
    struct Access_Info s;
    uint8_t data1;
    uint8_t data2;
    char data3[4];
    char data4[6];
    for (int i = start + 1; i <= end; i++) {
        int kinds = get_rand(1, 4);
        assert(kinds >= 1 && kinds <= 4);
        for (int j = 1; j <= kinds; j++) {
            setAccessInfoKey(&s.k, i, j);
            data1 = get_rand(0, 255);
            data2 = get_rand(0, 255);
            get_rand_str(data3, 3, 4);
            get_rand_str(data4, 5, 6);
            setAccessInfoValue(&s.v, data1, data2, data3, data4);
            db_insert(Access_Info, s.k, s.v, numAccessInfoValue, accessInfoValueSizeArr);
        }
    }
}

void tatp_loadSpecialFacility_CallForwarding(struct kvstore *kvstore, void *tcontext, int start, int end) {
    struct Special_Facility s;
    uint8_t is_active;
    uint8_t error_cntrl;
    uint8_t data_a;
    char data_b[6];
    for (int i = start + 1; i <= end; i++) {
        int sf_style = get_rand(1, 4);
        for (int j = 1; j <= sf_style; ++j) {
            setSpecialFacilityKey(&s.k, i, j);
            is_active = get_rand(0, 255) & 1;
            error_cntrl = get_rand(0, 255);
            data_a = get_rand(0, 255);
            get_rand_str(data_b, 5, 6);
            setSpecialFacilityValue(&s.v, is_active, error_cntrl, data_a, data_b);
            db_insert(Special_Facility, s.k, s.v, numSpecialFacilityValue, specialFacilityValueSizeArr);

            struct Call_Forwarding ss;
            int end_time;
            char numberx[16];
            int num_forward = get_rand(0, 3);
            assert(0 <= num_forward && num_forward <= 3);
            for (int k = 1; k <= num_forward; k++) {
                int startTime = get_rand(0, 3);
                if (startTime == 0) {
                    startTime = 0;
                } else if (startTime == 1) {
                    startTime = 8;
                } else if (startTime == 2) {
                    startTime = 16;
                }
                setCallForwardingKey(&ss.k, i, j, k);
                end_time = startTime + get_rand(1, 8);
                get_rand_int_str(numberx, 15, 16);
                setCallForwardingValue(&ss.v, end_time, numberx);
                db_insert(Call_Forwarding, ss.k, ss.v, numCallForwardValue, callForwardValueSizeArr);
            }
        }
    }
}

/* read transaction */
void getSubscriberData(struct kvstore *kvstore, void *tcontext) {
    /**
     SELECT s_id, sub_nbr,
        bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7,
        bit_8, bit_9, bit_10,
        hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7,
        hex_8, hex_9, hex_10,
        byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,
        byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,
        msc_location, vlr_location
    FROM Subscriber
    WHERE s_id = <s_id rnd>;
     */
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

    void *varr[33];
    int col_arr[33];
    size_t size_arr[33];
    set_v_arr(varr, 33, sub_nbr, &bit_1, &bit_2, &bit_3, &bit_4, &bit_5, &bit_6, &bit_7, &bit_8, &bit_9,
              &bit_10, &hex_1, &hex_2, &hex_3, &hex_4, &hex_5, &hex_6, &hex_7, &hex_8, &hex_9, &hex_10, &byte2_1,
              &byte2_2, &byte2_3, &byte2_4, &byte2_5, &byte2_6, &byte2_7, &byte2_8, &byte2_9, &byte2_10,
              &msc_location, &vlr_location);
    set_col_arr(col_arr, 33, e_Subscriber_sub_nbr, e_Subscriber_bit_1, e_Subscriber_bit_2, e_Subscriber_bit_3,
                e_Subscriber_bit_4, e_Subscriber_bit_5, e_Subscriber_bit_6, e_Subscriber_bit_7, e_Subscriber_bit_8,
                e_Subscriber_bit_9, e_Subscriber_bit_10, e_Subscriber_hex_1, e_Subscriber_hex_2, e_Subscriber_hex_3,
                e_Subscriber_hex_4, e_Subscriber_hex_5, e_Subscriber_hex_6, e_Subscriber_hex_7, e_Subscriber_hex_8,
                e_Subscriber_hex_9, e_Subscriber_hex_10, e_Subscriber_byte2_1, e_Subscriber_byte2_2,
                e_Subscriber_byte2_3, e_Subscriber_byte2_4, e_Subscriber_byte2_5, e_Subscriber_byte2_6,
                e_Subscriber_byte2_7, e_Subscriber_byte2_8, e_Subscriber_byte2_9, e_Subscriber_byte2_10,
                e_Subscriber_msc_location, e_Subscriber_vlr_location);
    set_size_arr(size_arr, 33, 15, sizeof(bit_1), sizeof(bit_2), sizeof(bit_3), sizeof(bit_4),
                 sizeof(bit_5), sizeof(bit_6), sizeof(bit_7), sizeof(bit_8), sizeof(bit_9), sizeof(bit_10),
                 sizeof(hex_1), sizeof(hex_2), sizeof(hex_3), sizeof(hex_4), sizeof(hex_5), sizeof(hex_6),
                 sizeof(hex_7), sizeof(hex_8), sizeof(hex_9), sizeof(hex_10), sizeof(byte2_1), sizeof(byte2_2),
                 sizeof(byte2_3), sizeof(byte2_4), sizeof(byte2_5), sizeof(byte2_6), sizeof(byte2_7),
                 sizeof(byte2_8), sizeof(byte2_9), sizeof(byte2_10), sizeof(msc_location), sizeof(vlr_location));
    struct Subscriber s;
    setSubscriberKey(&s.k, get_rand(0, NUM_SUBSCRIBER));
    db_lookup(Subscriber, s.k, 33, col_arr, size_arr, varr);
}

/* read transaction */
void getNewDestination(struct kvstore *kvstore, void *tcontext) {
    /**
     SELECT cf.numberx
    FROM Special_Facility AS sf, Call_Forwarding AS cf
    WHERE
        (sf.s_id = <s_id rnd>
        AND sf.sf_type = <sf_type rnd>
        AND sf.is_active = 1)
    AND (cf.s_id = sf.s_id
        AND cf.sf_type = sf.sf_type)
    AND (cf.start_time \<= <start_time rnd>
        AND <end_time rnd> \< cf.end_time);
     */
    /* scan is need */
    return;
}

/* read transaction */
void getAccessData(struct kvstore *kvstore, void *tcontext) {
    /**
    SELECT data1, data2, data3, data4
    FROM Access_Info
    WHERE s_id = <s_id rnd>
    AND ai_type = <ai_type rnd>
     */
    uint8_t data1 = 0;
    uint8_t data2 = 0;
    char data3[3] = {0, 0, 0};
    char data4[5] = {0, 0, 0, 0, 0};

    void *varr[4];
    int col_arr[4];
    size_t size_arr[4];
    set_v_arr(varr, 4, &data1, &data2, data3, data4);
    set_col_arr(col_arr, 4, e_Access_Info_data1, e_Access_Info_data2, e_Access_Info_data3, e_Access_Info_data4);
    set_size_arr(size_arr, 4, sizeof(data1), sizeof(data2), 3, 5);
    struct Access_Info a;
    setAccessInfoKey(&a.k, get_rand(1, NUM_SUBSCRIBER), get_rand(1, 4));
    db_lookup(Access_Info, a.k, 4, col_arr, size_arr, varr);
}

/* write transaction */
void updateSubscribeData(struct kvstore *kvstore, void *tcontext) {
    /**
    UPDATE Subscriber
    SET bit_1 = <bit_rnd>
    WHERE s_id = <s_id rnd subid>;
    UPDATE Special_Facility
    SET data_a = <data_a rnd>
    WHERE s_id = <s_id value subid>
    AND sf_type = <sf_type rnd>;
     */
    uint64_t s_id = get_rand(1, NUM_SUBSCRIBER);
    uint8_t new_bit_1 = get_rand(0, 255) & 1;
    uint8_t old_bit_1 = 255;
    void *varr[1];
    int col_arr[1];
    size_t size_arr[1];
    set_v_arr(varr, 1, &old_bit_1);
    set_col_arr(col_arr, 1, e_Subscriber_bit_1);
    set_size_arr(size_arr, 1, sizeof(new_bit_1));
    struct Subscriber s;
    setSubscriberKey(&s.k, s_id);
    db_lookup(Subscriber, s.k, 1, col_arr, size_arr, varr);
    if (old_bit_1 == 0 || old_bit_1 == 1) {
        set_v_arr(varr, 1, &new_bit_1);
        db_update(Subscriber, s.k, 1, col_arr, size_arr, varr);
    } else {
        kvstore->kv_txn_rollback(tcontext);
    }

    uint8_t old_data_a_1 = 0;
    uint8_t old_data_a_2 = 255;
    uint8_t new_data_a = get_rand(0, 255);
    set_v_arr(varr, 1, &old_data_a_1);
    set_col_arr(col_arr, 1, e_Special_Facility_data_a);
    set_size_arr(size_arr, 1, sizeof(new_data_a));
    struct Special_Facility sf;
    setSpecialFacilityKey(&sf.k, s_id, get_rand(1, 4));
    db_lookup(Special_Facility, sf.k, 1, col_arr, size_arr, varr);
    set_v_arr(varr, 1, &old_data_a_2);
    db_lookup(Special_Facility, sf.k, 1, col_arr, size_arr, varr);
    if (old_data_a_2 == old_data_a_1) {
        set_v_arr(varr, 1, &new_data_a);
        db_update(Special_Facility, sf.k, 1, col_arr, size_arr, varr);
    } else {
        kvstore->kv_txn_rollback(tcontext);
    }
}

/* write transaction */
void updateLocation(struct kvstore *kvstore, void *tcontext) {
    /**
    UPDATE Subscriber
    SET vlr_location = <vlr_location rnd>
    WHERE sub_nbr = <sub_nbr rndstr>;
     */
    /* scan is need */
}

/* write transaction */
void insertCallForwarding(struct kvstore *kvstore, void *tcontext) {
    /**
    SELECT <s_id bind subid s_id>
    FROM Subscriber
    WHERE sub_nbr = <sub_nbr rndstr>;

    SELECT <sf_type bind sfid sf_type>
    FROM Special_Facility
    WHERE s_id = <s_id value subid>:

    INSERT INTO Call_Forwarding
    VALUES (<s_id value subid>, <sf_type rnd sf_type>,
        <start_time rnd>, <end_time rnd>, <numberx rndstr>);
     */
   /* scan is need */
}

/* write transaction */
void deleteCallForwarding(struct kvstore *kvstore, void *tcontext) {
    /**
    SELECT <s_id bind subid s_id>
    FROM Subscriber
    WHERE sub_nbr = <sub_nbr rndstr>;

    DELETE FROM Call_Forwarding
    WHERE s_id = <s_id value subid>
    AND sf_type = <sf_type rnd>
    AND start_time = <start_time rnd>;
     */
   /* scan is need */
}

int tatp_work(struct kvstore *kvstore, void *tcontext, u32 t_id) {
//    int doWhich = get_rand(0, UINT32_MAX - 1);
//    double p = (double) doWhich / UINT32_MAX;
    kvstore->kv_txn_begin(tcontext);
//    if (p < 0.35) { 
    getSubscriberData(kvstore, tcontext);
//    } else if (p < 0.45) {
    getNewDestination(kvstore, tcontext);
//    } else if (p < 0.8) {
    getAccessData(kvstore, tcontext);
//    } else if (p < 0.82) {
    updateSubscribeData(kvstore, tcontext);
//    } else if (p < 0.96) {
    updateLocation(kvstore, tcontext);
//    } else if (p < 0.98) {
    insertCallForwarding(kvstore, tcontext);
//    } else {
    deleteCallForwarding(kvstore, tcontext);
//    }
    kvstore->kv_txn_commit(tcontext);

    return 0;
}

static const char* tatp_load_stage_fun(struct kvstore *kvstore, void *tcontext, int id) {
    if (id <= NUM_THREADS / 10) {
        int numThreadsInsertingSubscribers = NUM_THREADS / 10 + 1;
        int numSubscribersPerThread = NUM_SUBSCRIBER / numThreadsInsertingSubscribers;
        int start = id * numSubscribersPerThread;
        int end = (id + 1) * numSubscribersPerThread;
        tatp_loadSubscriber(kvstore, tcontext, start, end);
    } else if (NUM_THREADS / 10 < id && id <= NUM_THREADS / 10 + NUM_THREADS / 4) {
        int numThreadsInsertingSubscribers = NUM_THREADS / 4;
        int numSubscribersPerThread = NUM_SUBSCRIBER / numThreadsInsertingSubscribers;
        int newId = id - NUM_THREADS / 10 - 1;
        int start = newId * numSubscribersPerThread;
        int end = (newId + 1) * numSubscribersPerThread;
        tatp_loadAccessInfo(kvstore, tcontext, start, end);
    } else {
        int numThreadsInsertingSubscribers = NUM_THREADS - NUM_THREADS / 4 - NUM_THREADS / 10 - 1;
        int numSubscribersPerThread = NUM_SUBSCRIBER / numThreadsInsertingSubscribers;
        int newId = id - (NUM_THREADS / 10 + NUM_THREADS / 4) - 1;
        int start = newId * numSubscribersPerThread;
        int end = (newId + 1) * numSubscribersPerThread;
        tatp_loadSpecialFacility_CallForwarding(kvstore, tcontext, start, end);
    }

    return "load";
}

static const char* tatp_txn_stage_fun(struct kvstore *kvstore, void *tcontext, int id) {
    int num_works_left;

    num_works_left = num_work / NUM_THREADS;
    if (id == 0) {
        num_works_left += num_work % NUM_THREADS;
    }

    while(num_works_left--) {
		tatp_work(kvstore, tcontext, id);
    }

    return "txn";
}

void run_tatp(const char *kvlib, int num_work_) {
    const char *(*stage_func[])(struct kvstore *, void *, int) = { tatp_load_stage_fun, tatp_txn_stage_fun };
    struct kvstore kvstore;
    const char *engine;
    void *conf = NULL;
    num_work = num_work_;
    load_kvstore(&kvstore, kvlib);
    engine = kvstore.kv_engine();
    if (!strcmp(engine, "bonsai")) {
        struct bonsai_config *c = malloc(sizeof(*c));
        c->stm_support = 1;
        conf = c;
    }
    run_kvstore(&kvstore, conf, 2, stage_func);
    free(conf);
}
