#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct event_candidate {
    uint64_t key;
    uint64_t user;
    int64_t ingest;
    int64_t ts;
    uint32_t product_offset;
    uint32_t product_size;
    uint32_t cents;
};

uint64_t event_batch(const char* data, uint32_t size, struct event_candidate* output,
                     uint32_t capacity, int64_t start, int64_t end);


enum {
    direct_key_buckets = 512,
    direct_key_rows = 512,
    direct_person_buckets = 128,
    direct_person_rows = 2048
};

struct direct_key {
    uint64_t key;
    uint64_t time_ref;
};

struct direct_event {
    uint64_t person;
    uint64_t payload;
};

struct direct_alias {
    uint64_t hash;
    uint64_t person;
};

struct direct_state {
    uint32_t key_next[direct_key_buckets];
    uint32_t event_next[2 * direct_person_buckets];
    uint32_t key_blocks;
    uint32_t event_blocks;
};

uint64_t event_batch_direct(
    const char* data, uint32_t size, struct event_candidate* output, uint32_t output_capacity,
    int64_t start, int64_t end, int64_t ingest_base,
    struct direct_state* state, struct direct_key* keys, struct direct_event* events,
    uint16_t* key_owners, uint8_t* event_owners, uint32_t key_capacity, uint32_t event_capacity, uint8_t raw_users,
    const struct direct_alias* aliases, uint32_t alias_count, const uint64_t* filter, uint32_t filter_words);

#ifdef __cplusplus
}
#endif
