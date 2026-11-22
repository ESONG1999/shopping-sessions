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

#ifdef __cplusplus
}
#endif
