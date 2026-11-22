#include "event_batch.h"

#include <bit>
#include <climits>
#include <cstring>
#include <immintrin.h>

namespace {

constexpr int64_t day_ms = 86400000;
constexpr uint64_t high = 0x8080808080808080;
constexpr uint64_t ones = 0x0101010101010101;

inline uint64_t load8(const char* data) {
    uint64_t word;
    std::memcpy(&word, data, sizeof(word));
    return word;
}

struct field {
    const char* ptr;
    uint32_t size;
};

uint64_t pack_id(field f) {
    if (f.size == 8 || f.size == 9) {
        uint64_t word = load8(f.ptr);
        if ((word | ((word - ones) & ~word)) & high) return 0;
        word = __builtin_bswap64(word);
        uint64_t pairs = (word & 0x7f007f007f007f00) >> 1 | (word & 0x007f007f007f007f);
        uint64_t halves = (pairs & 0x3fff00003fff0000) >> 2 | (pairs & 0x00003fff00003fff);
        uint64_t key = ((halves & 0x0fffffff00000000) >> 4 | (halves & 0x000000000fffffff)) << 7;
        if (f.size == 8) return key;
        unsigned char last = f.ptr[8];
        if (last == 0 || last >= 128) return 0;
        return key | last;
    }
    if (f.size > 9) return 0;
    uint64_t key = 0;
    for (uint32_t i = 0; i < f.size; i++) {
        unsigned char byte = f.ptr[i];
        if (byte == 0 || byte >= 128) return 0;
        key = (key << 7) | byte;
    }
    return key << (7 * (9 - f.size));
}

uint64_t encode_id(const char* data, field id) {
    if (uint64_t packed = pack_id(id)) return packed;
    // The Go caller limits each batch's input to at most 1 GiB, so the
    // high bit can mark a slice without hiding any length bits.
    return (uint64_t{1} << 63) | (static_cast<uint64_t>(id.size) << 32) |
           static_cast<uint32_t>(id.ptr - data);
}

int two_digits(const char* data) {
    unsigned a = static_cast<unsigned char>(data[0]) - '0';
    unsigned b = static_cast<unsigned char>(data[1]) - '0';
    return a <= 9 && b <= 9 ? static_cast<int>(10 * a + b) : -1;
}

int64_t floor_div(int64_t value, int64_t divisor) {
    int64_t q = value / divisor;
    return q - (value % divisor < 0);
}

struct month {
    uint64_t prefix = 0;
    int64_t start = 0;
    int days = 0;
};

month decode_month(const char* p, uint64_t prefix) {
    if (p[4] != '-' || p[7] != '-') return {};
    int century = two_digits(p);
    int yy = two_digits(p + 2);
    int m = two_digits(p + 5);
    if (century < 0 || yy < 0 || m < 1 || m > 12) return {};
    int year = century * 100 + yy;
    bool leap = year % 4 == 0 && (yy != 0 || century % 4 == 0);
    static constexpr int lengths[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    static constexpr int before[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int64_t complete = year - 1;
    int64_t days = complete * 365 + floor_div(complete, 4) - floor_div(complete, 100) +
                   floor_div(complete, 400) - 719162 + before[m - 1] + (leap && m > 2);
    return {prefix, days * day_ms, lengths[m - 1] + (leap && m == 2)};
}

struct clock_decoder {
    month cache[8];

    int64_t calendar(field f) {
        if (f.size != 20 && f.size != 25) return INT64_MIN;
        const char* p = f.ptr;
        uint64_t prefix = load8(p);
        month& m = cache[static_cast<unsigned char>(p[6]) & 7];
        if (m.days == 0 || m.prefix != prefix) {
            m = decode_month(p, prefix);
            if (m.days == 0) return INT64_MIN;
        }
        // The first eight bytes establish the month. Unpack the four
        // adjacent pairs for day, hour, minute, and second together.
        __m128i tail = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + 4));
        __m128i shuffled = _mm_shuffle_epi8(tail, _mm_setr_epi8(
            4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1, -1, -1));
        __m128i digits = _mm_sub_epi8(shuffled, _mm_set1_epi8('0'));
        __m128i in_range = _mm_cmpeq_epi8(_mm_min_epu8(digits, _mm_set1_epi8(9)), digits);
        if ((_mm_movemask_epi8(in_range) & 255) != 255) return INT64_MIN;
        __m128i pairs = _mm_maddubs_epi16(digits, _mm_setr_epi8(
            10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0, 0, 0));
        int day = _mm_extract_epi16(pairs, 0);
        int hour = _mm_extract_epi16(pairs, 1);
        int minute = _mm_extract_epi16(pairs, 2);
        int second = _mm_extract_epi16(pairs, 3);
        if (p[10] != 'T' || p[13] != ':' || p[16] != ':' ||
            day == 0 || day > m.days || hour > 23 || minute > 59 || second > 59) return INT64_MIN;
        int offset = 0;
        if (f.size == 20) {
            if (p[19] != 'Z') return INT64_MIN;
        } else {
            int oh = two_digits(p + 20), om = two_digits(p + 23);
            if ((p[19] != '+' && p[19] != '-') || p[22] != ':' ||
                oh < 0 || oh > 23 || om < 0 || om > 59) return INT64_MIN;
            offset = 60 * oh + om;
            if (p[19] == '-') offset = -offset;
        }
        return m.start + (day - 1) * day_ms + (hour * 3600 + (minute - offset) * 60 + second) * 1000;
    }
};

int64_t digits(field f, int64_t limit) {
    if (f.size == 0) return INT64_MIN;
    uint64_t value = 0;
    uint64_t max = limit;
    const uint64_t cutoff = max / 10, last = max % 10;
    for (uint32_t i = 0; i < f.size; i++) {
        unsigned digit = static_cast<unsigned char>(f.ptr[i]) - '0';
        if (digit > 9 || value > cutoff || (value == cutoff && digit > last)) return INT64_MIN;
        value = value * 10 + digit;
    }
    return value;
}

// Return -1 for a non-decimal byte.
int32_t decimal8(uint64_t word) {
    constexpr uint64_t upper = 0xf0f0f0f0f0f0f0f0, ascii = 0x3030303030303030;
    if ((word & upper) != ascii || ((word + 0x0606060606060606) & upper) != ascii) return -1;
    word -= ascii;
    word = (10 * word + (word >> 8)) & 0x00ff00ff00ff00ff;
    word = (100 * word + (word >> 16)) & 0x0000ffff0000ffff;
    return static_cast<int32_t>(10000 * word + (word >> 32));
}

int64_t timestamp(field f, clock_decoder& decoder) {
    if (f.size >= 5 && f.ptr[4] == '-') return decoder.calendar(f);
    if (f.size < 8 || f.size > 18) return digits(f, INT64_MAX);

    int64_t value = decimal8(load8(f.ptr));
    if (value < 0) return INT64_MIN;
    if (f.size == 13) {
        // Load the trailing five digits with three leading zero digits.
        constexpr uint64_t prefix = (uint64_t{1} << 24) - 1;
        int32_t tail = decimal8((load8(f.ptr + 5) & ~prefix) | 0x303030);
        return tail < 0 ? INT64_MIN : 100000 * value + tail;
    }
    for (uint32_t i = 8; i < f.size; i++) {
        unsigned digit = static_cast<unsigned char>(f.ptr[i]) - '0';
        if (digit > 9) return INT64_MIN;
        value = 10 * value + digit;
    }
    return value;
}

template <typename Emit>
inline bool decode_event(const char* data, uint32_t start, const uint32_t* ends, int quotes,
                  int64_t from, int64_t until, clock_decoder& decoder, const Emit& emit, uint32_t index) {
    field fields[7];
    uint32_t cursor = start;
    int wrapped = 0;
    for (int column = 0; column < 7; column++) {
        const char* p = data + cursor;
        uint32_t len = ends[column] - cursor;
        if (quotes != 0 && len >= 2) {
            unsigned trim = (p[0] == '"') & (p[len - 1] == '"');
            p += trim;
            len -= 2 * trim;
            wrapped += trim;
        }
        fields[column] = {p, len};
        cursor = ends[column] + 1;
    }
    if (wrapped * 2 != quotes || fields[0].size == 0) return false;
    int64_t ingest = decoder.calendar(fields[3]);
    if (ingest == INT64_MIN) return false;
    uint64_t key = encode_id(data, fields[0]);
    uint64_t user = 0;
    int64_t event_ts = 0;
    uint32_t product_offset = 0, product_size = 0, amount = 0;
    if (fields[1].size != 0 && fields[5].size != 0) {
        int64_t ts = timestamp(fields[2], decoder);
        if (ts != INT64_MIN && ts >= from && ts < until && (ingest <= ts || ingest - ts <= day_ms)) {
            field kind = fields[4];
            if (kind.size == 4 &&
                (std::memcmp(kind.ptr, "view", 4) == 0 || std::memcmp(kind.ptr, "cart", 4) == 0)) {
                user = encode_id(data, fields[1]);
                event_ts = ts;
            } else if (kind.size == 8 && std::memcmp(kind.ptr, "purchase", 8) == 0) {
                int64_t cents = digits(fields[6], 10000000);
                if (cents > 0) {
                    user = encode_id(data, fields[1]);
                    event_ts = ts;
                    product_offset = fields[5].ptr - data;
                    product_size = fields[5].size;
                    amount = static_cast<uint32_t>(cents);
                }
            }
        }
    }
    return emit(index, key, user, ingest, event_ts, product_offset, product_size, amount);
}

// The emitter reports whether a result occupies a slot in the return batch.
template <typename Emit>
uint64_t scan_event_batch(const char* data, uint32_t size, uint32_t capacity,
                          int64_t start, int64_t end, Emit&& emit) {
    const __m256i comma_char = _mm256_set1_epi8(',');
    const __m256i quote_char = _mm256_set1_epi8('"');
    const __m256i newline_char = _mm256_set1_epi8('\n');
    clock_decoder decoder{};
    uint32_t ends[7];
    uint32_t row_start = 0, count = 0;
    unsigned columns = 0, row_quotes = 0;
    for (uint32_t offset = 0; offset <= size; offset += 64) {
        uint64_t commas = 0, quotes = 0, newlines = 0;
        if (size - offset >= 64) {
            __m256i low = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + offset));
            __m256i high = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + offset + 32));
            auto mask = [low, high](__m256i match) {
                return uint64_t(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(low, match)))) |
                       uint64_t(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(high, match)))) << 32;
            };
            commas = mask(comma_char);
            quotes = mask(quote_char);
            newlines = mask(newline_char);
        } else {
            for (uint32_t i = offset; i < size; i++) {
                uint64_t bit = uint64_t{1} << (i - offset);
                switch (data[i]) {
                case ',':  commas |= bit; break;
                case '"':  quotes |= bit; break;
                case '\n': newlines |= bit; break;
                }
            }
            if (size != 0 && data[size - 1] != '\n') newlines |= uint64_t{1} << (size - offset);
        }
        while (newlines != 0) {
            unsigned bit_index = std::countr_zero(newlines);
            uint64_t below = (uint64_t{1} << bit_index) - 1;
            uint64_t fields = commas & below;
            uint32_t row_end = offset + bit_index;
            if (columns + std::popcount(fields) == 6) {
                while (fields) {
                    ends[columns++] = offset + std::countr_zero(fields);
                    fields &= fields - 1;
                }
                ends[6] = row_end;
                if (decode_event(data, row_start, ends, row_quotes + std::popcount(quotes & below),
                                 start, end, decoder, emit, count) && ++count == capacity) {
                    uint32_t consumed = row_end < size ? row_end + 1 : size;
                    return (static_cast<uint64_t>(consumed) << 32) | count;
                }
            }
            commas &= ~below;
            quotes &= ~below;
            newlines &= newlines - 1;
            row_start = row_end + 1;
            columns = 0;
            row_quotes = 0;
        }
        unsigned last = columns + std::popcount(commas);
        if (last < 7) {
            while (commas) {
                ends[columns++] = offset + std::countr_zero(commas);
                commas &= commas - 1;
            }
        }
        columns = last < 7 ? last : 7;
        row_quotes += std::popcount(quotes);
    }
    return (static_cast<uint64_t>(size) << 32) | count;
}


constexpr uint32_t direct_ref = uint32_t{1} << 30;
constexpr uint32_t raw_user_ref = uint32_t{1} << 31;
constexpr uint64_t external_id = uint64_t{1} << 63;

uint64_t mix_id(uint64_t key) {
    key = (key ^ (key >> 30)) * 0xbf58476d1ce4e5b9;
    key = (key ^ (key >> 27)) * 0x94d049bb133111eb;
    return key ^ (key >> 31);
}

uint64_t canonical_user(uint64_t hash, const direct_alias* aliases, uint32_t count,
                        const uint64_t* filter, uint32_t words) {
    if (words != 0) {
        uint64_t bits = (uint64_t{1} << ((hash >> 52) & 63)) | (uint64_t{1} << (hash >> 58));
        if ((filter[(hash >> std::countr_zero(static_cast<unsigned>(direct_person_buckets))) & (words - 1)] & bits) == bits) {
            uint64_t slot = hash & (count - 1);
            for (;;) {
                const auto& entry = aliases[slot];
                if (entry.hash == hash) return entry.person;
                if (entry.hash == 0) break;
                slot = (slot + 1) & (count - 1);
            }
        }
    }
    return hash;
}

template <unsigned Rows>
bool needs_block(uint32_t next) {
    return next == 0 || ((next - 1) & (Rows - 1)) == 0;
}

template <unsigned Rows, typename Bucket>
uint32_t take_row(uint32_t& next, uint32_t& blocks, Bucket* owners, Bucket bucket) {
    if (needs_block<Rows>(next)) {
        owners[blocks] = bucket;
        next = blocks * Rows + 1;
        ++blocks;
    }
    return next++;
}

} // namespace

extern "C" uint64_t event_batch(const char* data, uint32_t size, event_candidate* output,
                                uint32_t capacity, int64_t start, int64_t end) {
    return scan_event_batch(data, size, capacity, start, end,
        [output](uint32_t i, uint64_t key, uint64_t user, int64_t ingest, int64_t ts, uint32_t product, uint32_t length, uint32_t amount) {
            output[i] = {key, user, ingest, ts, product, length, amount};
            return true;
        });
}

extern "C" uint64_t event_batch_direct(
    const char* data, uint32_t size, event_candidate* output, uint32_t output_capacity,
    int64_t start, int64_t end, int64_t ingest_base,
    direct_state* state, direct_key* keys, direct_event* events,
    uint16_t* key_owners, uint8_t* event_owners, uint32_t key_capacity, uint32_t event_capacity, uint8_t raw_users,
    const direct_alias* aliases, uint32_t alias_count, const uint64_t* filter, uint32_t filter_words) {

    return scan_event_batch(data, size, output_capacity, start, end,
        [&](uint32_t i, uint64_t packed_key, uint64_t user, int64_t ingest_ms, int64_t ts,
            uint32_t product, uint32_t length, uint32_t amount) {
            const auto defer = [&] {
                output[i] = {packed_key, user, ingest_ms, ts, product, length, amount};
                return true;
            };
            const uint64_t ingest = static_cast<uint64_t>(ingest_ms / 1000) - static_cast<uint64_t>(ingest_base);
            if (((packed_key | user) & external_id) || amount != 0 || ingest >= raw_user_ref) {
                return defer();
            }
            uint64_t key = mix_id(packed_key);
            uint16_t bucket = key & (direct_key_buckets - 1);
            auto& key_next = state->key_next[bucket];
            if (state->key_blocks == key_capacity && needs_block<direct_key_rows>(key_next)) return defer();

            uint32_t ref = 0;
            if (user != 0) {
                uint64_t delta = static_cast<uint64_t>(ts) - static_cast<uint64_t>(start);
                if (delta >= (uint64_t{1} << 31)) return defer();
                uint64_t person = canonical_user(mix_id(user), aliases, alias_count, filter, filter_words);
                uint8_t person_bucket = (person & (direct_person_buckets - 1)) |
                                        (raw_users ? direct_person_buckets : 0);
                auto& event_next = state->event_next[person_bucket];
                if (state->event_blocks == event_capacity && needs_block<direct_person_rows>(event_next)) return defer();
                uint32_t slot = take_row<direct_person_rows>(event_next, state->event_blocks, event_owners, person_bucket);
                events[slot - 1] = {person, delta << 32};
                ref = slot | direct_ref | (raw_users ? raw_user_ref : 0);
            }
            uint32_t slot = take_row<direct_key_rows>(key_next, state->key_blocks, key_owners, bucket);
            keys[slot - 1] = {key, ingest << 32 | ref};
            return false;
        });
}
