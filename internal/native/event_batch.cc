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

uint64_t mix(uint64_t key) {
    key = (key ^ (key >> 30)) * 0xbf58476d1ce4e5b9;
    key = (key ^ (key >> 27)) * 0x94d049bb133111eb;
    return key ^ (key >> 31);
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

int64_t timestamp(field f, clock_decoder& decoder) {
    if (f.size >= 5 && f.ptr[4] == '-') return decoder.calendar(f);
    if (f.size < 8 || f.size > 18) return digits(f, INT64_MAX);

    uint64_t word = load8(f.ptr);
    constexpr uint64_t upper = 0xf0f0f0f0f0f0f0f0, ascii = 0x3030303030303030;
    if ((word & upper) != ascii || ((word + 0x0606060606060606) & upper) != ascii) return INT64_MIN;
    word -= ascii;
    word = (10 * word + (word >> 8)) & 0x00ff00ff00ff00ff;
    word = (100 * word + (word >> 16)) & 0x0000ffff0000ffff;
    int64_t value = static_cast<uint32_t>(10000 * word + (word >> 32));
    for (uint32_t i = 8; i < f.size; i++) {
        unsigned digit = static_cast<unsigned char>(f.ptr[i]) - '0';
        if (digit > 9) return INT64_MIN;
        value = 10 * value + digit;
    }
    return value;
}

bool decode_event(const char* data, uint32_t start, const uint32_t* ends, int quotes,
                  int64_t from, int64_t until, clock_decoder& decoder, event_candidate& out) {
    field fields[7];
    uint32_t cursor = start;
    int wrapped = 0;
    for (int column = 0; column < 7; column++) {
        const char* p = data + cursor;
        uint32_t len = ends[column] - cursor;
        if (quotes != 0 && len >= 2 && p[0] == '"' && p[len - 1] == '"') {
            p++;
            len -= 2;
            wrapped++;
        }
        fields[column] = {p, len};
        cursor = ends[column] + 1;
    }
    if (wrapped * 2 != quotes || fields[0].size == 0) return false;
    int64_t ingest = decoder.calendar(fields[3]);
    if (ingest == INT64_MIN) return false;
    const auto fallback = [&] {
        out = {0, 0, 0, 0, start, ends[6] - start};
        return true;
    };
    uint64_t key = pack_id(fields[0]);
    if (key == 0) return fallback();

    out = {mix(key), 0, ingest, 0, 0, 0};
    if (fields[1].size == 0 || fields[5].size == 0) return true;
    int64_t ts = timestamp(fields[2], decoder);
    if (ts == INT64_MIN || ts < from || ts >= until || (ingest > ts && ingest - ts > day_ms)) return true;
    field kind = fields[4];
    if (kind.size == 4 &&
        (std::memcmp(kind.ptr, "view", 4) == 0 || std::memcmp(kind.ptr, "cart", 4) == 0)) {
        uint64_t user = pack_id(fields[1]);
        if (user == 0) return fallback();
        out.user = user;
        out.ts = ts;
    } else if (kind.size == 8 && std::memcmp(kind.ptr, "purchase", 8) == 0) {
        int64_t cents = digits(fields[6], 10000000);
        if (cents <= 0) return true;
        uint64_t user = pack_id(fields[1]);
        if (user == 0 || fields[5].size > 255) return fallback();
        out.user = user;
        out.ts = ts;
        out.offset = fields[5].ptr - data;
        out.meta = (fields[5].size << 24) | static_cast<uint32_t>(cents);
    }
    return true;
}

} // namespace

extern "C" uint64_t event_batch(const char* data, uint32_t size, event_candidate* output,
                                uint32_t capacity, int64_t start, int64_t end) {
    const __m256i comma_char = _mm256_set1_epi8(',');
    const __m256i quote_char = _mm256_set1_epi8('"');
    const __m256i newline_char = _mm256_set1_epi8('\n');
    clock_decoder decoder{};
    uint32_t ends[7];
    uint32_t row_start = 0, count = 0;
    unsigned columns = 0, row_quotes = 0;
    for (uint32_t offset = 0; offset <= size; offset += 32) {
        uint32_t commas = 0, quotes = 0, newlines = 0;
        if (size - offset >= 32) {
            __m256i bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + offset));
            commas = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, comma_char));
            quotes = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, quote_char));
            newlines = _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, newline_char));
        } else {
            for (uint32_t i = offset; i < size; i++) {
                uint32_t bit = 1U << (i - offset);
                switch (data[i]) {
                case ',':  commas |= bit; break;
                case '"':  quotes |= bit; break;
                case '\n': newlines |= bit; break;
                }
            }
            if (size != 0 && data[size - 1] != '\n') newlines |= 1U << (size - offset);
        }
        while (newlines != 0) {
            unsigned bit_index = std::countr_zero(newlines);
            uint32_t below = (1U << bit_index) - 1;
            uint32_t fields = commas & below;
            uint32_t row_end = offset + bit_index;
            if (columns + std::popcount(fields) == 6) {
                while (fields) {
                    ends[columns++] = offset + std::countr_zero(fields);
                    fields &= fields - 1;
                }
                ends[6] = row_end;
                if (decode_event(data, row_start, ends, row_quotes + std::popcount(quotes & below),
                                 start, end, decoder, output[count]) && ++count == capacity) {
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
