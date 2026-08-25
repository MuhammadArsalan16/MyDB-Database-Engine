#include "checksum.h"

uint32_t fnv1a(const void *data, size_t len)
{
    uint32_t h = 0x811c9dc5u;
    const uint8_t *p = data;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 0x01000193u;
    }
    return h;
}

/* Bit-by-bit reflected CRC-32 (polynomial 0xEDB88320, initial/final XOR
 * 0xFFFFFFFF — the standard CRC-32/ISO-HDLC parameters, same result a
 * table-driven zlib crc32() would give). No lookup table: this stays
 * simple and has no shared state to worry about once WAL's writer
 * threads call it concurrently later; buffers here are at most tens of
 * KB, so the per-bit cost is negligible. */
uint32_t crc32_update(uint32_t crc, const void *data, size_t len)
{
    const uint8_t *p = data;
    for (size_t i = 0; i < len; i++) {
        crc ^= p[i];
        for (int bit = 0; bit < 8; bit++) {
            uint32_t mask = -(crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return crc;
}

uint32_t crc32(const void *data, size_t len)
{
    return crc32_final(crc32_update(CRC32_INIT, data, len));
}
