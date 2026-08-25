#include "large_wal/large_wal_registry.h"

#include <unistd.h>
#include <string.h>
#include <stdlib.h>

int large_wal_registry_init(LargeWalRegistry *reg)
{
    if (!reg) return MYDB_ERR;
    memset(reg, 0, sizeof(*reg));
    return MYDB_OK;
}

int large_wal_registry_shutdown(LargeWalRegistry *reg)
{
    if (!reg) return MYDB_ERR;
    for (uint32_t i = 0; i < reg->count; i++) {
        if (reg->entries[i].owns_fd && reg->entries[i].fd >= 0)
            close(reg->entries[i].fd);
    }
    free(reg->entries);
    reg->entries  = NULL;
    reg->count    = 0;
    reg->capacity = 0;
    return MYDB_OK;
}

int large_wal_registry_register(LargeWalRegistry *reg, uint64_t segment_no, int fd, int owns_fd)
{
    if (!reg) return MYDB_ERR;

    for (uint32_t i = 0; i < reg->count; i++) {
        if (reg->entries[i].segment_no == segment_no) {
            reg->entries[i].fd      = fd;
            reg->entries[i].owns_fd = owns_fd;
            return MYDB_OK;
        }
    }

    if (reg->count == reg->capacity) {
        uint32_t new_cap = reg->capacity ? reg->capacity * 2 : 8;
        LargeWalRegistryEntry *ne = realloc(reg->entries, (size_t)new_cap * sizeof(LargeWalRegistryEntry));
        if (!ne) return MYDB_ERR;
        reg->entries  = ne;
        reg->capacity = new_cap;
    }
    reg->entries[reg->count].segment_no = segment_no;
    reg->entries[reg->count].fd         = fd;
    reg->entries[reg->count].owns_fd    = owns_fd;
    reg->count++;
    return MYDB_OK;
}

int large_wal_registry_lookup(const LargeWalRegistry *reg, uint64_t segment_no, int *out_fd)
{
    if (!reg || !out_fd) return MYDB_ERR;
    for (uint32_t i = 0; i < reg->count; i++) {
        if (reg->entries[i].segment_no == segment_no) {
            *out_fd = reg->entries[i].fd;
            return MYDB_OK;
        }
    }
    return MYDB_ERR_NOT_FOUND;
}

int large_wal_registry_remove(LargeWalRegistry *reg, uint64_t segment_no)
{
    if (!reg) return MYDB_ERR;
    for (uint32_t i = 0; i < reg->count; i++) {
        if (reg->entries[i].segment_no == segment_no) {
            reg->entries[i] = reg->entries[reg->count - 1];
            reg->count--;
            return MYDB_OK;
        }
    }
    return MYDB_OK;   /* not present -- harmless no-op */
}
