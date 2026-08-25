#include "large_wal/large_wal_buffer.h"

#include <stdlib.h>

int large_wal_buffer_acquire(LargeWalBuffer *buf, uint32_t total_size)
{
    if (!buf || total_size == 0) return MYDB_ERR;

    uint32_t page_count = (total_size + LARGE_WAL_PAGE_USABLE - 1) / LARGE_WAL_PAGE_USABLE;
    if (page_count > 255) return MYDB_ERR;   /* exceeds page_index/page_count's uint8_t range */

    buf->total_size = total_size;
    buf->page_count = page_count;

    if (page_count <= LARGE_WAL_STATIC_PAGES) {
        buf->buf     = buf->static_buf;
        buf->is_heap = 0;
    } else {
        buf->buf = malloc((size_t)page_count * PAGE_SIZE);
        if (!buf->buf) return MYDB_ERR;
        buf->is_heap = 1;
    }
    return MYDB_OK;
}

void large_wal_buffer_release(LargeWalBuffer *buf)
{
    if (!buf) return;
    if (buf->is_heap && buf->buf) free(buf->buf);
    buf->buf        = NULL;
    buf->is_heap    = 0;
    buf->total_size = 0;
    buf->page_count = 0;
}
