#include "large_wal/large_wal_writer.h"
#include "large_wal/large_wal_page.h"

#include <string.h>

/* ------------------------------------------------------------------
 * Packing — builds real LargeWalPageHeaders now that the writer knows
 * exactly where these pages are landing (closes Phase 4's "header
 * stamping" open item).
 * ------------------------------------------------------------------ */

static void pack_buffer(LargeWalBuffer *buf, uint64_t content_lsn, uint8_t rec_type,
                         const uint8_t *content, uint32_t total_size)
{
    uint32_t written = 0;
    for (uint32_t i = 0; i < buf->page_count; i++) {
        uint8_t *page = buf->buf + (size_t)i * PAGE_SIZE;
        memset(page, 0, PAGE_SIZE);

        uint32_t remaining = total_size - written;
        uint32_t chunk = remaining < LARGE_WAL_PAGE_USABLE ? remaining : LARGE_WAL_PAGE_USABLE;
        memcpy(page + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE, content + written, chunk);

        LargeWalPageHeader hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
        hdr.content_lsn  = content_lsn;
        hdr.record_type  = rec_type;
        hdr.page_index   = (uint8_t)i;
        hdr.data_len     = (uint16_t)chunk;
        hdr.flags        = (i > 0) ? LARGE_WAL_PAGE_FLAG_CONTINUATION : 0;
        large_wal_page_header_serialize(&hdr, page);

        written += chunk;
    }
}

/* ------------------------------------------------------------------
 * do_write — the 4-step sequence: fit-check+rollover, pack, one
 * byte-for-byte write, index+state.
 * ------------------------------------------------------------------ */

static int do_write(LargeWalWriter *w, const uint8_t *content, uint32_t total_size,
                     uint64_t content_lsn, uint8_t rec_type, LargeWalIndexEntry *out_entry)
{
    if (large_wal_buffer_acquire(&w->buf, total_size) != MYDB_OK) return MYDB_ERR;

    uint32_t remaining = LARGE_WAL_SEGMENT_PAGES_PER_FILE - w->cur_page_no;
    if (w->buf.page_count > remaining) {
        if (large_wal_segment_pool_mark_done(w->pool, w->worker, w->cur_slot_index,
                                              w->cur_segment_last_lsn,
                                              w->cur_page_no - 1) != MYDB_OK) {
            large_wal_buffer_release(&w->buf);
            return MYDB_ERR;
        }

        uint32_t new_slot;
        if (large_wal_segment_pool_claim_next(w->pool, &new_slot) != MYDB_OK) {
            large_wal_buffer_release(&w->buf);
            return MYDB_ERR;
        }
        w->cur_slot_index = new_slot;
        w->cur_page_no    = 1;

        if (large_wal_registry_register(w->registry, w->pool->slots[new_slot].header.segment_no,
                                         w->pool->slots[new_slot].fd, /*owns_fd=*/0) != MYDB_OK) {
            large_wal_buffer_release(&w->buf);
            return MYDB_ERR;
        }
    }

    pack_buffer(&w->buf, content_lsn, rec_type, content, total_size);

    uint32_t start_page_no = w->cur_page_no;
    uint32_t slot           = w->cur_slot_index;
    uint32_t page_no         = w->cur_page_no;
    uint32_t offset          = 0;
    if (large_wal_segment_pool_write(w->pool, w->worker, &slot, &page_no, &offset,
                                      w->buf.buf, (size_t)w->buf.page_count * PAGE_SIZE) != MYDB_OK) {
        large_wal_buffer_release(&w->buf);
        return MYDB_ERR;
    }

    /* Fit was already guaranteed above, so write() cannot have
     * auto-rolled mid-buffer -- trust its own cursor advancement
     * regardless, same as taking slot_index/page_no/offset as true
     * out-params anywhere else in this codebase. */
    w->cur_slot_index       = slot;
    w->cur_page_no          = page_no;
    w->cur_segment_last_lsn = content_lsn;

    LargeWalIndexEntry entry;
    memset(&entry, 0, sizeof(entry));
    entry.content_lsn   = content_lsn;
    entry.rec_type      = rec_type;
    entry.segment_no    = w->pool->slots[w->cur_slot_index].header.segment_no;
    entry.start_page_no = start_page_no;
    entry.offset        = 0;
    entry.page_count    = (uint8_t)w->buf.page_count;
    entry.total_size    = total_size;

    if (large_wal_index_insert(w->idx, &entry) != MYDB_OK) {
        large_wal_buffer_release(&w->buf);
        return MYDB_ERR;
    }
    if (large_wal_state_advance(w->state, content_lsn) != MYDB_OK) {
        large_wal_buffer_release(&w->buf);
        return MYDB_ERR;
    }

    large_wal_buffer_release(&w->buf);
    *out_entry = entry;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Thread loop
 * ------------------------------------------------------------------ */

static void *writer_main(void *arg)
{
    LargeWalWriter *w = (LargeWalWriter *)arg;

    for (;;) {
        pthread_mutex_lock(&w->lock);
        while (!w->request_pending && !w->stop_requested)
            pthread_cond_wait(&w->cond, &w->lock);

        if (w->stop_requested && !w->request_pending) {
            pthread_mutex_unlock(&w->lock);
            break;
        }

        const uint8_t *content     = w->req_content;
        uint32_t        total_size = w->req_total_size;
        uint64_t         content_lsn = w->req_content_lsn;
        uint8_t           rec_type   = w->req_rec_type;
        pthread_mutex_unlock(&w->lock);

        LargeWalIndexEntry entry;
        memset(&entry, 0, sizeof(entry));
        int rc = do_write(w, content, total_size, content_lsn, rec_type, &entry);

        pthread_mutex_lock(&w->lock);
        w->request_pending = 0;
        w->request_done    = 1;
        w->last_result      = rc;
        if (rc == MYDB_OK) w->req_out_entry = entry;
        pthread_cond_broadcast(&w->cond);
        pthread_mutex_unlock(&w->lock);
    }

    return NULL;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

static int find_or_claim_active(LargeWalWriter *w)
{
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (w->pool->slots[i].header.state == LSEG_ACTIVE) {
            w->cur_slot_index = i;
            w->cur_page_no    = w->pool->slots[i].header.data_pages + 1;
            return large_wal_registry_register(w->registry, w->pool->slots[i].header.segment_no,
                                                w->pool->slots[i].fd, /*owns_fd=*/0);
        }
    }

    uint32_t slot;
    if (large_wal_segment_pool_claim_next(w->pool, &slot) != MYDB_OK) return MYDB_ERR;
    w->cur_slot_index = slot;
    w->cur_page_no    = 1;
    return large_wal_registry_register(w->registry, w->pool->slots[slot].header.segment_no,
                                        w->pool->slots[slot].fd, /*owns_fd=*/0);
}

int large_wal_writer_init(LargeWalWriter *w, LargeWalSegmentPool *pool,
                           LargeWalRegistry *registry, LargeWalIndex *idx,
                           LargeWalState *state, WalWorker *worker)
{
    if (!w || !pool || !registry || !idx || !state) return MYDB_ERR;

    memset(w, 0, sizeof(*w));
    w->pool     = pool;
    w->registry = registry;
    w->idx      = idx;
    w->state    = state;
    w->worker   = worker;

    /* content_lsn is the same shared, monotonic sequence across the
     * whole engine (confirmed this session), and this writer only ever
     * has one segment open at a time, so state's own already-durable
     * flush_lsn is the correct "last content_lsn written into the
     * resumed segment" seed on reload -- a genuine value, not a
     * placeholder. */
    w->cur_segment_last_lsn = state->flush_lsn;

    return find_or_claim_active(w);
}

int large_wal_writer_start(LargeWalWriter *w)
{
    if (!w) return MYDB_ERR;

    if (pthread_mutex_init(&w->lock, NULL) != 0) return MYDB_ERR;
    if (pthread_cond_init(&w->cond, NULL) != 0) {
        pthread_mutex_destroy(&w->lock);
        return MYDB_ERR;
    }

    w->stop_requested = 0;
    if (pthread_create(&w->thread, NULL, writer_main, w) != 0) {
        pthread_cond_destroy(&w->cond);
        pthread_mutex_destroy(&w->lock);
        return MYDB_ERR;
    }

    w->started = 1;
    return MYDB_OK;
}

int large_wal_writer_stop(LargeWalWriter *w)
{
    if (!w || !w->started) return MYDB_OK;

    pthread_mutex_lock(&w->lock);
    w->stop_requested = 1;
    pthread_cond_broadcast(&w->cond);
    pthread_mutex_unlock(&w->lock);

    pthread_join(w->thread, NULL);

    pthread_cond_destroy(&w->cond);
    pthread_mutex_destroy(&w->lock);
    w->started = 0;

    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * submit — blocking request/response handoff
 * ------------------------------------------------------------------ */

int large_wal_writer_submit(LargeWalWriter *w, const uint8_t *content, uint32_t total_size,
                             uint64_t content_lsn, uint8_t rec_type,
                             LargeWalIndexEntry *out_entry)
{
    if (!w || !w->started || !content || !out_entry) return MYDB_ERR;

    pthread_mutex_lock(&w->lock);
    if (w->stop_requested) {
        pthread_mutex_unlock(&w->lock);
        return MYDB_ERR;
    }

    w->req_content    = content;
    w->req_total_size  = total_size;
    w->req_content_lsn = content_lsn;
    w->req_rec_type    = rec_type;
    w->request_pending = 1;
    w->request_done    = 0;
    pthread_cond_broadcast(&w->cond);

    while (!w->request_done)
        pthread_cond_wait(&w->cond, &w->lock);

    int rc = w->last_result;
    if (rc == MYDB_OK) *out_entry = w->req_out_entry;
    pthread_mutex_unlock(&w->lock);

    return rc;
}
