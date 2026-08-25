#include "normal_wal/wal_ring_buffer.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

static void reset_write_frame_header(WalPageHeader *hdr)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->id.file_type = FILETYPE_WAL_PAGE;
}

int wal_ring_buffer_init(WalRingBuffer *rb)
{
    if (!rb) return MYDB_ERR;

    rb->buf = malloc(WAL_RING_BUFFER_SIZE);
    if (!rb->buf) return MYDB_ERR;
    memset(rb->buf, 0, WAL_RING_BUFFER_SIZE);

    rb->current_lsn      = 0;
    rb->flush_lsn         = 0;
    rb->write_frame       = 0;
    rb->write_frame_used  = 0;
    reset_write_frame_header(&rb->write_frame_hdr);

    if (pthread_mutex_init(&rb->lock, NULL) != 0) {
        free(rb->buf);
        return MYDB_ERR;
    }
    if (pthread_mutex_init(&rb->flush_mutex, NULL) != 0) {
        pthread_mutex_destroy(&rb->lock);
        free(rb->buf);
        return MYDB_ERR;
    }
    if (pthread_cond_init(&rb->flush_cond, NULL) != 0) {
        pthread_mutex_destroy(&rb->flush_mutex);
        pthread_mutex_destroy(&rb->lock);
        free(rb->buf);
        return MYDB_ERR;
    }

    return MYDB_OK;
}

void wal_ring_buffer_shutdown(WalRingBuffer *rb)
{
    if (!rb) return;
    pthread_cond_destroy(&rb->flush_cond);
    pthread_mutex_destroy(&rb->flush_mutex);
    pthread_mutex_destroy(&rb->lock);
    free(rb->buf);
    rb->buf = NULL;
}

int wal_ring_buffer_append(WalRingBuffer *rb, WalRecordHeader *hdr,
                            const void *body, size_t body_len, uint64_t *out_lsn)
{
    if (!rb || !hdr || (!body && body_len > 0) || !out_lsn) return MYDB_ERR;

    uint32_t total_len = (uint32_t)(WAL_RECORD_HEADER_SIZE + body_len);
    if (total_len > WAL_MAX_RECORD_SIZE) return MYDB_ERR;   /* LARGE_WAL not built yet */

    pthread_mutex_lock(&rb->lock);

    /* Ring-full backpressure: current_lsn - flush_lsn approximates bytes
     * of unflushed data in the ring (LSN is byte-offset-based) — see
     * wal_ring_buffer.h for why this is a soft/conservative check, not
     * an exact one, and why flush_lsn is read here without flush_mutex. */
    uint64_t unflushed = rb->current_lsn - rb->flush_lsn;
    if (unflushed + total_len > WAL_RING_BUFFER_USABLE_BUDGET) {
        pthread_mutex_unlock(&rb->lock);
        return MYDB_ERR;   /* ring full */
    }

    if (rb->write_frame_used + total_len > WAL_PAGE_USABLE) {
        /* Close the current frame (if it ever held anything) by writing
         * its accumulated header into the ring buffer itself. */
        if (rb->write_frame_used > 0) {
            uint8_t *cur = rb->buf + (size_t)rb->write_frame * WAL_PAGE_SIZE;
            rb->write_frame_hdr.data_len = (uint16_t)rb->write_frame_used;
            wal_page_header_serialize(&rb->write_frame_hdr, cur);
        }

        rb->write_frame      = (rb->write_frame + 1) % WAL_RING_BUFFER_FRAMES;
        rb->write_frame_used = 0;
        reset_write_frame_header(&rb->write_frame_hdr);
    }

    uint64_t lsn = rb->current_lsn;
    hdr->lsn       = lsn;
    hdr->total_len = total_len;

    uint8_t *dest = rb->buf + (size_t)rb->write_frame * WAL_PAGE_SIZE
                   + WAL_PAGE_HEADER_SIZE + rb->write_frame_used;
    wal_record_header_serialize(hdr, body, body_len, dest);

    if (rb->write_frame_used == 0)
        rb->write_frame_hdr.page_lsn = lsn;   /* first record in this frame */
    rb->write_frame_hdr.end_lsn = lsn;         /* latest record in this frame */
    rb->write_frame_used += total_len;

    rb->current_lsn += total_len;

    pthread_mutex_unlock(&rb->lock);

    *out_lsn = lsn;
    return MYDB_OK;
}

int wal_ring_buffer_drain(WalRingBuffer *rb, WalSegmentPool *pool,
                           uint32_t *drain_frame,
                           uint32_t *seg_slot, uint32_t *seg_page_no, uint32_t *seg_offset)
{
    if (!rb || !pool || !drain_frame || !seg_slot || !seg_page_no || !seg_offset)
        return MYDB_ERR;

    while (*drain_frame != rb->write_frame) {
        uint8_t *frame = rb->buf + (size_t)(*drain_frame) * WAL_PAGE_SIZE;

        if (wal_segment_pool_write(pool, NULL, seg_slot, seg_page_no, seg_offset,
                                    frame, WAL_PAGE_SIZE) != MYDB_OK)
            return MYDB_ERR;

        /* The frame's own finalized header already carries the true
         * end_lsn — read it back from our own in-memory copy (no disk
         * round-trip needed, we just wrote these exact bytes). */
        WalPageHeader hdr;
        if (wal_page_header_deserialize(frame, &hdr) == MYDB_OK)
            wal_ring_buffer_mark_flushed(rb, hdr.end_lsn);

        *drain_frame = (*drain_frame + 1) % WAL_RING_BUFFER_FRAMES;
    }

    return MYDB_OK;
}

int wal_ring_buffer_snapshot(WalRingBuffer *rb, uint32_t *drain_frame,
                              uint8_t *scratch, uint32_t scratch_frame_cap,
                              uint32_t *out_num_frames, WalCursor *out_cursor)
{
    if (!rb || !drain_frame || !scratch || !out_num_frames || !out_cursor)
        return MYDB_ERR;

    pthread_mutex_lock(&rb->lock);

    uint32_t available = (rb->write_frame + WAL_RING_BUFFER_FRAMES - *drain_frame)
                          % WAL_RING_BUFFER_FRAMES;
    uint32_t n = (available < scratch_frame_cap) ? available : scratch_frame_cap;

    if (n == 0) {
        pthread_mutex_unlock(&rb->lock);
        *out_num_frames = 0;
        out_cursor->lsn      = 0;
        out_cursor->page_no  = *drain_frame;
        out_cursor->offset   = 0;
        return MYDB_OK;
    }

    /* Copy n frames starting at *drain_frame, possibly wrapping around
     * the ring's physical end — two memcpy's in that case, one otherwise. */
    uint32_t first_chunk = WAL_RING_BUFFER_FRAMES - *drain_frame;
    if (first_chunk > n) first_chunk = n;

    memcpy(scratch, rb->buf + (size_t)(*drain_frame) * WAL_PAGE_SIZE,
           (size_t)first_chunk * WAL_PAGE_SIZE);
    if (n > first_chunk) {
        memcpy(scratch + (size_t)first_chunk * WAL_PAGE_SIZE, rb->buf,
               (size_t)(n - first_chunk) * WAL_PAGE_SIZE);
    }

    *drain_frame = (*drain_frame + n) % WAL_RING_BUFFER_FRAMES;

    pthread_mutex_unlock(&rb->lock);

    /* Read the last copied frame's own header out of scratch — already
     * copied, no need to touch the ring (or disk) again. */
    WalPageHeader hdr;
    uint8_t *last_frame = scratch + (size_t)(n - 1) * WAL_PAGE_SIZE;
    uint64_t last_lsn = 0;
    if (wal_page_header_deserialize(last_frame, &hdr) == MYDB_OK)
        last_lsn = hdr.end_lsn;

    *out_num_frames      = n;
    out_cursor->lsn      = last_lsn;
    out_cursor->page_no  = *drain_frame;
    out_cursor->offset   = 0;
    return MYDB_OK;
}

void wal_ring_buffer_mark_flushed(WalRingBuffer *rb, uint64_t new_flush_lsn)
{
    if (!rb) return;

    pthread_mutex_lock(&rb->flush_mutex);
    if (new_flush_lsn > rb->flush_lsn)
        rb->flush_lsn = new_flush_lsn;
    pthread_cond_broadcast(&rb->flush_cond);
    pthread_mutex_unlock(&rb->flush_mutex);
}

int wal_ring_buffer_wait_until_flushed(WalRingBuffer *rb, uint64_t lsn)
{
    if (!rb) return MYDB_ERR;

    pthread_mutex_lock(&rb->flush_mutex);
    while (rb->flush_lsn < lsn)
        pthread_cond_wait(&rb->flush_cond, &rb->flush_mutex);
    pthread_mutex_unlock(&rb->flush_mutex);

    return MYDB_OK;
}