#ifndef STATS_BUFFER_H
#define STATS_BUFFER_H

#include <stdint.h>
#include "stats.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * stats_buffer.h — engine-level lazy cache of StatsFile handles.
 *
 * In v3, stats files move from <partition>/<schema>/__stats.mydb into
 * system_schema/stats/stats_<partition_id>_<schema_name>.mydb.  The
 * engine owns all open handles in a flat StatsBuffer array.
 *
 * Lifecycle:
 *   Open   : lazily on the first SELECT or ANALYZE that targets a schema.
 *   Create : ANALYZE TABLE calls sb_get(); if absent the file is created.
 *   Delete : DROP DATABASE calls sb_remove(); DROP USER removes all entries
 *            for that partition_id.
 *   Close  : engine shutdown, or when the owning partition is fully evicted.
 *
 * The planner reads from the StatsFile* passed via ExecContext.
 * ANALYZE writes to it through the same handle.
 * Cache 2 (PartitionBuffer) is never exposed to the planner.
 *
 * MAX_STATS_HANDLES = 16 partitions × 5 schemas (realistic hot set).
 * Entries beyond the hot set are opened on demand and not pre-allocated.
 */

#define MAX_STATS_HANDLES  80

typedef struct {
    uint32_t   partition_id;
    char       schema_name[32];
    StatsFile *sf;          /* NULL = not yet opened; heap-allocated on first access */
    uint8_t    is_valid;    /* 1 = slot occupied */
} StatsEntry;

typedef struct {
    StatsEntry  entries[MAX_STATS_HANDLES];
    int         n_entries;
    char        stats_dir[256]; /* absolute path to system_schema/stats/ */
} StatsBuffer;

/* Initialise the buffer and record the stats directory path.  Creates
 * stats_dir with mkdir if it does not exist.
 * Returns MYDB_OK or MYDB_ERR on mkdir failure. */
int sb_init(StatsBuffer *sb, const char *root_dir);

/* Return the StatsFile* for (partition_id, schema_name), lazily opening
 * or creating the file on first access.
 * Returns NULL if the handle table is full or on a fatal I/O error.
 * Returns a valid pointer even when the file does not exist yet (it is
 * created on the first sb_get call for that schema). */
StatsFile *sb_get(StatsBuffer *sb,
                  uint32_t partition_id,
                  const char *schema_name);

/* Close and remove the stats file for (partition_id, schema_name).
 * Drops any cached handle, then unlinks the file from disk by path.
 * Idempotent: succeeds (MYDB_OK) even when no in-memory entry exists or
 * the file is already gone — DROP DATABASE may run in a session that never
 * opened this schema's stats. */
int sb_remove(StatsBuffer *sb,
              uint32_t partition_id,
              const char *schema_name);

/* Remove all stats entries whose partition_id matches.  Used by
 * DROP USER to clean up an entire partition's stats files. */
int sb_remove_partition(StatsBuffer *sb, uint32_t partition_id);

/* Close all open StatsFile handles.  Does not unlink files. */
void sb_destroy(StatsBuffer *sb);

#ifdef __cplusplus
}
#endif

#endif /* STATS_BUFFER_H */
