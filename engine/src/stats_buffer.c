/*
 * stats_buffer.c — engine-level lazy cache of StatsFile handles.
 *
 * v3: stats files live under system_schema/stats/ as
 *     stats_<partition_id>_<schema_name>.mydb and are owned by the engine,
 *     NOT by partition_manager.  The planner reads through the StatsFile*
 *     the engine resolves into ExecContext.stats; ANALYZE writes through
 *     the same handle.  See stats_buffer.h for the contract.
 *
 * The handle table is a flat array scanned by `is_valid`.  Phase 1 keeps
 * it simple (linear scan, ≤ MAX_STATS_HANDLES entries); the access pattern
 * is one lookup per statement against a small hot set.
 */

#include "stats_buffer.h"
#include "stats.h"
#include "common.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* mkdir, ignoring EEXIST. Returns 0 if the directory now exists. */
static int ensure_dir(const char *path)
{
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
    }
    return -1;
}

/* Build the on-disk path for one (partition, schema) stats file. */
static int build_stats_path(const StatsBuffer *sb, uint32_t pid,
                            const char *schema, char *out, size_t cap)
{
    int n = snprintf(out, cap, "%s/stats_%u_%s.mydb",
                     sb->stats_dir, pid, schema);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

/* Locate the entry for (pid, schema), or NULL if not present. */
static StatsEntry *find_entry(StatsBuffer *sb, uint32_t pid, const char *schema)
{
    for (int i = 0; i < MAX_STATS_HANDLES; i++) {
        StatsEntry *e = &sb->entries[i];
        if (!e->is_valid) continue;
        if (e->partition_id != pid) continue;
        if (strncmp(e->schema_name, schema, sizeof(e->schema_name)) == 0)
            return e;
    }
    return NULL;
}

/* First free slot, or NULL if the table is full. */
static StatsEntry *free_slot(StatsBuffer *sb)
{
    for (int i = 0; i < MAX_STATS_HANDLES; i++)
        if (!sb->entries[i].is_valid) return &sb->entries[i];
    return NULL;
}

/* ------------------------------------------------------------------ */

int sb_init(StatsBuffer *sb, const char *root_dir)
{
    if (!sb || !root_dir) return MYDB_ERR;

    memset(sb, 0, sizeof(*sb));

    int n = snprintf(sb->stats_dir, sizeof(sb->stats_dir),
                     "%s/system_schema/stats", root_dir);
    if (n < 0 || (size_t)n >= sizeof(sb->stats_dir)) return MYDB_ERR;

    if (ensure_dir(sb->stats_dir) < 0) return MYDB_ERR;
    return MYDB_OK;
}

StatsFile *sb_get(StatsBuffer *sb, uint32_t partition_id, const char *schema_name)
{
    if (!sb || !schema_name) return NULL;

    StatsEntry *e = find_entry(sb, partition_id, schema_name);

    /* Already open — fast path. */
    if (e && e->sf) return e->sf;

    /* Allocate a slot if this is a first-time access. */
    if (!e) {
        e = free_slot(sb);
        if (!e) return NULL;                 /* handle table full */
        e->is_valid     = 1;
        e->partition_id = partition_id;
        strncpy(e->schema_name, schema_name, sizeof(e->schema_name) - 1);
        e->schema_name[sizeof(e->schema_name) - 1] = '\0';
        e->sf = NULL;
        sb->n_entries++;
    }

    /* Open the file; create it on first use if it does not exist yet. */
    char path[512];
    if (build_stats_path(sb, partition_id, schema_name, path, sizeof(path)) < 0)
        return NULL;

    StatsFile *sf = (StatsFile *)calloc(1, sizeof(StatsFile));
    if (!sf) return NULL;

    int rc = stats_open(path, sf);
    if (rc == MYDB_ERR_NOT_FOUND)
        rc = stats_create(path, schema_name, sf);
    if (rc != MYDB_OK) {
        free(sf);
        /* Leave the slot registered but unopened; a later ANALYZE may
         * succeed in creating the file. */
        e->sf = NULL;
        return NULL;
    }

    e->sf = sf;
    return sf;
}

int sb_remove(StatsBuffer *sb, uint32_t partition_id, const char *schema_name)
{
    if (!sb || !schema_name) return MYDB_ERR;

    /* Drop any cached handle for this (partition, schema).  There may be
     * none — e.g. a session that drops a database it never queried — so the
     * absence of an in-memory entry is not an error: the on-disk file must
     * still be unlinked. */
    StatsEntry *e = find_entry(sb, partition_id, schema_name);
    if (e) {
        if (e->sf) {
            stats_close(e->sf);
            free(e->sf);
            e->sf = NULL;
        }
        e->is_valid = 0;
        e->partition_id = 0;
        e->schema_name[0] = '\0';
        if (sb->n_entries > 0) sb->n_entries--;
    }

    /* Unlink the file by path regardless of cache state (idempotent). */
    char path[512];
    if (build_stats_path(sb, partition_id, schema_name, path, sizeof(path)) == 0)
        unlink(path);                       /* missing is fine */

    return MYDB_OK;
}

int sb_remove_partition(StatsBuffer *sb, uint32_t partition_id)
{
    if (!sb) return MYDB_ERR;

    for (int i = 0; i < MAX_STATS_HANDLES; i++) {
        StatsEntry *e = &sb->entries[i];
        if (!e->is_valid || e->partition_id != partition_id) continue;
        /* sb_remove looks the entry up again by name — safe to call here. */
        sb_remove(sb, partition_id, e->schema_name);
    }
    return MYDB_OK;
}

void sb_destroy(StatsBuffer *sb)
{
    if (!sb) return;
    for (int i = 0; i < MAX_STATS_HANDLES; i++) {
        StatsEntry *e = &sb->entries[i];
        if (e->sf) {
            stats_close(e->sf);
            free(e->sf);
            e->sf = NULL;
        }
        e->is_valid = 0;
    }
    sb->n_entries = 0;
}
