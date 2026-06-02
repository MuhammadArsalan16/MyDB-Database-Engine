#include "partition_buffer.h"
#include "schema_file.h"
#include "storage.h"

#include <string.h>
#include <stdlib.h>

int pb_init(PartitionBuffer *pb)
{
    if (!pb) return MYDB_ERR;
    memset(pb, 0, sizeof(*pb));   /* all slots[] = NULL, n_loaded = 0 */
    return MYDB_OK;
}

SchemaFile *pb_active_schema(PartitionBuffer *pb)
{
    if (!pb || pb->n_loaded == 0) return NULL;
    return pb->slots[pb->lru_order[0]];   /* pointer, not address-of */
}

const char *pb_active_schema_name(const PartitionBuffer *pb)
{
    if (!pb || pb->n_loaded == 0) return NULL;
    return pb->slots[pb->lru_order[0]]->header.schema_name;
}

/* Flush all dirty pages in the buffer pool for every cached schema.
 * se is the StorageEngine owned by the same PartitionCtx.
 * Does not close any SchemaFile handles. */
int pb_flush_all(PartitionBuffer *pb, StorageEngine *se)
{
    if (!pb || pb->n_loaded == 0) return MYDB_OK;
    return storage_flush_all_dirty(se);
}

void pb_destroy(PartitionBuffer *pb)
{
    if (!pb) return;
    /* Only iterate slots tracked in lru_order — safe on zero-initialized
     * state where n_loaded=0. */
    for (int i = 0; i < pb->n_loaded; i++) {
        int slot = pb->lru_order[i];
        SchemaFile *sf = pb->slots[slot];
        if (sf) {
            if (sf->fd > 0) schema_close(sf);
            free(sf);
            pb->slots[slot] = NULL;
        }
    }
    pb->n_loaded = 0;
}

/* pb_get — full LRU implementation added in Phase 3. */
SchemaFile *pb_get(PartitionBuffer *pb,
                   const char *schema_name,
                   const char *schema_path,
                   StorageEngine *se)
{
    (void)pb; (void)schema_name; (void)schema_path; (void)se;
    return NULL;   /* Phase 3 stub */
}
