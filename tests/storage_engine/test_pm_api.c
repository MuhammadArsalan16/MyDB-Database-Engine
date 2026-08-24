#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "common.h"
#include "partition_ctx.h"
#include "pm_api.h"

/* ------------------------------------------------------------------ */
/*  Phase 1 of the PartitionBuffer redesign (PARTITION_BUFFER_DESIGN.md):
 *  pin/release discipline for pm_find_relation_const()/pm_release_relation().
 *  This file exercises only that discipline — not pm_create_table's
 *  storage/quota machinery, which pin_count never touches, so the fixture
 *  builds the schema file directly with schema_create()/schema_add_relation()
 *  and loads it into the cache via pctx_open_schema(), skipping the catalog
 *  entirely (pm_find_relation_const/pm_release_relation never touch it).
 * ------------------------------------------------------------------ */

#define TEST_PART_DIR    "/tmp/mydb_test_pm_api_part"
#define TEST_SCHEMA_NAME "testschema"
#define TEST_SCHEMA_DIR  TEST_PART_DIR "/" TEST_SCHEMA_NAME
#define TEST_SCHEMA_FILE TEST_SCHEMA_DIR "/__schema.mydb"
#define TEST_CATALOG_FILE TEST_PART_DIR "/__catalog.mydb"
#define TEST_PID         91
#define TEST_SID         7

/* Real-table relation name used only by the Phase 3 rollback fixture
 * (setup_ctx_with_real_table) — kept distinct from "orders" (used by the
 * lightweight fixture) so their on-disk relation files never collide. */
#define TEST_REAL_TABLE  "widgets"
#define TEST_REAL_TABLE_FILE TEST_SCHEMA_DIR "/" TEST_REAL_TABLE ".mydb"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void)
{
    unlink(TEST_REAL_TABLE_FILE);
    unlink(TEST_SCHEMA_FILE);
    unlink(TEST_CATALOG_FILE);
    rmdir(TEST_SCHEMA_DIR);
    rmdir(TEST_PART_DIR);
}

/* Build a minimal RelationDef with the given name, two int columns,
 * pk on column 0. Returns by value. Copied from test_schema_file.c's
 * fixture builder — no shared test header exists in this codebase, so
 * each test file re-declares its own (established convention). */
static RelationDef make_simple_def(const char *name)
{
    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, name, MAX_TABLE_NAME - 1);
    r.num_columns = 2;
    strncpy(r.columns[0].name, "id",  MAX_COLUMN_NAME - 1);
    r.columns[0].type           = TYPE_INT;
    r.columns[0].is_primary_key = 1;
    r.columns[0].is_not_null    = 1;
    strncpy(r.columns[1].name, "age", MAX_COLUMN_NAME - 1);
    r.columns[1].type           = TYPE_INT;
    r.pk_col_idx = 0;
    r.root_page_no = 99;
    r.auto_incr_counter = 1;
    return r;
}

/* Build a PartitionCtx with one schema ("testschema") already active,
 * holding one relation with the given name. Catalog is left unopened
 * (fd == -1) since nothing under test touches it. */
static PartitionCtx *setup_ctx_with_relation(const char *relation_name)
{
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    mkdir(TEST_SCHEMA_DIR, 0755);

    PartitionCtx *ctx = pctx_init(TEST_PID, TEST_PART_DIR);
    if (!ctx) return NULL;

    SchemaFile sf;
    if (schema_create(TEST_SCHEMA_FILE, TEST_PID, TEST_SID, TEST_SCHEMA_NAME, &sf) != MYDB_OK) {
        pctx_close(ctx); free(ctx); return NULL;
    }
    RelationDef def = make_simple_def(relation_name);
    schema_add_relation(&sf, &def);
    schema_close(&sf);   /* pctx_open_schema below reopens it via pb_get */

    if (!pctx_open_schema(ctx, TEST_SCHEMA_NAME, TEST_SCHEMA_FILE)) {
        pctx_close(ctx); free(ctx); return NULL;
    }
    return ctx;
}

static void teardown_ctx(PartitionCtx *ctx)
{
    if (ctx) { pctx_close(ctx); free(ctx); }
    cleanup();
}

/* Heavier fixture for Phase 3 (write-back flush model): builds a real
 * on-disk table via pm_create_schema/pm_create_table, unlike
 * setup_ctx_with_relation above (which hand-builds the schema file
 * directly and leaves the catalog closed). Needed because pm_create_table
 * calls cat_alloc_table_id(), which requires a real, open Catalog
 * (cat->fd >= 0) — only a relation created through the real pm_create_table
 * path lets the rollback test prove pb_discard_slot_dirty actually reverts
 * a dirtied num_rows. */
static PartitionCtx *setup_ctx_with_real_table(const char *relation_name)
{
    cleanup();
    mkdir(TEST_PART_DIR, 0755);

    Catalog cat;
    if (cat_create(TEST_CATALOG_FILE, TEST_PID, TEST_PID,
                    100ULL * 1024 * 1024, &cat) != MYDB_OK)
        return NULL;
    cat_close(&cat);

    PartitionCtx *ctx = pctx_init(TEST_PID, TEST_PART_DIR);
    if (!ctx) return NULL;
    if (pctx_open_catalog(ctx) != MYDB_OK) {
        pctx_close(ctx); free(ctx); return NULL;
    }

    if (pm_create_schema(ctx, TEST_SCHEMA_NAME) != MYDB_OK) {
        pctx_close(ctx); free(ctx); return NULL;
    }
    if (!pctx_open_schema(ctx, TEST_SCHEMA_NAME, TEST_SCHEMA_FILE)) {
        pctx_close(ctx); free(ctx); return NULL;
    }

    RelationDef def = make_simple_def(relation_name);
    def.root_page_no = 0;   /* storage_create_table fills this in */
    if (pm_create_table(ctx, &def) != MYDB_OK) {
        pctx_close(ctx); free(ctx); return NULL;
    }
    return ctx;
}

/* ------------------------------------------------------------------ */
/*  Tests                                                              */
/* ------------------------------------------------------------------ */

static void test_find_increments_pin_count(void)
{
    printf("\n[test_find_increments_pin_count]\n");
    PartitionCtx *ctx = setup_ctx_with_relation("orders");
    CHECK(ctx != NULL, "fixture set up");
    if (!ctx) return;

    CHECK(pctx_debug_no_pinned_relations(ctx), "clean before any find");

    const RelationDef *r1 = pm_find_relation_const(ctx, "orders");
    const RelationDef *r2 = pm_find_relation_const(ctx, "orders");
    CHECK(r1 != NULL && r1 == r2, "repeated finds return the same pointer");

    /* Phase 2: pin_count lives on PBInnerFrame now, not SchemaFile. def is
     * the first member of PBInnerFrame, so this cast recovers the frame
     * (same technique pb_unpin_relation itself uses internally). */
    PBOuterSlot *outer = pctx_active_outer_slot(ctx);
    int          frame = (int)((const PBInnerFrame *)r1 - outer->inner);
    CHECK(outer->inner[frame].pin_count == 2, "pin_count incremented once per find");
    CHECK(!pctx_debug_no_pinned_relations(ctx), "leak check sees the outstanding pins");

    pm_release_relation(ctx, r1);
    pm_release_relation(ctx, r2);
    CHECK(outer->inner[frame].pin_count == 0, "pin_count back to 0 after matching releases");
    CHECK(pctx_debug_no_pinned_relations(ctx), "clean again after releasing");

    teardown_ctx(ctx);
}

static void test_release_clamped_at_zero(void)
{
    printf("\n[test_release_clamped_at_zero]\n");
    PartitionCtx *ctx = setup_ctx_with_relation("orders");
    CHECK(ctx != NULL, "fixture set up");
    if (!ctx) return;

    const RelationDef *r = pm_find_relation_const(ctx, "orders");
    PBOuterSlot *outer = pctx_active_outer_slot(ctx);
    int          frame = (int)((const PBInnerFrame *)r - outer->inner);

    CHECK(pm_release_relation(ctx, r) == MYDB_OK, "release matching the one find");
    CHECK(outer->inner[frame].pin_count == 0, "pin_count at 0");

    /* An extra release beyond what was actually pinned must clamp, not
     * underflow — pin_count is unsigned, so an unclamped decrement here
     * would wrap to 65535 instead of staying at 0. */
    CHECK(pm_release_relation(ctx, r) == MYDB_OK, "extra release does not error");
    CHECK(outer->inner[frame].pin_count == 0, "pin_count clamped at 0, not underflowed");

    teardown_ctx(ctx);
}

static void test_find_missing_relation(void)
{
    printf("\n[test_find_missing_relation]\n");
    PartitionCtx *ctx = setup_ctx_with_relation("orders");
    CHECK(ctx != NULL, "fixture set up");
    if (!ctx) return;

    CHECK(pm_find_relation_const(ctx, "ghost") == NULL,
          "missing relation returns NULL, no pin taken");
    CHECK(pctx_debug_no_pinned_relations(ctx), "leak check still clean");

    teardown_ctx(ctx);
}

static void test_debug_no_pinned_relations_null_safe(void)
{
    printf("\n[test_debug_no_pinned_relations_null_safe]\n");
    CHECK(pctx_debug_no_pinned_relations(NULL) == 1,
          "NULL ctx reports clean (nothing to leak)");
}

/* Phase 3: pm_rollback must undo dirty metadata, not just dirty data
 * pages. Before Phase 3, schema_bump_relation_rows wrote through
 * immediately and unconditionally, so a rolled-back INSERT still left
 * num_rows bumped — a real, pre-existing bug. pb_discard_slot_dirty
 * closes it: BEGIN; INSERT (dirties num_rows in memory only); ROLLBACK
 * must leave num_rows exactly where it started. */
static void test_rollback_discards_dirty_metadata(void)
{
    printf("\n[test_rollback_discards_dirty_metadata]\n");
    PartitionCtx *ctx = setup_ctx_with_real_table(TEST_REAL_TABLE);
    CHECK(ctx != NULL, "real-table fixture set up");
    if (!ctx) return;

    SchemaFile *sf = pctx_active_schema(ctx);
    RelationEntry *stat_before = schema_find_relation_stat(sf, TEST_REAL_TABLE);
    CHECK(stat_before != NULL && stat_before->num_rows == 0,
          "num_rows starts at 0");

    RelationDef *rel = pm_find_relation(ctx, TEST_REAL_TABLE);
    CHECK(rel != NULL, "relation found for insert");
    if (!rel) { teardown_ctx(ctx); return; }

    CHECK(pm_begin(ctx) == MYDB_OK, "BEGIN succeeds");

    Row row;
    memset(&row, 0, sizeof(row));
    row.num_cols = 2;
    row.cols[0].type = TYPE_INT; row.cols[0].v.int_val = 1;
    row.cols[1].type = TYPE_INT; row.cols[1].v.int_val = 42;
    CHECK(pm_insert(ctx, rel, &row) == MYDB_OK, "insert succeeds inside the transaction");

    RelationEntry *stat_dirty = schema_find_relation_stat(sf, TEST_REAL_TABLE);
    CHECK(stat_dirty != NULL && stat_dirty->num_rows == 1,
          "num_rows bumped to 1 in memory before rollback");

    pm_release_relation(ctx, rel);
    CHECK(pm_rollback(ctx) == MYDB_OK, "ROLLBACK succeeds");

    RelationEntry *stat_after = schema_find_relation_stat(sf, TEST_REAL_TABLE);
    CHECK(stat_after != NULL && stat_after->num_rows == 0,
          "num_rows reverted to 0 after rollback (dirty metadata discarded)");

    teardown_ctx(ctx);
}

/* Phase 4: the same discard-on-rollback proof as
 * test_rollback_discards_dirty_metadata above, but for PB[0]'s used_bytes
 * (via cat_track_alloc) instead of SchemaFile's num_rows. reconcile_growth
 * only calls cat_track_alloc when a DML statement actually changes the
 * relation's page count — a single tiny row never does, so this inserts
 * enough rows in one transaction to force at least one B+ tree page split
 * (record ~26B + 2B slot ~28B/row; a 16KB page holds roughly 580 before
 * splitting — 800 gives comfortable margin). */
static void test_rollback_discards_dirty_quota(void)
{
    printf("\n[test_rollback_discards_dirty_quota]\n");
    PartitionCtx *ctx = setup_ctx_with_real_table(TEST_REAL_TABLE);
    CHECK(ctx != NULL, "real-table fixture set up");
    if (!ctx) return;

    Catalog *cat = pctx_catalog(ctx);
    CHECK(cat != NULL, "catalog open on the fixture");
    if (!cat) { teardown_ctx(ctx); return; }
    uint64_t used_before = cat->header.used_bytes;

    RelationDef *rel = pm_find_relation(ctx, TEST_REAL_TABLE);
    CHECK(rel != NULL, "relation found for bulk insert");
    if (!rel) { teardown_ctx(ctx); return; }

    CHECK(pm_begin(ctx) == MYDB_OK, "BEGIN succeeds");

    const int N = 800;
    int insert_ok = 1;
    for (int i = 0; i < N; i++) {
        Row row;
        memset(&row, 0, sizeof(row));
        row.num_cols = 2;
        row.cols[0].type = TYPE_INT; row.cols[0].v.int_val = i + 1;
        row.cols[1].type = TYPE_INT; row.cols[1].v.int_val = i;
        if (pm_insert(ctx, rel, &row) != MYDB_OK) insert_ok = 0;
    }
    CHECK(insert_ok, "all N rows insert inside the transaction");
    pm_release_relation(ctx, rel);

    CHECK(cat->header.used_bytes > used_before,
          "used_bytes grew in memory — fixture forced at least one page split");

    CHECK(pm_rollback(ctx) == MYDB_OK, "ROLLBACK succeeds");

    CHECK(cat->header.used_bytes == used_before,
          "used_bytes reverted to its pre-transaction value after rollback "
          "(PB[0]'s dirty quota bookkeeping discarded, not just SchemaFile's)");

    teardown_ctx(ctx);
}

/* Phase 3, plan Verification item 4: a bulk INSERT loop inside one explicit
 * transaction must result in a single flush at COMMIT, not one
 * write-through pwrite per row. Proven via an independent, freshly-opened
 * SchemaFile handle on the same on-disk file: mid-transaction it must still
 * see num_rows == 0 (the N deferred bumps never reached disk), and only
 * after pm_commit does it see num_rows == N. */
static void test_bulk_insert_single_flush_at_commit(void)
{
    printf("\n[test_bulk_insert_single_flush_at_commit]\n");
    PartitionCtx *ctx = setup_ctx_with_real_table(TEST_REAL_TABLE);
    CHECK(ctx != NULL, "real-table fixture set up");
    if (!ctx) return;

    RelationDef *rel = pm_find_relation(ctx, TEST_REAL_TABLE);
    CHECK(rel != NULL, "relation found for bulk insert");
    if (!rel) { teardown_ctx(ctx); return; }

    CHECK(pm_begin(ctx) == MYDB_OK, "BEGIN succeeds");

    const int N = 20;
    int insert_ok = 1;
    for (int i = 0; i < N; i++) {
        Row row;
        memset(&row, 0, sizeof(row));
        row.num_cols = 2;
        row.cols[0].type = TYPE_INT; row.cols[0].v.int_val = i + 1;
        row.cols[1].type = TYPE_INT; row.cols[1].v.int_val = i;
        if (pm_insert(ctx, rel, &row) != MYDB_OK) insert_ok = 0;
    }
    CHECK(insert_ok, "all N rows insert inside the transaction");
    pm_release_relation(ctx, rel);

    SchemaFile disk_check;
    CHECK(schema_open(TEST_SCHEMA_FILE, &disk_check) == MYDB_OK,
          "independent reopen before commit");
    RelationEntry *e_before = schema_find_relation_stat(&disk_check, TEST_REAL_TABLE);
    CHECK(e_before != NULL && e_before->num_rows == 0,
          "disk still shows num_rows == 0 mid-transaction — writes were deferred, not per-row");
    schema_close(&disk_check);

    CHECK(pm_commit(ctx) == MYDB_OK, "COMMIT succeeds");

    CHECK(schema_open(TEST_SCHEMA_FILE, &disk_check) == MYDB_OK,
          "independent reopen after commit");
    RelationEntry *e_after = schema_find_relation_stat(&disk_check, TEST_REAL_TABLE);
    CHECK(e_after != NULL && e_after->num_rows == (uint32_t)N,
          "single flush at COMMIT persisted all N rows at once");
    schema_close(&disk_check);

    teardown_ctx(ctx);
}

/* Phase 4: the same single-flush-at-commit proof as
 * test_bulk_insert_single_flush_at_commit above, but for PB[0]'s
 * used_bytes — see test_rollback_discards_dirty_quota for why this needs
 * enough rows to force a page split (reconcile_growth only touches
 * cat_track_alloc when the page count actually changes). */
static void test_bulk_insert_defers_quota_until_commit(void)
{
    printf("\n[test_bulk_insert_defers_quota_until_commit]\n");
    PartitionCtx *ctx = setup_ctx_with_real_table(TEST_REAL_TABLE);
    CHECK(ctx != NULL, "real-table fixture set up");
    if (!ctx) return;

    Catalog *cat = pctx_catalog(ctx);
    CHECK(cat != NULL, "catalog open on the fixture");
    if (!cat) { teardown_ctx(ctx); return; }
    uint64_t used_before = cat->header.used_bytes;

    RelationDef *rel = pm_find_relation(ctx, TEST_REAL_TABLE);
    CHECK(rel != NULL, "relation found for bulk insert");
    if (!rel) { teardown_ctx(ctx); return; }

    CHECK(pm_begin(ctx) == MYDB_OK, "BEGIN succeeds");

    const int N = 800;
    int insert_ok = 1;
    for (int i = 0; i < N; i++) {
        Row row;
        memset(&row, 0, sizeof(row));
        row.num_cols = 2;
        row.cols[0].type = TYPE_INT; row.cols[0].v.int_val = i + 1;
        row.cols[1].type = TYPE_INT; row.cols[1].v.int_val = i;
        if (pm_insert(ctx, rel, &row) != MYDB_OK) insert_ok = 0;
    }
    CHECK(insert_ok, "all N rows insert inside the transaction");
    pm_release_relation(ctx, rel);

    CHECK(cat->header.used_bytes > used_before,
          "used_bytes grew in memory — fixture forced at least one page split");

    /* Independent, freshly-opened Catalog handle — must still see the
     * pre-transaction used_bytes, proving the deferred bumps never
     * reached disk mid-transaction. */
    Catalog disk_check;
    CHECK(cat_open(TEST_CATALOG_FILE, &disk_check) == MYDB_OK,
          "independent reopen before commit");
    CHECK(disk_check.header.used_bytes == used_before,
          "disk still shows the pre-transaction used_bytes mid-transaction");
    cat_close(&disk_check);

    CHECK(pm_commit(ctx) == MYDB_OK, "COMMIT succeeds");

    CHECK(cat_open(TEST_CATALOG_FILE, &disk_check) == MYDB_OK,
          "independent reopen after commit");
    CHECK(disk_check.header.used_bytes == cat->header.used_bytes,
          "single flush at COMMIT persisted the grown used_bytes");
    cat_close(&disk_check);

    teardown_ctx(ctx);
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_pm_api ===\n");

    test_find_increments_pin_count();
    test_release_clamped_at_zero();
    test_find_missing_relation();
    test_debug_no_pinned_relations_null_safe();
    test_rollback_discards_dirty_metadata();
    test_rollback_discards_dirty_quota();
    test_bulk_insert_single_flush_at_commit();
    test_bulk_insert_defers_quota_until_commit();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
