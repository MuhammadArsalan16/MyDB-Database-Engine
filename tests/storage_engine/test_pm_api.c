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
#define TEST_PID         91
#define TEST_SID         7

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void)
{
    unlink(TEST_SCHEMA_FILE);
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

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
