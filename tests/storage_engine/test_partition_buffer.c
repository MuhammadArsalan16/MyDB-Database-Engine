#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "common.h"
#include "partition_buffer.h"
#include "storage.h"

/* ------------------------------------------------------------------ */
/*  Phase 2 of the PartitionBuffer redesign (PARTITION_BUFFER_DESIGN.md):
 *  direct coverage of §7 (outer-slot eviction) and §8 (inner lazy-load/
 *  eviction) — the algorithms test_pm_api.c only exercises indirectly
 *  through pm_find_relation/pm_release_relation on a single schema.
 *  This file drives PartitionBuffer's own API directly, with multiple
 *  schemas and enough relations per schema to actually fill and overflow
 *  both the 8-slot outer cache and the 32-frame inner cache.
 *
 *  PartitionBuffer (~6.8MB, dominated by 8 outer slots x 32 inner frames
 *  each embedding a full RelationDef) and StorageEngine (~67MB, dominated
 *  by the buffer pool) are both always heap-allocated in the real code
 *  (pctx_init calloc's them) — never stack locals. This file follows the
 *  same discipline throughout.
 * ------------------------------------------------------------------ */

#define TEST_PART_DIR  "/tmp/mydb_test_partition_buffer_part"
#define TEST_PID       73

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Build a minimal RelationDef with the given name, two int columns,
 * pk on column 0. Returns by value. Copied per this codebase's established
 * convention — no shared test header exists, each file re-declares its own. */
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

static void schema_path(int idx, char *buf, size_t cap)
{
    snprintf(buf, cap, "%s/s%d_schema.mydb", TEST_PART_DIR, idx);
}

static void relname(int i, char *buf, size_t cap)
{
    snprintf(buf, cap, "r%d", i);
}

/* Removes every path this file could possibly have created — safe to call
 * even if only some of them exist. */
static void cleanup(void)
{
    char path[512];
    for (int i = 0; i < 16; i++) {
        schema_path(i, path, sizeof(path));
        unlink(path);
    }
    rmdir(TEST_PART_DIR);
}

/* Create schema index `idx` with n_relations relations named r0..r(n-1). */
static int make_schema(int idx, int n_relations)
{
    char path[512];
    schema_path(idx, path, sizeof(path));

    SchemaFile sf;
    if (schema_create(path, TEST_PID, (uint32_t)(idx + 1), "sX", &sf) != MYDB_OK)
        return MYDB_ERR;
    for (int i = 0; i < n_relations; i++) {
        char name[16];
        relname(i, name, sizeof(name));
        RelationDef d = make_simple_def(name);
        if (schema_add_relation(&sf, &d) != MYDB_OK) {
            schema_close(&sf);
            return MYDB_ERR;
        }
    }
    schema_close(&sf);
    return MYDB_OK;
}

/* Rename an on-disk schema's header.schema_name — make_schema() always
 * writes "sX"; tests needing multiple distinguishable schemas patch the
 * name in afterward rather than teaching make_schema a naming scheme. */
static void rename_schema(int idx, const char *new_name)
{
    char path[512];
    schema_path(idx, path, sizeof(path));
    SchemaFile tmp;
    schema_open(path, &tmp);
    strncpy(tmp.header.schema_name, new_name, sizeof(tmp.header.schema_name) - 1);
    schema_save_page0(&tmp);
    schema_close(&tmp);
}

/* Direct is-this-page-cached check against dir[] — the observable way to
 * tell "was this frame actually evicted" apart from "still resident,
 * still returns the same content on a hit" from outside partition_buffer.c. */
static int is_page_cached(PBOuterSlot *slot, uint8_t page_no)
{
    for (int i = 0; i < PB_INNER_SLOTS; i++)
        if (slot->dir[i].is_resident && slot->dir[i].page_no == page_no)
            return 1;
    return 0;
}

static uint8_t page_no_of(PBOuterSlot *slot, const char *relation_name)
{
    RelationEntry *e = schema_find_relation_stat(slot->sf, relation_name);
    return e ? e->page_no : 0;
}

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

static void test_init_destroy(void)
{
    printf("\n[test_init_destroy]\n");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    CHECK(pb_init(pb) == MYDB_OK, "pb_init succeeds");
    CHECK(pb->n_loaded == 0,      "starts empty");
    CHECK(pb_active_schema(pb) == NULL, "no active schema when empty");

    pb_destroy(pb);   /* must not crash on an empty buffer */
    CHECK(1, "pb_destroy on empty buffer does not crash");
    free(pb);
}

/* ------------------------------------------------------------------ */
/*  Outer slot (§7)                                                     */
/* ------------------------------------------------------------------ */

static void test_get_load_and_hit(void)
{
    printf("\n[test_get_load_and_hit]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    CHECK(make_schema(0, 1) == MYDB_OK, "fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);
    char path[512]; schema_path(0, path, sizeof(path));

    SchemaFile *sf1 = pb_get(pb, "sX", path, se);
    CHECK(sf1 != NULL,      "load succeeds");
    CHECK(pb->n_loaded == 1, "n_loaded bumped");

    SchemaFile *sf2 = pb_get(pb, "sX", path, se);
    CHECK(sf1 == sf2,       "cache hit returns the same pointer");
    CHECK(pb->n_loaded == 1, "still just 1 loaded — hit did not re-insert");

    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

static void test_outer_eviction_basic(void)
{
    printf("\n[test_outer_eviction_basic]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    for (int i = 0; i < 9; i++) CHECK(make_schema(i, 1) == MYDB_OK, "fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);

    /* Load e0..e7 (indices 0..7) — fills all 8 outer slots. e0 stays LRU
     * since nothing touches it again; each subsequent load makes the new
     * one MRU. */
    for (int i = 0; i < 8; i++) {
        char path[512], uniq_name[16];
        schema_path(i, path, sizeof(path));
        snprintf(uniq_name, sizeof(uniq_name), "e%d", i);
        rename_schema(i, uniq_name);

        SchemaFile *loaded = pb_get(pb, uniq_name, path, se);
        CHECK(loaded != NULL, "schema loads");
    }
    CHECK(pb->n_loaded == 8, "8 slots full");

    /* 9th distinct schema (e8) — must evict LRU (e0). */
    char path8[512];
    schema_path(8, path8, sizeof(path8));
    rename_schema(8, "e8");

    SchemaFile *loaded9 = pb_get(pb, "e8", path8, se);
    CHECK(loaded9 != NULL, "9th schema loads by evicting the LRU slot");
    CHECK(pb->n_loaded == 8, "still 8 loaded — eviction kept the count at cap");
    CHECK(pb_find(pb, "e0") == NULL, "LRU schema e0 was evicted");
    CHECK(pb_find(pb, "e8") != NULL, "new schema e8 is now cached");

    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

/* Same 9-schema setup as above, but e0 (the LRU candidate) has a pinned
 * inner frame — eviction must skip it and take the next LRU (e1) instead. */
static void test_outer_eviction_skips_pinned(void)
{
    printf("\n[test_outer_eviction_skips_pinned]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    for (int i = 0; i < 9; i++) CHECK(make_schema(i, 1) == MYDB_OK, "fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);

    for (int i = 0; i < 8; i++) {
        char path[512], uniq_name[16];
        schema_path(i, path, sizeof(path));
        snprintf(uniq_name, sizeof(uniq_name), "e%d", i);
        rename_schema(i, uniq_name);
        pb_get(pb, uniq_name, path, se);
    }

    /* Pin r0 in e0 (the would-be LRU eviction victim) and hold it. */
    PBOuterSlot *slot0 = pb_find_outer_slot(pb, "e0");
    RelationDef *pinned = pb_pin_relation(slot0, "r0");
    CHECK(pinned != NULL, "pin r0 in e0 succeeds");

    char path8[512];
    schema_path(8, path8, sizeof(path8));
    rename_schema(8, "e8");

    SchemaFile *loaded9 = pb_get(pb, "e8", path8, se);
    CHECK(loaded9 != NULL, "9th schema still loads");
    CHECK(pb_find(pb, "e0") != NULL, "pinned schema e0 survives eviction attempt");
    CHECK(pb_find(pb, "e1") == NULL, "e1 (next LRU, unpinned) evicted instead");

    pb_unpin_relation(slot0, pinned);
    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

/* All 8 outer slots have a pinned inner frame — the 9th load must fail
 * fast (NULL), not force-wait or crash. */
static void test_outer_fail_fast_when_all_pinned(void)
{
    printf("\n[test_outer_fail_fast_when_all_pinned]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    for (int i = 0; i < 9; i++) CHECK(make_schema(i, 1) == MYDB_OK, "fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);

    RelationDef *pinned[8];
    for (int i = 0; i < 8; i++) {
        char path[512], uniq_name[16];
        schema_path(i, path, sizeof(path));
        snprintf(uniq_name, sizeof(uniq_name), "e%d", i);
        rename_schema(i, uniq_name);
        pb_get(pb, uniq_name, path, se);

        PBOuterSlot *s = pb_find_outer_slot(pb, uniq_name);
        pinned[i] = pb_pin_relation(s, "r0");
        CHECK(pinned[i] != NULL, "pin r0 succeeds");
    }

    char path8[512];
    schema_path(8, path8, sizeof(path8));
    rename_schema(8, "e8");

    SchemaFile *result = pb_get(pb, "e8", path8, se);
    CHECK(result == NULL, "fails fast — every outer slot has a pinned frame");
    CHECK(pb->n_loaded == 8, "no existing slot was disturbed");

    for (int i = 0; i < 8; i++) {
        char uniq_name[16];
        snprintf(uniq_name, sizeof(uniq_name), "e%d", i);
        PBOuterSlot *s = pb_find_outer_slot(pb, uniq_name);
        pb_unpin_relation(s, pinned[i]);
    }
    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

/* ------------------------------------------------------------------ */
/*  Inner cache (§8)                                                    */
/* ------------------------------------------------------------------ */

static void test_inner_eviction(void)
{
    printf("\n[test_inner_eviction]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    CHECK(make_schema(0, 33) == MYDB_OK, "33-relation fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);
    char path[512]; schema_path(0, path, sizeof(path));
    pb_get(pb, "sX", path, se);
    PBOuterSlot *slot = pb_find_outer_slot(pb, "sX");

    uint8_t pno[33];
    for (int i = 0; i < 33; i++) {
        char name[16]; relname(i, name, sizeof(name));
        pno[i] = page_no_of(slot, name);
    }

    /* Fill the 32-frame cache exactly — pin+release r0..r31 in order. */
    for (int i = 0; i < 32; i++) {
        char name[16]; relname(i, name, sizeof(name));
        RelationDef *r = pb_pin_relation(slot, name);
        CHECK(r != NULL, "relation pins");
        pb_unpin_relation(slot, r);
    }
    CHECK(is_page_cached(slot, pno[0]),  "r0 still cached — cache exactly full, nothing evicted yet");
    CHECK(is_page_cached(slot, pno[31]), "r31 (most recently touched) cached");

    /* r32 is the 33rd distinct relation — must evict the inner LRU (r0,
     * touched first and never touched again). */
    RelationDef *r32 = pb_pin_relation(slot, "r32");
    CHECK(r32 != NULL, "r32 loads via inner eviction");
    CHECK(!is_page_cached(slot, pno[0]),  "r0 (LRU) evicted to make room");
    CHECK(is_page_cached(slot, pno[31]),  "r31 untouched by the eviction");
    CHECK(is_page_cached(slot, pno[32]),  "r32 now cached");

    pb_unpin_relation(slot, r32);
    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

/* r0 stays pinned throughout while 32 other relations cycle through —
 * inner eviction must never touch it, same invariant as
 * test_outer_eviction_skips_pinned but at the frame level. This is the
 * concrete, executable form of the StorageScan cross-call retention
 * invariant found during Phase 2 planning: a caller's pin (standing in
 * for a live Cursor's underlying RelationGuard) must survive unrelated
 * eviction pressure for as long as it's held. */
static void test_inner_eviction_skips_pinned(void)
{
    printf("\n[test_inner_eviction_skips_pinned]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    CHECK(make_schema(0, 33) == MYDB_OK, "33-relation fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);
    char path[512]; schema_path(0, path, sizeof(path));
    pb_get(pb, "sX", path, se);
    PBOuterSlot *slot = pb_find_outer_slot(pb, "sX");

    uint8_t pno0 = page_no_of(slot, "r0");

    RelationDef *held = pb_pin_relation(slot, "r0");
    CHECK(held != NULL, "r0 pinned and held");

    /* Cycle through r1..r32 (32 more distinct relations) — with r0 also
     * occupying a frame, this forces eviction among the 31 unpinned
     * frames + r0's permanently-occupied one, repeatedly. */
    for (int i = 1; i <= 32; i++) {
        char name[16]; relname(i, name, sizeof(name));
        RelationDef *r = pb_pin_relation(slot, name);
        CHECK(r != NULL, "unrelated relation pins despite r0 being held");
        pb_unpin_relation(slot, r);
    }

    CHECK(is_page_cached(slot, pno0), "r0 survived — never evicted while pinned");

    pb_unpin_relation(slot, held);
    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

static void test_inner_fail_fast_when_all_pinned(void)
{
    printf("\n[test_inner_fail_fast_when_all_pinned]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    CHECK(make_schema(0, 33) == MYDB_OK, "33-relation fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);
    char path[512]; schema_path(0, path, sizeof(path));
    pb_get(pb, "sX", path, se);
    PBOuterSlot *slot = pb_find_outer_slot(pb, "sX");

    RelationDef *held[32];
    for (int i = 0; i < 32; i++) {
        char name[16]; relname(i, name, sizeof(name));
        held[i] = pb_pin_relation(slot, name);
        CHECK(held[i] != NULL, "relation pins and stays held");
    }

    RelationDef *r32 = pb_pin_relation(slot, "r32");
    CHECK(r32 == NULL, "fails fast — all 32 inner frames are pinned");

    for (int i = 0; i < 32; i++) pb_unpin_relation(slot, held[i]);
    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

static void test_invalidate_page(void)
{
    printf("\n[test_invalidate_page]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    CHECK(make_schema(0, 1) == MYDB_OK, "fixture schema created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);
    char path[512]; schema_path(0, path, sizeof(path));
    pb_get(pb, "sX", path, se);
    PBOuterSlot *slot = pb_find_outer_slot(pb, "sX");

    RelationDef *r = pb_pin_relation(slot, "r0");
    pb_unpin_relation(slot, r);
    uint8_t pno = page_no_of(slot, "r0");
    CHECK(is_page_cached(slot, pno), "resident after pin+release");

    pb_invalidate_page(slot, pno);
    CHECK(!is_page_cached(slot, pno), "invalidated — no longer resident");

    /* Invalidation clears the cache entry, not the on-disk page — a
     * fresh pin still succeeds via reload. */
    RelationDef *r2 = pb_pin_relation(slot, "r0");
    CHECK(r2 != NULL, "re-pin after invalidate reloads from disk");
    pb_unpin_relation(slot, r2);

    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

/* Removing a schema must reset its outer slot's cache state — otherwise
 * a later schema reusing the same physical slot index would inherit
 * stale dir[]/inner[] entries from whatever occupied it before. */
static void test_remove_resets_cache(void)
{
    printf("\n[test_remove_resets_cache]\n");
    cleanup();
    mkdir(TEST_PART_DIR, 0755);
    CHECK(make_schema(0, 1) == MYDB_OK, "fixture schema rm1 created");
    CHECK(make_schema(1, 1) == MYDB_OK, "fixture schema rm2 created");

    PartitionBuffer *pb = (PartitionBuffer *)malloc(sizeof(PartitionBuffer));
    StorageEngine   *se = (StorageEngine *)malloc(sizeof(StorageEngine));
    pb_init(pb);
    storage_init(se);

    char path0[512], path1[512];
    schema_path(0, path0, sizeof(path0));
    schema_path(1, path1, sizeof(path1));
    rename_schema(0, "rm1");

    pb_get(pb, "rm1", path0, se);
    PBOuterSlot *slot1 = pb_find_outer_slot(pb, "rm1");
    RelationDef *r = pb_pin_relation(slot1, "r0");
    pb_unpin_relation(slot1, r);
    uint8_t pno_a = page_no_of(slot1, "r0");
    CHECK(is_page_cached(slot1, pno_a), "cached before remove");

    pb_remove(pb, "rm1");
    CHECK(pb_find(pb, "rm1") == NULL, "removed");
    CHECK(pb->n_loaded == 0,          "count dropped to 0");

    /* n_loaded == 0 means the next load takes the free-slot path, which
     * scans from index 0 — deterministically reusing the exact physical
     * slot rm1 just vacated. */
    rename_schema(1, "rm2");

    pb_get(pb, "rm2", path1, se);
    PBOuterSlot *slot2 = pb_find_outer_slot(pb, "rm2");
    CHECK(!is_page_cached(slot2, pno_a),
          "no stale residency carried over from the removed schema");

    pb_destroy(pb);
    free(pb); free(se);
    cleanup();
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_partition_buffer ===\n");

    test_init_destroy();

    test_get_load_and_hit();
    test_outer_eviction_basic();
    test_outer_eviction_skips_pinned();
    test_outer_fail_fast_when_all_pinned();

    test_inner_eviction();
    test_inner_eviction_skips_pinned();
    test_inner_fail_fast_when_all_pinned();
    test_invalidate_page();
    test_remove_resets_cache();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
