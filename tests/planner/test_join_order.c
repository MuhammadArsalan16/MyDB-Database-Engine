/*
 * test_join_order.c — unit tests for planner_plan_join_order and
 * planner_choose_join_algo.
 *
 * Fixtures are built directly in memory (no disk I/O, no engine bootstrap).
 * Follows the plain main() + CHECK macro convention used throughout tests/.
 */

#include <stdio.h>
#include <string.h>

#include "common.h"
#include "relation_def.h"
#include "schema_file.h"
#include "planner.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do {                                              \
    tests_run++;                                                           \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); }            \
    else      { printf("  FAIL: %s  (line %d)\n", msg, __LINE__); }        \
} while (0)

/* ------------------------------------------------------------------ */
/*  Fixture helpers                                                    */
/* ------------------------------------------------------------------ */

/* Populate one slot in an in-memory SchemaFile (no fd needed). */
static void sf_add(SchemaFile *sf, int slot, const char *name,
                   uint32_t rows, uint32_t pages, uint8_t height)
{
    strncpy(sf->relations[slot].relation_name, name, 31);
    sf->relations[slot].relation_name[31] = '\0';
    sf->relations[slot].is_valid    = 1;
    sf->relations[slot].num_rows    = rows;
    sf->relations[slot].num_pages   = pages;
    sf->relations[slot].tree_height = height;
    sf->header.num_relations++;
}

/*
 * Build a RelationDef with:
 *   col 0 — INT primary key
 *   col 1 — INT foreign-key / join column (optional secondary index)
 */
static RelationDef make_rel(const char *name, int has_sec_idx)
{
    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, name, MAX_TABLE_NAME - 1);
    r.num_columns      = 2;
    r.pk_col_idx       = 0;
    r.columns[0].is_primary_key = 1;
    r.columns[0].type  = TYPE_INT;
    r.columns[1].type  = TYPE_INT;
    if (has_sec_idx) {
        r.num_secondary_indexes = 1;
        r.secondary_col_idx[0]  = 1;
    }
    return r;
}

/* ------------------------------------------------------------------ */
/*  Test 1: 3-table hub topology — DP finds cheaper order than lexical */
/* ------------------------------------------------------------------ */
static void test_3table_reorder(void)
{
    printf("\n[3-table reorder]\n");

    /*
     * Hub schema:
     *   A ("ta") — 1 000 rows, 10 pages, height 3, secondary idx on col 1
     *   B ("tb") — 100 000 rows, 1 000 pages, height 4, secondary idx on col 1
     *   C ("tc") — 5 rows, 1 page, height 2, secondary idx on col 1
     *
     * Edges: A.id(col 0) = B.a_id(col 1)  AND  A.id(col 0) = C.a_id(col 1)
     *
     * Lexical order (A first → fold B → fold C):
     *   step1: SORT_MERGE(A,B) est. = pages_A + pages_B = 10 + 1000 = 1010
     *   card({A,B}) = 1000 * 100000 / max(1000, 100000) = 1000
     *   step2: HASH({A,B}, C) est. = pages_C + card = 1 + 1000 = 1001
     *   total_lexical ≈ 2011
     *
     * DP optimal (A seed → fold C → fold B):
     *   step1: SORT_MERGE(A,C) = 10 + 1 = 11
     *   card({A,C}) = 1000 * 5 / 1000 = 5
     *   step2: NLJ(5 left rows × B indexed) = 5 * (4+1) * 4 = 100
     *   total_dp ≈ 111
     */

    SchemaFile sf;
    memset(&sf, 0, sizeof(sf));
    sf.fd = -1;
    sf_add(&sf, 0, "ta", 1000,   10,  3);
    sf_add(&sf, 1, "tb", 100000, 1000, 4);
    sf_add(&sf, 2, "tc", 5,      1,   2);

    RelationDef ra = make_rel("ta", 1);
    RelationDef rb = make_rel("tb", 1);
    RelationDef rc = make_rel("tc", 1);

    const RelationDef *rels[3] = { &ra, &rb, &rc };

    /* A.col0 = B.col1  AND  A.col0 = C.col1 */
    JoinEdgeDef edges[2];
    edges[0].a_rel_idx = 0; edges[0].a_col_idx = 0;
    edges[0].b_rel_idx = 1; edges[0].b_col_idx = 1;
    edges[1].a_rel_idx = 0; edges[1].a_col_idx = 0;
    edges[1].b_rel_idx = 2; edges[1].b_col_idx = 1;

    JoinPlanResult plan = planner_plan_join_order(&sf, NULL, rels, 3, edges, 2);

    CHECK(plan.n_steps == 3, "3-table: n_steps == 3");
    CHECK(plan.steps[0].via_rel_idx == -1, "3-table: steps[0] is the seed");
    /* C (rel_idx=2) should be added before B (rel_idx=1) */
    CHECK(plan.steps[1].rel_idx == 2, "3-table: C (small) added before B (large)");
    CHECK(plan.total_cost < 200.0f,   "3-table: DP total cost below lexical estimate");
}

/* ------------------------------------------------------------------ */
/*  Test 2: NULL stats fallback — valid plan, no crash                */
/* ------------------------------------------------------------------ */
static void test_null_stats(void)
{
    printf("\n[null stats fallback]\n");

    SchemaFile sf;
    memset(&sf, 0, sizeof(sf));
    sf.fd = -1;
    sf_add(&sf, 0, "p", 100, 5, 3);
    sf_add(&sf, 1, "q", 200, 8, 3);
    sf_add(&sf, 2, "r",  50, 3, 2);

    RelationDef rp = make_rel("p", 0);
    RelationDef rq = make_rel("q", 1);
    RelationDef rr = make_rel("r", 1);

    const RelationDef *rels[3] = { &rp, &rq, &rr };

    JoinEdgeDef edges[2];
    edges[0].a_rel_idx = 0; edges[0].a_col_idx = 0;
    edges[0].b_rel_idx = 1; edges[0].b_col_idx = 1;
    edges[1].a_rel_idx = 0; edges[1].a_col_idx = 0;
    edges[1].b_rel_idx = 2; edges[1].b_col_idx = 1;

    /* sf=NULL forces NDV = num_rows fallback */
    JoinPlanResult plan = planner_plan_join_order(&sf, NULL, rels, 3, edges, 2);

    CHECK(plan.n_steps == 3,                "null-stats: n_steps == 3");
    CHECK(plan.steps[0].via_rel_idx == -1,  "null-stats: seed step valid");
    CHECK(plan.total_cost > 0.0f,           "null-stats: positive total cost");
}

/* ------------------------------------------------------------------ */
/*  Test 3: max group size (12 relations) — smoke test                */
/* ------------------------------------------------------------------ */
static void test_max_group(void)
{
    printf("\n[max group size n=12]\n");

    SchemaFile sf;
    memset(&sf, 0, sizeof(sf));
    sf.fd = -1;

    RelationDef rdefs[PLANNER_MAX_JOIN_GROUP];
    const RelationDef *rels[PLANNER_MAX_JOIN_GROUP];
    char names[PLANNER_MAX_JOIN_GROUP][8];

    /* Chain: rel[0].col0 = rel[1].col1, rel[1].col0 = rel[2].col1, … */
    JoinEdgeDef edges[PLANNER_MAX_JOIN_GROUP - 1];

    for (int i = 0; i < PLANNER_MAX_JOIN_GROUP; i++) {
        snprintf(names[i], sizeof(names[i]), "r%d", i);
        sf_add(&sf, i, names[i], (uint32_t)(100 * (i + 1)), (uint32_t)(i + 1), 3);
        rdefs[i] = make_rel(names[i], 1);
        rels[i]  = &rdefs[i];
    }
    for (int i = 0; i < PLANNER_MAX_JOIN_GROUP - 1; i++) {
        edges[i].a_rel_idx = i;   edges[i].a_col_idx = 0;
        edges[i].b_rel_idx = i+1; edges[i].b_col_idx = 1;
    }

    JoinPlanResult plan = planner_plan_join_order(&sf, NULL, rels, PLANNER_MAX_JOIN_GROUP,
                                                   edges, PLANNER_MAX_JOIN_GROUP - 1);

    CHECK(plan.n_steps == PLANNER_MAX_JOIN_GROUP, "max-group: n_steps == 12");
    CHECK(plan.total_cost > 0.0f,                 "max-group: positive total cost");
}

/* ------------------------------------------------------------------ */
/*  Test 4: no edge between two relations — cross join                */
/* ------------------------------------------------------------------ */
static void test_cross_join(void)
{
    printf("\n[cross join — no edge]\n");

    SchemaFile sf;
    memset(&sf, 0, sizeof(sf));
    sf.fd = -1;
    sf_add(&sf, 0, "x", 50, 3, 2);
    sf_add(&sf, 1, "y", 80, 4, 2);

    RelationDef rx = make_rel("x", 0);
    RelationDef ry = make_rel("y", 0);

    const RelationDef *rels[2] = { &rx, &ry };

    /* No edges */
    JoinPlanResult plan = planner_plan_join_order(&sf, NULL, rels, 2, NULL, 0);

    CHECK(plan.n_steps == 2,                "cross: n_steps == 2");
    CHECK(plan.steps[0].via_rel_idx == -1,  "cross: seed is valid");
    CHECK(plan.steps[1].is_cross == 1,      "cross: fold step flagged as cross");
}

/* ------------------------------------------------------------------ */
/*  Test 5: planner_choose_join_algo direct                           */
/* ------------------------------------------------------------------ */
static void test_choose_algo(void)
{
    printf("\n[planner_choose_join_algo]\n");

    /*
     * Size chosen so that each algorithm clearly wins:
     *
     *   Case A  NLJ vs HASH (no SORT_MERGE, is_first_step=0):
     *     right table: pages=1000, height=4.  left_card=2.
     *     NLJ  = 2 * (4+1) * 4 = 40
     *     HASH = 1000*1 + 2*1  = 1002   → NLJ wins
     *
     *   Case B  HASH only (right col not indexed):
     *     Same sizes; NLJ and SORT_MERGE not eligible → HASH is forced.
     *
     *   Case C  SORT_MERGE (is_first_step=1, both cols indexed):
     *     left pages=1, right pages=1, left_card=2, height=4.
     *     SORT_MERGE = 1 + 1 = 2
     *     NLJ        = 2 * (4+1) * 4 = 40
     *     HASH       = 1 + 2 = 3        → SORT_MERGE wins
     */

    SchemaFile sf;
    memset(&sf, 0, sizeof(sf));
    sf.fd = -1;
    sf_add(&sf, 0, "l",      500, 1000, 4);  /* large left side for case A */
    sf_add(&sf, 1, "r_idx",  500, 1000, 4);  /* indexed, large pages */
    sf_add(&sf, 2, "r_noidx",500, 1000, 4);  /* not indexed */
    sf_add(&sf, 3, "sl",       5,    1, 4);  /* small left for case C */
    sf_add(&sf, 4, "sr",       5,    1, 4);  /* small right for case C */

    RelationDef lrel     = make_rel("l",      0);  /* pk on col 0, no secondary */
    RelationDef rrel_idx = make_rel("r_idx",  1);  /* secondary on col 1 */
    RelationDef rrel_no  = make_rel("r_noidx",0);  /* no secondary */
    RelationDef slrel    = make_rel("sl",     0);  /* pk on col 0 */
    RelationDef srrel    = make_rel("sr",     1);  /* secondary on col 1 */

    /* Case A: right join col indexed, left_card small → NLJ cheaper than HASH */
    JoinAlgoChoice ca = planner_choose_join_algo(&sf, NULL,
                                                  &lrel,     0,  2.0f,
                                                  &rrel_idx, 1,
                                                  /*is_first_step=*/ 0);
    CHECK(ca.algo == JOIN_ALGO_NLJ,  "choose_algo: indexed right col → NLJ");

    /* Case B: right join col not indexed → only HASH is legal */
    JoinAlgoChoice cb = planner_choose_join_algo(&sf, NULL,
                                                  &lrel,    0, 2.0f,
                                                  &rrel_no, 1,
                                                  /*is_first_step=*/ 0);
    CHECK(cb.algo == JOIN_ALGO_HASH, "choose_algo: non-indexed right col → HASH");

    /* Case C: both cols indexed, is_first_step=1, tiny tables → SORT_MERGE cheapest */
    JoinAlgoChoice cc = planner_choose_join_algo(&sf, NULL,
                                                  &slrel, 0, 2.0f,
                                                  &srrel, 1,
                                                  /*is_first_step=*/ 1);
    CHECK(cc.algo == JOIN_ALGO_SORT_MERGE,
          "choose_algo: is_first_step, both indexed, small tables → SORT_MERGE");
}

/* ------------------------------------------------------------------ */
/*  main                                                               */
/* ------------------------------------------------------------------ */
int main(void)
{
    printf("=== test_join_order ===\n");

    test_3table_reorder();
    test_null_stats();
    test_max_group();
    test_cross_join();
    test_choose_algo();

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
