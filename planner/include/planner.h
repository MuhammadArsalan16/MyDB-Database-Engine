#ifndef PLANNER_H
#define PLANNER_H

#include "common.h"
#include "relation_def.h"
#include "stats.h"
#include "schema_file.h"   /* SchemaFile — physical-size metadata source */

#ifdef __cplusplus
extern "C" {
#endif

/*
 * planner.h — cost-based access path selection for the query planner.
 *
 * Sits between the execution engine (which extracts SARGs from the WHERE
 * clause) and the storage engine (which executes the chosen path).
 *
 * Two public entry points:
 *
 *   selectivity_estimate  — pure calculation from ColumnStats.  No file I/O.
 *                           Takes a pre-loaded ColumnStats pointer so it can
 *                           be called for each candidate column without
 *                           re-opening the stats file.
 *
 *   planner_choose_path   — opens __stats.mydb, applies short-circuit rules,
 *                           runs CBO when there is genuine ambiguity, and
 *                           returns the cheapest PlanNode.
 *
 * Cost unit: estimated page reads.
 *   SEQ_IO  = 1.0   (sequential: one cost unit per page, disk head stays put)
 *   RAND_IO = 4.0   (random:  4× penalty per page for seek + rotational wait)
 *
 * The five cost formulas:
 *   FULL_SCAN    = num_pages
 *   PK_LOOKUP    = tree_height × RAND_IO
 *   PK_RANGE     = tree_height × RAND_IO  +  sel × num_pages × SEQ_IO
 *   INDEX_LOOKUP = (tree_height + 1) × RAND_IO
 *   INDEX_RANGE  = tree_height × RAND_IO  +  sel × num_rows  × RAND_IO
 */

/* ------------------------------------------------------------------ */
/*  I/O cost constants                                                 */
/* ------------------------------------------------------------------ */
#define PLANNER_SEQ_IO   1.0f
#define PLANNER_RAND_IO  4.0f

/* ------------------------------------------------------------------ */
/*  AccessPathType                                                     */
/* ------------------------------------------------------------------ */
typedef enum {
    ACCESS_FULL_SCAN,      /* sequential scan of every leaf page       */
    ACCESS_PK_LOOKUP,      /* clustered index, exact PK equality        */
    ACCESS_PK_RANGE,       /* clustered index, PK range scan            */
    ACCESS_INDEX_LOOKUP,   /* unique secondary index, exact equality    */
    ACCESS_INDEX_RANGE     /* secondary index, range scan               */
} AccessPathType;

/* ------------------------------------------------------------------ */
/*  Sarg — one sargable predicate                                     */
/*                                                                    */
/*  A predicate is sargable when the WHERE clause has the form        */
/*    indexed_col  op  literal                                        */
/*  where op ∈ { =, !=, <, <=, >, >=, BETWEEN, IS_NULL, IS_NOT_NULL }*/
/*  and indexed_col is either the PK or has a secondary index.        */
/*                                                                    */
/*  lo/hi are int64-encoded using the same scheme as ColumnStats      */
/*  min_numeric/max_numeric:                                          */
/*    INT      → (int64_t) int_val                                    */
/*    DECIMAL  → decimal_val  (already int64, scaled)                 */
/*    DATE     → (int64_t) date_val                                   */
/*    DATETIME → datetime_val                                         */
/*    BOOL     → (int64_t) bool_val                                   */
/*    ENUM     → (int64_t) enum_val                                   */
/* ------------------------------------------------------------------ */
typedef struct {
    int     col_idx;   /* index into rel->columns[]                   */
    char    op[16];    /* "=","!=","<","<=",">",">=","BETWEEN",
                          "IS_NULL","IS_NOT_NULL"                      */
    int64_t lo;        /* lower bound (or the single value for =, !=) */
    int64_t hi;        /* upper bound — used only for BETWEEN          */
} Sarg;

/* ------------------------------------------------------------------ */
/*  PlanNode — chosen access path plus cost metadata                  */
/* ------------------------------------------------------------------ */
typedef struct {
    AccessPathType path;
    int            index_col_idx;   /* for INDEX_*: which secondary col;
                                       -1 for FULL_SCAN / PK_* paths  */
    float          cost;            /* estimated page reads             */
    float          selectivity;     /* fraction of rows expected        */
} PlanNode;

/* ------------------------------------------------------------------ */
/*  selectivity_estimate                                               */
/*                                                                    */
/*  Estimate the fraction of rows satisfying a single column          */
/*  predicate.  Returns a value in [0, 1].                            */
/*                                                                    */
/*  cs    — per-column stats from __stats.mydb; pass NULL or a zeroed */
/*          struct to fall back to hard-coded defaults.               */
/*  mcv   — pointer to the first MCVEntry in the blob pool, or NULL.  */
/*  hist  — pointer to the first HistBucket in the blob pool, or NULL.*/
/*  op    — one of: "=","!=","<","<=",">",">=","BETWEEN",             */
/*          "IS_NULL","IS_NOT_NULL"                                   */
/*  lo    — lower bound (or the sole value for =, !=, <, <=, >, >=)  */
/*  hi    — upper bound (used only when op == "BETWEEN")              */
/* ------------------------------------------------------------------ */
float selectivity_estimate(const ColumnStats *cs,
                            const MCVEntry   *mcv,
                            const HistBucket *hist,
                            const char       *op,
                            int64_t           lo,
                            int64_t           hi);

/* ------------------------------------------------------------------ */
/*  planner_choose_path                                                */
/*                                                                    */
/*  Choose the cheapest access path for a query against `rel` given   */
/*  the sargable predicates in sargs[0..n_sargs-1].                  */
/*                                                                    */
/*  The planner is engine-agnostic and does NO file I/O (v3): it       */
/*  receives the partition's active SchemaFile (for physical-size      */
/*  metadata) and a StatsFile* resolved by the engine's StatsBuffer    */
/*  pool.  `sf` may be NULL (ANALYZE never run / stats unavailable),   */
/*  in which case selectivity_estimate falls back to its defaults.     */
/*  Neither argument is mutated (stats pages are loaded on demand).    */
/*                                                                    */
/*  Short-circuit rules (evaluated before any cost calculation):      */
/*    R1. n_sargs == 0                   → FULL_SCAN immediately      */
/*    R2. WHERE pk = v                   → PK_LOOKUP immediately      */
/*    R3. WHERE unique_col = v           → INDEX_LOOKUP immediately   */
/*    R4. No sarg on any indexed column  → FULL_SCAN immediately      */
/*  CBO runs only when there is genuine ambiguity (range predicate or */
/*  multiple index candidates).                                       */
/* ------------------------------------------------------------------ */
PlanNode planner_choose_path(const SchemaFile   *schema,
                              StatsFile          *sf,
                              const RelationDef  *rel,
                              const Sarg         *sargs,
                              int                 n_sargs);

#ifdef __cplusplus
}
#endif

#endif /* PLANNER_H */
