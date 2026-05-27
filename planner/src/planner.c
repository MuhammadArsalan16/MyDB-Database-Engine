/*
 * planner.c — selectivity estimation and access path selection.
 *
 * =========================================================================
 * selectivity_estimate: formula derivations
 * =========================================================================
 *
 * All formulas return P(predicate satisfied | row sampled uniformly).
 * The core primitive is sel_le_v (P(col <= v)), from which every other
 * operator is derived algebraically.  This means tuning the one primitive
 * improves all operators automatically.
 *
 * --- sel_le_v: P(col <= v) ---
 *
 *   Three paths, evaluated in priority order:
 *
 *   1. Histogram (STATS_TYPE_HISTOGRAM):
 *      Walk sorted HistBucket array left to right.
 *        - Buckets fully to the left of v: add row_count directly.
 *        - The one partial bucket containing v: linear interpolation.
 *            frac = (v - prev_upper) / (bucket_upper - prev_upper)
 *            rows_in_fraction = row_count * frac
 *      Linear interpolation assumes uniform distribution within each
 *      bucket.  This is the standard equi-height histogram assumption.
 *
 *   2. MCV list (STATS_TYPE_MCV):
 *      Sum frequencies of MCV entries where value <= v.
 *      For values not in the MCV list (the "remainder"):
 *        remainder_rows     = non_null_rows - sum_of_mcv_frequencies
 *        remainder_distinct = num_distinct  - blob_count
 *      Assume remainder is uniformly spread over [min, max]:
 *        remainder_contrib = remainder_rows * (v - min) / (max - min)
 *
 *   3. No stats (STATS_TYPE_NONE, or ANALYZE not run):
 *      Linear interpolation using min_numeric / max_numeric, which the
 *      full scan always populates regardless of stats type.
 *      If min == max (all identical): return 0.5 * non_null_frac.
 *
 * --- Derived operators ---
 *
 *   P(col < v)         = P(col <= v) - P(col = v)
 *   P(col > v)         = 1 - P(col <= v) - P(IS NULL)
 *   P(col >= v)        = 1 - P(col < v)  - P(IS NULL)
 *                      = 1 - [P(col <= v) - P(col = v)] - P(IS NULL)
 *   P(col != v)        = 1 - P(col = v)
 *   P(lo <= col <= hi) = P(col <= hi) - P(col < lo)
 *                      = P(col <= hi) - [P(col <= lo) - P(col = lo)]
 *
 * --- sel_eq_v: P(col = v) ---
 *
 *   MCV hit:   return mcv[i].frequency / total_rows  (exact)
 *   MCV miss:  return (remainder_rows / remainder_distinct) / total_rows
 *   Histogram / no stats: 1/num_distinct * non_null_frac
 *
 * --- NULL handling ---
 *
 *   null_f = num_nulls / total_rows.  Comparison predicates cannot match
 *   NULL so P(comparison) <= non_null_frac = 1 - null_f.
 *   IS NULL / IS NOT NULL use null counts directly.
 *
 * --- Fallback defaults (has_stats == 0) ---
 *
 *   "="          -> 0.05   (1-in-20, safe over-estimate for equality)
 *   "IS_NULL"    -> 0.01
 *   "IS_NOT_NULL"-> 0.99
 *   "!="         -> 0.95
 *   range ops    -> 0.33   (biases toward full scan when data is unknown)
 *   unknown      -> 0.10
 *
 * =========================================================================
 * planner_choose_path: short-circuit rules + CBO
 * =========================================================================
 *
 * Short-circuit rules (no cost math, applied first):
 *   R1. n_sargs == 0                     -> FULL_SCAN
 *   R2. sarg on pk_col with op "="       -> PK_LOOKUP
 *   R3. sarg on UNIQUE indexed col, "="  -> INDEX_LOOKUP
 *   R4. no sarg touches any indexed col  -> FULL_SCAN
 *
 * CBO (only when genuine ambiguity remains):
 *   - Baseline: FULL_SCAN cost = num_pages * SEQ_IO
 *   - PK_RANGE candidates: any sarg on pk_col with a range op
 *       cost = tree_height * RAND_IO + sel * num_pages * SEQ_IO
 *   - INDEX_RANGE candidates: any sarg on a secondary indexed col
 *       cost = tree_height * RAND_IO + sel * num_rows  * RAND_IO
 *   Whichever cost is smallest wins.  Ties go to FULL_SCAN (simpler).
 *
 * Multiple sargs on the same column (e.g. age >= 20 AND age <= 30):
 *   The function finds the tightest BETWEEN by tracking the lowest lo
 *   from ">=" / ">" sargs and the highest hi from "<=" / "<" sargs on
 *   the same column, then calls selectivity_estimate with "BETWEEN".
 *   A plain "=" sarg is already handled by R2/R3; only range ops reach
 *   the CBO so the tightest-range logic is safe.
 */

#include "planner.h"
#include "engine.h"
#include "schema_file.h"

#include <string.h>
#include <math.h>
#include <stdio.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Clamp to [0, 1]                                                    */
/* ------------------------------------------------------------------ */
static float clamp01(float x)
{
    if (x < 0.0f) return 0.0f;
    if (x > 1.0f) return 1.0f;
    return x;
}

/* ================================================================== */
/*  sel_le_v — P(col <= v)                                            */
/* ================================================================== */
static float sel_le_v(const ColumnStats *cs,
                      const HistBucket  *hist,
                      const MCVEntry    *mcv,
                      int64_t            v)
{
    if (cs->total_rows == 0) return 0.0f;

    uint32_t total      = cs->total_rows;
    uint32_t nulls      = cs->num_nulls;
    float    non_null_f = (float)(total - nulls) / (float)total;

    if (total == nulls) return 0.0f;

    /* Out-of-range shortcuts using exact min/max from the full scan. */
    if (v >= cs->max_numeric) return non_null_f;
    if (v <  cs->min_numeric) return 0.0f;

    /* ---- Histogram path ---- */
    if (hist != NULL &&
        cs->stats_type == STATS_TYPE_HISTOGRAM &&
        cs->blob_count > 0)
    {
        uint64_t accum   = 0;
        int64_t  prev_ub = cs->min_numeric - 1;

        for (int b = 0; b < cs->blob_count; b++) {
            if (hist[b].upper_bound <= v) {
                accum  += hist[b].row_count;
                prev_ub = hist[b].upper_bound;
            } else {
                int64_t span = hist[b].upper_bound - prev_ub;
                if (span > 0) {
                    float frac = (float)(v - prev_ub) / (float)span;
                    accum += (uint64_t)((float)hist[b].row_count * frac);
                } else {
                    accum += hist[b].row_count;
                }
                break;
            }
        }
        return clamp01((float)accum / (float)total);
    }

    /* ---- MCV path ---- */
    if (mcv != NULL &&
        cs->stats_type == STATS_TYPE_MCV &&
        cs->blob_count > 0)
    {
        uint64_t mcv_accum = 0;
        uint64_t mcv_total = 0;

        for (int i = 0; i < cs->blob_count; i++) {
            mcv_total += mcv[i].frequency;
            if (mcv[i].value <= v)
                mcv_accum += mcv[i].frequency;
        }

        uint64_t non_null_rows  = (uint64_t)(total - nulls);
        uint64_t remainder_rows = (non_null_rows > mcv_total)
                                   ? non_null_rows - mcv_total : 0;

        if (remainder_rows > 0) {
            float range = (float)(cs->max_numeric - cs->min_numeric);
            float pos   = (range > 0.0f)
                           ? (float)(v - cs->min_numeric) / range
                           : 0.5f;
            mcv_accum += (uint64_t)((float)remainder_rows * clamp01(pos));
        }
        return clamp01((float)mcv_accum / (float)total);
    }

    /* ---- No stats: linear interpolation on [min, max] ---- */
    float range = (float)(cs->max_numeric - cs->min_numeric);
    if (range <= 0.0f)
        return 0.5f * non_null_f;

    return clamp01((float)(v - cs->min_numeric) / range) * non_null_f;
}

/* ================================================================== */
/*  sel_eq_v — P(col = v)                                             */
/* ================================================================== */
static float sel_eq_v(const ColumnStats *cs,
                      const MCVEntry    *mcv,
                      int64_t            v)
{
    if (cs->total_rows == 0) return 0.0f;

    uint32_t total = cs->total_rows;
    uint32_t nulls = cs->num_nulls;

    if (mcv != NULL &&
        cs->stats_type == STATS_TYPE_MCV &&
        cs->blob_count > 0)
    {
        for (int i = 0; i < cs->blob_count; i++) {
            if (mcv[i].value == v)
                return (float)mcv[i].frequency / (float)total;
        }

        /* Miss: uniform estimate for non-MCV values. */
        uint64_t mcv_total = 0;
        for (int i = 0; i < cs->blob_count; i++)
            mcv_total += mcv[i].frequency;

        uint64_t non_null_rows  = (uint64_t)(total - nulls);
        uint64_t remainder_rows = (non_null_rows > mcv_total)
                                   ? non_null_rows - mcv_total : 0;
        uint32_t rem_distinct   = (cs->num_distinct > (uint32_t)cs->blob_count)
                                   ? cs->num_distinct - (uint32_t)cs->blob_count : 0;

        if (rem_distinct == 0 || remainder_rows == 0) return 0.0f;
        return clamp01((float)(remainder_rows / rem_distinct) / (float)total);
    }

    /* Histogram or no stats: 1 / num_distinct * non_null_frac */
    if (cs->num_distinct > 0) {
        float non_null_f = (float)(total - nulls) / (float)total;
        return clamp01((1.0f / (float)cs->num_distinct) * non_null_f);
    }

    return 0.01f;
}

/* ================================================================== */
/*  selectivity_estimate — public                                     */
/* ================================================================== */
float selectivity_estimate(const ColumnStats *cs,
                            const MCVEntry   *mcv,
                            const HistBucket *hist,
                            const char       *op,
                            int64_t           lo,
                            int64_t           hi)
{
    /* Fallback when no stats collected */
    if (!cs || !cs->has_stats || cs->total_rows == 0) {
        if (strcmp(op, "=")          == 0) return 0.05f;
        if (strcmp(op, "IS_NULL")    == 0) return 0.01f;
        if (strcmp(op, "IS_NOT_NULL")== 0) return 0.99f;
        if (strcmp(op, "!=")         == 0) return 0.95f;
        return 0.33f;
    }

    float null_f = (float)cs->num_nulls / (float)cs->total_rows;

    if (strcmp(op, "IS_NULL") == 0)    return clamp01(null_f);
    if (strcmp(op, "IS_NOT_NULL") == 0) return clamp01(1.0f - null_f);
    if (strcmp(op, "=") == 0)          return sel_eq_v(cs, mcv, lo);
    if (strcmp(op, "!=") == 0)         return clamp01(1.0f - sel_eq_v(cs, mcv, lo));

    if (strcmp(op, "<=") == 0)
        return sel_le_v(cs, hist, mcv, lo);

    if (strcmp(op, "<") == 0) {
        float le = sel_le_v(cs, hist, mcv, lo);
        float eq = sel_eq_v(cs,       mcv, lo);
        return clamp01(le - eq);
    }

    if (strcmp(op, ">") == 0) {
        float le = sel_le_v(cs, hist, mcv, lo);
        return clamp01(1.0f - le - null_f);
    }

    if (strcmp(op, ">=") == 0) {
        float le = sel_le_v(cs, hist, mcv, lo);
        float eq = sel_eq_v(cs,       mcv, lo);
        return clamp01(1.0f - clamp01(le - eq) - null_f);
    }

    if (strcmp(op, "BETWEEN") == 0) {
        /* P(lo <= col <= hi) = P(col <= hi) - [P(col <= lo) - P(col = lo)] */
        float le_hi = sel_le_v(cs, hist, mcv, hi);
        float le_lo = sel_le_v(cs, hist, mcv, lo);
        float eq_lo = sel_eq_v(cs,       mcv, lo);
        return clamp01(le_hi - clamp01(le_lo - eq_lo));
    }

    return 0.1f;   /* LIKE or unknown: heuristic */
}

/* ================================================================== */
/*  planner_choose_path helpers                                       */
/* ================================================================== */

/*
 * Check whether col_idx has an entry in rel->secondary_col_idx[].
 * Returns the secondary index slot (0..n-1), or -1 if not indexed.
 */
static int find_secondary_slot(const RelationDef *rel, int col_idx)
{
    for (int j = 0; j < rel->num_secondary_indexes; j++)
        if ((int)rel->secondary_col_idx[j] == col_idx) return j;
    return -1;
}

/*
 * best_sarg_for_col — given multiple sargs on the same column, produce
 * the tightest selectivity estimate.
 *
 * Strategy: combine a lower bound (from >=, >) and upper bound (from
 * <=, <) into a BETWEEN.  A single-sided predicate keeps its own op.
 * If no range sargs exist for this column, returns 0 (no candidate).
 *
 * out_op, out_lo, out_hi receive the synthesised predicate.
 */
static int best_sarg_for_col(const Sarg *sargs, int n_sargs, int col_idx,
                               char *out_op, int64_t *out_lo, int64_t *out_hi)
{
    int   has_lo = 0, has_hi = 0;
    int64_t lo = 0, hi = 0;
    const char *lo_op = NULL, *hi_op = NULL;

    for (int i = 0; i < n_sargs; i++) {
        if (sargs[i].col_idx != col_idx) continue;
        const char *op = sargs[i].op;

        if (strcmp(op, ">=") == 0 || strcmp(op, ">") == 0) {
            if (!has_lo || sargs[i].lo > lo) {
                lo    = sargs[i].lo;
                lo_op = op;
                has_lo = 1;
            }
        } else if (strcmp(op, "<=") == 0 || strcmp(op, "<") == 0) {
            if (!has_hi || sargs[i].lo < hi) {
                hi    = sargs[i].lo;
                hi_op = op;
                has_hi = 1;
            }
        } else if (strcmp(op, "BETWEEN") == 0) {
            if (!has_lo || sargs[i].lo > lo) { lo = sargs[i].lo; has_lo = 1; lo_op = ">="; }
            if (!has_hi || sargs[i].hi < hi) { hi = sargs[i].hi; has_hi = 1; hi_op = "<="; }
        }
    }

    if (!has_lo && !has_hi) return 0;  /* no usable range sarg */

    if (has_lo && has_hi) {
        strncpy(out_op, "BETWEEN", 15);
        *out_lo = lo;
        *out_hi = hi;
    } else if (has_lo) {
        strncpy(out_op, lo_op, 15);
        *out_lo = lo;
        *out_hi = lo;
    } else {
        strncpy(out_op, hi_op, 15);
        *out_lo = hi;
        *out_hi = hi;
    }
    return 1;
}

/* ================================================================== */
/*  planner_choose_path — public                                      */
/* ================================================================== */
PlanNode planner_choose_path(struct EngineState *eng,
                              const RelationDef  *rel,
                              const Sarg         *sargs,
                              int                 n_sargs)
{
    PlanNode p;
    memset(&p, 0, sizeof(p));

    /* ----------------------------------------------------------------
     * Fetch physical-size metadata from the RelationEntry.
     * These three fields feed every cost formula.
     * ---------------------------------------------------------------- */
    RelationEntry *ent = schema_find_relation_stat(
                             &eng->active_schema, rel->relation_name);

    uint32_t num_pages   = (ent && ent->num_pages   > 0) ? ent->num_pages   : 1;
    uint32_t num_rows    = (ent && ent->num_rows     > 0) ? ent->num_rows    : 1;
    uint8_t  tree_height = (ent && ent->tree_height  > 0) ? ent->tree_height : 3;
    int      slot_idx    = ent ? (int)(ent - eng->active_schema.relations) : -1;

    int pk_col = (int)rel->pk_col_idx;

    /* ================================================================
     * Short-circuit R1: no WHERE predicates at all.
     * ================================================================ */
    if (n_sargs == 0) {
        p.path          = ACCESS_FULL_SCAN;
        p.index_col_idx = -1;
        p.selectivity   = 1.0f;
        p.cost          = (float)num_pages * PLANNER_SEQ_IO;
        return p;
    }

    /* ================================================================
     * Short-circuit R2: WHERE pk = v
     *
     * Descend the clustered B-tree to the single matching leaf.
     * No stats needed — there is exactly one valid path for this query.
     * ================================================================ */
    for (int i = 0; i < n_sargs; i++) {
        if (sargs[i].col_idx == pk_col && strcmp(sargs[i].op, "=") == 0) {
            p.path          = ACCESS_PK_LOOKUP;
            p.index_col_idx = -1;
            /* Selectivity = 1 row / num_rows (guaranteed unique PK). */
            p.selectivity   = (num_rows > 0) ? 1.0f / (float)num_rows : 0.0f;
            p.cost          = (float)tree_height * PLANNER_RAND_IO;
            return p;
        }
    }

    /* ================================================================
     * Short-circuit R3: WHERE unique_col = v
     *
     * Descend the secondary unique B-tree, then one clustered fetch.
     * Also only one valid path — the secondary index is guaranteed to
     * hold at most one matching key.
     * ================================================================ */
    for (int i = 0; i < n_sargs; i++) {
        int ci = sargs[i].col_idx;
        if (strcmp(sargs[i].op, "=") != 0)          continue;
        if (!rel->columns[ci].is_unique)             continue;
        if (ci == pk_col)                            continue;
        if (find_secondary_slot(rel, ci) < 0)        continue;

        p.path          = ACCESS_INDEX_LOOKUP;
        p.index_col_idx = ci;
        p.selectivity   = (num_rows > 0) ? 1.0f / (float)num_rows : 0.0f;
        /* +1 for the clustered page fetch after the secondary leaf. */
        p.cost          = (float)(tree_height + 1) * PLANNER_RAND_IO;
        return p;
    }

    /* ================================================================
     * Short-circuit R4: no sarg touches any indexed column.
     *
     * If none of the predicates can use an index (e.g. only non-indexed
     * columns appear in the WHERE clause), a full scan is forced.
     * ================================================================ */
    int any_indexed = 0;
    for (int i = 0; i < n_sargs && !any_indexed; i++) {
        int ci = sargs[i].col_idx;
        if (ci == pk_col) { any_indexed = 1; break; }
        if (find_secondary_slot(rel, ci) >= 0) { any_indexed = 1; break; }
    }
    if (!any_indexed) {
        p.path          = ACCESS_FULL_SCAN;
        p.index_col_idx = -1;
        p.selectivity   = 1.0f;
        p.cost          = (float)num_pages * PLANNER_SEQ_IO;
        return p;
    }

    /* ================================================================
     * CBO: genuine ambiguity — compare FULL_SCAN vs index candidates.
     *
     * Open __stats.mydb to get column selectivity estimates.  If the
     * file doesn't exist (ANALYZE hasn't been run), selectivity_estimate
     * falls back to hard-coded defaults, which bias toward FULL_SCAN.
     * ================================================================ */
    StatsFile sf;
    int file_opened = 0;
    int stats_ok    = 0;

    if (slot_idx >= 0) {
        char stats_path[512];
        snprintf(stats_path, sizeof(stats_path), "%s/%s/__stats.mydb",
                 eng->current_partition_path,
                 eng->current_schema_name);

        if (stats_open(stats_path, &sf) == MYDB_OK) {
            file_opened = 1;
            stats_ok = (stats_load_relation(&sf, slot_idx) == MYDB_OK);
        }
    }

    /* Baseline: FULL_SCAN.  Every candidate must beat this to win. */
    float best_cost = (float)num_pages * PLANNER_SEQ_IO;

    p.path          = ACCESS_FULL_SCAN;
    p.index_col_idx = -1;
    p.selectivity   = 1.0f;
    p.cost          = best_cost;

    /* ----------------------------------------------------------------
     * Candidate 1: PK_RANGE
     *
     * Any range sarg on the PK column drives a clustered range scan.
     * The leaf pages are in key order so the post-descent scan is
     * sequential — the cheapest possible index scan.
     *
     * Cost = tree_height * RAND_IO            (descent to first leaf)
     *      + sel * num_pages * SEQ_IO         (forward leaf scan)
     * ---------------------------------------------------------------- */
    {
        char    op[16];
        int64_t lo, hi;

        if (best_sarg_for_col(sargs, n_sargs, pk_col, op, &lo, &hi)) {
            ColumnStats *cs   = stats_ok
                                 ? stats_get_column(&sf, slot_idx, pk_col) : NULL;
            MCVEntry    *mcv  = stats_ok
                                 ? stats_get_mcv   (&sf, slot_idx, pk_col) : NULL;
            HistBucket  *hist = stats_ok
                                 ? stats_get_hist  (&sf, slot_idx, pk_col) : NULL;

            float sel  = selectivity_estimate(cs, mcv, hist, op, lo, hi);
            float cost = (float)tree_height * PLANNER_RAND_IO
                       + sel * (float)num_pages * PLANNER_SEQ_IO;

            if (cost < best_cost) {
                best_cost       = cost;
                p.path          = ACCESS_PK_RANGE;
                p.index_col_idx = -1;
                p.selectivity   = sel;
                p.cost          = cost;
            }
        }
    }

    /* ----------------------------------------------------------------
     * Candidate 2+: INDEX_RANGE for each secondary index column.
     *
     * Secondary index leaves are in indexed-column order, NOT in PK
     * order.  Each matching row requires a separate random clustered
     * page fetch → random I/O per matching row.
     *
     * Cost = tree_height * RAND_IO            (descent to first leaf)
     *      + sel * num_rows * RAND_IO         (one random fetch per row)
     *
     * This formula shows why secondary index scans lose to FULL_SCAN
     * at moderate-to-high selectivity: once sel > 1/(RAND_IO * rpp),
     * where rpp = rows per page, FULL_SCAN's sequential reads win.
     * ---------------------------------------------------------------- */
    for (int j = 0; j < rel->num_secondary_indexes; j++) {
        int ci = (int)rel->secondary_col_idx[j];

        char    op[16];
        int64_t lo, hi;

        if (!best_sarg_for_col(sargs, n_sargs, ci, op, &lo, &hi))
            continue;

        ColumnStats *cs   = stats_ok
                             ? stats_get_column(&sf, slot_idx, ci) : NULL;
        MCVEntry    *mcv  = stats_ok
                             ? stats_get_mcv   (&sf, slot_idx, ci) : NULL;
        HistBucket  *hist = stats_ok
                             ? stats_get_hist  (&sf, slot_idx, ci) : NULL;

        float sel  = selectivity_estimate(cs, mcv, hist, op, lo, hi);
        float cost = (float)tree_height * PLANNER_RAND_IO
                   + sel * (float)num_rows * PLANNER_RAND_IO;

        if (cost < best_cost) {
            best_cost       = cost;
            p.path          = ACCESS_INDEX_RANGE;
            p.index_col_idx = ci;
            p.selectivity   = sel;
            p.cost          = cost;
        }
    }

    if (file_opened) stats_close(&sf);
    return p;
}
