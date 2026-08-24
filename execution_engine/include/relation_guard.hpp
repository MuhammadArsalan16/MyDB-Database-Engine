#pragma once
/*
 * relation_guard.hpp — RAII release for pm_find_relation_const() pointers.
 *
 * Phase 1 of the PartitionBuffer redesign (PARTITION_BUFFER_DESIGN.md)
 * retrofits a pin/release discipline onto every RelationDef* the execution
 * engine resolves, ahead of a later phase where the underlying frame
 * becomes a real evictable cache. Matching every pm_find_relation_const()
 * with a pm_release_relation() by hand, at every early-return path across
 * exec_insert/exec_update/exec_delete/exec_create_table/exec_create_index/
 * exec_drop_table/exec_analyze_table/exec_describe_table/exec_join_select/
 * exec_select, is exactly the shape of bug a leaked pin was flagged as —
 * so release is destructor-driven instead: declare a RelationGuard as a
 * local (or hold one per JoinSeg entry), and every return path — success,
 * early error, or a vector of guards partially populated when an error
 * hits mid-loop — releases correctly for free via normal C++ scope rules.
 *
 * Usage:
 *   RelationGuard rel_guard(ectx->partition,
 *                           pm_find_relation_const(ectx->partition, name));
 *   const RelationDef *rel = rel_guard.get();
 *   if (!rel) { ... }  // guard destructor no-ops on a null pointer
 */

extern "C" {
#include "pm_api.h"
}

class RelationGuard {
public:
    RelationGuard(PartitionCtx *ctx, const RelationDef *rel)
        : ctx_(ctx), rel_(rel) {}

    ~RelationGuard() {
        if (rel_) pm_release_relation(ctx_, rel_);
    }

    RelationGuard(const RelationGuard &) = delete;
    RelationGuard &operator=(const RelationGuard &) = delete;

    RelationGuard(RelationGuard &&other) noexcept
        : ctx_(other.ctx_), rel_(other.rel_) {
        other.rel_ = nullptr;
    }
    RelationGuard &operator=(RelationGuard &&other) noexcept {
        if (this != &other) {
            if (rel_) pm_release_relation(ctx_, rel_);
            ctx_ = other.ctx_;
            rel_ = other.rel_;
            other.rel_ = nullptr;
        }
        return *this;
    }

    const RelationDef *get() const { return rel_; }

private:
    PartitionCtx *ctx_;
    const RelationDef *rel_;
};
