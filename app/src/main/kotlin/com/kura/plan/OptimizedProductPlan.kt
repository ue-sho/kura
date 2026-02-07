package com.kura.plan

import com.kura.query.Scan
import com.kura.record.Schema

/**
 * A Plan class corresponding to the product relational algebra operator
 * that determines the most efficient ordering of its inputs.
 */
class OptimizedProductPlan(p1: Plan, p2: Plan) : Plan {
    private val bestPlan: Plan

    init {
        val prod1 = ProductPlan(p1, p2)
        val prod2 = ProductPlan(p2, p1)
        bestPlan = if (prod1.blocksAccessed() < prod2.blocksAccessed()) prod1 else prod2
    }

    override fun open(): Scan {
        return bestPlan.open()
    }

    override fun blocksAccessed(): Int {
        return bestPlan.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        return bestPlan.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return bestPlan.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return bestPlan.schema()
    }
}
