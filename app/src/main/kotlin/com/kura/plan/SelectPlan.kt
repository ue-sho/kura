package com.kura.plan

import com.kura.query.Predicate
import com.kura.query.Scan
import com.kura.query.SelectScan
import com.kura.record.Schema

/**
 * The Plan class corresponding to the select
 * relational algebra operator.
 */
class SelectPlan(
    private val plan: Plan,
    private val predicate: Predicate
) : Plan {

    /**
     * Creates a select scan for this query.
     * @see Plan.open
     */
    override fun open(): Scan {
        val scan = plan.open()
        return SelectScan(scan, predicate)
    }

    /**
     * Estimates the number of block accesses in the selection,
     * which is the same as in the underlying query.
     * @see Plan.blocksAccessed
     */
    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    /**
     * Estimates the number of output records in the selection,
     * which is determined by the reduction factor of the predicate.
     * @see Plan.recordsOutput
     */
    override fun recordsOutput(): Int {
        return plan.recordsOutput() / predicate.reductionFactor(plan)
    }

    /**
     * Estimates the number of distinct field values
     * in the projection.
     * If the predicate contains a term equating the specified
     * field to a constant, then this value will be 1.
     * Otherwise, it will be the number of the distinct values
     * in the underlying query
     * (but not more than the size of the output table).
     * @see Plan.distinctValues
     */
    override fun distinctValues(fieldName: String): Int {
        if (predicate.equatesWithConstant(fieldName) != null) {
            return 1
        } else {
            val fieldName2 = predicate.equatesWithField(fieldName)
            return if (fieldName2 != null) {
                minOf(plan.distinctValues(fieldName), plan.distinctValues(fieldName2))
            } else {
                plan.distinctValues(fieldName)
            }
        }
    }

    /**
     * Returns the schema of the selection,
     * which is the same as in the underlying query.
     * @see Plan.schema
     */
    override fun schema(): Schema {
        return plan.schema()
    }
}