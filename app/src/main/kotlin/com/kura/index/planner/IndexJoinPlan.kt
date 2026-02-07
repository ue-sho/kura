package com.kura.index.planner

import com.kura.index.query.IndexJoinScan
import com.kura.metadata.IndexInfo
import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.record.Schema
import com.kura.record.TableScan

/**
 * The Plan class corresponding to the indexjoin
 * relational algebra operator.
 */
class IndexJoinPlan(
    private val plan1: Plan,
    private val plan2: Plan,
    private val indexInfo: IndexInfo,
    private val joinField: String
) : Plan {
    private val schema = Schema()

    init {
        schema.addAll(plan1.schema())
        schema.addAll(plan2.schema())
    }

    /**
     * Opens an indexjoin scan for this query.
     */
    override fun open(): Scan {
        val scan = plan1.open()
        // Throws an exception if plan2 is not a table plan.
        val ts = plan2.open() as TableScan
        val idx = indexInfo.open()
        return IndexJoinScan(scan, idx, joinField, ts)
    }

    /**
     * Estimates the number of block accesses to compute the join.
     * The formula is:
     * B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx) + R(indexjoin(p1,p2,idx))
     */
    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() +
            (plan1.recordsOutput() * indexInfo.blocksAccessed()) +
            recordsOutput()
    }

    /**
     * Estimates the number of output records in the join.
     * The formula is:
     * R(indexjoin(p1,p2,idx)) = R(p1)*R(idx)
     */
    override fun recordsOutput(): Int {
        return plan1.recordsOutput() * indexInfo.recordsOutput()
    }

    /**
     * Estimates the number of distinct values for the specified field.
     */
    override fun distinctValues(fieldName: String): Int {
        return if (plan1.schema().hasField(fieldName)) {
            plan1.distinctValues(fieldName)
        } else {
            plan2.distinctValues(fieldName)
        }
    }

    /**
     * Returns the schema of the index join.
     */
    override fun schema(): Schema {
        return schema
    }
}
