package com.kura.index.planner

import com.kura.index.query.IndexSelectScan
import com.kura.metadata.IndexInfo
import com.kura.plan.Plan
import com.kura.query.Constant
import com.kura.query.Scan
import com.kura.record.Schema
import com.kura.record.TableScan

/**
 * The Plan class corresponding to the indexselect
 * relational algebra operator.
 */
class IndexSelectPlan(
    private val plan: Plan,
    private val indexInfo: IndexInfo,
    private val searchKey: Constant
) : Plan {

    /**
     * Creates a new indexselect scan for this query.
     */
    override fun open(): Scan {
        // Throws an exception if plan is not a table plan.
        val ts = plan.open() as TableScan
        val idx = indexInfo.open()
        return IndexSelectScan(ts, idx, searchKey)
    }

    /**
     * Estimates the number of block accesses to compute the
     * index selection, which is the same as the
     * index traversal cost plus the number of matching data records.
     */
    override fun blocksAccessed(): Int {
        return indexInfo.blocksAccessed() + recordsOutput()
    }

    /**
     * Estimates the number of output records in the index selection,
     * which is the same as the number of search key values
     * for the index.
     */
    override fun recordsOutput(): Int {
        return indexInfo.recordsOutput()
    }

    /**
     * Returns the distinct values as defined by the index.
     */
    override fun distinctValues(fieldName: String): Int {
        return indexInfo.distinctValues(fieldName)
    }

    /**
     * Returns the schema of the data table.
     */
    override fun schema(): Schema {
        return plan.schema()
    }
}
