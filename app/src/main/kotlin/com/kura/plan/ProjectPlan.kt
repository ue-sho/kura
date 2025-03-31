package com.kura.plan

import com.kura.query.ProjectScan
import com.kura.query.Scan
import com.kura.record.Schema

/**
 * The Plan class corresponding to the project
 * relational algebra operator.
 */
class ProjectPlan(
    private val plan: Plan,
    private val fieldList: List<String>
) : Plan {
    private val schema = Schema()

    init {
        for (fieldName in fieldList) {
            schema.add(fieldName, plan.schema())
        }
    }

    /**
     * Creates a project scan for this query.
     * @see Plan.open
     */
    override fun open(): Scan {
        val scan = plan.open()
        return ProjectScan(scan, schema.fields())
    }

    /**
     * Estimates the number of block accesses in the projection,
     * which is the same as in the underlying query.
     * @see Plan.blocksAccessed
     */
    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    /**
     * Estimates the number of output records in the projection,
     * which is the same as in the underlying query.
     * @see Plan.recordsOutput
     */
    override fun recordsOutput(): Int {
        return plan.recordsOutput()
    }

    /**
     * Estimates the number of distinct field values
     * in the projection,
     * which is the same as in the underlying query.
     * @see Plan.distinctValues
     */
    override fun distinctValues(fieldName: String): Int {
        return plan.distinctValues(fieldName)
    }

    /**
     * Returns the schema of the projection,
     * which is taken from the field list.
     * @see Plan.schema
     */
    override fun schema(): Schema {
        return schema
    }
}