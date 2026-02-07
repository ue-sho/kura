package com.kura.materialize

import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.transaction.Transaction
import kotlin.math.ceil

/**
 * The Plan class for the materialize operator.
 */
class MaterializePlan(
    private val transaction: Transaction,
    private val srcPlan: Plan
) : Plan {

    /**
     * This method loops through the underlying query,
     * copying its output records into a temporary table.
     * It then returns a table scan for that table.
     */
    override fun open(): Scan {
        val schema = srcPlan.schema()
        val temp = TempTable(transaction, schema)
        val src = srcPlan.open()
        val dest = temp.open()
        while (src.next()) {
            dest.insert()
            for (fieldName in schema.fields()) {
                dest.setVal(fieldName, src.getVal(fieldName))
            }
        }
        src.close()
        dest.beforeFirst()
        return dest
    }

    /**
     * Return the estimated number of blocks in the
     * materialized table.
     * It does not include the one-time cost
     * of materializing the records.
     */
    override fun blocksAccessed(): Int {
        val layout = Layout(srcPlan.schema())
        val rpb = transaction.blockSize().toDouble() / layout.slotSize().toDouble()
        return ceil(srcPlan.recordsOutput() / rpb).toInt()
    }

    /**
     * Return the number of records in the materialized table,
     * which is the same as in the underlying plan.
     */
    override fun recordsOutput(): Int {
        return srcPlan.recordsOutput()
    }

    /**
     * Return the number of distinct field values,
     * which is the same as in the underlying plan.
     */
    override fun distinctValues(fieldName: String): Int {
        return srcPlan.distinctValues(fieldName)
    }

    /**
     * Return the schema of the materialized table,
     * which is the same as in the underlying plan.
     */
    override fun schema(): Schema {
        return srcPlan.schema()
    }
}
