package com.kura.materialize

import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.record.Schema
import com.kura.transaction.Transaction

/**
 * The Plan class for the groupby operator.
 */
class GroupByPlan(
    transaction: Transaction,
    plan: Plan,
    private val groupFields: List<String>,
    private val aggFns: List<AggregationFn>
) : Plan {
    private val plan: Plan = SortPlan(transaction, plan, groupFields)
    private val schema: Schema = Schema()

    init {
        for (fieldName in groupFields) {
            schema.add(fieldName, plan.schema())
        }
        for (fn in aggFns) {
            schema.addIntField(fn.fieldName())
        }
    }

    /**
     * This method opens a sort plan for the specified plan.
     * The sort plan ensures that the underlying records
     * will be appropriately grouped.
     */
    override fun open(): Scan {
        val s = plan.open()
        return GroupByScan(s, groupFields, aggFns)
    }

    /**
     * Return the number of blocks required to
     * compute the aggregation,
     * which is one pass through the sorted table.
     * It does not include the one-time cost
     * of materializing and sorting the records.
     */
    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    /**
     * Return the number of groups. Assuming equal distribution,
     * this is the product of the distinct values
     * for each grouping field.
     */
    override fun recordsOutput(): Int {
        var numGroups = 1
        for (fieldName in groupFields) {
            numGroups *= plan.distinctValues(fieldName)
        }
        return numGroups
    }

    /**
     * Return the number of distinct values for the
     * specified field. If the field is a grouping field,
     * then the number of distinct values is the same
     * as in the underlying query.
     * If the field is an aggregate field, then we
     * assume that all values are distinct.
     */
    override fun distinctValues(fieldName: String): Int {
        return if (plan.schema().hasField(fieldName)) {
            plan.distinctValues(fieldName)
        } else {
            recordsOutput()
        }
    }

    /**
     * Returns the schema of the output table.
     * The schema consists of the group fields,
     * plus one field for each aggregation function.
     */
    override fun schema(): Schema {
        return schema
    }
}
