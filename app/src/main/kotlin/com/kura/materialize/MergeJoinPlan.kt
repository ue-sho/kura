package com.kura.materialize

import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.record.Schema
import com.kura.transaction.Transaction

/**
 * The Plan class for the mergejoin operator.
 */
class MergeJoinPlan(
    transaction: Transaction,
    p1: Plan,
    p2: Plan,
    private val fieldName1: String,
    private val fieldName2: String
) : Plan {
    private val p1: Plan
    private val p2: Plan
    private val schema: Schema = Schema()

    init {
        val sortList1 = listOf(fieldName1)
        this.p1 = SortPlan(transaction, p1, sortList1)

        val sortList2 = listOf(fieldName2)
        this.p2 = SortPlan(transaction, p2, sortList2)

        schema.addAll(p1.schema())
        schema.addAll(p2.schema())
    }

    /**
     * The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     */
    override fun open(): Scan {
        val s1 = p1.open()
        val s2 = p2.open() as SortScan
        return MergeJoinScan(s1, s2, fieldName1, fieldName2)
    }

    /**
     * Return the number of block accesses required to
     * mergejoin the sorted tables.
     * Since a mergejoin can be performed with a single
     * pass through each table, the method returns
     * the sum of the block accesses of the
     * materialized sorted tables.
     * It does not include the one-time cost
     * of materializing and sorting the records.
     */
    override fun blocksAccessed(): Int {
        return p1.blocksAccessed() + p2.blocksAccessed()
    }

    /**
     * Return the number of records in the join.
     * Assuming uniform distribution, the formula is:
     * R(join(p1,p2)) = R(p1)*R(p2)/max(V(p1,F1),V(p2,F2))
     */
    override fun recordsOutput(): Int {
        val maxVals = maxOf(p1.distinctValues(fieldName1), p2.distinctValues(fieldName2))
        return (p1.recordsOutput() * p2.recordsOutput()) / maxVals
    }

    /**
     * Estimate the distinct number of field values in the join.
     * Since the join does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     */
    override fun distinctValues(fieldName: String): Int {
        return if (p1.schema().hasField(fieldName)) {
            p1.distinctValues(fieldName)
        } else {
            p2.distinctValues(fieldName)
        }
    }

    /**
     * Return the schema of the join,
     * which is the union of the schemas of the underlying queries.
     */
    override fun schema(): Schema {
        return schema
    }
}
