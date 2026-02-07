package com.kura.multibuffer

import com.kura.materialize.MaterializePlan
import com.kura.materialize.TempTable
import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.query.UpdateScan
import com.kura.record.Schema
import com.kura.transaction.Transaction

/**
 * The Plan class for the multi-buffer version of the
 * product operator.
 */
class MultibufferProductPlan(
    private val transaction: Transaction,
    lhs: Plan,
    private val rhs: Plan
) : Plan {
    private val lhs: Plan = MaterializePlan(transaction, lhs)
    private val schema: Schema = Schema()

    init {
        schema.addAll(lhs.schema())
        schema.addAll(rhs.schema())
    }

    /**
     * A scan for this query is created and returned, as follows.
     * First, the method materializes its LHS and RHS queries.
     * It then determines the optimal chunk size,
     * based on the size of the materialized RHS file and the
     * number of available buffers.
     */
    override fun open(): Scan {
        val leftScan = lhs.open()
        val tt = copyRecordsFrom(rhs)
        return MultibufferProductScan(transaction, leftScan, tt.tableName(), tt.getLayout())
    }

    /**
     * Returns an estimate of the number of block accesses
     * required to execute the query. The formula is:
     * B(product(p1,p2)) = B(p2) + B(p1) * C(p2)
     * where C(p2) is the number of chunks of p2.
     */
    override fun blocksAccessed(): Int {
        val avail = transaction.availableBuffers()
        val size = MaterializePlan(transaction, rhs).blocksAccessed()
        val numChunks = size / avail
        return rhs.blocksAccessed() + (lhs.blocksAccessed() * numChunks)
    }

    /**
     * Estimates the number of output records in the product.
     * R(product(p1,p2)) = R(p1) * R(p2)
     */
    override fun recordsOutput(): Int {
        return lhs.recordsOutput() * rhs.recordsOutput()
    }

    /**
     * Estimates the distinct number of field values in the product.
     * Since the product does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     */
    override fun distinctValues(fieldName: String): Int {
        return if (lhs.schema().hasField(fieldName)) {
            lhs.distinctValues(fieldName)
        } else {
            rhs.distinctValues(fieldName)
        }
    }

    /**
     * Returns the schema of the product,
     * which is the union of the schemas of the underlying queries.
     */
    override fun schema(): Schema {
        return schema
    }

    private fun copyRecordsFrom(plan: Plan): TempTable {
        val src = plan.open()
        val sch = plan.schema()
        val t = TempTable(transaction, sch)
        val dest: UpdateScan = t.open()
        while (src.next()) {
            dest.insert()
            for (fieldName in sch.fields()) {
                dest.setVal(fieldName, src.getVal(fieldName))
            }
        }
        src.close()
        dest.close()
        return t
    }
}
