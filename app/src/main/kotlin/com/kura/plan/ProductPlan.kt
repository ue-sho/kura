package com.kura.plan

import com.kura.query.ProductScan
import com.kura.query.Scan
import com.kura.record.Schema

/**
 * The Plan class corresponding to the product
 * relational algebra operator.
 */
class ProductPlan(
    private val plan1: Plan,
    private val plan2: Plan
) : Plan {
    private val schema = Schema()

    init {
        schema.addAll(plan1.schema())
        schema.addAll(plan2.schema())
    }

    /**
     * Creates a product scan for this query.
     * @see Plan.open
     */
    override fun open(): Scan {
        val scan1 = plan1.open()
        val scan2 = plan2.open()
        return ProductScan(scan1, scan2)
    }

    /**
     * Estimates the number of block accesses in the product.
     * The formula is:
     * B(product(p1,p2)) = B(p1) + R(p1)*B(p2)
     * @see Plan.blocksAccessed
     */
    override fun blocksAccessed(): Int {
        return plan1.blocksAccessed() + (plan1.recordsOutput() * plan2.blocksAccessed())
    }

    /**
     * Estimates the number of output records in the product.
     * The formula is:
     * R(product(p1,p2)) = R(p1)*R(p2)
     * @see Plan.recordsOutput
     */
    override fun recordsOutput(): Int {
        return plan1.recordsOutput() * plan2.recordsOutput()
    }

    /**
     * Estimates the distinct number of field values in the product.
     * Since the product does not increase or decrease field values,
     * the estimate is the same as in the appropriate underlying query.
     * @see Plan.distinctValues
     */
    override fun distinctValues(fieldName: String): Int {
        return if (plan1.schema().hasField(fieldName)) {
            plan1.distinctValues(fieldName)
        } else {
            plan2.distinctValues(fieldName)
        }
    }

    /**
     * Returns the schema of the product,
     * which is the union of the schemas of the underlying queries.
     * @see Plan.schema
     */
    override fun schema(): Schema {
        return schema
    }
}