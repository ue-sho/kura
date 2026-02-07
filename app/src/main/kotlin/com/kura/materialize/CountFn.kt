package com.kura.materialize

import com.kura.query.Constant
import com.kura.query.Scan

/**
 * The count aggregation function.
 */
class CountFn(
    private val fieldName: String
) : AggregationFn {
    private var count: Int = 0

    /**
     * Start a new count.
     * Since Kura does not support null values,
     * every record will be counted,
     * regardless of the field.
     * The current count is thus set to 1.
     */
    override fun processFirst(scan: Scan) {
        count = 1
    }

    /**
     * Since Kura does not support null values,
     * this method always increments the count,
     * regardless of the field.
     */
    override fun processNext(scan: Scan) {
        count++
    }

    /**
     * Return the field's name, prepended by "countof".
     */
    override fun fieldName(): String {
        return "countof$fieldName"
    }

    /**
     * Return the current count.
     */
    override fun value(): Constant {
        return Constant(count)
    }
}
