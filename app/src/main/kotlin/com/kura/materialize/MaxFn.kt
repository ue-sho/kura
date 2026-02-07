package com.kura.materialize

import com.kura.query.Constant
import com.kura.query.Scan

/**
 * The max aggregation function.
 */
class MaxFn(
    private val fieldName: String
) : AggregationFn {
    private lateinit var value: Constant

    /**
     * Start a new maximum to be the
     * field value in the current record.
     */
    override fun processFirst(scan: Scan) {
        value = scan.getVal(fieldName)
    }

    /**
     * Replace the current maximum by the field value
     * in the current record, if it is higher.
     */
    override fun processNext(scan: Scan) {
        val newValue = scan.getVal(fieldName)
        if (newValue.compareTo(value) > 0) {
            value = newValue
        }
    }

    /**
     * Return the field's name, prepended by "maxof".
     */
    override fun fieldName(): String {
        return "maxof$fieldName"
    }

    /**
     * Return the current maximum.
     */
    override fun value(): Constant {
        return value
    }
}
