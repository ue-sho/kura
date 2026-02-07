package com.kura.materialize

import com.kura.query.Constant
import com.kura.query.Scan

/**
 * The interface implemented by aggregation functions.
 * Aggregation functions are used by the groupby operator.
 */
interface AggregationFn {

    /**
     * Use the current record of the specified scan
     * to be the first record in the group.
     */
    fun processFirst(scan: Scan)

    /**
     * Use the current record of the specified scan
     * to be the next record in the group.
     */
    fun processNext(scan: Scan)

    /**
     * Return the name of the new aggregation field.
     */
    fun fieldName(): String

    /**
     * Return the computed aggregation value.
     */
    fun value(): Constant
}
