package com.kura.materialize

import com.kura.query.Constant
import com.kura.query.Scan

/**
 * The Scan class for the groupby operator.
 */
class GroupByScan(
    private val scan: Scan,
    private val groupFields: List<String>,
    private val aggFns: List<AggregationFn>
) : Scan {
    private var groupVal: GroupValue? = null
    private var moreGroups: Boolean = false

    init {
        beforeFirst()
    }

    /**
     * Position the scan before the first group.
     * Internally, the underlying scan is always
     * positioned at the first record of a group, which
     * means that this method moves to the
     * first underlying record.
     */
    override fun beforeFirst() {
        scan.beforeFirst()
        moreGroups = scan.next()
    }

    /**
     * Move to the next group.
     * The key of the group is determined by the
     * group values at the current record.
     * The method repeatedly reads underlying records until
     * it encounters a record having a different key.
     * The aggregation functions are called for each record
     * in the group.
     * The values of the grouping fields for the group are saved.
     */
    override fun next(): Boolean {
        if (!moreGroups) {
            return false
        }
        for (fn in aggFns) {
            fn.processFirst(scan)
        }
        groupVal = GroupValue(scan, groupFields)
        while (scan.next().also { moreGroups = it }) {
            val gv = GroupValue(scan, groupFields)
            if (groupVal != gv) {
                break
            }
            for (fn in aggFns) {
                fn.processNext(scan)
            }
        }
        return true
    }

    /**
     * Close the scan by closing the underlying scan.
     */
    override fun close() {
        scan.close()
    }

    /**
     * Get the Constant value of the specified field.
     * If the field is a group field, then its value can
     * be obtained from the saved group value.
     * Otherwise, the value is obtained from the
     * appropriate aggregation function.
     */
    override fun getVal(fieldName: String): Constant {
        if (groupFields.contains(fieldName)) {
            return groupVal!!.getVal(fieldName)
        }
        for (fn in aggFns) {
            if (fn.fieldName() == fieldName) {
                return fn.value()
            }
        }
        throw RuntimeException("field $fieldName not found.")
    }

    /**
     * Get the integer value of the specified field.
     */
    override fun getInt(fieldName: String): Int {
        return getVal(fieldName).asInt()
    }

    /**
     * Get the string value of the specified field.
     */
    override fun getString(fieldName: String): String {
        return getVal(fieldName).asString()
    }

    /**
     * Return true if the specified field is either a
     * grouping field or created by an aggregation function.
     */
    override fun hasField(fieldName: String): Boolean {
        if (groupFields.contains(fieldName)) {
            return true
        }
        for (fn in aggFns) {
            if (fn.fieldName() == fieldName) {
                return true
            }
        }
        return false
    }
}
