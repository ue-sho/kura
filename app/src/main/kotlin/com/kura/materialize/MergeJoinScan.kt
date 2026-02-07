package com.kura.materialize

import com.kura.query.Constant
import com.kura.query.Scan

/**
 * The Scan class for the mergejoin operator.
 */
class MergeJoinScan(
    private val s1: Scan,
    private val s2: SortScan,
    private val fieldName1: String,
    private val fieldName2: String
) : Scan {
    private var joinVal: Constant? = null

    init {
        beforeFirst()
    }

    /**
     * Close the scan by closing the two underlying scans.
     */
    override fun close() {
        s1.close()
        s2.close()
    }

    /**
     * Position the scan before the first record,
     * by positioning each underlying scan before
     * their first records.
     */
    override fun beforeFirst() {
        s1.beforeFirst()
        s2.beforeFirst()
    }

    /**
     * Move to the next record. This is where the action is.
     *
     * If the next RHS record has the same join value,
     * then move to it.
     * Otherwise, if the next LHS record has the same join value,
     * then reposition the RHS scan back to the first record
     * having that join value.
     * Otherwise, repeatedly move the scan having the smallest
     * value until a common join value is found.
     * When one of the scans runs out of records, return false.
     */
    override fun next(): Boolean {
        var hasMore2 = s2.next()
        if (hasMore2 && s2.getVal(fieldName2) == joinVal) {
            return true
        }

        var hasMore1 = s1.next()
        if (hasMore1 && s1.getVal(fieldName1) == joinVal) {
            s2.restorePosition()
            return true
        }

        while (hasMore1 && hasMore2) {
            val v1 = s1.getVal(fieldName1)
            val v2 = s2.getVal(fieldName2)
            if (v1.compareTo(v2) < 0) {
                hasMore1 = s1.next()
            } else if (v1.compareTo(v2) > 0) {
                hasMore2 = s2.next()
            } else {
                s2.savePosition()
                joinVal = s2.getVal(fieldName2)
                return true
            }
        }
        return false
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     */
    override fun getInt(fieldName: String): Int {
        return if (s1.hasField(fieldName)) {
            s1.getInt(fieldName)
        } else {
            s2.getInt(fieldName)
        }
    }

    /**
     * Return the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     */
    override fun getString(fieldName: String): String {
        return if (s1.hasField(fieldName)) {
            s1.getString(fieldName)
        } else {
            s2.getString(fieldName)
        }
    }

    /**
     * Return the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     */
    override fun getVal(fieldName: String): Constant {
        return if (s1.hasField(fieldName)) {
            s1.getVal(fieldName)
        } else {
            s2.getVal(fieldName)
        }
    }

    /**
     * Return true if the specified field is in
     * either of the underlying scans.
     */
    override fun hasField(fieldName: String): Boolean {
        return s1.hasField(fieldName) || s2.hasField(fieldName)
    }
}
