package com.kura.materialize

import com.kura.query.Constant
import com.kura.query.Scan

/**
 * An object that holds the values of the grouping fields
 * for the current record of a scan.
 */
class GroupValue(
    scan: Scan,
    fields: List<String>
) {
    private val values: Map<String, Constant> = fields.associateWith { scan.getVal(it) }

    /**
     * Return the Constant value of the specified field in the group.
     */
    fun getVal(fieldName: String): Constant {
        return values[fieldName]!!
    }

    /**
     * Two GroupValue objects are equal if they have the same values
     * for their grouping fields.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is GroupValue) return false
        for ((fieldName, v1) in values) {
            val v2 = other.getVal(fieldName)
            if (v1 != v2) return false
        }
        return true
    }

    /**
     * The hashcode of a GroupValue object is the sum of the
     * hashcodes of its field values.
     */
    override fun hashCode(): Int {
        var hashVal = 0
        for (c in values.values) {
            hashVal += c.hashCode()
        }
        return hashVal
    }
}
