package com.kura.materialize

import com.kura.query.Scan

/**
 * A comparator for scans.
 */
class RecordComparator(
    private val fields: List<String>
) : Comparator<Scan> {

    /**
     * Compare the current records of the two specified scans.
     * The sort fields are considered in turn.
     * When a field is encountered for which the records have
     * different values, those values are used as the result
     * of the comparison.
     * If the two records have the same values for all
     * sort fields, then the method returns 0.
     */
    override fun compare(s1: Scan, s2: Scan): Int {
        for (fieldName in fields) {
            val val1 = s1.getVal(fieldName)
            val val2 = s2.getVal(fieldName)
            val result = val1.compareTo(val2)
            if (result != 0) {
                return result
            }
        }
        return 0
    }
}
