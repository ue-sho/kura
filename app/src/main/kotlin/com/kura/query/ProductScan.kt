package com.kura.query

/**
 * The scan class corresponding to the <i>product</i> relational
 * algebra operator.
 */
class ProductScan(
    private val scan1: Scan,
    private val scan2: Scan
) : Scan {

    private var isFirstScanAdvanced = false

    init {
        beforeFirst()
    }

    /**
     * Position the scan before its first record.
     * In particular, the LHS scan is positioned at
     * its first record, and the RHS scan
     * is positioned before its first record.
     */
    override fun beforeFirst() {
        scan1.beforeFirst()
        scan2.beforeFirst()
        isFirstScanAdvanced = false
    }

    /**
     * Move the scan to the next record.
     * The method moves to the next RHS record, if possible.
     * Otherwise, it moves to the next LHS record and the
     * first RHS record.
     * If there are no more LHS records, the method returns false.
     */
    override fun next(): Boolean {
        // If this is the first call to next(), we need to advance scan1
        if (!isFirstScanAdvanced) {
            if (!scan1.next()) {
                return false // No records in the first scan
            }
            isFirstScanAdvanced = true
        }

        if (scan2.next()) {
            return true
        } else {
            scan2.beforeFirst()
            if (!scan2.next()) {
                return false // No records in the second scan
            }
            return scan1.next() // Try to advance to the next record in scan1
        }
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     */
    override fun getInt(fieldName: String): Int {
        return if (scan1.hasField(fieldName)) {
            scan1.getInt(fieldName)
        } else {
            scan2.getInt(fieldName)
        }
    }

    /**
     * Returns the string value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     */
    override fun getString(fieldName: String): String {
        return if (scan1.hasField(fieldName)) {
            scan1.getString(fieldName)
        } else {
            scan2.getString(fieldName)
        }
    }

    /**
     * Return the value of the specified field.
     * The value is obtained from whichever scan
     * contains the field.
     */
    override fun getVal(fieldName: String): Constant {
        return if (scan1.hasField(fieldName)) {
            scan1.getVal(fieldName)
        } else {
            scan2.getVal(fieldName)
        }
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     */
    override fun hasField(fieldName: String): Boolean {
        return scan1.hasField(fieldName) || scan2.hasField(fieldName)
    }

    /**
     * Close both underlying scans.
     */
    override fun close() {
        scan1.close()
        scan2.close()
    }
}