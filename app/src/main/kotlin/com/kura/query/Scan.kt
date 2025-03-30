package com.kura.query

/**
 * The interface will be implemented by each query scan.
 * There is a Scan class for each relational
 * algebra operator.
 */
interface Scan {

    /**
     * Position the scan before its first record. A
     * subsequent call to next() will return the first record.
     */
    fun beforeFirst()

    /**
     * Move the scan to the next record.
     * @return false if there is no next record
     */
    fun next(): Boolean

    /**
     * Return the value of the specified integer field
     * in the current record.
     * @param fieldName the name of the field
     * @return the field's integer value in the current record
     */
    fun getInt(fieldName: String): Int

    /**
     * Return the value of the specified string field
     * in the current record.
     * @param fieldName the name of the field
     * @return the field's string value in the current record
     */
    fun getString(fieldName: String): String

    /**
     * Return the value of the specified field in the current record.
     * The value is expressed as a Constant.
     * @param fieldName the name of the field
     * @return the value of that field, expressed as a Constant.
     */
    fun getVal(fieldName: String): Constant

    /**
     * Return true if the scan has the specified field.
     * @param fieldName the name of the field
     * @return true if the scan has that field
     */
    fun hasField(fieldName: String): Boolean

    /**
     * Close the scan and its subscans, if any.
     */
    fun close()
}