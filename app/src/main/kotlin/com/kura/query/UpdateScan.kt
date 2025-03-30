package com.kura.query

import com.kura.record.RecordId

/**
 * The interface implemented by all updateable scans.
 */
interface UpdateScan : Scan {

    /**
     * Modify the field value of the current record.
     * @param fieldName the name of the field
     * @param value the new value, expressed as a Constant
     */
    fun setVal(fieldName: String, value: Constant)

    /**
     * Modify the field value of the current record.
     * @param fieldName the name of the field
     * @param value the new integer value
     */
    fun setInt(fieldName: String, value: Int)

    /**
     * Modify the field value of the current record.
     * @param fieldName the name of the field
     * @param value the new string value
     */
    fun setString(fieldName: String, value: String)

    /**
     * Insert a new record somewhere in the scan.
     */
    fun insert()

    /**
     * Delete the current record from the scan.
     */
    fun delete()

    /**
     * Return the id of the current record.
     * @return the id of the current record
     */
    fun getRecordId(): RecordId

    /**
     * Position the scan so that the current record has
     * the specified id.
     * @param recordId the id of the desired record
     */
    fun moveToRecordId(recordId: RecordId)
}