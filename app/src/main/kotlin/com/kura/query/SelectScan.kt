package com.kura.query

import com.kura.record.RecordId

/**
 * The scan class corresponding to the <i>select</i> relational
 * algebra operator.
 * All methods except next delegate their work to the
 * underlying scan.
 */
class SelectScan(
    private val scan: Scan,
    private val predicate: Predicate
) : UpdateScan {

    // Scan methods

    override fun beforeFirst() {
        scan.beforeFirst()
    }

    override fun next(): Boolean {
        while (scan.next()) {
            if (predicate.isSatisfied(scan)) {
                return true
            }
        }
        return false
    }

    override fun getInt(fieldName: String): Int {
        return scan.getInt(fieldName)
    }

    override fun getString(fieldName: String): String {
        return scan.getString(fieldName)
    }

    override fun getVal(fieldName: String): Constant {
        return scan.getVal(fieldName)
    }

    override fun hasField(fieldName: String): Boolean {
        return scan.hasField(fieldName)
    }

    override fun close() {
        scan.close()
    }

    // UpdateScan methods

    override fun setInt(fieldName: String, value: Int) {
        val updateScan = scan as UpdateScan
        updateScan.setInt(fieldName, value)
    }

    override fun setString(fieldName: String, value: String) {
        val updateScan = scan as UpdateScan
        updateScan.setString(fieldName, value)
    }

    override fun setVal(fieldName: String, value: Constant) {
        val updateScan = scan as UpdateScan
        updateScan.setVal(fieldName, value)
    }

    override fun delete() {
        val updateScan = scan as UpdateScan
        updateScan.delete()
    }

    override fun insert() {
        val updateScan = scan as UpdateScan
        updateScan.insert()
    }

    override fun getRecordId(): RecordId {
        val updateScan = scan as UpdateScan
        return updateScan.getRecordId()
    }

    override fun moveToRecordId(recordId: RecordId) {
        val updateScan = scan as UpdateScan
        updateScan.moveToRecordId(recordId)
    }
}