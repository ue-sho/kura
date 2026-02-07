package com.kura.index.query

import com.kura.index.Index
import com.kura.query.Constant
import com.kura.query.Scan
import com.kura.record.TableScan

/**
 * The scan class corresponding to the index join relational
 * algebra operator.
 * The code is very similar to that of ProductScan,
 * which makes sense because an index join is essentially
 * the product of each LHS record with the matching RHS index records.
 */
class IndexJoinScan(
    private val lhs: Scan,
    private val idx: Index,
    private val joinField: String,
    private val rhs: TableScan
) : Scan {

    init {
        beforeFirst()
    }

    /**
     * Positions the scan before the first record.
     * That is, the LHS scan will be positioned at its
     * first record, and the index will be positioned
     * before the first record for the join value.
     */
    override fun beforeFirst() {
        lhs.beforeFirst()
        lhs.next()
        resetIndex()
    }

    /**
     * Moves the scan to the next record.
     * The method moves to the next index record, if possible.
     * Otherwise, it moves to the next LHS record and the
     * first index record.
     * If there are no more LHS records, the method returns false.
     */
    override fun next(): Boolean {
        while (true) {
            if (idx.next()) {
                rhs.moveToRecordId(idx.getDataRecordId())
                return true
            }
            if (!lhs.next()) {
                return false
            }
            resetIndex()
        }
    }

    override fun getInt(fieldName: String): Int {
        return if (rhs.hasField(fieldName)) rhs.getInt(fieldName) else lhs.getInt(fieldName)
    }

    override fun getString(fieldName: String): String {
        return if (rhs.hasField(fieldName)) rhs.getString(fieldName) else lhs.getString(fieldName)
    }

    override fun getVal(fieldName: String): Constant {
        return if (rhs.hasField(fieldName)) rhs.getVal(fieldName) else lhs.getVal(fieldName)
    }

    override fun hasField(fieldName: String): Boolean {
        return rhs.hasField(fieldName) || lhs.hasField(fieldName)
    }

    /**
     * Closes the scan by closing its LHS scan, RHS index, and RHS table scan.
     */
    override fun close() {
        lhs.close()
        idx.close()
        rhs.close()
    }

    private fun resetIndex() {
        val searchKey = lhs.getVal(joinField)
        idx.beforeFirst(searchKey)
    }
}
