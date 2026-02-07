package com.kura.index.query

import com.kura.index.Index
import com.kura.query.Constant
import com.kura.query.Scan
import com.kura.record.TableScan

/**
 * The scan class corresponding to the select relational
 * algebra operator, using an index.
 */
class IndexSelectScan(
    private val ts: TableScan,
    private val idx: Index,
    private val searchKey: Constant
) : Scan {

    init {
        beforeFirst()
    }

    /**
     * Positions the scan before the first record,
     * which in this case means positioning the index
     * before the first instance of the selection constant.
     */
    override fun beforeFirst() {
        idx.beforeFirst(searchKey)
    }

    /**
     * Moves to the next record, which in this case means
     * moving the index to the next record satisfying the
     * selection constant, and returning false if there are
     * no more such index records.
     * If there is a next record, the method moves the
     * table scan to the corresponding data record.
     */
    override fun next(): Boolean {
        val ok = idx.next()
        if (ok) {
            val rid = idx.getDataRecordId()
            ts.moveToRecordId(rid)
        }
        return ok
    }

    override fun getInt(fieldName: String): Int {
        return ts.getInt(fieldName)
    }

    override fun getString(fieldName: String): String {
        return ts.getString(fieldName)
    }

    override fun getVal(fieldName: String): Constant {
        return ts.getVal(fieldName)
    }

    override fun hasField(fieldName: String): Boolean {
        return ts.hasField(fieldName)
    }

    /**
     * Closes the scan by closing the index and the table scan.
     */
    override fun close() {
        idx.close()
        ts.close()
    }
}
