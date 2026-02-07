package com.kura.materialize

import com.kura.query.Constant
import com.kura.query.Scan
import com.kura.query.UpdateScan
import com.kura.record.RecordId

/**
 * The Scan class for the sort operator.
 */
class SortScan(
    runs: List<TempTable>,
    private val comp: RecordComparator
) : Scan {
    private val s1: UpdateScan = runs[0].open()
    private var s2: UpdateScan? = null
    private var currentScan: UpdateScan? = null
    private var hasMore1: Boolean = s1.next()
    private var hasMore2: Boolean = false
    private var savedPosition: List<RecordId?> = emptyList()

    init {
        if (runs.size > 1) {
            s2 = runs[1].open()
            hasMore2 = s2!!.next()
        }
    }

    /**
     * Position the scan before the first record in sorted order.
     * Internally, it moves to the first record of each underlying scan.
     * The variable currentScan is set to null, indicating that there is
     * no current scan.
     */
    override fun beforeFirst() {
        currentScan = null
        s1.beforeFirst()
        hasMore1 = s1.next()
        if (s2 != null) {
            s2!!.beforeFirst()
            hasMore2 = s2!!.next()
        }
    }

    /**
     * Move to the next record in sorted order.
     * First, the current scan is moved to the next record.
     * Then the lowest record of the two scans is found, and that
     * scan is chosen to be the new current scan.
     */
    override fun next(): Boolean {
        if (currentScan != null) {
            if (currentScan === s1) {
                hasMore1 = s1.next()
            } else if (currentScan === s2) {
                hasMore2 = s2!!.next()
            }
        }

        if (!hasMore1 && !hasMore2) {
            return false
        } else if (hasMore1 && hasMore2) {
            currentScan = if (comp.compare(s1, s2!!) < 0) s1 else s2
        } else if (hasMore1) {
            currentScan = s1
        } else if (hasMore2) {
            currentScan = s2
        }
        return true
    }

    /**
     * Close the two underlying scans.
     */
    override fun close() {
        s1.close()
        s2?.close()
    }

    /**
     * Get the Constant value of the specified field
     * of the current scan.
     */
    override fun getVal(fieldName: String): Constant {
        return currentScan!!.getVal(fieldName)
    }

    /**
     * Get the integer value of the specified field
     * of the current scan.
     */
    override fun getInt(fieldName: String): Int {
        return currentScan!!.getInt(fieldName)
    }

    /**
     * Get the string value of the specified field
     * of the current scan.
     */
    override fun getString(fieldName: String): String {
        return currentScan!!.getString(fieldName)
    }

    /**
     * Return true if the specified field is in the current scan.
     */
    override fun hasField(fieldName: String): Boolean {
        return currentScan!!.hasField(fieldName)
    }

    /**
     * Save the position of the current record,
     * so that it can be restored at a later time.
     */
    fun savePosition() {
        val rid1 = s1.getRecordId()
        val rid2 = s2?.getRecordId()
        savedPosition = listOf(rid1, rid2)
    }

    /**
     * Move the scan to its previously-saved position.
     */
    fun restorePosition() {
        val rid1 = savedPosition[0]!!
        val rid2 = savedPosition[1]
        s1.moveToRecordId(rid1)
        if (rid2 != null) {
            s2!!.moveToRecordId(rid2)
        }
    }
}
