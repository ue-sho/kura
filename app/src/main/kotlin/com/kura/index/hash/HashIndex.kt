package com.kura.index.hash

import com.kura.index.Index
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordId
import com.kura.record.TableScan
import com.kura.transaction.Transaction

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 */
class HashIndex(
    private val tx: Transaction,
    private val indexName: String,
    private val layout: Layout
) : Index {
    companion object {
        const val NUM_BUCKETS = 100

        /**
         * Returns the cost of searching an index file having the
         * specified number of blocks.
         * The method assumes that all buckets are about the
         * same size, and so the cost is simply the size of
         * the bucket.
         * @param numBlocks the number of blocks of index records
         * @param recordsPerBlock the number of records per block (not used here)
         * @return the cost of traversing the index
         */
        @JvmStatic
        fun searchCost(numBlocks: Int, recordsPerBlock: Int): Int {
            return numBlocks / NUM_BUCKETS
        }
    }

    private var searchKey: Constant? = null
    private var ts: TableScan? = null

    /**
     * Positions the index before the first index record
     * having the specified search key.
     * The method hashes the search key to determine the bucket,
     * and then opens a table scan on the file
     * corresponding to the bucket.
     * The table scan for the previous bucket (if any) is closed.
     * @param searchKey the search key value
     */
    override fun beforeFirst(searchKey: Any) {
        close()
        this.searchKey = searchKey as Constant
        val bucket = searchKey.hashCode() % NUM_BUCKETS
        val tableName = "$indexName$bucket"
        ts = TableScan(tx, tableName, layout)
    }

    /**
     * Moves to the next record having the search key.
     * The method loops through the table scan for the bucket,
     * looking for a matching record, and returning false
     * if there are no more such records.
     * @return false if there are no more such records
     */
    override fun next(): Boolean {
        while (ts!!.next()) {
            if (ts!!.getVal("dataval").equals(searchKey)) {
                return true
            }
        }
        return false
    }

    /**
     * Retrieves the data record ID from the current record
     * in the table scan for the bucket.
     * @return the data record ID
     */
    override fun getDataRecordId(): RecordId {
        val blockNumber = ts!!.getInt("block")
        val slot = ts!!.getInt("id")
        return RecordId(blockNumber, slot)
    }

    /**
     * Inserts a new record into the table scan for the bucket.
     * @param key the key in the index
     * @param recordId the data record ID
     */
    override fun insert(key: Any, recordId: RecordId) {
        beforeFirst(key)
        ts!!.insert()
        ts!!.setInt("block", recordId.blockNumber())
        ts!!.setInt("id", recordId.slot())
        ts!!.setVal("dataval", key as Constant)
    }

    /**
     * Deletes the specified record from the table scan for
     * the bucket. The method starts at the beginning of the
     * scan, and loops through the records until the
     * specified record is found.
     * @param key the key in the index
     * @param recordId the data record ID
     */
    override fun delete(key: Any, recordId: RecordId) {
        beforeFirst(key)
        while (next()) {
            if (getDataRecordId().equals(recordId)) {
                ts!!.delete()
                return
            }
        }
    }

    /**
     * Closes the index by closing the current table scan.
     */
    override fun close() {
        if (ts != null) {
            ts!!.close()
        }
    }
}