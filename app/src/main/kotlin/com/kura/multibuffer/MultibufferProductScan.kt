package com.kura.multibuffer

import com.kura.query.Constant
import com.kura.query.ProductScan
import com.kura.query.Scan
import com.kura.record.Layout
import com.kura.transaction.Transaction

/**
 * The Scan class for the multi-buffer version of the
 * product operator.
 */
class MultibufferProductScan(
    private val transaction: Transaction,
    private val lhsScan: Scan,
    tableName: String,
    private val layout: Layout
) : Scan {
    private val filename: String = "$tableName.tbl"
    private val fileSize: Int = transaction.size(filename)
    private val chunkSize: Int = BufferNeeds.bestFactor(transaction.availableBuffers(), fileSize)
    private var nextBlockNum = 0
    private var rhsScan: Scan? = null
    private lateinit var productScan: Scan

    init {
        beforeFirst()
    }

    /**
     * Positions the scan before the first record.
     * That is, the LHS scan is positioned at its first record,
     * and the RHS scan is positioned before the first record of the first chunk.
     */
    override fun beforeFirst() {
        nextBlockNum = 0
        useNextChunk()
    }

    /**
     * Moves to the next record in the current scan.
     * If there are no more records in the current chunk,
     * then move to the next LHS record and the beginning of that chunk.
     * If there are no more LHS records, then move to the next chunk
     * and begin again.
     */
    override fun next(): Boolean {
        while (!productScan.next()) {
            if (!useNextChunk()) {
                return false
            }
        }
        return true
    }

    override fun close() {
        productScan.close()
    }

    override fun getVal(fieldName: String): Constant {
        return productScan.getVal(fieldName)
    }

    override fun getInt(fieldName: String): Int {
        return productScan.getInt(fieldName)
    }

    override fun getString(fieldName: String): String {
        return productScan.getString(fieldName)
    }

    override fun hasField(fieldName: String): Boolean {
        return productScan.hasField(fieldName)
    }

    private fun useNextChunk(): Boolean {
        if (nextBlockNum >= fileSize) {
            return false
        }
        rhsScan?.close()
        var end = nextBlockNum + chunkSize - 1
        if (end >= fileSize) {
            end = fileSize - 1
        }
        val chunkScan = ChunkScan(transaction, filename, layout, nextBlockNum, end)
        rhsScan = chunkScan
        lhsScan.beforeFirst()
        productScan = ProductScan(lhsScan, chunkScan)
        nextBlockNum = end + 1
        return true
    }
}
