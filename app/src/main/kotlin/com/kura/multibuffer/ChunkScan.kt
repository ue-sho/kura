package com.kura.multibuffer

import com.kura.file.BlockId
import com.kura.query.Constant
import com.kura.query.Scan
import com.kura.record.Layout
import com.kura.record.RecordPage
import com.kura.transaction.Transaction
import java.sql.Types

/**
 * The class for the chunk operator.
 * A chunk scan pins a contiguous range of blocks
 * and iterates through their records.
 */
class ChunkScan(
    private val transaction: Transaction,
    private val filename: String,
    private val layout: Layout,
    private val startBlockNum: Int,
    private val endBlockNum: Int
) : Scan {
    private val buffers: MutableList<RecordPage> = mutableListOf()
    private lateinit var recordPage: RecordPage
    private var currentBlockNum = 0
    private var currentSlot = 0

    init {
        for (i in startBlockNum..endBlockNum) {
            val block = BlockId(filename, i)
            buffers.add(RecordPage(transaction, block, layout))
        }
        moveToBlock(startBlockNum)
    }

    override fun close() {
        for (i in buffers.indices) {
            val block = BlockId(filename, startBlockNum + i)
            transaction.unpin(block)
        }
    }

    override fun beforeFirst() {
        moveToBlock(startBlockNum)
    }

    /**
     * Moves to the next record in the current block of the chunk.
     * If there are no more records, then make
     * the next block be current.
     * If there are no more blocks in the chunk, return false.
     */
    override fun next(): Boolean {
        currentSlot = recordPage.nextAfter(currentSlot)
        while (currentSlot < 0) {
            if (currentBlockNum == endBlockNum) {
                return false
            }
            moveToBlock(recordPage.block().blockNum + 1)
            currentSlot = recordPage.nextAfter(currentSlot)
        }
        return true
    }

    override fun getInt(fieldName: String): Int {
        return recordPage.getInt(currentSlot, fieldName)
    }

    override fun getString(fieldName: String): String {
        return recordPage.getString(currentSlot, fieldName)
    }

    override fun getVal(fieldName: String): Constant {
        return if (layout.schema().type(fieldName) == Types.INTEGER) {
            Constant(getInt(fieldName))
        } else {
            Constant(getString(fieldName))
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return layout.schema().hasField(fieldName)
    }

    private fun moveToBlock(blockNum: Int) {
        currentBlockNum = blockNum
        recordPage = buffers[currentBlockNum - startBlockNum]
        currentSlot = -1
    }
}
