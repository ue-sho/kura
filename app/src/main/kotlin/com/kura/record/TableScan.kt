package com.kura.record

import java.sql.Types
import com.kura.file.BlockId
import com.kura.transaction.Transaction

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 */
class TableScan(
    private val transaction: Transaction,
    tableName: String,
    private val layout: Layout
) {

    private lateinit var recordPage: RecordPage
    private var currentSlot: Int = -1
    private val filename: String = tableName + ".tbl"

    init {
        if (transaction.size(filename) == 0) {
            moveToNewBlock()
        } else {
            moveToBlock(0)
        }
    }

    // Methods that implement Scan

    fun beforeFirst() {
        moveToBlock(0)
    }

    fun next(): Boolean {
        currentSlot = recordPage.nextAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false
            }
            moveToBlock(recordPage.block().blockNum + 1)
            currentSlot = recordPage.nextAfter(currentSlot)
        }
        return true
    }

    fun getInt(fieldName: String): Int {
        return recordPage.getInt(currentSlot, fieldName)
    }

    fun getString(fieldName: String): String {
        return recordPage.getString(currentSlot, fieldName)
    }

    fun hasField(fieldName: String): Boolean {
        return layout.schema().hasField(fieldName)
    }

    fun close() {
        if (::recordPage.isInitialized) {
            transaction.unpin(recordPage.block())
        }
    }

    // Methods that implement UpdateScan

    fun setInt(fieldName: String, value: Int) {
        recordPage.setInt(currentSlot, fieldName, value)
    }

    fun setString(fieldName: String, value: String) {
        recordPage.setString(currentSlot, fieldName, value)
    }

    fun insert() {
        currentSlot = recordPage.insertAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock()
            } else {
                moveToBlock(recordPage.block().blockNum + 1)
            }
            currentSlot = recordPage.insertAfter(currentSlot)
        }
    }

    fun delete() {
        recordPage.delete(currentSlot)
    }

    fun moveToRecordId(recordId: RecordId) {
        close()
        val blk = BlockId(filename, recordId.blockNumber())
        recordPage = RecordPage(transaction, blk, layout)
        currentSlot = recordId.slot()
    }

    fun getRecordId(): RecordId {
        return RecordId(recordPage.block().blockNum, currentSlot)
    }

    // Private auxiliary methods

    private fun moveToBlock(blockNum: Int) {
        close()
        val blk = BlockId(filename, blockNum)
        recordPage = RecordPage(transaction, blk, layout)
        currentSlot = -1
    }

    private fun moveToNewBlock() {
        close()
        val blk = transaction.append(filename)
        recordPage = RecordPage(transaction, blk, layout)
        recordPage.format()
        currentSlot = -1
    }

    private fun atLastBlock(): Boolean {
        return recordPage.block().blockNum == transaction.size(filename) - 1
    }
}