package com.kura.record

import java.sql.Types
import com.kura.file.BlockId
import com.kura.query.Constant
import com.kura.query.UpdateScan
import com.kura.transaction.Transaction

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 */
class TableScan(
    private val transaction: Transaction,
    tableName: String,
    private val layout: Layout
) : UpdateScan {

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

    override fun beforeFirst() {
        moveToBlock(0)
    }

    override fun next(): Boolean {
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

    override fun close() {
        if (::recordPage.isInitialized) {
            transaction.unpin(recordPage.block())
        }
    }

    // Methods that implement UpdateScan

    override fun setInt(fieldName: String, value: Int) {
        recordPage.setInt(currentSlot, fieldName, value)
    }

    override fun setString(fieldName: String, value: String) {
        recordPage.setString(currentSlot, fieldName, value)
    }

    override fun setVal(fieldName: String, value: Constant) {
        if (layout.schema().type(fieldName) == Types.INTEGER) {
            setInt(fieldName, value.asInt())
        } else {
            setString(fieldName, value.asString())
        }
    }

    override fun insert() {
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

    override fun delete() {
        recordPage.delete(currentSlot)
    }

    override fun moveToRecordId(recordId: RecordId) {
        close()
        val blk = BlockId(filename, recordId.blockNumber())
        recordPage = RecordPage(transaction, blk, layout)
        currentSlot = recordId.slot()
    }

    override fun getRecordId(): RecordId {
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