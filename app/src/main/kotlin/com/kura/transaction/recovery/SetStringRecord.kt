package com.kura.transaction.recovery

import com.kura.file.BlockId
import com.kura.file.Page
import com.kura.log.LogManager
import com.kura.transaction.Transaction

/**
 * A log record for a modification to a string value in a page.
 */
class SetStringRecord(page: Page) : LogRecord {
    private val txNum: Int
    private val offset: Int
    private val value: String
    private val block: BlockId

    init {
        val tPos = Int.SIZE_BYTES
        txNum = page.getInt(tPos)
        val fPos = tPos + Int.SIZE_BYTES
        val filename = page.getString(fPos)
        val bPos = fPos + Page.maxLength(filename.length)
        val blockNum = page.getInt(bPos)
        block = BlockId(filename, blockNum)
        val oPos = bPos + Int.SIZE_BYTES
        offset = page.getInt(oPos)
        val vPos = oPos + Int.SIZE_BYTES
        value = page.getString(vPos)
    }

    override fun operation(): Int {
        return LogRecord.SET_STRING
    }

    override fun transactionNumber(): Int {
        return txNum
    }

    override fun toString(): String {
        return "<SET_STRING $txNum $block $offset $value>"
    }

    /**
     * Replace the specified data value with the value saved in the log record.
     * The method pins a buffer to the specified block,
     * calls setString to restore the saved value,
     * and unpins the buffer.
     */
    override fun undo(transaction: Transaction) {
        transaction.pin(block)
        transaction.setString(block, offset, value, false) // don't log the undo!
        transaction.unpin(block)
    }

    companion object {
        /**
         * A static method to write a setString record to the log.
         * This log record contains the SET_STRING operator,
         * followed by the transaction id, the filename, number,
         * and offset of the modified block, and the previous
         * string value at that offset.
         * @return the LSN of the last log value
         */
        fun writeToLog(logManager: LogManager, txNum: Int, block: BlockId, offset: Int, value: String): Int {
            val tPos = Int.SIZE_BYTES
            val fPos = tPos + Int.SIZE_BYTES
            val bPos = fPos + Page.maxLength(block.fileName.length)
            val oPos = bPos + Int.SIZE_BYTES
            val vPos = oPos + Int.SIZE_BYTES
            val valueSize = Page.maxLength(value.length)
            val rec = ByteArray(vPos + valueSize)
            val page = Page(rec)
            page.setInt(0, LogRecord.SET_STRING)
            page.setInt(tPos, txNum)
            page.setString(fPos, block.fileName)
            page.setInt(bPos, block.blockNum)
            page.setInt(oPos, offset)
            page.setString(vPos, value)
            return logManager.append(rec)
        }
    }
}