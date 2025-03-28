package com.kura.transaction.recovery

import com.kura.file.Page
import com.kura.log.LogManager
import com.kura.transaction.Transaction

/**
 * A log record for a rollback operation.
 */
class RollbackRecord : LogRecord {
    private val txNum: Int

    constructor(page: Page) {
        txNum = page.getInt(Int.SIZE_BYTES)
    }

    override fun operation(): Int {
        return LogRecord.ROLLBACK
    }

    override fun transactionNumber(): Int {
        return txNum
    }

    /**
     * Does nothing, because a rollback record
     * contains no undo information.
     */
    override fun undo(transaction: Transaction) {
        // Does nothing
    }

    override fun toString(): String {
        return "<ROLLBACK $txNum>"
    }

    companion object {
        /**
         * A static method to write a rollback record to the log.
         * @param logManager the log manager
         * @param txNum the ID of the specified transaction
         * @return the LSN of the last log value
         */
        fun writeToLog(logManager: LogManager, txNum: Int): Int {
            val rec = ByteArray(2 * Int.SIZE_BYTES)
            val page = Page(rec)
            page.setInt(0, LogRecord.ROLLBACK)
            page.setInt(Int.SIZE_BYTES, txNum)
            return logManager.append(rec)
        }
    }
}