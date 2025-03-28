package com.kura.transaction.recovery

import com.kura.file.Page
import com.kura.log.LogManager
import com.kura.transaction.Transaction

/**
 * A log record for a checkpoint.
 */
class CheckpointRecord : LogRecord {
    override fun operation(): Int {
        return LogRecord.CHECKPOINT
    }

    override fun transactionNumber(): Int {
        return -1 // Checkpoints are not associated with a transaction
    }

    /**
     * Does nothing, because a checkpoint record
     * contains no undo information.
     */
    override fun undo(transaction: Transaction) {
        // Does nothing
    }

    override fun toString(): String {
        return "<CHECKPOINT>"
    }

    companion object {
        /**
         * A static method to write a checkpoint record to the log.
         * This log record contains just the CHECKPOINT operator.
         * @return the LSN of the last log value
         */
        fun writeToLog(logManager: LogManager): Int {
            val rec = ByteArray(Int.SIZE_BYTES)
            val page = Page(rec)
            page.setInt(0, LogRecord.CHECKPOINT)
            return logManager.append(rec)
        }
    }
}