package com.kura.transaction.recovery

import com.kura.file.Page
import com.kura.transaction.Transaction

/**
 * The interface implemented by each type of log record.
 */
interface LogRecord {
    /**
     * Returns the log record's type.
     * @return the log record's type
     */
    fun operation(): Int

    /**
     * Returns the transaction id stored with
     * the log record.
     * @return the log record's transaction id
     */
    fun transactionNumber(): Int

    /**
     * Undoes the operation encoded by this log record.
     * The only log record types for which this method
     * does anything interesting are SETINT and SETSTRING.
     * @param transaction the transaction that is performing the undo.
     */
    fun undo(transaction: Transaction)

    companion object {
        const val CHECKPOINT = 0
        const val START = 1
        const val COMMIT = 2
        const val ROLLBACK = 3
        const val SET_INT = 4
        const val SET_STRING = 5

        /**
         * Interpret the bytes returned by the log iterator.
         * @param bytes the bytes from the log
         * @return the log record
         */
        fun createLogRecord(bytes: ByteArray): LogRecord {
            val page = Page(bytes)
            return when (page.getInt(0)) {
                CHECKPOINT -> CheckpointRecord()
                START -> StartRecord(page)
                COMMIT -> CommitRecord(page)
                ROLLBACK -> RollbackRecord(page)
                SET_INT -> SetIntRecord(page)
                SET_STRING -> SetStringRecord(page)
                else -> throw IllegalArgumentException("Unknown log record type")
            }
        }
    }
}