package com.kura.transaction.recovery

import com.kura.file.BlockId
import com.kura.log.LogManager
import com.kura.buffer.Buffer
import com.kura.buffer.BufferManager
import com.kura.transaction.Transaction
import com.kura.transaction.recovery.LogRecord.Companion.CHECKPOINT
import com.kura.transaction.recovery.LogRecord.Companion.COMMIT
import com.kura.transaction.recovery.LogRecord.Companion.ROLLBACK
import com.kura.transaction.recovery.LogRecord.Companion.START

/**
 * The recovery manager.
 * Each transaction has its own recovery manager.
 */
class RecoveryManager(
    private val transaction: Transaction,
    private val transactionNumber: Int,
    private val logManager: LogManager,
    private val bufferManager: BufferManager
) {
    /**
     * Create a recovery manager for the specified transaction and log manager.
     * Write a START record to the log, and return its LSN.
     * @return the LSN of the START record
     */
    private val startLSN: Int = StartRecord.writeToLog(logManager, transactionNumber)

    /**
     * Write a COMMIT record to the log and flushes it to disk.
     */
    fun commit() {
        bufferManager.flushAll(transactionNumber)
        val lsn = CommitRecord.writeToLog(logManager, transactionNumber)
        logManager.flush(lsn)
    }

    /**
     * Write a ROLLBACK record to the log and flushes it to disk.
     */
    fun rollback() {
        doRollback()
        bufferManager.flushAll(transactionNumber)
        val lsn = RollbackRecord.writeToLog(logManager, transactionNumber)
        logManager.flush(lsn)
    }

    /**
     * Recover uncompleted transactions from the log
     * and then write a CHECKPOINT record to the log and flush it.
     */
    fun recover() {
        doRecover()
        bufferManager.flushAll(transactionNumber)
        val lsn = CheckpointRecord.writeToLog(logManager)
        logManager.flush(lsn)
    }

    /**
     * Write a setint record to the log and return its lsn.
     * @param buffer the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newValue the value to be written
     * @return the LSN of the log record
     */
    fun setInt(buffer: Buffer, offset: Int, newValue: Int): Int {
        val oldValue = buffer.contents().getInt(offset)
        val block = buffer.block() ?: throw IllegalStateException("Buffer block is null")
        return SetIntRecord.writeToLog(logManager, transactionNumber, block, offset, oldValue)
    }

    /**
     * Write a setstring record to the log and return its lsn.
     * @param buffer the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newValue the value to be written
     * @return the LSN of the log record
     */
    fun setString(buffer: Buffer, offset: Int, newValue: String): Int {
        val oldValue = buffer.contents().getString(offset)
        val block = buffer.block() ?: throw IllegalStateException("Buffer block is null")
        return SetStringRecord.writeToLog(logManager, transactionNumber, block, offset, oldValue)
    }

    /**
     * Rollback the transaction, by iterating
     * through the log records until it finds
     * the transaction's START record,
     * calling undo() for each of the transaction's
     * log records.
     */
    private fun doRollback() {
        val iterator = logManager.iterator()
        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes)
            if (record.transactionNumber() == transactionNumber) {
                if (record.operation() == START) {
                    return
                }
                record.undo(transaction)
            }
        }
    }

    /**
     * Do a complete database recovery.
     * The method iterates through the log records.
     * Whenever it finds a log record for an unfinished
     * transaction, it calls undo() on that record.
     * The method stops when it encounters a CHECKPOINT record
     * or the end of the log.
     */
    private fun doRecover() {
        val finishedTransactions: MutableCollection<Int> = ArrayList()
        val iterator = logManager.iterator()
        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes)
            when (record.operation()) {
                CHECKPOINT -> return
                COMMIT, ROLLBACK -> finishedTransactions.add(record.transactionNumber())
                else -> if (!finishedTransactions.contains(record.transactionNumber())) {
                    record.undo(transaction)
                }
            }
        }
    }
}