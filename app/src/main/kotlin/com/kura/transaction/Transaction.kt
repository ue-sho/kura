package com.kura.transaction

import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.buffer.Buffer
import com.kura.buffer.BufferManager
import com.kura.transaction.recovery.RecoveryManager
import com.kura.transaction.concurrency.ConcurrencyManager

/**
 * Provide transaction management for clients,
 * ensuring that all transactions are serializable, recoverable,
 * and in general satisfy the ACID properties.
 */
class Transaction(
    private val fileManager: FileManager,
    logManager: LogManager,
    private val bufferManager: BufferManager
) {
    private val transactionNumber: Int = nextTransactionNumber()
    private val recoveryManager: RecoveryManager
    private val concurrencyManager: ConcurrencyManager
    private val bufferList: BufferList

    companion object {
        private const val END_OF_FILE = -1
        private var nextTransactionNum = 0

        @Synchronized
        private fun nextTransactionNumber(): Int {
            nextTransactionNum++
            return nextTransactionNum
        }
    }

    init {
        recoveryManager = RecoveryManager(this, transactionNumber, logManager, bufferManager)
        concurrencyManager = ConcurrencyManager()
        bufferList = BufferList(bufferManager)
    }

    /**
     * Commit the current transaction.
     * Flush all modified buffers (and their log records),
     * write and flush a commit record to the log,
     * release all locks, and unpin any pinned buffers.
     */
    fun commit() {
        recoveryManager.commit()
        println("transaction $transactionNumber committed")
        concurrencyManager.release()
        bufferList.unpinAll()
    }

    /**
     * Rollback the current transaction.
     * Undo any modified values,
     * flush those buffers,
     * write and flush a rollback record to the log,
     * release all locks, and unpin any pinned buffers.
     */
    fun rollback() {
        recoveryManager.rollback()
        println("transaction $transactionNumber rolled back")
        concurrencyManager.release()
        bufferList.unpinAll()
    }

    /**
     * Flush all modified buffers.
     * Then go through the log, rolling back all
     * uncommitted transactions. Finally,
     * write a quiescent checkpoint record to the log.
     * This method is called during system startup,
     * before user transactions begin.
     */
    fun recover() {
        bufferManager.flushAll(transactionNumber)
        recoveryManager.recover()
    }

    /**
     * Pin the specified block.
     * The transaction manages the buffer for the client.
     * @param block a reference to the disk block
     */
    fun pin(block: BlockId) {
        bufferList.pin(block)
    }

    /**
     * Unpin the specified block.
     * The transaction looks up the buffer pinned to this block,
     * and unpins it.
     * @param block a reference to the disk block
     */
    fun unpin(block: BlockId) {
        bufferList.unpin(block)
    }

    /**
     * Return the integer value stored at the
     * specified offset of the specified block.
     * The method first obtains an SLock on the block,
     * then it calls the buffer to retrieve the value.
     * @param block a reference to a disk block
     * @param offset the byte offset within the block
     * @return the integer stored at that offset
     */
    fun getInt(block: BlockId, offset: Int): Int {
        concurrencyManager.sLock(block)
        val buffer = bufferList.getBuffer(block)
        return buffer?.contents()?.getInt(offset) ?: 0
    }

    /**
     * Return the string value stored at the
     * specified offset of the specified block.
     * The method first obtains an SLock on the block,
     * then it calls the buffer to retrieve the value.
     * @param block a reference to a disk block
     * @param offset the byte offset within the block
     * @return the string stored at that offset
     */
    fun getString(block: BlockId, offset: Int): String {
        concurrencyManager.sLock(block)
        val buffer = bufferList.getBuffer(block)
        return buffer?.contents()?.getString(offset) ?: ""
    }

    /**
     * Store an integer at the specified offset
     * of the specified block.
     * The method first obtains an XLock on the block.
     * It then reads the current value at that offset,
     * puts it into an update log record, and
     * writes that record to the log.
     * Finally, it calls the buffer to store the value,
     * passing in the LSN of the log record and the transaction's id.
     * @param block a reference to the disk block
     * @param offset a byte offset within that block
     * @param value the value to be stored
     * @param okToLog whether logging is allowed
     */
    fun setInt(block: BlockId, offset: Int, value: Int, okToLog: Boolean) {
        concurrencyManager.xLock(block)
        val buffer = bufferList.getBuffer(block) ?: return
        var lsn = -1
        if (okToLog) {
            lsn = recoveryManager.setInt(buffer, offset, value)
        }
        val page = buffer.contents()
        page.setInt(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    /**
     * Store a string at the specified offset
     * of the specified block.
     * The method first obtains an XLock on the block.
     * It then reads the current value at that offset,
     * puts it into an update log record, and
     * writes that record to the log.
     * Finally, it calls the buffer to store the value,
     * passing in the LSN of the log record and the transaction's id.
     * @param block a reference to the disk block
     * @param offset a byte offset within that block
     * @param value the value to be stored
     * @param okToLog whether logging is allowed
     */
    fun setString(block: BlockId, offset: Int, value: String, okToLog: Boolean) {
        concurrencyManager.xLock(block)
        val buffer = bufferList.getBuffer(block) ?: return
        var lsn = -1
        if (okToLog) {
            lsn = recoveryManager.setString(buffer, offset, value)
        }
        val page = buffer.contents()
        page.setString(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    /**
     * Return the number of blocks in the specified file.
     * This method first obtains an SLock on the
     * "end of the file", before asking the file manager
     * to return the file size.
     * @param filename the name of the file
     * @return the number of blocks in the file
     */
    fun size(filename: String): Int {
        val dummyBlock = BlockId(filename, END_OF_FILE)
        concurrencyManager.sLock(dummyBlock)
        return fileManager.length(filename)
    }

    /**
     * Append a new block to the end of the specified file
     * and returns a reference to it.
     * This method first obtains an XLock on the
     * "end of the file", before performing the append.
     * @param filename the name of the file
     * @return a reference to the newly-created disk block
     */
    fun append(filename: String): BlockId {
        val dummyBlock = BlockId(filename, END_OF_FILE)
        concurrencyManager.xLock(dummyBlock)
        return fileManager.append(filename)
    }

    /**
     * Return the block size of the file manager.
     * @return the block size
     */
    fun blockSize(): Int {
        return fileManager.blockSize()
    }

    /**
     * Return the number of available buffers.
     * @return the number of available buffers
     */
    fun availableBuffers(): Int {
        return bufferManager.available()
    }
}