package com.kura.buffer

import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.file.Page
import com.kura.log.LogManager

/**
 * An individual buffer. A buffer wraps a page
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 */
class Buffer(
    private val fileManager: FileManager,
    private val logManager: LogManager
) {
    private val contents: Page = Page(fileManager.blockSize())
    private var blockId: BlockId? = null
    private var pins: Int = 0
    private var transactionNumber: Int = -1
    private var logSequenceNumber: Int = -1

    /**
     * Returns the page stored in this buffer.
     * @return the page
     */
    fun contents(): Page {
        return contents
    }

    /**
     * Returns the page stored in this buffer.
     * @return the page
     */
    fun getContents(): Page {
        return contents
    }

    /**
     * Returns a reference to the disk block
     * allocated to the buffer.
     * @return a reference to a disk block
     */
    fun block(): BlockId? {
        return blockId
    }

    /**
     * Returns a reference to the disk block
     * allocated to the buffer.
     * @return a reference to a disk block
     */
    fun getBlock(): BlockId? {
        return blockId
    }

    /**
     * Set this buffer as modified by the specified transaction and log sequence number.
     * @param transactionNumber the transaction ID
     * @param logSequenceNumber the log sequence number
     */
    fun setModified(transactionNumber: Int, logSequenceNumber: Int) {
        this.transactionNumber = transactionNumber
        if (logSequenceNumber >= 0) {
            this.logSequenceNumber = logSequenceNumber
        }
    }

    /**
     * Return true if the buffer is currently pinned
     * (that is, if it has a nonzero pin count).
     * @return true if the buffer is pinned
     */
    fun isPinned(): Boolean {
        return pins > 0
    }

    /**
     * Returns the ID of the transaction that modified this buffer.
     * @return the ID of the modifying transaction
     */
    fun getModifyingTransaction(): Int {
        return transactionNumber
    }

    /**
     * Reads the contents of the specified block into
     * the contents of the buffer.
     * If the buffer was dirty, then its previous contents
     * are first written to disk.
     * @param block a reference to the data block
     */
    fun assignToBlock(block: BlockId) {
        flush()
        blockId = block
        fileManager.read(block, contents)
        pins = 0
    }

    /**
     * Write the buffer to its disk block if it is dirty.
     */
    fun flush() {
        if (transactionNumber >= 0) {
            logManager.flush(logSequenceNumber)
            blockId?.let {
                fileManager.write(it, contents)
            }
            transactionNumber = -1
        }
    }

    /**
     * Increase the buffer's pin count.
     */
    fun pin() {
        pins++
    }

    /**
     * Decrease the buffer's pin count.
     */
    fun unpin() {
        pins--
    }
}