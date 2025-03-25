package com.uesho.kura.buffer

import com.uesho.kura.file.BlockId
import com.uesho.kura.file.FileManager
import com.uesho.kura.log.LogManager

/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BufferManager(
    fileManager: FileManager,
    logManager: LogManager,
    numberOfBuffers: Int
) {
    private val bufferPool: Array<Buffer> = Array(numberOfBuffers) { Buffer(fileManager, logManager) }
    private var numAvailable: Int = numberOfBuffers
    // Using an object for synchronization
    private val lock = Object()

    companion object {
        private const val MAX_TIME: Long = 10000 // 10 seconds
    }

    /**
     * Returns the number of available (i.e. unpinned) buffers.
     * @return the number of available buffers
     */
    fun available(): Int {
        synchronized(lock) {
            return numAvailable
        }
    }

    /**
     * Flushes the dirty buffers modified by the specified transaction.
     * @param transactionNumber the transaction's id number
     */
    fun flushAll(transactionNumber: Int) {
        synchronized(lock) {
            for (buffer in bufferPool) {
                if (buffer.getModifyingTransaction() == transactionNumber) {
                    buffer.flush()
                }
            }
        }
    }

    /**
     * Unpins the specified data buffer. If its pin count
     * goes to zero, then notify any waiting threads.
     * @param buffer the buffer to be unpinned
     */
    fun unpin(buffer: Buffer) {
        synchronized(lock) {
            buffer.unpin()
            if (!buffer.isPinned()) {
                numAvailable++
                lock.notifyAll()
            }
        }
    }

    /**
     * Pins a buffer to the specified block, potentially
     * waiting until a buffer becomes available.
     * If no buffer becomes available within a fixed
     * time period, then a [BufferAbortException] is thrown.
     * @param block a reference to a disk block
     * @return the buffer pinned to that block
     */
    fun pin(block: BlockId): Buffer {
        synchronized(lock) {
            try {
                val timestamp = System.currentTimeMillis()
                var buffer = tryToPin(block)
                while (buffer == null && !waitingTooLong(timestamp)) {
                    lock.wait(MAX_TIME)
                    buffer = tryToPin(block)
                }
                return buffer ?: throw BufferAbortException()
            } catch (e: InterruptedException) {
                throw BufferAbortException()
            }
        }
    }

    /**
     * Determines if the wait time has exceeded the maximum allowed time.
     * @param startTime the starting timestamp
     * @return true if waiting too long, false otherwise
     */
    private fun waitingTooLong(startTime: Long): Boolean {
        return System.currentTimeMillis() - startTime > MAX_TIME
    }

    /**
     * Tries to pin a buffer to the specified block.
     * If there is already a buffer assigned to that block
     * then that buffer is used;
     * otherwise, an unpinned buffer from the pool is chosen.
     * Returns a null value if there are no available buffers.
     * @param block a reference to a disk block
     * @return the pinned buffer or null if none is available
     */
    private fun tryToPin(block: BlockId): Buffer? {
        var buffer = findExistingBuffer(block)
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer()
            if (buffer == null) {
                return null
            }
            buffer.assignToBlock(block)
        }
        if (!buffer.isPinned()) {
            numAvailable--
        }
        buffer.pin()
        return buffer
    }

    /**
     * Finds the buffer that is currently associated with the specified block.
     * @param block a reference to a disk block
     * @return the associated buffer or null if not found
     */
    private fun findExistingBuffer(block: BlockId): Buffer? {
        for (buffer in bufferPool) {
            val currentBlock = buffer.getBlock()
            if (currentBlock != null && currentBlock == block) {
                return buffer
            }
        }
        return null
    }

    /**
     * Chooses an unpinned buffer from the buffer pool.
     * @return an unpinned buffer or null if none exists
     */
    private fun chooseUnpinnedBuffer(): Buffer? {
        for (buffer in bufferPool) {
            if (!buffer.isPinned()) {
                return buffer
            }
        }
        return null
    }
}