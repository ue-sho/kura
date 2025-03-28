package com.kura.transaction.concurrency

import com.kura.file.BlockId
import java.util.*

/**
 * The concurrency manager for the transaction.
 * Each transaction has its own concurrency manager.
 * The concurrency manager keeps track of which locks the
 * transaction currently has, and interacts with the
 * global lock table as needed.
 */
class ConcurrencyManager {
    companion object {
        /**
         * The global lock table. This variable is static because
         * all transactions share the same table.
         */
        private val lockTable = LockTable()
    }

    private val locks: MutableMap<BlockId, String> = HashMap()

    /**
     * Obtain an SLock on the block, if necessary.
     * The method will ask the lock table for an SLock
     * if the transaction currently has no locks on that block.
     * @param block a reference to the disk block
     */
    fun sLock(block: BlockId) {
        if (locks[block] == null) {
            lockTable.sLock(block)
            locks[block] = "S"
        }
    }

    /**
     * Obtain an XLock on the block, if necessary.
     * If the transaction does not have an XLock on that block,
     * then the method first gets an SLock on that block
     * (if necessary), and then upgrades it to an XLock.
     * @param block a reference to the disk block
     */
    fun xLock(block: BlockId) {
        if (!hasXLock(block)) {
            sLock(block)
            lockTable.xLock(block)
            locks[block] = "X"
        }
    }

    /**
     * Release all locks by asking the lock table to
     * unlock each one.
     */
    fun release() {
        for (block in locks.keys) {
            lockTable.unlock(block)
        }
        locks.clear()
    }

    private fun hasXLock(block: BlockId): Boolean {
        val lockType = locks[block]
        return lockType != null && lockType == "X"
    }
}