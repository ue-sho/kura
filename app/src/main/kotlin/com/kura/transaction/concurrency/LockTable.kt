package com.kura.transaction.concurrency

import com.kura.file.BlockId
import java.util.*

/**
 * The lock table, which provides methods to lock and unlock blocks.
 * If a transaction requests a lock that causes a conflict with an
 * existing lock, then that transaction is placed on a wait list.
 * There is only one wait list for all blocks.
 * When the last lock on a block is unlocked, then all transactions
 * are removed from the wait list and rescheduled.
 * If one of those transactions discovers that the lock it is waiting for
 * is still locked, it will place itself back on the wait list.
 */
class LockTable {
    companion object {
        private const val MAX_TIME = 10000L // 10 seconds
    }

    private val locks: MutableMap<BlockId, Int> = HashMap()

    /**
     * Grant an SLock on the specified block.
     * If an XLock exists when the method is called,
     * then the calling thread will be placed on a wait list
     * until the lock is released.
     * If the thread remains on the wait list for a certain
     * amount of time (currently 10 seconds),
     * then an exception is thrown.
     * @param block a reference to the disk block
     */
    @Synchronized
    fun sLock(block: BlockId) {
        try {
            val timestamp = System.currentTimeMillis()
            while (hasXLock(block) && !waitingTooLong(timestamp)) {
                (this as Object).wait(MAX_TIME)
            }
            if (hasXLock(block)) {
                throw LockAbortException()
            }
            val lockValue = getLockValue(block) // will not be negative
            locks[block] = lockValue + 1
        } catch (e: InterruptedException) {
            throw LockAbortException()
        }
    }

    /**
     * Grant an XLock on the specified block.
     * If a lock of any type exists when the method is called,
     * then the calling thread will be placed on a wait list
     * until the locks are released.
     * If the thread remains on the wait list for a certain
     * amount of time (currently 10 seconds),
     * then an exception is thrown.
     * @param block a reference to the disk block
     */
    @Synchronized
    fun xLock(block: BlockId) {
        try {
            val timestamp = System.currentTimeMillis()
            while (hasOtherSLocks(block) && !waitingTooLong(timestamp)) {
                (this as Object).wait(MAX_TIME)
            }
            if (hasOtherSLocks(block)) {
                throw LockAbortException()
            }
            locks[block] = -1
        } catch (e: InterruptedException) {
            throw LockAbortException()
        }
    }

    /**
     * Release a lock on the specified block.
     * If this lock is the last lock on that block,
     * then the waiting transactions are notified.
     * @param block a reference to the disk block
     */
    @Synchronized
    fun unlock(block: BlockId) {
        val lockValue = getLockValue(block)
        if (lockValue > 1) {
            locks[block] = lockValue - 1
        } else {
            locks.remove(block)
            (this as Object).notifyAll()
        }
    }

    private fun hasXLock(block: BlockId): Boolean {
        return getLockValue(block) < 0
    }

    private fun hasOtherSLocks(block: BlockId): Boolean {
        return getLockValue(block) > 1
    }

    private fun waitingTooLong(startTime: Long): Boolean {
        return System.currentTimeMillis() - startTime > MAX_TIME
    }

    private fun getLockValue(block: BlockId): Int {
        return locks[block] ?: 0
    }
}