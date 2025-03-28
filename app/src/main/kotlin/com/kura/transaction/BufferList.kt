package com.kura.transaction

import com.kura.file.BlockId
import com.kura.buffer.Buffer
import com.kura.buffer.BufferManager

/**
 * Manage the transaction's currently-pinned buffers.
 */
class BufferList(private val bufferManager: BufferManager) {
    private val buffers: MutableMap<BlockId, Buffer> = HashMap()
    private val pins: MutableList<BlockId> = ArrayList()

    /**
     * Return the buffer pinned to the specified block.
     * The method returns null if the transaction has not
     * pinned the block.
     * @param block a reference to the disk block
     * @return the buffer pinned to that block
     */
    fun getBuffer(block: BlockId): Buffer? {
        return buffers[block]
    }

    /**
     * Pin the block and keep track of the buffer internally.
     * @param block a reference to the disk block
     */
    fun pin(block: BlockId) {
        val buffer = bufferManager.pin(block)
        buffers[block] = buffer
        pins.add(block)
    }

    /**
     * Unpin the specified block.
     * @param block a reference to the disk block
     */
    fun unpin(block: BlockId) {
        val buffer = buffers[block] ?: return
        bufferManager.unpin(buffer)
        pins.remove(block)
        if (!pins.contains(block)) {
            buffers.remove(block)
        }
    }

    /**
     * Unpin any buffers still pinned by this transaction.
     */
    fun unpinAll() {
        for (block in pins) {
            val buffer = buffers[block] ?: continue
            bufferManager.unpin(buffer)
        }
        buffers.clear()
        pins.clear()
    }
}