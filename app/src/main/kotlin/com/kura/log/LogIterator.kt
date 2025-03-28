package com.kura.log

import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.file.Page

/**
 * A class that provides the ability to move through the records of the log file in reverse order.
 */
internal class LogIterator(
    private val fileManager: FileManager,
    private var block: BlockId
) : Iterator<ByteArray> {
    private val page: Page
    private var currentPos: Int = 0
    private var boundary: Int = 0

    init {
        val bytes = ByteArray(fileManager.blockSize())
        page = Page(bytes)
        moveToBlock(block)
    }

    /**
     * Determines if the current log record is the earliest record in the log file.
     *
     * @return true if there is an earlier record
     */
    override fun hasNext(): Boolean {
        return currentPos < fileManager.blockSize() || block.blockNum > 0
    }

    /**
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block and return the log record from there.
     *
     * @return the next earliest log record
     */
    override fun next(): ByteArray {
        if (currentPos == fileManager.blockSize()) {
            block = BlockId(block.fileName, block.blockNum - 1)
            moveToBlock(block)
        }
        return page.getBytes(currentPos).also {
            currentPos += Int.SIZE_BYTES + it.size
        }
    }

    /**
     * Moves to the specified log block and positions it at the first record
     * in that block (i.e., the most recent one).
     */
    private fun moveToBlock(block: BlockId) {
        fileManager.read(block, page)
        boundary = page.getInt(0)
        currentPos = boundary
    }
}