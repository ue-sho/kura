package com.kura.index.btree

import com.kura.file.BlockId
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.transaction.Transaction

/**
 * A B-tree directory page.
 * Each entry in the directory consists of a dataval and a child block number.
 */
class BTreeDir(
    private val tx: Transaction,
    private val layout: Layout,
    block: BlockId
) {
    private var contents: BTPage = BTPage(tx, block, layout)
    private val filename: String = block.fileName

    /**
     * Closes the directory page.
     */
    fun close() {
        contents.close()
    }

    /**
     * Returns the block number of the B-tree leaf block
     * that would contain the specified search key.
     */
    fun search(searchKey: Constant): Int {
        var childBlock = findChildBlock(searchKey)
        while (contents.getFlag() > 0) {
            contents.close()
            contents = BTPage(tx, BlockId(filename, childBlock), layout)
            childBlock = findChildBlock(searchKey)
        }
        return childBlock
    }

    /**
     * Creates a new root page by creating a new B-tree page,
     * transferring the contents of the current root to that page,
     * and then making the root page contain only one entry
     * pointing to that new page.
     */
    fun makeNewRoot(entry: DirEntry) {
        val firstVal = contents.getDataVal(0)
        val level = contents.getFlag()
        val newBlock = contents.split(0, level) // transfer all records
        val oldEntry = DirEntry(firstVal, newBlock.blockNum)
        insertEntry(oldEntry)
        insertEntry(entry)
        contents.setFlag(level + 1)
    }

    /**
     * Inserts a new directory entry into the B-tree directory.
     * If the page becomes full, the page is split and the method
     * returns the entry for the new page; otherwise null is returned.
     */
    fun insert(entry: DirEntry): DirEntry? {
        if (contents.getFlag() > 0) {
            val childBlock = findChildBlock(entry.dataVal)
            val child = BTreeDir(tx, layout, BlockId(filename, childBlock))
            val myEntry = child.insert(entry)
            child.close()
            if (myEntry == null) {
                return null
            }
            return insertEntry(myEntry)
        } else {
            return insertEntry(entry)
        }
    }

    private fun insertEntry(entry: DirEntry): DirEntry? {
        val newSlot = 1 + contents.findSlotBefore(entry.dataVal)
        contents.insertDir(newSlot, entry.dataVal, entry.blockNumber)
        if (!contents.isFull()) {
            return null
        }
        // The page is full, so split it.
        val level = contents.getFlag()
        val splitPos = contents.getNumRecords() / 2
        val splitVal = contents.getDataVal(splitPos)
        val newBlock = contents.split(splitPos, level)
        return DirEntry(splitVal, newBlock.blockNum)
    }

    private fun findChildBlock(searchKey: Constant): Int {
        var slot = contents.findSlotBefore(searchKey)
        if (contents.getDataVal(slot + 1) == searchKey) {
            slot++
        }
        return contents.getChildNum(slot)
    }
}
