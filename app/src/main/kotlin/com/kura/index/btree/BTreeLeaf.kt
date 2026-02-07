package com.kura.index.btree

import com.kura.file.BlockId
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordId
import com.kura.transaction.Transaction

/**
 * An object that holds the contents of a B-tree leaf block.
 */
class BTreeLeaf(
    private val tx: Transaction,
    private val layout: Layout,
    private val searchKey: Constant,
    block: BlockId
) {
    private var contents: BTPage = BTPage(tx, block, layout)
    private var currentBlock: BlockId = block
    private var currentSlot: Int = contents.findSlotBefore(searchKey)

    /**
     * Closes the leaf page.
     */
    fun close() {
        contents.close()
    }

    /**
     * Moves to the next leaf record having the
     * previously-specified search key.
     * Returns false if there is no more such records.
     */
    fun next(): Boolean {
        currentSlot++
        if (currentSlot >= contents.getNumRecords()) {
            return tryOverflow()
        }
        return if (contents.getDataVal(currentSlot) == searchKey) {
            true
        } else {
            tryOverflow()
        }
    }

    /**
     * Returns the dataRecordId value of the current leaf record.
     */
    fun getDataRecordId(): RecordId {
        return contents.getDataRecordId(currentSlot)
    }

    /**
     * Deletes the leaf record having the specified dataval
     * and dataRecordId values.
     */
    fun delete(dataVal: Constant, recordId: RecordId) {
        while (next()) {
            if (getDataRecordId() == recordId) {
                contents.delete(currentSlot)
                return
            }
        }
    }

    /**
     * Inserts a new leaf record having the specified dataval
     * and dataRecordId values.
     * If the insertion causes the page to split, then
     * the method returns the directory entry of the new page;
     * otherwise, the method returns null.
     */
    fun insert(dataVal: Constant, recordId: RecordId): DirEntry? {
        // If the new key is greater than all existing keys, or equal to one and the current record
        // is the first matching one, we can insert at the next slot.
        if (contents.getFlag() >= 0 && contents.getDataVal(0) > dataVal) {
            val firstVal = contents.getDataVal(0)
            val newBlock = contents.split(0, contents.getFlag())
            currentSlot = 0
            contents.setFlag(newBlock.blockNum)
            contents.insertLeaf(currentSlot, dataVal, recordId)
            return DirEntry(firstVal, newBlock.blockNum)
        }

        currentSlot++
        contents.insertLeaf(currentSlot, dataVal, recordId)

        if (!contents.isFull()) {
            return null
        }

        // The page is full, so split it.
        val firstKey = contents.getDataVal(0)
        val lastKey = contents.getDataVal(contents.getNumRecords() - 1)
        if (lastKey == firstKey) {
            // Create an overflow block to hold all but the first record.
            val newBlock = contents.split(1, contents.getFlag())
            contents.setFlag(newBlock.blockNum)
            return null
        } else {
            var splitPos = contents.getNumRecords() / 2
            var splitKey = contents.getDataVal(splitPos)
            if (splitKey == firstKey) {
                // Move right, looking for the next key.
                while (contents.getDataVal(splitPos) == splitKey) {
                    splitPos++
                }
                splitKey = contents.getDataVal(splitPos)
            } else {
                // Move left, looking for first entry having that key.
                while (contents.getDataVal(splitPos - 1) == splitKey) {
                    splitPos--
                }
            }
            val newBlock = contents.split(splitPos, contents.getFlag())
            return DirEntry(splitKey, newBlock.blockNum)
        }
    }

    private fun tryOverflow(): Boolean {
        val firstKey = contents.getDataVal(0)
        val flag = contents.getFlag()
        if (searchKey != firstKey || flag < 0) {
            return false
        }
        contents.close()
        val nextBlock = BlockId(currentBlock.fileName, flag)
        contents = BTPage(tx, nextBlock, layout)
        currentBlock = nextBlock
        currentSlot = 0
        return true
    }
}
