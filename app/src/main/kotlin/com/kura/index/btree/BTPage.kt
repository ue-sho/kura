package com.kura.index.btree

import com.kura.file.BlockId
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordId
import com.kura.record.Schema
import com.kura.transaction.Transaction
import java.sql.Types

/**
 * B-tree page that stores sorted records.
 * Each page has:
 * - A flag (int at offset 0): 0 for leaf, >0 for directory level
 * - A record count (int at offset 4)
 * - Sorted record slots starting at offset 8
 *
 * Leaf records contain: (dataval, block, id) -> maps a search key to a RecordId
 * Directory records contain: (dataval, block) -> maps a search key to a child block number
 */
class BTPage(
    private val tx: Transaction,
    private var currentBlock: BlockId,
    private val layout: Layout
) {
    init {
        tx.pin(currentBlock)
    }

    /**
     * Calculates the position in the slot that could contain
     * the specified search key.
     * Returns the index of the last slot whose dataval is less than
     * or equal to the specified key (or -1 if no such slot).
     * The caller can then look at the next slot to find the first
     * record having that key.
     */
    fun findSlotBefore(searchKey: Constant): Int {
        var slot = 0
        while (slot < getNumRecords() && getDataVal(slot) < searchKey) {
            slot++
        }
        return slot - 1
    }

    /**
     * Closes the page by unpinning its block.
     */
    fun close() {
        if (currentBlock.blockNum >= 0) {
            tx.unpin(currentBlock)
        }
        currentBlock = BlockId(currentBlock.fileName, -1)
    }

    /**
     * Returns true if the block is full.
     * A block is full when the number of records in the block
     * equals the number of records that can fit in a block.
     */
    fun isFull(): Boolean {
        return slotPosition(getNumRecords() + 1) >= tx.blockSize()
    }

    /**
     * Splits the page at the specified position.
     * A new page is created, and the records from the
     * split position to the end are transferred to the new page.
     */
    fun split(splitPosition: Int, flag: Int): BlockId {
        val newBlock = appendNew(flag)
        val newPage = BTPage(tx, newBlock, layout)
        transferRecords(splitPosition, newPage)
        newPage.setFlag(flag)
        newPage.close()
        return newBlock
    }

    /**
     * Returns the dataval of the record at the specified slot.
     */
    fun getDataVal(slot: Int): Constant {
        return getVal(slot, "dataval")
    }

    /**
     * Returns the flag value of this page.
     * The flag is stored at offset 0.
     * For leaves, flag=0; for directory pages, flag>0.
     */
    fun getFlag(): Int {
        return tx.getInt(currentBlock, 0)
    }

    /**
     * Sets the flag value of this page.
     */
    fun setFlag(flag: Int) {
        tx.setInt(currentBlock, 0, flag, true)
    }

    /**
     * Appends a new block to the file, formatting it with default values.
     */
    fun appendNew(flag: Int): BlockId {
        val newBlock = tx.append(currentBlock.fileName)
        tx.pin(newBlock)
        format(newBlock, flag)
        return newBlock
    }

    /**
     * Formats the specified block by initializing it with default values.
     */
    fun format(block: BlockId, flag: Int) {
        tx.setInt(block, 0, flag, false)
        tx.setInt(block, Int.SIZE_BYTES, 0, false) // numRecords = 0
        val recordSize = layout.slotSize()
        var pos = 2 * Int.SIZE_BYTES
        while (pos + recordSize <= tx.blockSize()) {
            makeDefaultRecord(block, pos)
            pos += recordSize
        }
    }

    // Methods called only by BTreeLeaf

    /**
     * Returns the dataRecordId at the specified slot (leaf only).
     */
    fun getDataRecordId(slot: Int): RecordId {
        return RecordId(getInt(slot, "block"), getInt(slot, "id"))
    }

    /**
     * Inserts a leaf record at the specified slot.
     */
    fun insertLeaf(slot: Int, dataVal: Constant, recordId: RecordId) {
        insert(slot)
        setVal(slot, "dataval", dataVal)
        setInt(slot, "block", recordId.blockNumber())
        setInt(slot, "id", recordId.slot())
    }

    /**
     * Deletes the record at the specified slot.
     */
    fun delete(slot: Int) {
        for (i in slot until getNumRecords() - 1) {
            copyRecord(i + 1, i)
        }
        setNumRecords(getNumRecords() - 1)
    }

    // Methods called only by BTreeDir

    /**
     * Returns the child block number at the specified slot (directory only).
     */
    fun getChildNum(slot: Int): Int {
        return getInt(slot, "block")
    }

    /**
     * Inserts a directory record at the specified slot.
     */
    fun insertDir(slot: Int, dataVal: Constant, blockNumber: Int) {
        insert(slot)
        setVal(slot, "dataval", dataVal)
        setInt(slot, "block", blockNumber)
    }

    // Methods to get/set values

    /**
     * Returns the number of records in this page.
     */
    fun getNumRecords(): Int {
        return tx.getInt(currentBlock, Int.SIZE_BYTES)
    }

    // Private helper methods

    private fun getInt(slot: Int, fieldName: String): Int {
        val pos = fieldPosition(slot, fieldName)
        return tx.getInt(currentBlock, pos)
    }

    private fun getString(slot: Int, fieldName: String): String {
        val pos = fieldPosition(slot, fieldName)
        return tx.getString(currentBlock, pos)
    }

    private fun getVal(slot: Int, fieldName: String): Constant {
        val type = layout.schema().type(fieldName)
        return if (type == Types.INTEGER) {
            Constant(getInt(slot, fieldName))
        } else {
            Constant(getString(slot, fieldName))
        }
    }

    private fun setInt(slot: Int, fieldName: String, value: Int) {
        val pos = fieldPosition(slot, fieldName)
        tx.setInt(currentBlock, pos, value, true)
    }

    private fun setString(slot: Int, fieldName: String, value: String) {
        val pos = fieldPosition(slot, fieldName)
        tx.setString(currentBlock, pos, value, true)
    }

    private fun setVal(slot: Int, fieldName: String, value: Constant) {
        val type = layout.schema().type(fieldName)
        if (type == Types.INTEGER) {
            setInt(slot, fieldName, value.asInt())
        } else {
            setString(slot, fieldName, value.asString())
        }
    }

    private fun setNumRecords(n: Int) {
        tx.setInt(currentBlock, Int.SIZE_BYTES, n, true)
    }

    private fun insert(slot: Int) {
        for (i in getNumRecords() downTo slot + 1) {
            copyRecord(i - 1, i)
        }
        setNumRecords(getNumRecords() + 1)
    }

    private fun copyRecord(from: Int, to: Int) {
        val schema = layout.schema()
        for (fieldName in schema.fields()) {
            setVal(to, fieldName, getVal(from, fieldName))
        }
    }

    private fun transferRecords(slot: Int, dest: BTPage) {
        var destSlot = 0
        while (slot < getNumRecords()) {
            dest.insert(destSlot)
            val schema = layout.schema()
            for (fieldName in schema.fields()) {
                dest.setVal(destSlot, fieldName, getVal(slot, fieldName))
            }
            delete(slot)
            destSlot++
        }
    }

    private fun fieldPosition(slot: Int, fieldName: String): Int {
        val offset = layout.offset(fieldName)
        return slotPosition(slot) + offset
    }

    private fun slotPosition(slot: Int): Int {
        val slotSize = layout.slotSize()
        return Int.SIZE_BYTES + Int.SIZE_BYTES + (slot * slotSize)
    }

    private fun makeDefaultRecord(block: BlockId, pos: Int) {
        val schema = layout.schema()
        for (fieldName in schema.fields()) {
            val offset = layout.offset(fieldName)
            if (schema.type(fieldName) == Types.INTEGER) {
                tx.setInt(block, pos + offset, 0, false)
            } else {
                tx.setString(block, pos + offset, "", false)
            }
        }
    }
}
