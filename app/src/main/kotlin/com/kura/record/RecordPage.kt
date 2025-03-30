package com.kura.record

import java.sql.Types
import com.kura.file.BlockId
import com.kura.transaction.Transaction

/**
 * Store a record at a given location in a block.
 */
class RecordPage(
    private val transaction: Transaction,
    private val block: BlockId,
    private val layout: Layout
) {
    companion object {
        const val EMPTY = 0
        const val USED = 1
    }

    init {
        transaction.pin(block)
    }

    /**
     * Return the integer value stored for the
     * specified field of a specified slot.
     * @param slot the slot of the record
     * @param fieldName the name of the field
     * @return the integer stored in that field
     */
    fun getInt(slot: Int, fieldName: String): Int {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        return transaction.getInt(block, fieldPosition)
    }

    /**
     * Return the string value stored for the
     * specified field of the specified slot.
     * @param slot the slot of the record
     * @param fieldName the name of the field
     * @return the string stored in that field
     */
    fun getString(slot: Int, fieldName: String): String {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        return transaction.getString(block, fieldPosition)
    }

    /**
     * Store an integer at the specified field
     * of the specified slot.
     * @param slot the slot of the record
     * @param fieldName the name of the field
     * @param value the integer value stored in that field
     */
    fun setInt(slot: Int, fieldName: String, value: Int) {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        transaction.setInt(block, fieldPosition, value, true)
    }

    /**
     * Store a string at the specified field
     * of the specified slot.
     * @param slot the slot of the record
     * @param fieldName the name of the field
     * @param value the string value stored in that field
     */
    fun setString(slot: Int, fieldName: String, value: String) {
        val fieldPosition = offset(slot) + layout.offset(fieldName)
        transaction.setString(block, fieldPosition, value, true)
    }

    /**
     * Delete the record at the specified slot.
     * @param slot the slot of the record to be deleted
     */
    fun delete(slot: Int) {
        setFlag(slot, EMPTY)
    }

    /**
     * Use the layout to format a new block of records.
     * These values should not be logged
     * (because the old values are meaningless).
     */
    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            transaction.setInt(block, offset(slot), EMPTY, false)
            val schema = layout.schema()
            for (fieldName in schema.fields()) {
                val fieldPosition = offset(slot) + layout.offset(fieldName)
                if (schema.type(fieldName) == Types.INTEGER) {
                    transaction.setInt(block, fieldPosition, 0, false)
                } else {
                    transaction.setString(block, fieldPosition, "", false)
                }
            }
            slot++
        }
    }

    /**
     * Find the next used slot after the specified slot.
     * @param slot the starting slot
     * @return the next used slot, or -1 if no such slot exists
     */
    fun nextAfter(slot: Int): Int {
        return searchAfter(slot, USED)
    }

    /**
     * Find the next empty slot after the specified slot.
     * If found, mark the slot as used.
     * @param slot the starting slot
     * @return the next empty slot, or -1 if no such slot exists
     */
    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, EMPTY)
        if (newSlot >= 0) {
            setFlag(newSlot, USED)
        }
        return newSlot
    }

    /**
     * Get the block for this record page
     * @return the block
     */
    fun block(): BlockId {
        return block
    }

    // Private auxiliary methods

    /**
     * Set the record's empty/inuse flag.
     * @param slot the slot of the record
     * @param flag the value of the flag
     */
    private fun setFlag(slot: Int, flag: Int) {
        transaction.setInt(block, offset(slot), flag, true)
    }

    /**
     * Search the block for a slot with the specified flag value,
     * starting at the specified slot.
     * @param slot the starting slot position
     * @param flag the flag value to look for
     * @return the slot with the specified flag value, or -1 if not found
     */
    private fun searchAfter(slot: Int, flag: Int): Int {
        var currentSlot = slot + 1
        while (isValidSlot(currentSlot)) {
            if (transaction.getInt(block, offset(currentSlot)) == flag) {
                return currentSlot
            }
            currentSlot++
        }
        return -1
    }

    /**
     * Check if the specified slot is valid.
     * @param slot the slot to check
     * @return true if the slot is valid, false otherwise
     */
    private fun isValidSlot(slot: Int): Boolean {
        return offset(slot + 1) <= transaction.blockSize()
    }

    /**
     * Calculate the offset of the specified slot.
     * @param slot the slot
     * @return the offset
     */
    private fun offset(slot: Int): Int {
        return slot * layout.slotSize()
    }
}