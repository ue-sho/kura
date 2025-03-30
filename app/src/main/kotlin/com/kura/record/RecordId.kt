package com.kura.record

/**
 * An identifier for a record within a file.
 * A RecordId consists of the block number in the file,
 * and the location of the record in that block.
 */
class RecordId(private val blockNumber: Int, private val slot: Int) {

    /**
     * Return the block number associated with this RecordId.
     * @return the block number
     */
    fun blockNumber(): Int {
        return blockNumber
    }

    /**
     * Return the slot associated with this RecordId.
     * @return the slot
     */
    fun slot(): Int {
        return slot
    }

    override fun equals(other: Any?): Boolean {
        if (other !is RecordId) return false
        return blockNumber == other.blockNumber && slot == other.slot
    }

    override fun hashCode(): Int {
        return 31 * blockNumber + slot
    }

    override fun toString(): String {
        return "[$blockNumber, $slot]"
    }
}