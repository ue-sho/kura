package com.kura.metadata

/**
 * A StatisticsInfo object holds three pieces of
 * statistical information about a table:
 * the number of blocks, the number of records,
 * and the number of distinct values for each field.
 */
class StatisticsInfo(
    private val numBlocks: Int,
    private val numRecords: Int
) {
    /**
     * Return the estimated number of blocks in the table.
     * @return the estimated number of blocks in the table
     */
    fun blocksAccessed(): Int {
        return numBlocks
    }

    /**
     * Return the estimated number of records in the table.
     * @return the estimated number of records in the table
     */
    fun recordsOutput(): Int {
        return numRecords
    }

    /**
     * Return the estimated number of distinct values
     * for the specified field.
     * This estimate is a complete guess, because doing something
     * reasonable is beyond the scope of this system.
     * @param fieldName the name of the field
     * @return a guess as to the number of distinct field values
     */
    fun distinctValues(fieldName: String): Int {
        return 1 + (numRecords / 3)
    }
}