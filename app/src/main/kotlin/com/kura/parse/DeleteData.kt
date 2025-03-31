package com.kura.parse

import com.kura.query.Predicate

/**
 * Class representing data for SQL delete statements.
 */
class DeleteData(
    private val tableName: String,
    private val pred: Predicate
) {
    /**
     * Returns the table to delete from.
     * @return the table name
     */
    fun tableName(): String {
        return tableName
    }

    /**
     * Returns the predicate that describes which records should be deleted.
     * @return the delete predicate
     */
    fun pred(): Predicate {
        return pred
    }

    override fun toString(): String {
        var result = "delete from $tableName"
        val predString = pred.toString()
        if (predString.isNotEmpty()) {
            result += " where $predString"
        }
        return result
    }
}