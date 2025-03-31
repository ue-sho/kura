package com.kura.parse

import com.kura.query.Constant

/**
 * Class representing data for SQL insert statements.
 */
class InsertData(
    private val tableName: String,
    private val fields: List<String>,
    private val values: List<Constant>
) {
    /**
     * Returns the table to insert into.
     * @return the table name
     */
    fun tableName(): String {
        return tableName
    }

    /**
     * Returns the fields specified in the insert statement.
     * @return a list of field names
     */
    fun fields(): List<String> {
        return fields
    }

    /**
     * Returns the values specified in the insert statement.
     * @return a list of constant values
     */
    fun values(): List<Constant> {
        return values
    }

    override fun toString(): String {
        var result = "insert into $tableName ("

        // Create field list
        for (fldname in fields) {
            result += "$fldname, "
        }
        // Remove final comma
        result = result.substring(0, result.length - 2)
        result += ") values ("

        // Create value list
        for (val1 in values) {
            result += "$val1, "
        }
        // Remove final comma
        result = result.substring(0, result.length - 2)
        result += ")"

        return result
    }
}