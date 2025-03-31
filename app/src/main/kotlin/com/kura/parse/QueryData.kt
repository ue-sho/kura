package com.kura.parse

import com.kura.query.Predicate

/**
 * Class representing data for SQL select statements.
 */
class QueryData(
    private val fields: List<String>,
    private val tables: Collection<String>,
    private val pred: Predicate
) {
    /**
     * Returns the fields specified in the select clause.
     * @return a list of field names
     */
    fun fields(): List<String> {
        return fields
    }

    /**
     * Returns the tables specified in the from clause.
     * @return a collection of table names
     */
    fun tables(): Collection<String> {
        return tables
    }

    /**
     * Returns the predicate that describes which records
     * should be included in the output table.
     * @return the query predicate
     */
    fun pred(): Predicate {
        return pred
    }

    override fun toString(): String {
        var result = "select "
        for (fldname in fields) {
            result += "$fldname, "
        }
        // Remove final comma
        result = result.substring(0, result.length - 2)

        result += " from "
        for (tblname in tables) {
            result += "$tblname, "
        }
        // Remove final comma
        result = result.substring(0, result.length - 2)

        val predString = pred.toString()
        if (predString.isNotEmpty()) {
            result += " where $predString"
        }

        return result
    }
}