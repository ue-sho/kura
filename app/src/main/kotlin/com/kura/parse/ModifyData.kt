package com.kura.parse

import com.kura.query.Expression
import com.kura.query.Predicate

/**
 * Class representing data for SQL update statements.
 */
class ModifyData(
    private val tableName: String,
    private val fieldName: String,
    private val newValue: Expression,
    private val pred: Predicate
) {
    /**
     * Returns the table to be modified.
     * @return the table name
     */
    fun tableName(): String {
        return tableName
    }

    /**
     * Returns the field whose values will be modified.
     * @return the field name
     */
    fun fieldName(): String {
        return fieldName
    }

    /**
     * Returns the expression whose value will be the new value of the field.
     * @return the new value expression
     */
    fun newValue(): Expression {
        return newValue
    }

    /**
     * Returns the predicate that describes which records should be modified.
     * @return the update predicate
     */
    fun pred(): Predicate {
        return pred
    }

    override fun toString(): String {
        var result = "update $tableName "
        result += "set $fieldName = $newValue"
        val predString = pred.toString()
        if (predString.isNotEmpty()) {
            result += " where $predString"
        }
        return result
    }
}