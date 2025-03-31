package com.kura.parse

import com.kura.record.Schema

/**
 * Class representing data for SQL create table statements.
 */
class CreateTableData(
    private val tableName: String,
    private val schema: Schema
) {
    /**
     * Returns the name of the new table.
     * @return the table name
     */
    fun tableName(): String {
        return tableName
    }

    /**
     * Returns the schema of the new table.
     * @return the schema
     */
    fun schema(): Schema {
        return schema
    }

    override fun toString(): String {
        val result = StringBuilder("create table ")
        result.append(tableName)
        result.append(" (")
        val fields = schema.fields()
        for (i in fields.indices) {
            val fieldName = fields[i]
            result.append(fieldName)
            result.append(" ")
            val type = schema.type(fieldName)
            result.append(type)
            if (i != fields.size - 1) {
                result.append(", ")
            }
        }
        result.append(")")
        return result.toString()
    }
}