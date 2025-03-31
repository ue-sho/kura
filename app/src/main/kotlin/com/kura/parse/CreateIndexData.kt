package com.kura.parse

/**
 * Class representing data for SQL create index statements.
 */
class CreateIndexData(
    private val indexName: String,
    private val tableName: String,
    private val fieldName: String
) {
    /**
     * Returns the name of the new index.
     * @return the index name
     */
    fun indexName(): String {
        return indexName
    }

    /**
     * Returns the name of the indexed table.
     * @return the table name
     */
    fun tableName(): String {
        return tableName
    }

    /**
     * Returns the name of the indexed field.
     * @return the field name
     */
    fun fieldName(): String {
        return fieldName
    }

    override fun toString(): String {
        return "create index $indexName on $tableName ($fieldName)"
    }
}