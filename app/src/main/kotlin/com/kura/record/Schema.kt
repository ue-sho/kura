package com.kura.record

import java.sql.Types

/**
 * The record schema of a table.
 * A schema contains the name and type of
 * each field of the table, as well as the length
 * of each varchar field.
 */
class Schema {
    private val fields: MutableList<String> = ArrayList()
    private val info: MutableMap<String, FieldInfo> = HashMap()

    /**
     * Add a field to the schema having a specified
     * name, type, and length.
     * If the field type is "integer", then the length
     * value is irrelevant.
     * @param fieldName the name of the field
     * @param type the type of the field, according to the constants in java.sql.Types
     * @param length the conceptual length of a string field.
     */
    fun addField(fieldName: String, type: Int, length: Int) {
        fields.add(fieldName)
        info[fieldName] = FieldInfo(type, length)
    }

    /**
     * Add an integer field to the schema.
     * @param fieldName the name of the field
     */
    fun addIntField(fieldName: String) {
        addField(fieldName, Types.INTEGER, 0)
    }

    /**
     * Add a string field to the schema.
     * The length is the conceptual length of the field.
     * For example, if the field is defined as varchar(8),
     * then its length is 8.
     * @param fieldName the name of the field
     * @param length the number of chars in the varchar definition
     */
    fun addStringField(fieldName: String, length: Int) {
        addField(fieldName, Types.VARCHAR, length)
    }

    /**
     * Add a field to the schema having the same
     * type and length as the corresponding field
     * in another schema.
     * @param fieldName the name of the field
     * @param schema the other schema
     */
    fun add(fieldName: String, schema: Schema) {
        val type = schema.type(fieldName)
        val length = schema.length(fieldName)
        addField(fieldName, type, length)
    }

    /**
     * Add all of the fields in the specified schema
     * to the current schema.
     * @param schema the other schema
     */
    fun addAll(schema: Schema) {
        for (fieldName in schema.fields()) {
            add(fieldName, schema)
        }
    }

    /**
     * Return a collection containing the name of
     * each field in the schema.
     * @return the collection of the schema's field names
     */
    fun fields(): List<String> {
        return fields
    }

    /**
     * Return true if the specified field
     * is in the schema
     * @param fieldName the name of the field
     * @return true if the field is in the schema
     */
    fun hasField(fieldName: String): Boolean {
        return fields.contains(fieldName)
    }

    /**
     * Return the type of the specified field, using the
     * constants in java.sql.Types.
     * @param fieldName the name of the field
     * @return the integer type of the field
     */
    fun type(fieldName: String): Int {
        return info[fieldName]?.type ?: 0
    }

    /**
     * Return the conceptual length of the specified field.
     * If the field is not a string field, then
     * the return value is undefined.
     * @param fieldName the name of the field
     * @return the conceptual length of the field
     */
    fun length(fieldName: String): Int {
        return info[fieldName]?.length ?: 0
    }

    private data class FieldInfo(val type: Int, val length: Int)
}