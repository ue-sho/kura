package com.kura.record

import java.sql.Types
import com.kura.file.Page

/**
 * Description of the structure of a record.
 * It contains the name, type, length and offset of
 * each field of the table.
 */
class Layout {
    private val schema: Schema
    private val offsets: MutableMap<String, Int>
    private val slotSize: Int

    /**
     * This constructor creates a Layout object from a schema.
     * This constructor is used when a table
     * is created. It determines the physical offset of
     * each field within the record.
     * @param schema the schema of the table's records
     */
    constructor(schema: Schema) {
        this.schema = schema
        offsets = HashMap()
        var pos = Int.SIZE_BYTES // leave space for the empty/inuse flag
        for (fieldName in schema.fields()) {
            offsets[fieldName] = pos
            pos += lengthInBytes(fieldName)
        }
        slotSize = pos
    }

    /**
     * Create a Layout object from the specified metadata.
     * This constructor is used when the metadata
     * is retrieved from the catalog.
     * @param schema the schema of the table's records
     * @param offsets the already-calculated offsets of the fields within a record
     * @param slotSize the already-calculated length of each record
     */
    constructor(schema: Schema, offsets: Map<String, Int>, slotSize: Int) {
        this.schema = schema
        this.offsets = HashMap(offsets)
        this.slotSize = slotSize
    }

    /**
     * Return the schema of the table's records
     * @return the table's record schema
     */
    fun schema(): Schema {
        return schema
    }

    /**
     * Return the offset of a specified field within a record
     * @param fieldName the name of the field
     * @return the offset of that field within a record
     */
    fun offset(fieldName: String): Int {
        return offsets[fieldName] ?: 0
    }

    /**
     * Return the size of a slot, in bytes.
     * @return the size of a slot
     */
    fun slotSize(): Int {
        return slotSize
    }

    private fun lengthInBytes(fieldName: String): Int {
        val fieldType = schema.type(fieldName)
        return if (fieldType == Types.INTEGER) {
            Int.SIZE_BYTES
        } else { // fieldType == VARCHAR
            Page.maxLength(schema.length(fieldName))
        }
    }
}