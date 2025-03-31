package com.kura.metadata

import com.kura.record.*
import com.kura.transaction.Transaction
import java.util.*

/**
 * The table manager.
 * There are methods to create a table, save the metadata
 * in the catalog, and obtain the metadata of a
 * previously-created table.
 */
class TableManager(isNew: Boolean, tx: Transaction) {
    companion object {
        // The max characters a tablename or fieldname can have.
        const val MAX_NAME: Int = 16
    }

    private val tableLayoutCatalog: Layout
    private val fieldLayoutCatalog: Layout

    init {
        val tableSchemaForCatalog = Schema()
        tableSchemaForCatalog.addStringField("tblname", MAX_NAME)
        tableSchemaForCatalog.addIntField("slotsize")
        tableLayoutCatalog = Layout(tableSchemaForCatalog)

        val fieldSchemaForCatalog = Schema()
        fieldSchemaForCatalog.addStringField("tblname", MAX_NAME)
        fieldSchemaForCatalog.addStringField("fldname", MAX_NAME)
        fieldSchemaForCatalog.addIntField("type")
        fieldSchemaForCatalog.addIntField("length")
        fieldSchemaForCatalog.addIntField("offset")
        fieldLayoutCatalog = Layout(fieldSchemaForCatalog)

        if (isNew) {
            createTable("tblcat", tableSchemaForCatalog, tx)
            createTable("fldcat", fieldSchemaForCatalog, tx)
        }
    }

    /**
     * Create a new table having the specified name and schema.
     * @param tableName the name of the new table
     * @param schema the table's schema
     * @param tx the transaction creating the table
     */
    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        val layout = Layout(schema)
        // insert one record into tblcat
        val tableCatalog = TableScan(tx, "tblcat", tableLayoutCatalog)
        tableCatalog.insert()
        tableCatalog.setString("tblname", tableName)
        tableCatalog.setInt("slotsize", layout.slotSize())
        tableCatalog.close()

        // insert a record into fldcat for each field
        val fieldCatalog = TableScan(tx, "fldcat", fieldLayoutCatalog)
        for (fieldName in schema.fields()) {
            fieldCatalog.insert()
            fieldCatalog.setString("tblname", tableName)
            fieldCatalog.setString("fldname", fieldName)
            fieldCatalog.setInt("type", schema.type(fieldName))
            fieldCatalog.setInt("length", schema.length(fieldName))
            fieldCatalog.setInt("offset", layout.offset(fieldName))
        }
        fieldCatalog.close()
    }

    /**
     * Retrieve the layout of the specified table
     * from the catalog.
     * @param tableName the name of the table
     * @param tx the transaction
     * @return the table's stored metadata
     */
    fun getLayout(tableName: String, tx: Transaction): Layout {
        var size = -1
        val tableCatalog = TableScan(tx, "tblcat", tableLayoutCatalog)
        while (tableCatalog.next()) {
            if (tableCatalog.getString("tblname") == tableName) {
                size = tableCatalog.getInt("slotsize")
                break
            }
        }
        tableCatalog.close()

        val schema = Schema()
        val offsets = HashMap<String, Int>()
        val fieldCatalog = TableScan(tx, "fldcat", fieldLayoutCatalog)
        while (fieldCatalog.next()) {
            if (fieldCatalog.getString("tblname") == tableName) {
                val fieldName = fieldCatalog.getString("fldname")
                val fieldType = fieldCatalog.getInt("type")
                val fieldLength = fieldCatalog.getInt("length")
                val offset = fieldCatalog.getInt("offset")
                offsets[fieldName] = offset
                schema.addField(fieldName, fieldType, fieldLength)
            }
        }
        fieldCatalog.close()
        return Layout(schema, offsets, size)
    }
}