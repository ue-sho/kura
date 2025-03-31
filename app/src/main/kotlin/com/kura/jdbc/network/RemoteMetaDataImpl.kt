package com.kura.jdbc.network

import com.kura.record.Schema
import java.rmi.RemoteException
import java.rmi.server.UnicastRemoteObject
import java.sql.Types.INTEGER

/**
 * The RMI server-side implementation of RemoteMetaData.
 */
class RemoteMetaDataImpl(private val sch: Schema) : UnicastRemoteObject(), RemoteMetaData {
    private val fields: MutableList<String> = ArrayList()

    init {
        for (fld in sch.fields()) {
            fields.add(fld)
        }
    }

    /**
     * Returns the size of the field list.
     */
    @Throws(RemoteException::class)
    override fun getColumnCount(): Int {
        return fields.size
    }

    /**
     * Returns the field name for the specified column number.
     * In JDBC, column numbers start with 1, so the field
     * is taken from position (column-1) in the list.
     */
    @Throws(RemoteException::class)
    override fun getColumnName(column: Int): String {
        return fields[column - 1]
    }

    /**
     * Returns the type of the specified column.
     * The method first finds the name of the field in that column,
     * and then looks up its type in the schema.
     */
    @Throws(RemoteException::class)
    override fun getColumnType(column: Int): Int {
        val fldname = getColumnName(column)
        return sch.type(fldname)
    }

    /**
     * Returns the number of characters required to display the
     * specified column.
     * For a string-type field, the method simply looks up the
     * field's length in the schema and returns that.
     * For an int-type field, the method needs to decide how
     * large integers can be.
     * Here, the method arbitrarily chooses 6 characters,
     * which means that integers over 999,999 will
     * probably get displayed improperly.
     */
    @Throws(RemoteException::class)
    override fun getColumnDisplaySize(column: Int): Int {
        val fldname = getColumnName(column)
        val fldtype = sch.type(fldname)
        val fldlength = if (fldtype == INTEGER) 6 else sch.length(fldname)
        return Math.max(fldname.length, fldlength) + 1
    }
}