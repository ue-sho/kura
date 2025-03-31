package com.kura.jdbc

import java.sql.ResultSetMetaData
import java.sql.SQLException

/**
 * This class implements all of the methods of the ResultSetMetaData interface,
 * by throwing an exception for each one.
 * Subclasses can override those methods that they want to implement.
 */
abstract class ResultSetMetaDataAdapter : ResultSetMetaData {
    @Throws(SQLException::class)
    override fun getCatalogName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getColumnClassName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getColumnCount(): Int {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getColumnDisplaySize(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getColumnLabel(column: Int): String {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getColumnName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getColumnType(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getColumnTypeName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getPrecision(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getScale(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getSchemaName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun getTableName(column: Int): String {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isAutoIncrement(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isCaseSensitive(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isCurrency(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isDefinitelyWritable(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isNullable(column: Int): Int {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isReadOnly(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isSearchable(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isSigned(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isWritable(column: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>): Boolean {
        throw SQLException("operation not implemented")
    }

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T {
        throw SQLException("operation not implemented")
    }
}