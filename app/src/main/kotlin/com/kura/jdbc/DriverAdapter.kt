package com.kura.jdbc

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.Properties
import java.util.logging.Logger

/**
 * This class implements all of the methods of the Driver interface,
 * by throwing an exception for each one.
 * Subclasses can override those methods that they want to implement.
 */
abstract class DriverAdapter : Driver {
    override fun connect(url: String, info: Properties?): Connection {
        throw SQLException("operation not implemented")
    }

    override fun acceptsURL(url: String): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun getPropertyInfo(url: String, info: Properties?): Array<DriverPropertyInfo> {
        throw SQLException("operation not implemented")
    }

    override fun getMajorVersion(): Int {
        return 0
    }

    override fun getMinorVersion(): Int {
        return 1
    }

    override fun jdbcCompliant(): Boolean {
        return false
    }

    override fun getParentLogger(): Logger {
        throw SQLFeatureNotSupportedException("operation not implemented")
    }
}