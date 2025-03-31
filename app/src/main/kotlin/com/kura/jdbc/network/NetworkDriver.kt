package com.kura.jdbc.network

import com.kura.jdbc.DriverAdapter
import java.rmi.registry.LocateRegistry
import java.sql.Connection
import java.sql.SQLException
import java.util.Properties

/**
 * The KuraDB database driver for network connections.
 * This driver connects to a KuraDB server through RMI.
 */
class NetworkDriver : DriverAdapter() {
    /**
     * Connects to the KuraDB server on the specified host.
     * The URL format is: jdbc:kuradb://host[:port]
     * Default port is 1099
     */
    override fun connect(url: String, info: Properties?): Connection {
        if (!acceptsURL(url)) {
            throw SQLException("Invalid URL format: $url")
        }

        try {
            // URLから接続情報を取得
            val hostAndPort = url.replace("jdbc:kuradb://", "")
            val parts = hostAndPort.split(":")
            val host = parts[0]
            val port = if (parts.size > 1) parts[1].toInt() else 1099

            // RMIレジストリに接続
            val registry = LocateRegistry.getRegistry(host, port)
            val remoteDriver = registry.lookup("kuradb") as RemoteDriver
            val remoteConnection = remoteDriver.connect()
            return NetworkConnection(remoteConnection)
        } catch (e: Exception) {
            throw SQLException("Failed to connect to $url: ${e.message}", e)
        }
    }

    /**
     * Returns true if the driver can connect to the given URL.
     * The URL must start with jdbc:kuradb://
     */
    override fun acceptsURL(url: String): Boolean {
        return url.startsWith("jdbc:kuradb://") ?: false
    }
}