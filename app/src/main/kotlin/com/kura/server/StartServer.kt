package com.kura.server

import java.rmi.registry.LocateRegistry
import com.kura.jdbc.network.RemoteDriver
import com.kura.jdbc.network.RemoteDriverImpl

/**
 * Server startup class for Kura database.
 */
object StartServer {
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            // configure and initialize the database
            val dirname = if (args.isEmpty()) "studentdb" else args[0]
            val db = KuraDB(dirname)

            // create a registry specific for the server on the default port
            val registry = LocateRegistry.createRegistry(1099)

            // and post the server entry in it
            val driver: RemoteDriver = RemoteDriverImpl(db)
            registry.rebind("kuradb", driver)

            println("Database server ready")
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}