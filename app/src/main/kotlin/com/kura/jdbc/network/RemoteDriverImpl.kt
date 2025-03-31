package com.kura.jdbc.network

import com.kura.server.KuraDB
import java.rmi.RemoteException
import java.rmi.server.UnicastRemoteObject

/**
 * The RMI server-side implementation of RemoteDriver.
 */
class RemoteDriverImpl(private val db: KuraDB) : UnicastRemoteObject(), RemoteDriver {

    /**
     * Creates a new RemoteConnectionImpl object and
     * returns it.
     */
    @Throws(RemoteException::class)
    override fun connect(): RemoteConnection {
        return RemoteConnectionImpl(db)
    }
}