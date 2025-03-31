package com.kura.jdbc.network

import com.kura.plan.Plan
import com.kura.query.Scan
import com.kura.record.Schema
import java.rmi.RemoteException
import java.rmi.server.UnicastRemoteObject

/**
 * The RMI server-side implementation of RemoteResultSet.
 */
class RemoteResultSetImpl : UnicastRemoteObject, RemoteResultSet {
    private val scan: Scan
    private val schema: Schema
    private val remoteConnection: RemoteConnectionImpl

    /**
     * Creates a RemoteResultSet object.
     * The specified plan is opened, and the scan is saved.
     */
    constructor(plan: Plan, remoteConnection: RemoteConnectionImpl) : super() {
        this.scan = plan.open()
        this.schema = plan.schema()
        this.remoteConnection = remoteConnection
    }

    /**
     * Moves to the next record in the result set,
     * by moving to the next record in the saved scan.
     */
    @Throws(RemoteException::class)
    override fun next(): Boolean {
        return try {
            scan.next()
        } catch (e: RuntimeException) {
            remoteConnection.rollback()
            throw e
        }
    }

    /**
     * Returns the integer value of the specified field,
     * by returning the corresponding value on the saved scan.
     */
    @Throws(RemoteException::class)
    override fun getInt(fieldName: String): Int {
        return try {
            val lowerCaseFieldName = fieldName.lowercase() // to ensure case-insensitivity
            scan.getInt(lowerCaseFieldName)
        } catch (e: RuntimeException) {
            remoteConnection.rollback()
            throw e
        }
    }

    /**
     * Returns the string value of the specified field,
     * by returning the corresponding value on the saved scan.
     */
    @Throws(RemoteException::class)
    override fun getString(fieldName: String): String {
        return try {
            val lowerCaseFieldName = fieldName.lowercase() // to ensure case-insensitivity
            scan.getString(lowerCaseFieldName)
        } catch (e: RuntimeException) {
            remoteConnection.rollback()
            throw e
        }
    }

    /**
     * Returns the boolean value of the specified field.
     * The field is assumed to be an integer, with
     * 0 representing false and 1 representing true.
     */
    @Throws(RemoteException::class)
    override fun getBoolean(fieldName: String): Boolean {
        return try {
            val lowerCaseFieldName = fieldName.lowercase() // to ensure case-insensitivity
            scan.getInt(lowerCaseFieldName) == 1
        } catch (e: RuntimeException) {
            remoteConnection.rollback()
            throw e
        }
    }

    /**
     * Closes the result set by closing its scan.
     */
    @Throws(RemoteException::class)
    override fun close() {
        scan.close()
    }

    /**
     * Returns the metadata associated with this result set.
     */
    @Throws(RemoteException::class)
    override fun getMetaData(): RemoteMetaData {
        return RemoteMetaDataImpl(schema)
    }
}