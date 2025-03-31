package com.kura.jdbc.network

import com.kura.plan.Plan
import com.kura.plan.Planner
import com.kura.transaction.Transaction
import java.rmi.RemoteException
import java.rmi.server.UnicastRemoteObject

/**
 * The RMI server-side implementation of RemoteStatement.
 */
class RemoteStatementImpl : UnicastRemoteObject, RemoteStatement {
    private val remoteConnection: RemoteConnectionImpl
    private val planner: Planner

    constructor(remoteConnection: RemoteConnectionImpl, planner: Planner) : super() {
        this.remoteConnection = remoteConnection
        this.planner = planner
    }

    /**
     * Executes the specified SQL query string.
     * The method calls the query planner to create a plan
     * for the query. It then sends the plan to the
     * RemoteResultSetImpl constructor for processing.
     */
    @Throws(RemoteException::class)
    override fun executeQuery(query: String): RemoteResultSet {
        return try {
            val transaction = remoteConnection.getTransaction()
            val plan = planner.createQueryPlan(query, transaction)
            RemoteResultSetImpl(plan, remoteConnection)
        } catch (e: RuntimeException) {
            remoteConnection.rollback()
            throw e
        }
    }

    /**
     * Executes the specified SQL update command.
     * The method sends the command to the update planner,
     * which executes it.
     */
    @Throws(RemoteException::class)
    override fun executeUpdate(command: String): Int {
        return try {
            val transaction = remoteConnection.getTransaction()
            val result = planner.executeUpdate(command, transaction)
            remoteConnection.commit()
            result
        } catch (e: RuntimeException) {
            remoteConnection.rollback()
            throw e
        }
    }

    @Throws(RemoteException::class)
    override fun close() {
        // Not implemented
    }
}