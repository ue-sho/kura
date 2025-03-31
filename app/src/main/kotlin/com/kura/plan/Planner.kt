package com.kura.plan

import com.kura.parse.CreateIndexData
import com.kura.parse.CreateTableData
import com.kura.parse.CreateViewData
import com.kura.parse.DeleteData
import com.kura.parse.InsertData
import com.kura.parse.ModifyData
import com.kura.parse.Parser
import com.kura.parse.QueryData
import com.kura.transaction.Transaction

/**
 * The object that executes SQL statements.
 */
class Planner(
    private val queryPlanner: QueryPlanner,
    private val updatePlanner: UpdatePlanner
) {
    /**
     * Creates a plan for an SQL select statement, using the supplied planner.
     * @param query the SQL query string
     * @param transaction the transaction
     * @return the scan corresponding to the query plan
     */
    fun createQueryPlan(query: String, transaction: Transaction): Plan {
        val parser = Parser(query)
        val data = parser.query()
        verifyQuery(data)
        return queryPlanner.createPlan(data, transaction)
    }

    /**
     * Executes an SQL insert, delete, modify, or
     * create statement.
     * The method dispatches to the appropriate method of the
     * supplied update planner,
     * depending on what the parser returns.
     * @param command the SQL update string
     * @param transaction the transaction
     * @return an integer denoting the number of affected records
     */
    fun executeUpdate(command: String, transaction: Transaction): Int {
        val parser = Parser(command)
        val data = parser.updateCommand()
        verifyUpdate(data)
        return when (data) {
            is InsertData -> updatePlanner.executeInsert(data, transaction)
            is DeleteData -> updatePlanner.executeDelete(data, transaction)
            is ModifyData -> updatePlanner.executeModify(data, transaction)
            is CreateTableData -> updatePlanner.executeCreateTable(data, transaction)
            is CreateViewData -> updatePlanner.executeCreateView(data, transaction)
            is CreateIndexData -> updatePlanner.executeCreateIndex(data, transaction)
            else -> 0
        }
    }

    // Kura does not verify queries, although it should.
    private fun verifyQuery(data: QueryData) {
        // 実装しない
    }

    // Kura does not verify updates, although it should.
    private fun verifyUpdate(data: Any) {
        // 実装しない
    }
}