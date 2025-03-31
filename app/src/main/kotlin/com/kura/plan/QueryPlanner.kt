package com.kura.plan

import com.kura.parse.QueryData
import com.kura.transaction.Transaction

/**
 * The interface implemented by planners for
 * the SQL select statement.
 */
interface QueryPlanner {
    /**
     * Creates a plan for the parsed query.
     * @param data the parsed representation of the query
     * @param transaction the calling transaction
     * @return a plan for that query
     */
    fun createPlan(data: QueryData, transaction: Transaction): Plan
}