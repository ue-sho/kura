package com.kura.opt

import com.kura.metadata.MetadataManager
import com.kura.parse.QueryData
import com.kura.plan.Plan
import com.kura.plan.ProjectPlan
import com.kura.plan.QueryPlanner
import com.kura.transaction.Transaction

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 */
class HeuristicQueryPlanner(
    private val metadataManager: MetadataManager
) : QueryPlanner {

    private val tablePlanners: MutableCollection<TablePlanner> = mutableListOf()

    /**
     * Creates an optimized left-deep query plan using the following
     * heuristics.
     * H1. Choose the smallest table (considering selection predicates)
     * to be first in the join order.
     * H2. Add the table to the join order which
     * results in the smallest output.
     */
    override fun createPlan(data: QueryData, transaction: Transaction): Plan {
        // Step 1: Create a TablePlanner object for each mentioned table
        tablePlanners.clear()
        for (tableName in data.tables()) {
            val tp = TablePlanner(tableName, data.pred(), transaction, metadataManager)
            tablePlanners.add(tp)
        }

        // Step 2: Choose the lowest-size plan to begin the join order
        var currentPlan = getLowestSelectPlan()

        // Step 3: Repeatedly add a plan to the join order
        while (tablePlanners.isNotEmpty()) {
            val p = getLowestJoinPlan(currentPlan)
            currentPlan = p ?: getLowestProductPlan(currentPlan)
        }

        // Step 4: Project on the field names and return
        return ProjectPlan(currentPlan, data.fields())
    }

    private fun getLowestSelectPlan(): Plan {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tp in tablePlanners) {
            val plan = tp.makeSelectPlan()
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTablePlanner = tp
                bestPlan = plan
            }
        }
        tablePlanners.remove(bestTablePlanner)
        return bestPlan!!
    }

    private fun getLowestJoinPlan(current: Plan): Plan? {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tp in tablePlanners) {
            val plan = tp.makeJoinPlan(current)
            if (plan != null && (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput())) {
                bestTablePlanner = tp
                bestPlan = plan
            }
        }
        if (bestPlan != null) {
            tablePlanners.remove(bestTablePlanner)
        }
        return bestPlan
    }

    private fun getLowestProductPlan(current: Plan): Plan {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tp in tablePlanners) {
            val plan = tp.makeProductPlan(current)
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTablePlanner = tp
                bestPlan = plan
            }
        }
        tablePlanners.remove(bestTablePlanner)
        return bestPlan!!
    }
}
