package com.kura.plan

import com.kura.metadata.MetadataManager
import com.kura.parse.Parser
import com.kura.parse.QueryData
import com.kura.transaction.Transaction

/**
 * A small improvement on the basic query planner.
 * When forming products, it tries both orderings and
 * chooses the one having the lowest cost.
 */
class BetterQueryPlanner(private val metadataManager: MetadataManager) : QueryPlanner {

    /**
     * Creates a query plan as follows. It first takes
     * the product of all tables and views; it then selects on the predicate;
     * and finally it projects on the field list.
     */
    override fun createPlan(data: QueryData, transaction: Transaction): Plan {
        // Step 1: Create a plan for each mentioned table or view.
        val plans = mutableListOf<Plan>()
        for (tableName in data.tables()) {
            val viewDefinition = metadataManager.getViewDefinition(tableName, transaction)
            if (viewDefinition != null) {
                val parser = Parser(viewDefinition)
                val viewData = parser.query()
                plans.add(createPlan(viewData, transaction))
            } else {
                plans.add(TablePlan(transaction, tableName, metadataManager))
            }
        }

        // Step 2: Create the product of all table plans
        // Try both orderings and choose the one having lowest cost
        var plan = plans.removeAt(0)
        for (nextPlan in plans) {
            val choice1 = ProductPlan(nextPlan, plan)
            val choice2 = ProductPlan(plan, nextPlan)
            plan = if (choice1.blocksAccessed() < choice2.blocksAccessed()) choice1 else choice2
        }

        // Step 3: Add a selection plan for the predicate
        plan = SelectPlan(plan, data.pred())

        // Step 4: Project on the field names
        plan = ProjectPlan(plan, data.fields())
        return plan
    }
}
