package com.kura.plan

import com.kura.query.Constant
import com.kura.query.Expression
import com.kura.query.Predicate
import com.kura.query.Term
import com.kura.server.KuraDB

/**
 * Test for single table plans.
 */
class SingleTablePlanTest {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val db = KuraDB("studentdb")
            val metadataManager = db.metadataManager()
            val transaction = db.newTransaction()
            if (metadataManager == null) {
                throw RuntimeException("Metadata manager is null")
            }

            // the STUDENT node
            val plan1 = TablePlan(transaction, "student", metadataManager)

            // the Select node for "majorid = 10"
            val term = Term(Expression("majorid"), Expression(Constant(10)))
            val predicate = Predicate(term)
            val plan2 = SelectPlan(plan1, predicate)

            // the Select node for "gradyear = 2020"
            val term2 = Term(Expression("gradyear"), Expression(Constant(2020)))
            val predicate2 = Predicate(term2)
            val plan3 = SelectPlan(plan2, predicate2)

            // the Project node
            val columns = listOf("sname", "majorid", "gradyear")
            val plan4 = ProjectPlan(plan3, columns)

            // Look at R(p) and B(p) for each plan p.
            printStats(1, plan1)
            printStats(2, plan2)
            printStats(3, plan3)
            printStats(4, plan4)

            // Change plan2 to be plan2, plan3, or plan4 to see the other scans in action.
            // Changing plan2 to plan4 will throw an exception because SID is not in the projection list.
            val scan = plan2.open()
            while (scan.next()) {
                println("${scan.getInt("sid")} ${scan.getString("sname")} " +
                        "${scan.getInt("majorid")} ${scan.getInt("gradyear")}")
            }
            scan.close()
        }

        private fun printStats(n: Int, plan: Plan) {
            println("Here are the stats for plan p$n")
            println("\tR(p$n): ${plan.recordsOutput()}")
            println("\tB(p$n): ${plan.blocksAccessed()}")
            println()
        }
    }
}