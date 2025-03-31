package com.kura.plan

import com.kura.server.KuraDB

/**
 * Test for the Planner class with student database.
 */
class PlannerStudentTest {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val db = KuraDB("studentdb")
            val planner = db.planner()
            val transaction = db.newTransaction()
            if (planner == null) {
                throw RuntimeException("Planner is null")
            }

            // part 1: Process a query
            val qry = "select sname, gradyear from student"
            val plan = planner.createQueryPlan(qry, transaction)
            val scan = plan.open()
            while (scan.next()) {
                println("${scan.getString("sname")} ${scan.getInt("gradyear")}")
            }
            scan.close()

            // part 2: Process an update command
            val cmd = "delete from STUDENT where MajorId = 30"
            val num = planner.executeUpdate(cmd, transaction)
            println("$num students were deleted")

            transaction.commit()
        }
    }
}