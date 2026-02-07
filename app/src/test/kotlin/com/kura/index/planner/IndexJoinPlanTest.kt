package com.kura.index.planner

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.metadata.MetadataManager
import com.kura.parse.CreateIndexData
import com.kura.parse.CreateTableData
import com.kura.parse.InsertData
import com.kura.plan.TablePlan
import com.kura.query.Constant
import com.kura.record.Schema
import com.kura.transaction.Transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class IndexJoinPlanTest {
    @TempDir
    lateinit var tempDir: File

    private lateinit var fm: FileManager
    private lateinit var lm: LogManager
    private lateinit var bm: BufferManager
    private lateinit var mdm: MetadataManager
    private val blockSize = 400

    @BeforeEach
    fun setUp() {
        fm = FileManager(tempDir, blockSize)
        lm = LogManager(fm, "testlog")
        bm = BufferManager(fm, lm, 8)

        val initTx = Transaction(fm, lm, bm)
        mdm = MetadataManager(true, initTx)
        initTx.commit()
    }

    private fun newTx(): Transaction = Transaction(fm, lm, bm)

    @Test
    fun `should join two tables via index`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        // Create "dept" table (LHS)
        val deptSchema = Schema()
        deptSchema.addIntField("did")
        deptSchema.addStringField("dname", 10)
        planner.executeCreateTable(CreateTableData("dept", deptSchema), tx)

        // Create "emp" table (RHS)
        val empSchema = Schema()
        empSchema.addIntField("eid")
        empSchema.addStringField("ename", 10)
        empSchema.addIntField("deptid")
        planner.executeCreateTable(CreateTableData("emp", empSchema), tx)

        // Create index on emp.deptid
        planner.executeCreateIndex(CreateIndexData("idx_deptid", "emp", "deptid"), tx)

        // Insert departments
        planner.executeInsert(InsertData("dept", listOf("did", "dname"), listOf(Constant(1), Constant("eng"))), tx)
        planner.executeInsert(InsertData("dept", listOf("did", "dname"), listOf(Constant(2), Constant("sales"))), tx)

        // Insert employees
        planner.executeInsert(InsertData("emp", listOf("eid", "ename", "deptid"), listOf(Constant(10), Constant("alice"), Constant(1))), tx)
        planner.executeInsert(InsertData("emp", listOf("eid", "ename", "deptid"), listOf(Constant(20), Constant("bob"), Constant(1))), tx)
        planner.executeInsert(InsertData("emp", listOf("eid", "ename", "deptid"), listOf(Constant(30), Constant("carol"), Constant(2))), tx)

        // IndexJoinPlan: dept JOIN emp ON dept.did = emp.deptid
        val deptPlan = TablePlan(tx, "dept", mdm)
        val empPlan = TablePlan(tx, "emp", mdm)
        val indexInfo = mdm.getIndexInfo("emp", tx)["deptid"]!!
        val joinPlan = IndexJoinPlan(deptPlan, empPlan, indexInfo, "did")

        // Verify schema contains fields from both tables
        val schema = joinPlan.schema()
        assertTrue(schema.hasField("did"))
        assertTrue(schema.hasField("dname"))
        assertTrue(schema.hasField("eid"))
        assertTrue(schema.hasField("ename"))
        assertTrue(schema.hasField("deptid"))

        // Execute scan and collect results
        val scan = joinPlan.open()
        val results = mutableListOf<String>()
        while (scan.next()) {
            val dname = scan.getString("dname")
            val ename = scan.getString("ename")
            results.add("$dname:$ename")
        }
        scan.close()

        // eng has 2 employees, sales has 1
        assertEquals(3, results.size)
        assertEquals(2, results.count { it.startsWith("eng:") })
        assertEquals(1, results.count { it.startsWith("sales:") })

        tx.commit()
    }

    @Test
    fun `should return empty result when no matches`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        // Create LHS table
        val lhsSchema = Schema()
        lhsSchema.addIntField("key")
        planner.executeCreateTable(CreateTableData("lhs", lhsSchema), tx)

        // Create RHS table with index
        val rhsSchema = Schema()
        rhsSchema.addIntField("fk")
        rhsSchema.addStringField("val", 5)
        planner.executeCreateTable(CreateTableData("rhs", rhsSchema), tx)
        planner.executeCreateIndex(CreateIndexData("idx_fk", "rhs", "fk"), tx)

        // Insert LHS record with key=99, but no matching RHS
        planner.executeInsert(InsertData("lhs", listOf("key"), listOf(Constant(99))), tx)
        planner.executeInsert(InsertData("rhs", listOf("fk", "val"), listOf(Constant(1), Constant("x"))), tx)

        val lhsPlan = TablePlan(tx, "lhs", mdm)
        val rhsPlan = TablePlan(tx, "rhs", mdm)
        val indexInfo = mdm.getIndexInfo("rhs", tx)["fk"]!!
        val joinPlan = IndexJoinPlan(lhsPlan, rhsPlan, indexInfo, "key")

        val scan = joinPlan.open()
        assertFalse(scan.next())
        scan.close()

        tx.commit()
    }

    @Test
    fun `distinctValues should delegate to correct plan`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        val s1 = Schema()
        s1.addIntField("a")
        planner.executeCreateTable(CreateTableData("t1", s1), tx)

        val s2 = Schema()
        s2.addIntField("b")
        planner.executeCreateTable(CreateTableData("t2", s2), tx)
        planner.executeCreateIndex(CreateIndexData("idx_b", "t2", "b"), tx)

        val p1 = TablePlan(tx, "t1", mdm)
        val p2 = TablePlan(tx, "t2", mdm)
        val indexInfo = mdm.getIndexInfo("t2", tx)["b"]!!
        val joinPlan = IndexJoinPlan(p1, p2, indexInfo, "a")

        // distinctValues for field in plan1 should come from plan1
        // distinctValues for field in plan2 should come from plan2
        assertTrue(joinPlan.distinctValues("a") >= 0)
        assertTrue(joinPlan.distinctValues("b") >= 0)

        tx.commit()
    }
}
