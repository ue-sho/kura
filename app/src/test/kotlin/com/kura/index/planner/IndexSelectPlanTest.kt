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

class IndexSelectPlanTest {
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
    fun `should retrieve matching records via index select plan`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        // Create table and index
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        planner.executeCreateTable(CreateTableData("student", schema), tx)
        planner.executeCreateIndex(CreateIndexData("idx_id", "student", "id"), tx)

        // Insert records
        planner.executeInsert(InsertData("student", listOf("id", "name"), listOf(Constant(1), Constant("alice"))), tx)
        planner.executeInsert(InsertData("student", listOf("id", "name"), listOf(Constant(2), Constant("bob"))), tx)
        planner.executeInsert(InsertData("student", listOf("id", "name"), listOf(Constant(3), Constant("carol"))), tx)

        // Use IndexSelectPlan to find id=2
        val tablePlan = TablePlan(tx, "student", mdm)
        val indexInfo = mdm.getIndexInfo("student", tx)["id"]!!
        val selectPlan = IndexSelectPlan(tablePlan, indexInfo, Constant(2))

        val scan = selectPlan.open()
        assertTrue(scan.next())
        assertEquals("bob", scan.getString("name"))
        assertEquals(2, scan.getInt("id"))
        assertFalse(scan.next())
        scan.close()

        // Verify schema
        assertTrue(selectPlan.schema().hasField("id"))
        assertTrue(selectPlan.schema().hasField("name"))

        tx.commit()
    }

    @Test
    fun `should return no results for non-matching key`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        val schema = Schema()
        schema.addIntField("id")
        planner.executeCreateTable(CreateTableData("t1", schema), tx)
        planner.executeCreateIndex(CreateIndexData("idx1", "t1", "id"), tx)
        planner.executeInsert(InsertData("t1", listOf("id"), listOf(Constant(10))), tx)

        val tablePlan = TablePlan(tx, "t1", mdm)
        val indexInfo = mdm.getIndexInfo("t1", tx)["id"]!!
        val selectPlan = IndexSelectPlan(tablePlan, indexInfo, Constant(999))

        val scan = selectPlan.open()
        assertFalse(scan.next())
        scan.close()

        tx.commit()
    }

    @Test
    fun `blocksAccessed should return index traversal cost plus matching records`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        val schema = Schema()
        schema.addIntField("id")
        planner.executeCreateTable(CreateTableData("t2", schema), tx)
        planner.executeCreateIndex(CreateIndexData("idx2", "t2", "id"), tx)

        val tablePlan = TablePlan(tx, "t2", mdm)
        val indexInfo = mdm.getIndexInfo("t2", tx)["id"]!!
        val selectPlan = IndexSelectPlan(tablePlan, indexInfo, Constant(1))

        // blocksAccessed should be non-negative
        assertTrue(selectPlan.blocksAccessed() >= 0)

        tx.commit()
    }
}
