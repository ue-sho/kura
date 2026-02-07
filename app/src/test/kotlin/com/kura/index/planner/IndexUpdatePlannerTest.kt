package com.kura.index.planner

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.metadata.MetadataManager
import com.kura.parse.CreateIndexData
import com.kura.parse.CreateTableData
import com.kura.parse.DeleteData
import com.kura.parse.InsertData
import com.kura.parse.ModifyData
import com.kura.query.Constant
import com.kura.query.Expression
import com.kura.query.Predicate
import com.kura.query.Term
import com.kura.record.Layout
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class IndexUpdatePlannerTest {
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

        // Initialize metadata with first transaction
        val initTx = Transaction(fm, lm, bm)
        mdm = MetadataManager(true, initTx)
        initTx.commit()
    }

    private fun newTx(): Transaction = Transaction(fm, lm, bm)

    @Test
    fun `executeInsert should insert record and update index`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        // Create table
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        planner.executeCreateTable(CreateTableData("student", schema), tx)

        // Create index on "id"
        planner.executeCreateIndex(CreateIndexData("idx_id", "student", "id"), tx)

        // Insert a record
        val fields = listOf("id", "name")
        val values = listOf(Constant(1), Constant("alice"))
        val count = planner.executeInsert(InsertData("student", fields, values), tx)
        assertEquals(1, count)

        // Verify the record exists in the table
        val layout = mdm.getLayout("student", tx)
        val ts = TableScan(tx, "student", layout)
        assertTrue(ts.next())
        assertEquals(1, ts.getInt("id"))
        assertEquals("alice", ts.getString("name"))
        assertFalse(ts.next())
        ts.close()

        // Verify the index entry exists
        val indexes = mdm.getIndexInfo("student", tx)
        val idx = indexes["id"]!!.open()
        idx.beforeFirst(Constant(1))
        assertTrue(idx.next())
        idx.close()

        tx.commit()
    }

    @Test
    fun `executeDelete should delete record and remove index entry`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        // Create table and index
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        planner.executeCreateTable(CreateTableData("student2", schema), tx)
        planner.executeCreateIndex(CreateIndexData("idx_id2", "student2", "id"), tx)

        // Insert two records
        planner.executeInsert(InsertData("student2", listOf("id", "name"), listOf(Constant(1), Constant("alice"))), tx)
        planner.executeInsert(InsertData("student2", listOf("id", "name"), listOf(Constant(2), Constant("bob"))), tx)

        // Delete where id = 1
        val pred = Predicate(Term(Expression("id"), Expression(Constant(1))))
        val deleteCount = planner.executeDelete(DeleteData("student2", pred), tx)
        assertEquals(1, deleteCount)

        // Verify only bob remains
        val layout = mdm.getLayout("student2", tx)
        val ts = TableScan(tx, "student2", layout)
        assertTrue(ts.next())
        assertEquals("bob", ts.getString("name"))
        assertFalse(ts.next())
        ts.close()

        // Verify index entry for id=1 is removed
        val idx = mdm.getIndexInfo("student2", tx)["id"]!!.open()
        idx.beforeFirst(Constant(1))
        assertFalse(idx.next())
        idx.close()

        tx.commit()
    }

    @Test
    fun `executeModify should update record and index`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        // Create table and index
        val schema = Schema()
        schema.addIntField("id")
        schema.addIntField("score")
        planner.executeCreateTable(CreateTableData("grades", schema), tx)
        planner.executeCreateIndex(CreateIndexData("idx_score", "grades", "score"), tx)

        // Insert a record
        planner.executeInsert(InsertData("grades", listOf("id", "score"), listOf(Constant(1), Constant(80))), tx)

        // Modify score to 95 where id = 1
        val pred = Predicate(Term(Expression("id"), Expression(Constant(1))))
        val modifyData = ModifyData("grades", "score", Expression(Constant(95)), pred)
        val count = planner.executeModify(modifyData, tx)
        assertEquals(1, count)

        // Verify record updated
        val layout = mdm.getLayout("grades", tx)
        val ts = TableScan(tx, "grades", layout)
        assertTrue(ts.next())
        assertEquals(95, ts.getInt("score"))
        ts.close()

        // Verify old index entry removed and new one exists
        val idx = mdm.getIndexInfo("grades", tx)["score"]!!.open()
        idx.beforeFirst(Constant(80))
        assertFalse(idx.next()) // old value gone
        idx.close()

        val idx2 = mdm.getIndexInfo("grades", tx)["score"]!!.open()
        idx2.beforeFirst(Constant(95))
        assertTrue(idx2.next()) // new value exists
        idx2.close()

        tx.commit()
    }

    @Test
    fun `executeCreateTable should create table`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        val schema = Schema()
        schema.addIntField("x")
        val count = planner.executeCreateTable(CreateTableData("newtable", schema), tx)
        assertEquals(0, count)

        // Verify table exists by getting layout
        val layout = mdm.getLayout("newtable", tx)
        assertTrue(layout.schema().hasField("x"))

        tx.commit()
    }

    @Test
    fun `executeCreateIndex should create index`() {
        val tx = newTx()
        val planner = IndexUpdatePlanner(mdm)

        val schema = Schema()
        schema.addIntField("a")
        planner.executeCreateTable(CreateTableData("idxtable", schema), tx)

        val count = planner.executeCreateIndex(CreateIndexData("myidx", "idxtable", "a"), tx)
        assertEquals(0, count)

        // Verify index exists
        val indexes = mdm.getIndexInfo("idxtable", tx)
        assertTrue(indexes.containsKey("a"))

        tx.commit()
    }
}
