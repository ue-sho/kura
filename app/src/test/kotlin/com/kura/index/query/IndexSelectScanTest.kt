package com.kura.index.query

import com.kura.buffer.BufferManager
import com.kura.file.FileManager
import com.kura.index.hash.HashIndex
import com.kura.log.LogManager
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordId
import com.kura.record.Schema
import com.kura.record.TableScan
import com.kura.transaction.Transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class IndexSelectScanTest {
    @TempDir
    lateinit var tempDir: File

    private lateinit var fm: FileManager
    private lateinit var lm: LogManager
    private lateinit var bm: BufferManager
    private val blockSize = 400

    @BeforeEach
    fun setUp() {
        fm = FileManager(tempDir, blockSize)
        lm = LogManager(fm, "testlog")
        bm = BufferManager(fm, lm, 8)
    }

    private fun createTableSchema(): Schema {
        val schema = Schema()
        schema.addIntField("id")
        schema.addStringField("name", 10)
        schema.addIntField("score")
        return schema
    }

    private fun createIndexLayout(): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")
        schema.addIntField("dataval")
        return Layout(schema)
    }

    @Test
    fun `should retrieve records matching search key via index`() {
        val tx = Transaction(fm, lm, bm)
        val tableSchema = createTableSchema()
        val tableLayout = Layout(tableSchema)
        val indexLayout = createIndexLayout()

        // Insert data into the table
        val ts = TableScan(tx, "testdata", tableLayout)
        ts.insert(); ts.setInt("id", 1); ts.setString("name", "alice"); ts.setInt("score", 90)
        val rid1 = ts.getRecordId()
        ts.insert(); ts.setInt("id", 2); ts.setString("name", "bob"); ts.setInt("score", 80)
        ts.insert(); ts.setInt("id", 3); ts.setString("name", "carol"); ts.setInt("score", 90)
        val rid3 = ts.getRecordId()
        ts.close()

        // Build index on "score" field
        val idx = HashIndex(tx, "scoreidx", indexLayout)
        idx.insert(Constant(90), rid1)
        idx.insert(Constant(80), RecordId(rid1.blockNumber(), rid1.slot() + 1)) // approximate rid2
        idx.insert(Constant(90), rid3)
        idx.close()

        // Use IndexSelectScan to find records with score=90
        val ts2 = TableScan(tx, "testdata", tableLayout)
        val idx2 = HashIndex(tx, "scoreidx", indexLayout)
        val scan = IndexSelectScan(ts2, idx2, Constant(90))

        val names = mutableListOf<String>()
        while (scan.next()) {
            assertEquals(90, scan.getInt("score"))
            names.add(scan.getString("name"))
        }
        assertEquals(2, names.size)
        scan.close()
        tx.commit()
    }

    @Test
    fun `should return no results for non-matching key`() {
        val tx = Transaction(fm, lm, bm)
        val tableSchema = createTableSchema()
        val tableLayout = Layout(tableSchema)
        val indexLayout = createIndexLayout()

        // Insert data into the table
        val ts = TableScan(tx, "testdata2", tableLayout)
        ts.insert(); ts.setInt("id", 1); ts.setString("name", "alice"); ts.setInt("score", 90)
        val rid1 = ts.getRecordId()
        ts.close()

        // Build index
        val idx = HashIndex(tx, "scoreidx2", indexLayout)
        idx.insert(Constant(90), rid1)
        idx.close()

        // Search for non-existent key
        val ts2 = TableScan(tx, "testdata2", tableLayout)
        val idx2 = HashIndex(tx, "scoreidx2", indexLayout)
        val scan = IndexSelectScan(ts2, idx2, Constant(999))

        assertFalse(scan.next())
        scan.close()
        tx.commit()
    }

    @Test
    fun `should support hasField`() {
        val tx = Transaction(fm, lm, bm)
        val tableSchema = createTableSchema()
        val tableLayout = Layout(tableSchema)
        val indexLayout = createIndexLayout()

        val ts = TableScan(tx, "testdata3", tableLayout)
        ts.insert(); ts.setInt("id", 1); ts.setString("name", "alice"); ts.setInt("score", 90)
        val rid = ts.getRecordId()
        ts.close()

        val idx = HashIndex(tx, "scoreidx3", indexLayout)
        idx.insert(Constant(90), rid)
        idx.close()

        val ts2 = TableScan(tx, "testdata3", tableLayout)
        val idx2 = HashIndex(tx, "scoreidx3", indexLayout)
        val scan = IndexSelectScan(ts2, idx2, Constant(90))

        assertTrue(scan.hasField("id"))
        assertTrue(scan.hasField("name"))
        assertTrue(scan.hasField("score"))
        assertFalse(scan.hasField("nonexistent"))

        scan.close()
        tx.commit()
    }
}
