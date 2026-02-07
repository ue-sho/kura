package com.kura.index.btree

import com.kura.buffer.BufferManager
import com.kura.file.BlockId
import com.kura.file.FileManager
import com.kura.log.LogManager
import com.kura.query.Constant
import com.kura.record.Layout
import com.kura.record.RecordId
import com.kura.record.Schema
import com.kura.transaction.Transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.sql.Types

class BTPageTest {
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

    private fun createLeafLayout(): Layout {
        val schema = Schema()
        schema.addIntField("block")
        schema.addIntField("id")
        schema.addIntField("dataval")
        return Layout(schema)
    }

    @Test
    fun `should format and read empty page`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val block = tx.append("testbtpage")
        val page = BTPage(tx, block, layout)
        page.format(block, 0)

        assertEquals(0, page.getFlag())
        assertEquals(0, page.getNumRecords())

        page.close()
        tx.commit()
    }

    @Test
    fun `should insert and read leaf records`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val block = tx.append("testbtpage")
        val page = BTPage(tx, block, layout)
        page.format(block, 0)

        // Insert records in sorted order
        page.insertLeaf(0, Constant(10), RecordId(1, 0))
        page.insertLeaf(1, Constant(20), RecordId(2, 1))
        page.insertLeaf(2, Constant(30), RecordId(3, 2))

        assertEquals(3, page.getNumRecords())
        assertEquals(Constant(10), page.getDataVal(0))
        assertEquals(Constant(20), page.getDataVal(1))
        assertEquals(Constant(30), page.getDataVal(2))

        val rid = page.getDataRecordId(1)
        assertEquals(2, rid.blockNumber())
        assertEquals(1, rid.slot())

        page.close()
        tx.commit()
    }

    @Test
    fun `should find slot before search key`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val block = tx.append("testbtpage")
        val page = BTPage(tx, block, layout)
        page.format(block, 0)

        page.insertLeaf(0, Constant(10), RecordId(1, 0))
        page.insertLeaf(1, Constant(20), RecordId(2, 0))
        page.insertLeaf(2, Constant(30), RecordId(3, 0))

        // Key 15 is between 10 and 20, so slot before is 0
        assertEquals(0, page.findSlotBefore(Constant(15)))
        // Key 10 matches slot 0, so slot before is -1
        assertEquals(-1, page.findSlotBefore(Constant(10)))
        // Key 25 is between 20 and 30, so slot before is 1
        assertEquals(1, page.findSlotBefore(Constant(25)))
        // Key 35 is after 30, so slot before is 2
        assertEquals(2, page.findSlotBefore(Constant(35)))

        page.close()
        tx.commit()
    }

    @Test
    fun `should delete record and shift remaining`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val block = tx.append("testbtpage")
        val page = BTPage(tx, block, layout)
        page.format(block, 0)

        page.insertLeaf(0, Constant(10), RecordId(1, 0))
        page.insertLeaf(1, Constant(20), RecordId(2, 0))
        page.insertLeaf(2, Constant(30), RecordId(3, 0))

        page.delete(1)

        assertEquals(2, page.getNumRecords())
        assertEquals(Constant(10), page.getDataVal(0))
        assertEquals(Constant(30), page.getDataVal(1))

        page.close()
        tx.commit()
    }

    @Test
    fun `should split page`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val block = tx.append("testbtpage")
        val page = BTPage(tx, block, layout)
        page.format(block, 0)

        page.insertLeaf(0, Constant(10), RecordId(1, 0))
        page.insertLeaf(1, Constant(20), RecordId(2, 0))
        page.insertLeaf(2, Constant(30), RecordId(3, 0))
        page.insertLeaf(3, Constant(40), RecordId(4, 0))

        // Split at position 2: records at positions 2,3 go to new page
        val newBlock = page.split(2, 0)
        assertEquals(2, page.getNumRecords())
        assertEquals(Constant(10), page.getDataVal(0))
        assertEquals(Constant(20), page.getDataVal(1))

        // Verify new page
        val newPage = BTPage(tx, newBlock, layout)
        assertEquals(2, newPage.getNumRecords())
        assertEquals(Constant(30), newPage.getDataVal(0))
        assertEquals(Constant(40), newPage.getDataVal(1))

        newPage.close()
        page.close()
        tx.commit()
    }

    @Test
    fun `should set and get flag`() {
        val tx = Transaction(fm, lm, bm)
        val layout = createLeafLayout()
        val block = tx.append("testbtpage")
        val page = BTPage(tx, block, layout)
        page.format(block, 0)

        assertEquals(0, page.getFlag())

        page.setFlag(5)
        assertEquals(5, page.getFlag())

        page.close()
        tx.commit()
    }

    @Test
    fun `should handle directory records`() {
        val tx = Transaction(fm, lm, bm)
        val dirSchema = Schema()
        dirSchema.addIntField("block")
        dirSchema.addIntField("dataval")
        val layout = Layout(dirSchema)

        val block = tx.append("testbtdir")
        val page = BTPage(tx, block, layout)
        page.format(block, 1) // flag=1 means directory level 1

        page.insertDir(0, Constant(10), 5)
        page.insertDir(1, Constant(20), 8)

        assertEquals(1, page.getFlag())
        assertEquals(2, page.getNumRecords())
        assertEquals(5, page.getChildNum(0))
        assertEquals(8, page.getChildNum(1))
        assertEquals(Constant(10), page.getDataVal(0))
        assertEquals(Constant(20), page.getDataVal(1))

        page.close()
        tx.commit()
    }
}
