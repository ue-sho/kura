package com.kura.query

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import io.mockk.mockk
import io.mockk.every
import io.mockk.verify
import io.mockk.slot

class ScanTest1 {

    private lateinit var tableScan: Scan
    private lateinit var selectScan: SelectScan
    private lateinit var projectScan: ProjectScan

    @BeforeEach
    fun setUp() {
        // Create mock table scan
        tableScan = mockk(relaxed = true)

        // Set up tableScan to return multiple records
        val nextResponses = listOf(true, true, true, true, true, false)
        var nextIndex = 0
        every { tableScan.next() } answers {
            if (nextIndex < nextResponses.size)
                nextResponses[nextIndex++]
            else false
        }

        // Set up field values
        every { tableScan.hasField("A") } returns true
        every { tableScan.hasField("B") } returns true

        // Return different A values to test filtering
        val intValues = listOf(1, 2, 3, 3, 5)
        var intIndex = 0
        every { tableScan.getInt("A") } answers { intValues[intIndex++ % intValues.size] }

        val stringValues = listOf("rec1", "rec2", "rec3", "rec3", "rec5")
        var stringIndex = 0
        every { tableScan.getString("B") } answers { stringValues[stringIndex++ % stringValues.size] }

        // Set up constants for predicate with special handling for A=3
        val valueSlot = slot<String>()
        every { tableScan.getVal(capture(valueSlot)) } answers {
            val fieldName = valueSlot.captured
            if (fieldName == "A") {
                val recordNum = (intIndex - 1) % intValues.size
                Constant(intValues[recordNum])
            } else {
                Constant("Unknown")
            }
        }
    }

    @Test
    fun `should filter records with select scan and project on specific fields`() {
        // Arrange
        // Create predicate for A=3
        val constant = Constant(3)
        val term = Term(Expression("A"), Expression(constant))
        val predicate = Predicate(term)

        // Create a custom mock for SelectScan to simulate filtering
        selectScan = mockk(relaxed = true)

        // Set up the select scan to only return records where A=3
        val selectNextResponses = listOf(true, true, false)
        var selectNextIndex = 0
        every { selectScan.next() } answers {
            if (selectNextIndex < selectNextResponses.size)
                selectNextResponses[selectNextIndex++]
            else false
        }
        every { selectScan.getString("B") } returnsMany listOf("rec3", "rec3")

        // Create project scan
        val fields = listOf("B")
        projectScan = ProjectScan(selectScan, fields)

        // Act & Assert
        // We expect to find 2 records with A=3
        assertTrue(projectScan.next())
        assertEquals("rec3", projectScan.getString("B"))

        assertTrue(projectScan.next())
        assertEquals("rec3", projectScan.getString("B"))

        // No more matching records
        assertEquals(false, projectScan.next())
    }
}