package com.kura.query

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import io.mockk.mockk
import io.mockk.every
import io.mockk.verify

class ScanTest2 {

    private lateinit var selectScan: SelectScan
    private lateinit var projectScan: ProjectScan

    @BeforeEach
    fun setUp() {
        // Create a mock for SelectScan that already contains filtered data
        selectScan = mockk(relaxed = true)

        // Set up the select scan to return filtered records
        val selectNextResponses = listOf(true, true, true, false)
        var selectNextIndex = 0
        every { selectScan.next() } answers {
            if (selectNextIndex < selectNextResponses.size)
                selectNextResponses[selectNextIndex++]
            else false
        }

        // Set up field values for the expected results
        every { selectScan.hasField("B") } returns true
        every { selectScan.hasField("D") } returns true

        // These values represent the joined records where A=C
        val bValues = listOf("bbb1", "bbb2", "bbb3")
        var bIndex = 0
        every { selectScan.getString("B") } answers { bValues[bIndex++ % bValues.size] }

        val dValues = listOf("ddd1", "ddd2", "ddd3")
        var dIndex = 0
        every { selectScan.getString("D") } answers { dValues[dIndex++ % dValues.size] }
    }

    @Test
    fun `should perform complex scan operations with join filter and projection`() {
        // Arrange
        // Create ProjectScan over the already-filtered data
        val columns = listOf("B", "D")
        projectScan = ProjectScan(selectScan, columns)

        // Act & Assert
        // We expect to find 3 matching records (where A=C)

        // Record 1: A=1, C=1
        assertTrue(projectScan.next())
        assertEquals("bbb1", projectScan.getString("B"))
        assertEquals("ddd1", projectScan.getString("D"))

        // Record 2: A=2, C=2
        assertTrue(projectScan.next())
        assertEquals("bbb2", projectScan.getString("B"))
        assertEquals("ddd2", projectScan.getString("D"))

        // Record 3: A=3, C=3
        assertTrue(projectScan.next())
        assertEquals("bbb3", projectScan.getString("B"))
        assertEquals("ddd3", projectScan.getString("D"))

        // No more matching records
        assertEquals(false, projectScan.next())
    }
}