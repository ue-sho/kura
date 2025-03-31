package com.kura.plan

import com.kura.parse.CreateIndexData
import com.kura.parse.CreateTableData
import com.kura.parse.CreateViewData
import com.kura.parse.DeleteData
import com.kura.parse.InsertData
import com.kura.parse.ModifyData
import com.kura.parse.Parser
import com.kura.parse.QueryData
import com.kura.transaction.Transaction
import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class PlannerTest {
    private lateinit var queryPlanner: QueryPlanner
    private lateinit var updatePlanner: UpdatePlanner
    private lateinit var transaction: Transaction
    private lateinit var planner: Planner

    @BeforeEach
    fun setUp() {
        // Arrange
        queryPlanner = mockk<QueryPlanner>(relaxed = true)
        updatePlanner = mockk<UpdatePlanner>(relaxed = true)
        transaction = mockk<Transaction>(relaxed = true)

        planner = Planner(queryPlanner, updatePlanner)
    }

    @Test
    fun `createQueryPlan should delegate to queryPlanner`() {
        // Arrange
        val query = "SELECT * FROM test_table"
        val queryData = mockk<QueryData>()
        val plan = mockk<Plan>()

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().query() } returns queryData
        every { queryPlanner.createPlan(queryData, transaction) } returns plan

        // Act
        val result = planner.createQueryPlan(query, transaction)

        // Assert
        assertEquals(plan, result)
        verify { queryPlanner.createPlan(queryData, transaction) }

        unmockkConstructor(Parser::class)
    }

    @Test
    fun `executeUpdate should delegate to updatePlanner for INSERT statement`() {
        // Arrange
        val updateCmd = "INSERT INTO test_table VALUES (1, 'test')"
        val insertData = mockk<InsertData>()
        val expectedRowsAffected = 1

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().updateCommand() } returns insertData
        every { updatePlanner.executeInsert(insertData, transaction) } returns expectedRowsAffected

        // Act
        val result = planner.executeUpdate(updateCmd, transaction)

        // Assert
        assertEquals(expectedRowsAffected, result)
        verify { updatePlanner.executeInsert(insertData, transaction) }

        unmockkConstructor(Parser::class)
    }

    @Test
    fun `executeUpdate should delegate to updatePlanner for DELETE statement`() {
        // Arrange
        val updateCmd = "DELETE FROM test_table WHERE id = 1"
        val deleteData = mockk<DeleteData>()
        val expectedRowsAffected = 1

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().updateCommand() } returns deleteData
        every { updatePlanner.executeDelete(deleteData, transaction) } returns expectedRowsAffected

        // Act
        val result = planner.executeUpdate(updateCmd, transaction)

        // Assert
        assertEquals(expectedRowsAffected, result)
        verify { updatePlanner.executeDelete(deleteData, transaction) }

        unmockkConstructor(Parser::class)
    }

    @Test
    fun `executeUpdate should delegate to updatePlanner for UPDATE statement`() {
        // Arrange
        val updateCmd = "UPDATE test_table SET name = 'new_name' WHERE id = 1"
        val modifyData = mockk<ModifyData>()
        val expectedRowsAffected = 1

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().updateCommand() } returns modifyData
        every { updatePlanner.executeModify(modifyData, transaction) } returns expectedRowsAffected

        // Act
        val result = planner.executeUpdate(updateCmd, transaction)

        // Assert
        assertEquals(expectedRowsAffected, result)
        verify { updatePlanner.executeModify(modifyData, transaction) }

        unmockkConstructor(Parser::class)
    }

    @Test
    fun `executeUpdate should delegate to updatePlanner for CREATE TABLE statement`() {
        // Arrange
        val updateCmd = "CREATE TABLE test_table (id INT, name VARCHAR(20))"
        val createTableData = mockk<CreateTableData>()
        val expectedRowsAffected = 0

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().updateCommand() } returns createTableData
        every { updatePlanner.executeCreateTable(createTableData, transaction) } returns expectedRowsAffected

        // Act
        val result = planner.executeUpdate(updateCmd, transaction)

        // Assert
        assertEquals(expectedRowsAffected, result)
        verify { updatePlanner.executeCreateTable(createTableData, transaction) }

        unmockkConstructor(Parser::class)
    }

    @Test
    fun `executeUpdate should delegate to updatePlanner for CREATE VIEW statement`() {
        // Arrange
        val updateCmd = "CREATE VIEW test_view AS SELECT * FROM test_table"
        val createViewData = mockk<CreateViewData>()
        val expectedRowsAffected = 0

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().updateCommand() } returns createViewData
        every { updatePlanner.executeCreateView(createViewData, transaction) } returns expectedRowsAffected

        // Act
        val result = planner.executeUpdate(updateCmd, transaction)

        // Assert
        assertEquals(expectedRowsAffected, result)
        verify { updatePlanner.executeCreateView(createViewData, transaction) }

        unmockkConstructor(Parser::class)
    }

    @Test
    fun `executeUpdate should delegate to updatePlanner for CREATE INDEX statement`() {
        // Arrange
        val updateCmd = "CREATE INDEX idx_test ON test_table (id)"
        val createIndexData = mockk<CreateIndexData>()
        val expectedRowsAffected = 0

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().updateCommand() } returns createIndexData
        every { updatePlanner.executeCreateIndex(createIndexData, transaction) } returns expectedRowsAffected

        // Act
        val result = planner.executeUpdate(updateCmd, transaction)

        // Assert
        assertEquals(expectedRowsAffected, result)
        verify { updatePlanner.executeCreateIndex(createIndexData, transaction) }

        unmockkConstructor(Parser::class)
    }

    @Test
    fun `executeUpdate should return 0 for unknown command type`() {
        // Arrange
        val updateCmd = "UNKNOWN COMMAND"
        val unknownData = mockk<Any>()

        mockkConstructor(Parser::class)
        every { anyConstructed<Parser>().updateCommand() } returns unknownData

        // Act
        val result = planner.executeUpdate(updateCmd, transaction)

        // Assert
        assertEquals(0, result)

        unmockkConstructor(Parser::class)
    }
}