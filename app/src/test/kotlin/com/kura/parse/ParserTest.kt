package com.kura.parse

import com.kura.query.Expression
import com.kura.query.Predicate
import com.kura.query.Term
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.Types

class ParserTest {

    @Test
    fun `should parse simple select query`() {
        // Arrange
        val query = "select id from students"
        val parser = Parser(query)

        // Act
        val queryData = parser.query()

        // Assert
        assertEquals(listOf("id"), queryData.fields())
        assertEquals(listOf("students"), queryData.tables())
        assertTrue(queryData.pred().terms().isEmpty())
    }

    @Test
    fun `should parse select query with where clause`() {
        // Arrange
        val query = "select name from students where id = 10"
        val parser = Parser(query)

        // Act
        val queryData = parser.query()

        // Assert
        assertEquals(listOf("name"), queryData.fields())
        assertEquals(listOf("students"), queryData.tables())
        assertEquals(1, queryData.pred().terms().size)
        val term = queryData.pred().terms()[0]
        assertTrue(term.lhs().isFieldName())
        assertEquals("id", term.lhs().asFieldName())
        assertTrue(term.rhs().isConstant())
        assertEquals(10, term.rhs().asConstant()?.intValue())
    }

    @Test
    fun `should parse select query with multiple fields`() {
        // Arrange
        val query = "select id, name, age from students"
        val parser = Parser(query)

        // Act
        val queryData = parser.query()

        // Assert
        assertEquals(listOf("id", "name", "age"), queryData.fields())
        assertEquals(listOf("students"), queryData.tables())
    }

    @Test
    fun `should parse select query with multiple tables`() {
        // Arrange
        val query = "select id, name from students, classes"
        val parser = Parser(query)

        // Act
        val queryData = parser.query()

        // Assert
        assertEquals(listOf("id", "name"), queryData.fields())
        assertEquals(listOf("students", "classes"), queryData.tables())
    }

    @Test
    fun `should parse select query with compound where clause`() {
        // Arrange
        val query = "select id from students where id = 10 and name = 'John'"
        val parser = Parser(query)

        // Act
        val queryData = parser.query()

        // Assert
        assertEquals(listOf("id"), queryData.fields())
        assertEquals(listOf("students"), queryData.tables())
        assertEquals(2, queryData.pred().terms().size)

        val term1 = queryData.pred().terms()[0]
        assertTrue(term1.lhs().isFieldName())
        assertEquals("id", term1.lhs().asFieldName())
        assertTrue(term1.rhs().isConstant())
        assertEquals(10, term1.rhs().asConstant()?.intValue())

        val term2 = queryData.pred().terms()[1]
        assertTrue(term2.lhs().isFieldName())
        assertEquals("name", term2.lhs().asFieldName())
        assertTrue(term2.rhs().isConstant())
        assertEquals("John", term2.rhs().asConstant()?.stringValue())
    }

    @Test
    fun `should parse insert statement`() {
        // Arrange
        val insert = "insert into students (id, name) values (10, 'John')"
        val parser = Parser(insert)

        // Act
        val insertData = parser.insert()

        // Assert
        assertEquals("students", insertData.tableName())
        assertEquals(listOf("id", "name"), insertData.fields())
        assertEquals(2, insertData.values().size)
        assertEquals(10, insertData.values()[0].intValue())
        assertEquals("John", insertData.values()[1].stringValue())
    }

    @Test
    fun `should parse delete statement`() {
        // Arrange
        val delete = "delete from students where id = 10"
        val parser = Parser(delete)

        // Act
        val deleteData = parser.delete()

        // Assert
        assertEquals("students", deleteData.tableName())
        assertEquals(1, deleteData.pred().terms().size)
        val term = deleteData.pred().terms()[0]
        assertEquals("id", term.lhs().asFieldName())
        assertEquals(10, term.rhs().asConstant()?.intValue())
    }

    @Test
    fun `should parse update statement`() {
        // Arrange
        val update = "update students set name = 'Jane' where id = 10"
        val parser = Parser(update)

        // Act
        val modifyData = parser.modify()

        // Assert
        assertEquals("students", modifyData.tableName())
        assertEquals("name", modifyData.fieldName())
        assertTrue(modifyData.newValue().isConstant())
        assertEquals("Jane", modifyData.newValue().asConstant()?.stringValue())
        assertEquals(1, modifyData.pred().terms().size)
        val term = modifyData.pred().terms()[0]
        assertEquals("id", term.lhs().asFieldName())
        assertEquals(10, term.rhs().asConstant()?.intValue())
    }

    @Test
    fun `should parse create table statement`() {
        // Arrange
        val createTable = "create table students (id int, name varchar(20))"
        val parser = Parser(createTable)

        // Act
        val result = parser.updateCommand()

        // Assert
        assertTrue(result is CreateTableData)
        val createTableData = result as CreateTableData
        assertEquals("students", createTableData.tableName())
        val schema = createTableData.schema()
        assertTrue(schema.hasField("id"))
        assertTrue(schema.hasField("name"))
        assertTrue(schema.type("id") == Types.INTEGER)
        assertTrue(schema.type("name") == Types.VARCHAR)
        assertEquals(20, schema.length("name"))
    }

    @Test
    fun `should parse create view statement`() {
        // Arrange
        val createView = "create view student_names as select name from students"
        val parser = Parser(createView)

        // Act
        val result = parser.updateCommand()

        // Assert
        assertTrue(result is CreateViewData)
        val createViewData = result as CreateViewData
        assertEquals("student_names", createViewData.viewName())
        val queryData = createViewData.viewDef()
        assertEquals(listOf("name"), queryData.fields())
        assertEquals(listOf("students"), queryData.tables())
    }

    @Test
    fun `should parse create index statement`() {
        // Arrange
        val createIndex = "create index idx_student_name on students (name)"
        val parser = Parser(createIndex)

        // Act
        val result = parser.updateCommand()

        // Assert
        assertTrue(result is CreateIndexData)
        val createIndexData = result as CreateIndexData
        assertEquals("idx_student_name", createIndexData.indexName())
        assertEquals("students", createIndexData.tableName())
        assertEquals("name", createIndexData.fieldName())
    }

    @Test
    fun `should throw BadSyntaxException for invalid syntax`() {
        // Arrange
        val invalidQuery = "slect id from students"
        val parser = Parser(invalidQuery)

        // Act & Assert
        assertThrows(BadSyntaxException::class.java) {
            parser.query()
        }
    }
}