package com.kura.record

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Types

class SchemaTest {
    private lateinit var schema: Schema

    @BeforeEach
    fun setUp() {
        schema = Schema()
    }

    @Test
    fun `should add int field to schema`() {
        // Arrange
        val fieldName = "id"

        // Act
        schema.addIntField(fieldName)

        // Assert
        assertTrue(schema.hasField(fieldName))
        assertEquals(Types.INTEGER, schema.type(fieldName))
        assertEquals(0, schema.length(fieldName))
    }

    @Test
    fun `should add string field to schema`() {
        // Arrange
        val fieldName = "name"
        val length = 20

        // Act
        schema.addStringField(fieldName, length)

        // Assert
        assertTrue(schema.hasField(fieldName))
        assertEquals(Types.VARCHAR, schema.type(fieldName))
        assertEquals(length, schema.length(fieldName))
    }

    @Test
    fun `should add multiple fields to schema`() {
        // Arrange
        val idField = "id"
        val nameField = "name"
        val ageField = "age"

        // Act
        schema.addIntField(idField)
        schema.addStringField(nameField, 20)
        schema.addIntField(ageField)

        // Assert
        assertEquals(3, schema.fields().size)
        assertTrue(schema.fields().containsAll(listOf(idField, nameField, ageField)))
    }

    @Test
    fun `should add field from another schema`() {
        // Arrange
        val otherSchema = Schema()
        val fieldName = "id"
        otherSchema.addIntField(fieldName)

        // Act
        schema.add(fieldName, otherSchema)

        // Assert
        assertTrue(schema.hasField(fieldName))
        assertEquals(Types.INTEGER, schema.type(fieldName))
    }

    @Test
    fun `should add all fields from another schema`() {
        // Arrange
        val otherSchema = Schema()
        otherSchema.addIntField("id")
        otherSchema.addStringField("name", 20)

        // Act
        schema.addAll(otherSchema)

        // Assert
        assertEquals(2, schema.fields().size)
        assertTrue(schema.fields().containsAll(listOf("id", "name")))
        assertEquals(Types.INTEGER, schema.type("id"))
        assertEquals(Types.VARCHAR, schema.type("name"))
        assertEquals(20, schema.length("name"))
    }
}