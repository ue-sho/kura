package com.kura.parse

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LexerTest {

    @Test
    fun `should correctly identify and eat keywords`() {
        // Arrange
        val lexer = Lexer("select from where and insert into values delete update set create table int varchar view as index on")

        // Act & Assert
        assertTrue(lexer.matchKeyword("select"))
        lexer.eatKeyword("select")

        assertTrue(lexer.matchKeyword("from"))
        lexer.eatKeyword("from")

        assertTrue(lexer.matchKeyword("where"))
        lexer.eatKeyword("where")

        assertTrue(lexer.matchKeyword("and"))
        lexer.eatKeyword("and")

        assertTrue(lexer.matchKeyword("insert"))
        lexer.eatKeyword("insert")

        assertTrue(lexer.matchKeyword("into"))
        lexer.eatKeyword("into")

        assertTrue(lexer.matchKeyword("values"))
        lexer.eatKeyword("values")

        assertTrue(lexer.matchKeyword("delete"))
        lexer.eatKeyword("delete")

        assertTrue(lexer.matchKeyword("update"))
        lexer.eatKeyword("update")

        assertTrue(lexer.matchKeyword("set"))
        lexer.eatKeyword("set")

        assertTrue(lexer.matchKeyword("create"))
        lexer.eatKeyword("create")

        assertTrue(lexer.matchKeyword("table"))
        lexer.eatKeyword("table")

        assertTrue(lexer.matchKeyword("int"))
        lexer.eatKeyword("int")

        assertTrue(lexer.matchKeyword("varchar"))
        lexer.eatKeyword("varchar")

        assertTrue(lexer.matchKeyword("view"))
        lexer.eatKeyword("view")

        assertTrue(lexer.matchKeyword("as"))
        lexer.eatKeyword("as")

        assertTrue(lexer.matchKeyword("index"))
        lexer.eatKeyword("index")

        assertTrue(lexer.matchKeyword("on"))
        lexer.eatKeyword("on")
    }

    @Test
    fun `should correctly identify and eat identifiers`() {
        // Arrange
        val lexer = Lexer("id name student_table")

        // Act & Assert
        assertTrue(lexer.matchId())
        assertEquals("id", lexer.eatId())

        assertTrue(lexer.matchId())
        assertEquals("name", lexer.eatId())

        assertTrue(lexer.matchId())
        assertEquals("student_table", lexer.eatId())
    }

    @Test
    fun `should correctly identify and eat integer constants`() {
        // Arrange
        val lexer = Lexer("123 456 789")

        // Act & Assert
        assertEquals(123, lexer.eatIntConstant())
        assertEquals(456, lexer.eatIntConstant())
        assertEquals(789, lexer.eatIntConstant())
    }

    @Test
    fun `should correctly identify and eat string constants`() {
        // Arrange
        val lexer = Lexer("'John' 'Doe' 'Hello, World!'")

        // Act & Assert
        assertTrue(lexer.matchStringConstant())
        assertEquals("John", lexer.eatStringConstant())

        assertTrue(lexer.matchStringConstant())
        assertEquals("Doe", lexer.eatStringConstant())

        assertTrue(lexer.matchStringConstant())
        assertEquals("Hello, World!", lexer.eatStringConstant())
    }

    @Test
    fun `should correctly identify and eat delimiters`() {
        // Arrange
        val lexer = Lexer("=,();")

        // Act & Assert
        assertTrue(lexer.matchDelim('='))
        lexer.eatDelim('=')

        assertTrue(lexer.matchDelim(','))
        lexer.eatDelim(',')

        assertTrue(lexer.matchDelim('('))
        lexer.eatDelim('(')

        assertTrue(lexer.matchDelim(')'))
        lexer.eatDelim(')')

        assertTrue(lexer.matchDelim(';'))
        lexer.eatDelim(';')
    }

    @Test
    fun `should throw BadSyntaxException when expected keyword is not found`() {
        // Arrange
        val lexer = Lexer("select from")

        // Act & Assert
        lexer.eatKeyword("select")
        assertThrows<BadSyntaxException> {
            lexer.eatKeyword("where") // expecting "where" but found "from"
        }
    }

    @Test
    fun `should throw BadSyntaxException when expected delimiter is not found`() {
        // Arrange
        val lexer = Lexer("id = 10")

        // Act & Assert
        lexer.eatId()
        assertThrows<BadSyntaxException> {
            lexer.eatDelim(',') // expecting ',' but found '='
        }
    }

    @Test
    fun `should parse complex SQL statement`() {
        // Arrange
        val lexer = Lexer("select id, name from students where id = 10 and name = 'John'")

        // Act & Assert
        lexer.eatKeyword("select")
        assertEquals("id", lexer.eatId())
        lexer.eatDelim(',')
        assertEquals("name", lexer.eatId())
        lexer.eatKeyword("from")
        assertEquals("students", lexer.eatId())
        lexer.eatKeyword("where")
        assertEquals("id", lexer.eatId())
        lexer.eatDelim('=')
        assertEquals(10, lexer.eatIntConstant())
        lexer.eatKeyword("and")
        assertEquals("name", lexer.eatId())
        lexer.eatDelim('=')
        assertEquals("John", lexer.eatStringConstant())
    }
}