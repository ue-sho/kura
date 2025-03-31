package com.kura.parse

import com.kura.query.Constant
import com.kura.query.Expression
import com.kura.query.Predicate
import com.kura.query.Term
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PredParserTest {

    @Test
    fun `should throw BadSyntaxException for invalid syntax`() {
        // Arrange
        val invalidPred = "id 10"
        val predParser = PredParser(invalidPred)

        // Act & Assert
        assertThrows(BadSyntaxException::class.java) {
            predParser.predicate()
        }
    }
}