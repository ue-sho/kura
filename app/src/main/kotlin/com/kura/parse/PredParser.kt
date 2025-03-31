package com.kura.parse

/**
 * Predicate parser.
 * A simple parser class for parsing only the predicate parts of SQL.
 */
class PredParser(s: String) {
    private val lexer: Lexer = Lexer(s)

    /**
     * Parses a field name.
     * @return the field name
     */
    fun field(): String {
        return lexer.eatId()
    }

    /**
     * Parses a constant.
     */
    fun constant() {
        if (lexer.matchStringConstant()) {
            lexer.eatStringConstant()
        } else {
            lexer.eatIntConstant()
        }
    }

    /**
     * Parses an expression.
     */
    fun expression() {
        if (lexer.matchId()) {
            field()
        } else {
            constant()
        }
    }

    /**
     * Parses a term.
     */
    fun term() {
        expression()
        lexer.eatDelim('=')
        expression()
    }

    /**
     * Parses a predicate.
     */
    fun predicate() {
        term()
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and")
            predicate()
        }
    }
}