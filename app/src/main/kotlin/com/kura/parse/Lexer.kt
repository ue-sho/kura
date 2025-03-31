package com.kura.parse

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader

/**
 * Lexical analyzer.
 * Class for breaking down SQL strings into tokens for parsing.
 */
class Lexer(s: String) {
    private val keywords: List<String> = listOf(
        "select", "from", "where", "and",
        "insert", "into", "values", "delete", "update", "set",
        "create", "table", "int", "varchar", "view", "as", "index", "on"
    )
    private val tokenizer: StreamTokenizer = StreamTokenizer(StringReader(s)).apply {
        ordinaryChar('.'.code) // Don't allow "." in identifiers
        wordChars('_'.code, '_'.code) // Allow "_" in identifiers
        lowerCaseMode(true) // Convert identifiers and keywords to lowercase
    }

    init {
        nextToken()
    }

    // Methods to check the status of the current token

    /**
     * Returns whether the current token is the specified delimiter character.
     * @param d The delimiter character
     * @return true if the delimiter is the current token
     */
    fun matchDelim(d: Char): Boolean {
        return d == tokenizer.ttype.toChar()
    }

    /**
     * Returns whether the current token is an integer.
     * @return true if the current token is an integer
     */
    fun matchIntConstant(): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER
    }

    /**
     * Returns whether the current token is a string.
     * @return true if the current token is a string
     */
    fun matchStringConstant(): Boolean {
        return '\'' == tokenizer.ttype.toChar()
    }

    /**
     * Returns whether the current token is the specified keyword.
     * @param w The keyword string
     * @return true if that keyword is the current token
     */
    fun matchKeyword(w: String): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && tokenizer.sval == w
    }

    /**
     * Returns whether the current token is a valid identifier.
     * @return true if the current token is an identifier
     */
    fun matchId(): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tokenizer.sval)
    }

    // Methods to "consume" the current token

    /**
     * Throws an exception if the current token is not the specified delimiter.
     * Otherwise, moves to the next token.
     * @param d The delimiter character
     */
    fun eatDelim(d: Char) {
        if (!matchDelim(d))
            throw BadSyntaxException()
        nextToken()
    }

    /**
     * Throws an exception if the current token is not an integer.
     * Otherwise, returns that integer and moves to the next token.
     * @return the integer value of the current token
     */
    fun eatIntConstant(): Int {
        if (!matchIntConstant())
            throw BadSyntaxException()
        val i = tokenizer.nval.toInt()
        nextToken()
        return i
    }

    /**
     * Throws an exception if the current token is not a string.
     * Otherwise, returns that string and moves to the next token.
     * @return the string value of the current token
     */
    fun eatStringConstant(): String {
        if (!matchStringConstant())
            throw BadSyntaxException()
        val s = tokenizer.sval // Constants are not converted to lowercase
        nextToken()
        return s
    }

    /**
     * Throws an exception if the current token is not the specified keyword.
     * Otherwise, moves to the next token.
     * @param w The keyword string
     */
    fun eatKeyword(w: String) {
        if (!matchKeyword(w))
            throw BadSyntaxException()
        nextToken()
    }

    /**
     * Throws an exception if the current token is not an identifier.
     * Otherwise, returns the identifier string and moves to the next token.
     * @return the string value of the current token
     */
    fun eatId(): String {
        if (!matchId())
            throw BadSyntaxException()
        val s = tokenizer.sval
        nextToken()
        return s
    }

    private fun nextToken() {
        try {
            tokenizer.nextToken()
        } catch (e: IOException) {
            throw BadSyntaxException()
        }
    }
}