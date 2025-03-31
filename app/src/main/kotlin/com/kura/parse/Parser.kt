package com.kura.parse

import com.kura.query.Constant
import com.kura.query.Expression
import com.kura.query.Predicate
import com.kura.query.Term
import com.kura.record.Schema

/**
 * SQL parser for the Kura database.
 */
class Parser(s: String) {
    private val lexer: Lexer = Lexer(s)

    // Methods for parsing predicates, terms, expressions, constants, and fields

    /**
     * Parses a field name.
     * @return the field name
     */
    fun field(): String {
        return lexer.eatId()
    }

    /**
     * Parses a constant.
     * @return a constant object
     */
    fun constant(): Constant {
        return if (lexer.matchStringConstant()) {
            Constant(lexer.eatStringConstant())
        } else {
            Constant(lexer.eatIntConstant())
        }
    }

    /**
     * Parses an expression.
     * @return an expression object
     */
    fun expression(): Expression {
        return if (lexer.matchId()) {
            Expression(field())
        } else {
            Expression(constant())
        }
    }

    /**
     * Parses a term.
     * @return a term object
     */
    fun term(): Term {
        val lhs = expression()
        lexer.eatDelim('=')
        val rhs = expression()
        return Term(lhs, rhs)
    }

    /**
     * Parses a predicate.
     * @return a predicate object
     */
    fun predicate(): Predicate {
        val pred = Predicate(term())
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and")
            pred.conjoinWith(predicate())
        }
        return pred
    }

    // Methods for parsing queries

    /**
     * Parses a SELECT statement.
     * @return a query data object
     */
    fun query(): QueryData {
        lexer.eatKeyword("select")
        val fields = selectList()
        lexer.eatKeyword("from")
        val tables = tableList()
        var pred = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            pred = predicate()
        }
        return QueryData(fields, tables, pred)
    }

    private fun selectList(): List<String> {
        val list = mutableListOf<String>()
        list.add(field())
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',')
            list.addAll(selectList())
        }
        return list
    }

    private fun tableList(): Collection<String> {
        val list = mutableListOf<String>()
        list.add(lexer.eatId())
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',')
            list.addAll(tableList())
        }
        return list
    }

    // Methods for parsing various update commands

    /**
     * Parses an update command.
     * @return an update data object
     */
    fun updateCommand(): Any {
        return when {
            lexer.matchKeyword("insert") -> insert()
            lexer.matchKeyword("delete") -> delete()
            lexer.matchKeyword("update") -> modify()
            else -> create()
        }
    }

    private fun create(): Any {
        lexer.eatKeyword("create")
        return when {
            lexer.matchKeyword("table") -> createTable()
            lexer.matchKeyword("view") -> createView()
            else -> createIndex()
        }
    }

    // Methods for parsing DELETE statements

    /**
     * Parses a DELETE statement.
     * @return a delete data object
     */
    fun delete(): DeleteData {
        lexer.eatKeyword("delete")
        lexer.eatKeyword("from")
        val tableName = lexer.eatId()
        var pred = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            pred = predicate()
        }
        return DeleteData(tableName, pred)
    }

    // Methods for parsing INSERT statements

    /**
     * Parses an INSERT statement.
     * @return an insert data object
     */
    fun insert(): InsertData {
        lexer.eatKeyword("insert")
        lexer.eatKeyword("into")
        val tableName = lexer.eatId()
        lexer.eatDelim('(')
        val fields = fieldList()
        lexer.eatDelim(')')
        lexer.eatKeyword("values")
        lexer.eatDelim('(')
        val values = constList()
        lexer.eatDelim(')')
        return InsertData(tableName, fields, values)
    }

    private fun fieldList(): List<String> {
        val list = mutableListOf<String>()
        list.add(field())
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',')
            list.addAll(fieldList())
        }
        return list
    }

    private fun constList(): List<Constant> {
        val list = mutableListOf<Constant>()
        list.add(constant())
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',')
            list.addAll(constList())
        }
        return list
    }

    // Methods for parsing UPDATE statements

    /**
     * Parses an UPDATE statement.
     * @return a modify data object
     */
    fun modify(): ModifyData {
        lexer.eatKeyword("update")
        val tableName = lexer.eatId()
        lexer.eatKeyword("set")
        val fieldName = field()
        lexer.eatDelim('=')
        val newValue = expression()
        var pred = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            pred = predicate()
        }
        return ModifyData(tableName, fieldName, newValue, pred)
    }

    // Methods for parsing CREATE TABLE statements

    /**
     * Parses a CREATE TABLE statement.
     * @return a create table data object
     */
    fun createTable(): CreateTableData {
        lexer.eatKeyword("table")
        val tableName = lexer.eatId()
        lexer.eatDelim('(')
        val schema = fieldDefs()
        lexer.eatDelim(')')
        return CreateTableData(tableName, schema)
    }

    private fun fieldDefs(): Schema {
        val schema = fieldDef()
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',')
            val schema2 = fieldDefs()
            schema.addAll(schema2)
        }
        return schema
    }

    private fun fieldDef(): Schema {
        val fieldName = field()
        return fieldType(fieldName)
    }

    private fun fieldType(fieldName: String): Schema {
        val schema = Schema()
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int")
            schema.addIntField(fieldName)
        } else {
            lexer.eatKeyword("varchar")
            lexer.eatDelim('(')
            val strLen = lexer.eatIntConstant()
            lexer.eatDelim(')')
            schema.addStringField(fieldName, strLen)
        }
        return schema
    }

    // Methods for parsing CREATE VIEW statements

    /**
     * Parses a CREATE VIEW statement.
     * @return a create view data object
     */
    fun createView(): CreateViewData {
        lexer.eatKeyword("view")
        val viewName = lexer.eatId()
        lexer.eatKeyword("as")
        val queryData = query()
        return CreateViewData(viewName, queryData)
    }

    // Methods for parsing CREATE INDEX statements

    /**
     * Parses a CREATE INDEX statement.
     * @return a create index data object
     */
    fun createIndex(): CreateIndexData {
        lexer.eatKeyword("index")
        val indexName = lexer.eatId()
        lexer.eatKeyword("on")
        val tableName = lexer.eatId()
        lexer.eatDelim('(')
        val fieldName = field()
        lexer.eatDelim(')')
        return CreateIndexData(indexName, tableName, fieldName)
    }
}