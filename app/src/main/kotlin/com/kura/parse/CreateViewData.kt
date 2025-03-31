package com.kura.parse

/**
 * Class representing data for SQL create view statements.
 */
class CreateViewData(
    private val viewName: String,
    private val viewDef: QueryData
) {
    /**
     * Returns the name of the new view.
     * @return the view name
     */
    fun viewName(): String {
        return viewName
    }

    /**
     * Returns the query definition of the new view.
     * @return the query data definition
     */
    fun viewDef(): QueryData {
        return viewDef
    }

    override fun toString(): String {
        return "create view $viewName as ${viewDef}"
    }
}