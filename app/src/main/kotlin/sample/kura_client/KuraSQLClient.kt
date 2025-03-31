package sample.kura_client

import com.kura.jdbc.network.NetworkDriver
import java.sql.Driver
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Scanner

/**
 * SQL Command Interface (for KuraDB)
 * A simple CLI program to connect to a KuraDB server and execute SQL commands
 */
object KuraSQLClient {
    private const val DEFAULT_URL = "jdbc:kuradb://localhost"

    @JvmStatic
    fun main(args: Array<String>) {
        val scanner = Scanner(System.`in`)

        // Get connection URL
        print("Connection target [${DEFAULT_URL}]> ")
        var connectionUrl = scanner.nextLine().trim()
        if (connectionUrl.isEmpty()) {
            connectionUrl = DEFAULT_URL
        }

        // Initialize SQL driver
        val driver: Driver = NetworkDriver()

        try {
            println("Connecting to KuraDB: $connectionUrl...")

            // Connect to the database
            driver.connect(connectionUrl, null).use { conn ->
                println("Connection successful")

                conn.createStatement().use { stmt ->
                    runCommandLoop(scanner, stmt)
                }
            }
        } catch (e: SQLException) {
            System.err.println("Database connection error: ${e.message}")
            e.printStackTrace()
        } finally {
            scanner.close()
        }
    }

    private fun runCommandLoop(scanner: Scanner, stmt: Statement) {
        println("\nSQL> ")

        while (scanner.hasNextLine()) {
            val cmd = scanner.nextLine().trim()

            when {
                cmd.isEmpty() -> {
                    // Ignore empty commands
                }
                cmd.equals("exit", ignoreCase = true) ||
                cmd.equals("quit", ignoreCase = true) -> {
                    println("Exiting...")
                    break
                }
                cmd.equals("help", ignoreCase = true) -> {
                    printHelp()
                }
                cmd.lowercase().startsWith("select") -> {
                    executeQuery(stmt, cmd)
                }
                else -> {
                    executeUpdate(stmt, cmd)
                }
            }

            print("\nSQL> ")
        }
    }

    private fun executeQuery(stmt: Statement, cmd: String) {
        try {
            val startTime = System.currentTimeMillis()

            stmt.executeQuery(cmd).use { rs ->
                displayResultSetSimple(rs)

                val elapsedTime = System.currentTimeMillis() - startTime
                println("\nQuery executed in ${elapsedTime}ms")
            }
        } catch (e: SQLException) {
            System.err.println("SQL execution error: ${e.message}")
        }
    }

    private fun executeUpdate(stmt: Statement, cmd: String) {
        try {
            val startTime = System.currentTimeMillis()

            val count = stmt.executeUpdate(cmd)

            val elapsedTime = System.currentTimeMillis() - startTime
            println("${count} records affected (${elapsedTime}ms)")
        } catch (e: SQLException) {
            System.err.println("SQL execution error: ${e.message}")
        }
    }

    private fun displayResultSetSimple(rs: ResultSet) {
        try {
            // Simple display method
            val metaData = rs.metaData
            val columnCount = metaData.columnCount

            if (columnCount == 0) {
                println("(No results)")
                return
            }

            // Create array of column names
            val columnNames = Array(columnCount) { i ->
                metaData.getColumnName(i + 1)
            }

            // Display column names
            for (i in 0 until columnCount) {
                print(columnNames[i])
                if (i < columnCount - 1) print(" | ")
            }
            println("\n" + "-".repeat(40))

            // Display each row
            var rowCount = 0
            while (rs.next()) {
                rowCount++
                for (i in 0 until columnCount) {
                    // Get data using column name
                    val columnName = columnNames[i]
                    val value = if (columnName == "id") {
                        rs.getInt(columnName)
                    } else {
                        rs.getString(columnName)
                    }
                    print(value)
                    if (i < columnCount - 1) print(" | ")
                }
                println()
            }

            println("\n${rowCount} records found")
        } catch (e: SQLException) {
            System.err.println("Result set processing error: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun printHelp() {
        println("""
            |KuraDB SQL Interface - Help
            |---------------------------
            |Usage:
            |  SQL> [SQL Command]
            |
            |Example commands:
            |  SELECT * FROM tablename;
            |  CREATE TABLE tablename (id int, name varchar(20));
            |  INSERT INTO tablename VALUES (1, 'value');
            |  UPDATE tablename SET name='newvalue' WHERE id=1;
            |  DELETE FROM tablename WHERE id=1;
            |
            |Special commands:
            |  exit, quit - Exit the program
            |  help - Display this help
        """.trimMargin())
    }
}