package sample.kura_client

import com.kura.jdbc.network.NetworkDriver
import java.sql.Driver
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.Scanner

/**
 * SQLコマンドインターフェース（KuraDB用）
 * KuraDBサーバーに接続してSQLコマンドを実行するシンプルなCLIプログラム
 */
object KuraSQLClient {
    private const val DEFAULT_URL = "jdbc:kuradb://localhost"

    @JvmStatic
    fun main(args: Array<String>) {
        val scanner = Scanner(System.`in`)

        // 接続URLの取得
        print("接続先 [${DEFAULT_URL}]> ")
        var connectionUrl = scanner.nextLine().trim()
        if (connectionUrl.isEmpty()) {
            connectionUrl = DEFAULT_URL
        }

        // SQLドライバーの初期化
        val driver: Driver = NetworkDriver()

        try {
            println("KuraDBに接続しています: $connectionUrl...")

            // データベースに接続
            driver.connect(connectionUrl, null).use { conn ->
                println("接続に成功しました")

                conn.createStatement().use { stmt ->
                    runCommandLoop(scanner, stmt)
                }
            }
        } catch (e: SQLException) {
            System.err.println("データベース接続エラー: ${e.message}")
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
                    // 空のコマンドは無視
                }
                cmd.equals("exit", ignoreCase = true) ||
                cmd.equals("quit", ignoreCase = true) -> {
                    println("終了します...")
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
            System.err.println("SQL実行エラー: ${e.message}")
        }
    }

    private fun executeUpdate(stmt: Statement, cmd: String) {
        try {
            val startTime = System.currentTimeMillis()

            val count = stmt.executeUpdate(cmd)

            val elapsedTime = System.currentTimeMillis() - startTime
            println("${count}件のレコードが影響を受けました (${elapsedTime}ms)")
        } catch (e: SQLException) {
            System.err.println("SQL実行エラー: ${e.message}")
        }
    }

    private fun displayResultSetSimple(rs: ResultSet) {
        try {
            // シンプルな表示方法
            val metaData = rs.metaData
            val columnCount = metaData.columnCount

            if (columnCount == 0) {
                println("(結果なし)")
                return
            }

            // カラム名の配列を作成
            val columnNames = Array(columnCount) { i ->
                metaData.getColumnName(i + 1)
            }

            // カラム名を表示
            for (i in 0 until columnCount) {
                print(columnNames[i])
                if (i < columnCount - 1) print(" | ")
            }
            println("\n" + "-".repeat(40))

            // 各行を表示
            var rowCount = 0
            while (rs.next()) {
                rowCount++
                for (i in 0 until columnCount) {
                    // 列名を使用してデータを取得
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

            println("\n${rowCount}件のレコードが見つかりました")
        } catch (e: SQLException) {
            System.err.println("結果セット処理エラー: ${e.message}")
            e.printStackTrace()
        }
    }

    private fun printHelp() {
        println("""
            |KuraDB SQL Interface - ヘルプ
            |---------------------------
            |使い方:
            |  SQL> [SQLコマンド]
            |
            |コマンド例:
            |  SELECT * FROM tablename;
            |  CREATE TABLE tablename (id int, name varchar(20));
            |  INSERT INTO tablename VALUES (1, 'value');
            |  UPDATE tablename SET name='newvalue' WHERE id=1;
            |  DELETE FROM tablename WHERE id=1;
            |
            |特殊コマンド:
            |  exit, quit - プログラムを終了
            |  help - このヘルプを表示
        """.trimMargin())
    }
}