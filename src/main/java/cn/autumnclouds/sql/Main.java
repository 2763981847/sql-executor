package cn.autumnclouds.sql;

import cn.autumnclouds.sql.core.DatabaseContainer;
import cn.autumnclouds.sql.core.SQLExecutor;
import java.util.Scanner;

/**
 * @author Oreki
 * @since ${DATE}
 */
public class Main {
    public static final String test_sql = "CREATE DATABASE IF NOT EXISTS test;USE test;CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR);INSERT INTO test_table (id,name) VALUES (1, 'test1'), (2, 'test2'), (3, ‘test3’);select * from test_table order by id desc;DELETE FROM test_table WHERE id = 1;SELECT * FROM test_table;UPDATE test_table SET name = 'test4' WHERE id = 2;SELECT * FROM test_table WHERE id = 2;DROP TABLE test_table;DROP DATABASE test;";

    public static void main(String[] args) {
        SQLExecutor sqlExecutor = new SQLExecutor(DatabaseContainer.getInstance());
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String sql = scanner.nextLine();
            if ("exit".equals(sql)) {
                break;
            }
            sqlExecutor.executeSql(sql);
        }
    }
}