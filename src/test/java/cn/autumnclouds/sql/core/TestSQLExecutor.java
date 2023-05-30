package cn.autumnclouds.sql.core;

/**
 * @author Oreki
 * @since 2023/5/29
 */
public class TestSQLExecutor {
    public static void main(String[] args) {
        SQLExecutor sqlExecutor = new SQLExecutor(DatabaseContainer.getInstance());
        String sql = "CREATE DATABASE IF NOT EXISTS test;";
        sqlExecutor.executeSql(sql);
        sql = "USE test;";
        sqlExecutor.executeSql(sql);
        sql = "CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR);";
        sqlExecutor.executeSql(sql);
        sql = "INSERT INTO test_table (id,name) VALUES (1, 'test1'), (2, 'test2'), (3, ‘test3’);";
        sqlExecutor.executeSql(sql);
        sql = "select * from test_table order by id desc;";
        sqlExecutor.executeSql(sql);
        sql = "DELETE FROM test_table WHERE id = 1;";
        sqlExecutor.executeSql(sql);
        sql = "SELECT * FROM test_table;";
        sqlExecutor.executeSql(sql);
        sql = "UPDATE test_table SET name = 'test4' WHERE id = 2;";
        sqlExecutor.executeSql(sql);
        sql = "SELECT * FROM test_table WHERE id = 2;";
        sqlExecutor.executeSql(sql);
        sql = "DROP TABLE test_table;";
        sqlExecutor.executeSql(sql);
        sql = "DROP DATABASE test;";
        sqlExecutor.executeSql(sql);
    }
}
