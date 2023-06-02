package cn.autumnclouds.sql.core;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * SQL执行器，用于执行SQL语句
 * 支持的操作包括：创建数据库、创建表、删除数据库、删除表、插入数据、删除数据、更新数据、查询数据、切换数据库
 * 使用阿里巴巴的Druid库进行SQL解析和处理
 *
 * @author Oreki
 * @since 2023/5/29
 */
public class SQLExecutor {

    private final DatabaseContainer databaseContainer;

    /**
     * 构造函数，接收一个DatabaseContainer对象作为参数
     *
     * @param databaseContainer 数据库容器对象，用于管理数据库和表
     */
    public SQLExecutor(DatabaseContainer databaseContainer) {
        this.databaseContainer = databaseContainer;
    }

    /**
     * 批量执行SQL语句
     *
     * @param sqls SQL语句数组
     */
    private void executeBatch(String[] sqls) {
        for (String sql : sqls) {
            // 创建SQL语句解析器，并指定使用的数据库类型为MySQL
            SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql);
            // 获取SQL语句的类型
            SQLType sqlType = SQLParserUtils.getSQLType(sql, DbType.mysql);
            if (sqlType.equals(SQLType.CREATE)) {
                // 判断是创建数据库还是创建表
                SQLStatement sqlCreateStatement = sqlStatementParser.parseCreate();
                if (sqlCreateStatement instanceof SQLCreateDatabaseStatement) {
                    createDatabase((SQLCreateDatabaseStatement) sqlCreateStatement);
                } else if (sqlCreateStatement instanceof SQLCreateTableStatement) {
                    createTable((SQLCreateTableStatement) sqlCreateStatement);
                }
            } else if (sqlType.equals(SQLType.DROP)) {
                // 判断是删除数据库还是删除表
                SQLStatement sqlDropStatement = sqlStatementParser.parseDrop();
                if (sqlDropStatement instanceof SQLDropDatabaseStatement) {
                    dropDatabase((SQLDropDatabaseStatement) sqlDropStatement);
                } else if (sqlDropStatement instanceof SQLDropTableStatement) {
                    dropTable((SQLDropTableStatement) sqlDropStatement);
                }
            } else if (sqlType.equals(SQLType.INSERT)) {
                SQLStatement sqlInsertStatement = sqlStatementParser.parseInsert();
                insertRow((SQLInsertStatement) sqlInsertStatement);
            } else if (sqlType.equals(SQLType.DELETE)) {
                SQLDeleteStatement sqlDeleteStatement = sqlStatementParser.parseDeleteStatement();
                deleteRow(sqlDeleteStatement);
            } else if (sqlType.equals(SQLType.UPDATE)) {
                SQLUpdateStatement sqlUpdateStatement = sqlStatementParser.parseUpdateStatement();
                updateRow(sqlUpdateStatement);
            } else if (sqlType.equals(SQLType.SELECT)) {
                SQLStatement sqlSelectStatement = sqlStatementParser.parseSelect();
                selectRow((SQLSelectStatement) sqlSelectStatement);
            } else if (sqlType.equals(SQLType.USE)) {
                SQLUseStatement sqlUseStatement = sqlStatementParser.parseUse();
                useDatabase(sqlUseStatement);
            } else {
                throw new RuntimeException("Unsupported SQL Type");
            }
        }
    }

    /**
     * 执行单条SQL语句
     *
     * @param sql 要执行的SQL语句
     */
    public void executeSql(String sql) {
        // 将SQL语句按分号进行分割成多个语句，并执行批量执行
        String[] split = sql.split(";");
        executeBatch(split);
    }

    /**
     * 切换数据库
     *
     * @param sqlUseStatement USE语句对象
     */
    private void useDatabase(SQLUseStatement sqlUseStatement) {
        String databaseName = sqlUseStatement.getDatabase().getSimpleName();
        databaseContainer.useDatabase(databaseName);
    }

    /**
     * 创建数据库
     *
     * @param sqlCreateDatabaseStatement CREATE DATABASE语句对象
     */
    private void createDatabase(SQLCreateDatabaseStatement sqlCreateDatabaseStatement) {
        String databaseName = sqlCreateDatabaseStatement.getDatabaseName();
        databaseContainer.createDatabase(databaseName);
    }

    /**
     * 删除数据库
     *
     * @param sqlDropDatabaseStatement DROP DATABASE语句对象
     */
    private void dropDatabase(SQLDropDatabaseStatement sqlDropDatabaseStatement) {
        String databaseName = sqlDropDatabaseStatement.getDatabaseName();
        databaseContainer.dropDatabase(databaseName);
    }

    /**
     * 创建表
     *
     * @param sqlCreateTableStatement CREATE TABLE语句对象
     */
    private void createTable(SQLCreateTableStatement sqlCreateTableStatement) {
        SQLExprTableSource tableSource = sqlCreateTableStatement.getTableSource();
        List<SQLColumnDefinition> columnDefinitions = sqlCreateTableStatement.getColumnDefinitions();
        databaseContainer.createTable(tableSource, columnDefinitions);
    }

    /**
     * 删除表
     *
     * @param sqlDropTableStatement DROP TABLE语句对象
     */
    private void dropTable(SQLDropTableStatement sqlDropTableStatement) {
        List<SQLExprTableSource> tableSources = sqlDropTableStatement.getTableSources();
        databaseContainer.dropTable(tableSources);
    }

    /**
     * 插入数据
     *
     * @param sqlInsertStatement INSERT语句对象
     */
    private void insertRow(SQLInsertStatement sqlInsertStatement) {
        SQLExprTableSource tableSource = sqlInsertStatement.getTableSource();
        List<SQLExpr> columns = sqlInsertStatement.getColumns();
        List<String> columnNames = columns.stream().map(Object::toString).collect(Collectors.toList());
        List<SQLInsertStatement.ValuesClause> valuesList = sqlInsertStatement.getValuesList();
        databaseContainer.insertRow(tableSource, columnNames, valuesList);
    }

    /**
     * 删除数据
     *
     * @param sqlDeleteStatement DELETE语句对象
     */
    private void deleteRow(SQLDeleteStatement sqlDeleteStatement) {
        SQLExprTableSource tableSource = sqlDeleteStatement.getExprTableSource();
        SQLExpr where = sqlDeleteStatement.getWhere();
        databaseContainer.deleteRow(tableSource, where);
    }

    /**
     * 更新数据
     *
     * @param sqlUpdateStatement UPDATE语句对象
     */
    private void updateRow(SQLUpdateStatement sqlUpdateStatement) {
        SQLTableSource tableSource = sqlUpdateStatement.getTableSource();
        SQLExprTableSource exprTableSource = (SQLExprTableSource) tableSource;
        List<SQLUpdateSetItem> items = sqlUpdateStatement.getItems();
        SQLExpr where = sqlUpdateStatement.getWhere();
        databaseContainer.updateRow(exprTableSource, items, where);
    }

    /**
     * 查询数据
     *
     * @param sqlSelectStatement SELECT语句对象
     */
    private void selectRow(SQLSelectStatement sqlSelectStatement) {
        SQLSelectQueryBlock queryBlock = sqlSelectStatement.getSelect().getFirstQueryBlock();
        SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.getFrom();
        SQLExpr where = queryBlock.getWhere();
        List<SQLSelectItem> selectList = queryBlock.getSelectList();
        SQLOrderBy orderBy = queryBlock.getOrderBy();
        databaseContainer.selectRow(tableSource, selectList, where, orderBy);
    }

}
