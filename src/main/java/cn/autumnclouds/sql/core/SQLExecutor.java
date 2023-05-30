package cn.autumnclouds.sql.core;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.*;

import java.util.List;
import java.util.stream.Collectors;


/**
 * @author Oreki
 * @since 2023/5/29
 */
public class SQLExecutor {

    private final DatabaseContainer databaseContainer;

    public SQLExecutor(DatabaseContainer databaseContainer) {
        this.databaseContainer = databaseContainer;
    }

    private void executeSql(String[] sqls) {
        for (String sql : sqls) {
            SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql);
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

    public void executeSql(String sql) {
        String[] split = sql.split(";");
        executeSql(split);
    }

    private void useDatabase(SQLUseStatement sqlUseStatement) {
        String databaseName = sqlUseStatement.getDatabase().getSimpleName();
        databaseContainer.useDatabase(databaseName);
    }


    private void createDatabase(SQLCreateDatabaseStatement sqlCreateDatabaseStatement) {
        String databaseName = sqlCreateDatabaseStatement.getDatabaseName();
        databaseContainer.createDatabase(databaseName);
    }

    private void dropDatabase(SQLDropDatabaseStatement sqlDropDatabaseStatement) {
        String databaseName = sqlDropDatabaseStatement.getDatabaseName();
        databaseContainer.dropDatabase(databaseName);
    }


    private void createTable(SQLCreateTableStatement sqlCreateTableStatement) {
        SQLExprTableSource tableSource = sqlCreateTableStatement.getTableSource();
        List<SQLColumnDefinition> columnDefinitions = sqlCreateTableStatement.getColumnDefinitions();
        databaseContainer.createTable(tableSource, columnDefinitions);
    }

    private void dropTable(SQLDropTableStatement sqlDropTableStatement) {
        List<SQLExprTableSource> tableSources = sqlDropTableStatement.getTableSources();
        databaseContainer.dropTable(tableSources);
    }

    private void insertRow(SQLInsertStatement sqlInsertStatement) {
        SQLExprTableSource tableSource = sqlInsertStatement.getTableSource();
        List<SQLExpr> columns = sqlInsertStatement.getColumns();
        List<String> columnNames = columns.stream().map(Object::toString).collect(Collectors.toList());
        List<SQLInsertStatement.ValuesClause> valuesList = sqlInsertStatement.getValuesList();
        databaseContainer.insertRow(tableSource, columnNames, valuesList);
    }

    private void deleteRow(SQLDeleteStatement sqlDeleteStatement) {
        SQLExprTableSource tableSource = sqlDeleteStatement.getExprTableSource();
        SQLExpr where = sqlDeleteStatement.getWhere();
        databaseContainer.deleteRow(tableSource, where);
    }

    private void updateRow(SQLUpdateStatement sqlUpdateStatement) {
        SQLTableSource tableSource = sqlUpdateStatement.getTableSource();
        SQLExprTableSource exprTableSource = (SQLExprTableSource) tableSource;
        List<SQLUpdateSetItem> items = sqlUpdateStatement.getItems();
        SQLExpr where = sqlUpdateStatement.getWhere();
        databaseContainer.updateRow(exprTableSource, items, where);
    }

    private void selectRow(SQLSelectStatement sqlSelectStatement) {
        SQLSelectQueryBlock queryBlock = sqlSelectStatement.getSelect().getFirstQueryBlock();
        SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.getFrom();
        SQLExpr where = queryBlock.getWhere();
        List<SQLSelectItem> selectList = queryBlock.getSelectList();
        SQLOrderBy orderBy = queryBlock.getOrderBy();
        databaseContainer.selectRow(tableSource, selectList, where, orderBy);
    }

}