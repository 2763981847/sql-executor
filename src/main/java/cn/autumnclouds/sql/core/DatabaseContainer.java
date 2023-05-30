package cn.autumnclouds.sql.core;

import cn.autumnclouds.sql.entity.Database;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Oreki
 * @since 2023/5/29
 */
public class DatabaseContainer {
    private final Map<String, Database> databaseMap;

    private static final String DEFAULT_DATA_BASE_NAME = "default";
    private String currentDatabaseName;


    public void useDatabase(String databaseName) {
        currentDatabaseName = databaseName;
    }


    public void createDatabase(String databaseName) {
        databaseMap.put(databaseName, new Database(databaseName));
    }

    public void dropDatabase(String databaseName) {
        databaseMap.remove(databaseName);
    }


    public void createTable(SQLExprTableSource tableSource, List<SQLColumnDefinition> columnDefinitions) {
        Database database = getDatabase(tableSource);
        String tableName = tableSource.getTableName();
        database.creatTable(tableName, columnDefinitions);
    }

    public void dropTable(List<SQLExprTableSource> tableSources) {
        for (SQLExprTableSource tableSource : tableSources) {
            Database database = getDatabase(tableSource);
            String tableName = tableSource.getTableName();
            database.dropTable(tableName);
        }
    }


    public void insertRow(SQLExprTableSource tableSource, List<String> columnNames, List<SQLInsertStatement.ValuesClause> valuesList) {
        Database database = getDatabase(tableSource);
        String tableName = tableSource.getTableName();
        database.insertRow(tableName, columnNames, valuesList);
    }

    public void deleteRow(SQLExprTableSource exprTableSource, SQLExpr where) {
        Database database = getDatabase(exprTableSource);
        String tableName = exprTableSource.getTableName();
        database.deleteRow(tableName, where);
    }


    public void updateRow(SQLExprTableSource exprTableSource, List<SQLUpdateSetItem> updateSetItems, SQLExpr where) {
        Database database = getDatabase(exprTableSource);
        database.updateRow(exprTableSource.getTableName(), updateSetItems, where);
    }

    public void selectRow(SQLExprTableSource tableSource, List<SQLSelectItem> selectList, SQLExpr where, SQLOrderBy orderBy) {
        Database database = getDatabase(tableSource);
        String tableName = tableSource.getTableName();
        database.selectRow(tableName, selectList, where, orderBy);
    }

    private Database getDatabase(SQLExprTableSource exprTableSource) {
        String databaseName = exprTableSource.getSchema();
        if (StringUtils.isEmpty(databaseName)) {
            databaseName = currentDatabaseName;
        }
        Database database = databaseMap.get(databaseName);
        if (database == null) {
            throw new RuntimeException("database not exist");
        }
        return database;
    }

    //单例模式
    private DatabaseContainer() {
        databaseMap = new HashMap<>();
        databaseMap.put(DEFAULT_DATA_BASE_NAME, new Database("default"));
        currentDatabaseName = DEFAULT_DATA_BASE_NAME;
    }

    public static DatabaseContainer getInstance() {
        return DatabaseContainerHolder.INSTANCE;
    }

    private static class DatabaseContainerHolder {
        private static final DatabaseContainer INSTANCE = new DatabaseContainer();
    }
}
