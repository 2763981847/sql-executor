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
 * 数据库容器类，用于管理多个数据库对象。
 *
 * @author Oreki
 * @since 2023/5/29
 */
public class DatabaseContainer {
    private final Map<String, Database> databaseMap;  // 数据库对象映射

    private static final String DEFAULT_DATA_BASE_NAME = "default";  // 默认数据库名称
    private String currentDatabaseName;  // 当前数据库名称

    /**
     * 切换使用指定名称的数据库。
     *
     * @param databaseName  数据库名称
     */
    public void useDatabase(String databaseName) {
        currentDatabaseName = databaseName;
    }

    /**
     * 创建数据库。
     *
     * @param databaseName  数据库名称
     */
    public void createDatabase(String databaseName) {
        databaseMap.put(databaseName, new Database(databaseName));
    }

    /**
     * 删除数据库。
     *
     * @param databaseName  数据库名称
     */
    public void dropDatabase(String databaseName) {
        databaseMap.remove(databaseName);
    }

    /**
     * 创建表格。
     *
     * @param tableSource          表格源
     * @param columnDefinitions    列定义列表
     */
    public void createTable(SQLExprTableSource tableSource, List<SQLColumnDefinition> columnDefinitions) {
        Database database = getDatabase(tableSource);
        String tableName = tableSource.getTableName();
        database.creatTable(tableName, columnDefinitions);
    }

    /**
     * 删除表格。
     *
     * @param tableSources  表格源列表
     */
    public void dropTable(List<SQLExprTableSource> tableSources) {
        for (SQLExprTableSource tableSource : tableSources) {
            Database database = getDatabase(tableSource);
            String tableName = tableSource.getTableName();
            database.dropTable(tableName);
        }
    }

    /**
     * 向表格中插入行。
     *
     * @param tableSource   表格源
     * @param columnNames   列名称列表
     * @param valuesList    值列表
     */
    public void insertRow(SQLExprTableSource tableSource, List<String> columnNames, List<SQLInsertStatement.ValuesClause> valuesList) {
        Database database = getDatabase(tableSource);
        String tableName = tableSource.getTableName();
        database.insertRow(tableName, columnNames, valuesList);
    }

    /**
     * 删除表格中的行。
     *
     * @param exprTableSource  表格源
     * @param where            删除条件
     */
    public void deleteRow(SQLExprTableSource exprTableSource, SQLExpr where) {
        Database database = getDatabase(exprTableSource);
        String tableName = exprTableSource.getTableName();
        database.deleteRow(tableName, where);
    }

    /**
     * 更新表格中的行。
     *
     * @param exprTableSource   表格源
     * @param updateSetItems    更新项列表
     * @param where             更新条件
     */
    public void updateRow(SQLExprTableSource exprTableSource, List<SQLUpdateSetItem> updateSetItems, SQLExpr where) {
        Database database = getDatabase(exprTableSource);
        database.updateRow(exprTableSource.getTableName(), updateSetItems, where);
    }

    /**
     * 查询表格中的行。
     *
     * @param tableSource    表格源
     * @param selectList     查询项列表
     * @param where          查询条件
     * @param orderBy        排序规则
     */
    public void selectRow(SQLExprTableSource tableSource, List<SQLSelectItem> selectList, SQLExpr where, SQLOrderBy orderBy) {
        Database database = getDatabase(tableSource);
        String tableName = tableSource.getTableName();
        database.selectRow(tableName, selectList, where, orderBy);
    }

    /**
     * 根据表格源获取对应的数据库对象。
     *
     * @param exprTableSource  表格源
     * @return                 数据库对象
     * @throws RuntimeException 若数据库不存在时抛出异常
     */
    private Database getDatabase(SQLExprTableSource exprTableSource) {
        String databaseName = exprTableSource.getSchema();
        if (StringUtils.isEmpty(databaseName)) {
            databaseName = currentDatabaseName;
        }
        Database database = databaseMap.get(databaseName);
        if (database == null) {
            throw new RuntimeException("database named " + databaseName + " does not exist");
        }
        return database;
    }

    // 单例模式
    private DatabaseContainer() {
        databaseMap = new HashMap<>();
        databaseMap.put(DEFAULT_DATA_BASE_NAME, new Database(DEFAULT_DATA_BASE_NAME));
        currentDatabaseName = DEFAULT_DATA_BASE_NAME;
    }

    /**
     * 获取 DatabaseContainer 的实例。
     *
     * @return DatabaseContainer 的实例
     */
    public static DatabaseContainer getInstance() {
        return DatabaseContainerHolder.INSTANCE;
    }

    private static class DatabaseContainerHolder {
        private static final DatabaseContainer INSTANCE = new DatabaseContainer();
    }
}
