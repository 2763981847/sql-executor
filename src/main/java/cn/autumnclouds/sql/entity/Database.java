package cn.autumnclouds.sql.entity;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.statement.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库类，表示一个数据库对象。
 *
 * @author Oreki
 * @since 2023/5/29
 */
public class Database {
    private final String databaseName;        // 数据库名称
    private final Map<String, Table> tables;  // 数据库中的表集合

    /**
     * 创建一个新的 Database 对象。
     *
     * @param databaseName 数据库名称
     */
    public Database(String databaseName) {
        this.databaseName = databaseName;
        tables = new HashMap<>();
    }

    /**
     * 创建表格。
     *
     * @param tableName          表格名称
     * @param columnDefinitions  列定义列表
     */
    public void creatTable(String tableName, List<SQLColumnDefinition> columnDefinitions) {
        tables.put(tableName, new Table(tableName, columnDefinitions));
    }

    /**
     * 删除表格。
     *
     * @param tableName  要删除的表格名称
     */
    public void dropTable(String tableName) {
        tables.remove(tableName);
    }

    /**
     * 向表格中插入行。
     *
     * @param tableName      表格名称
     * @param columnNames    列名称列表
     * @param valuesList     值列表
     */
    public void insertRow(String tableName, List<String> columnNames, List<SQLInsertStatement.ValuesClause> valuesList) {
        Table table = getTable(tableName);
        table.insertRow(columnNames, valuesList);
    }

    /**
     * 删除表格中的行。
     *
     * @param tableName  表格名称
     * @param where      删除条件
     */
    public void deleteRow(String tableName, SQLExpr where) {
        Table table = getTable(tableName);
        table.deleteRow(where);
    }

    /**
     * 更新表格中的行。
     *
     * @param tableName       表格名称
     * @param updateSetItems  更新项列表
     * @param where           更新条件
     */
    public void updateRow(String tableName, List<SQLUpdateSetItem> updateSetItems, SQLExpr where) {
        Table table = getTable(tableName);
        table.updateRow(updateSetItems, where);
    }

    /**
     * 查询表格中的行。
     *
     * @param tableName    表格名称
     * @param selectList   查询项列表
     * @param where        查询条件
     * @param orderBy      排序规则
     */
    public void selectRow(String tableName, List<SQLSelectItem> selectList, SQLExpr where, SQLOrderBy orderBy) {
        Table table = getTable(tableName);
        table.selectRow(selectList, where, orderBy);
    }

    /**
     * 获取指定名称的表格对象。
     *
     * @param tableName  表格名称
     * @return           表格对象
     * @throws RuntimeException 若表格不存在时抛出异常
     */
    private Table getTable(String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new RuntimeException("table not exist");
        }
        return table;
    }
}
