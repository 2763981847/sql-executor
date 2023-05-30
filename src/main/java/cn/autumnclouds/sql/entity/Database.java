package cn.autumnclouds.sql.entity;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.statement.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Oreki
 * @since 2023/5/29
 */
public class Database {
    private final String databaseName;
    private final Map<String, Table> tables;

    public Database(String databaseName) {
        this.databaseName = databaseName;
        tables = new HashMap<>();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Map<String, Table> getTables() {
        return tables;
    }

    public void creatTable(String tableName, List<SQLColumnDefinition> columnDefinitions) {
        tables.put(tableName, new Table(tableName, columnDefinitions));
    }

    public void dropTable(String tableName) {
        tables.remove(tableName);
    }

    public void insertRow(String tableName, List<String> columnNames, List<SQLInsertStatement.ValuesClause> valuesList) {
        Table table = getTable(tableName);
        table.insertRow(columnNames, valuesList);
    }

    public void deleteRow(String tableName, SQLExpr where) {
        Table table = getTable(tableName);
        table.deleteRow(where);
    }

    public void updateRow(String tableName, List<SQLUpdateSetItem> updateSetItems, SQLExpr where) {
        Table table = getTable(tableName);
        table.updateRow(updateSetItems, where);
    }

    public void selectRow(String tableName, List<SQLSelectItem> selectList, SQLExpr where, SQLOrderBy orderBy) {
        Table table = getTable(tableName);
        table.selectRow(selectList, where, orderBy);
    }

    private Table getTable(String tableName) {
        Table table = tables.get(tableName);
        if (table == null) {
            throw new RuntimeException("table not exist");
        }
        return table;
    }

}

