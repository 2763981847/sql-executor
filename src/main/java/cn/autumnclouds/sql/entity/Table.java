package cn.autumnclouds.sql.entity;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.statement.*;
import cn.autumnclouds.sql.util.ConvertUtils;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Oreki
 * @since 2023/5/29
 */
public class Table {
    private final String tableName;
    private final List<SQLColumnDefinition> columnDefinitions;

    private final Map<String, Integer> nameToIndex;
    private final List<List<Object>> data;

    public Table(String tableName, List<SQLColumnDefinition> columnDefinitions) {
        this.tableName = tableName;
        this.columnDefinitions = columnDefinitions;
        this.nameToIndex = new HashMap<>(columnDefinitions.size());
        for (int i = 0; i < columnDefinitions.size(); i++) {
            nameToIndex.put(columnDefinitions.get(i).getColumnName(), i);
        }
        this.data = new LinkedList<>();
    }

    public String getTableName() {
        return tableName;
    }

    public List<SQLColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public List<List<Object>> getData() {
        return data;
    }

    public void insertRow(List<String> columnNames, List<SQLInsertStatement.ValuesClause> valuesList) {
        valuesList.forEach(valuesClause -> insertRow(columnNames, valuesClause));
    }

    private void insertRow(List<String> columnNames, SQLInsertStatement.ValuesClause valuesClause) {
        int[] projection = getProjection(columnNames);
        List<SQLExpr> values = valuesClause.getValues();
        List<Object> row = new ArrayList<>(Collections.nCopies(projection.length, null));
        for (int i = 0; i < projection.length; i++) {
            int index = projection[i];
            SQLExpr value = values.get(index);
            String columnName = columnNames.get(index);
            Object convertedValue = ConvertUtils.convertValue(columnName, value.toString());
            row.set(i, convertedValue);
        }
        data.add(row);
    }

    public <T> void deleteRow(SQLExpr where) {
        Condition<Comparable<T>> condition = handleWhere(where);
        data.removeIf(row -> condition.getPredicate().test((Comparable<T>) row.get(condition.getIndex())));
    }

    public <T> void updateRow(List<SQLUpdateSetItem> updateSetItems, SQLExpr where) {
        Condition<Comparable<T>> condition = handleWhere(where);
        data.stream().filter(row -> condition.getPredicate().test((Comparable<T>) row.get(condition.getIndex())))
                .forEach(row -> updateRow(row, updateSetItems));
    }

    private void updateRow(List<Object> row, List<SQLUpdateSetItem> updateSetItems) {
        updateSetItems.forEach(updateSetItem -> updateItem(row, updateSetItem));
    }

    private void updateItem(List<Object> row, SQLUpdateSetItem updateSetItem) {
        String columnName = updateSetItem.getColumn().toString();
        Object value = ConvertUtils.convertValue(getColumnType(columnName), updateSetItem.getValue().toString());
        row.set(getIndex(columnName), value);
    }

    public <T> void selectRow(List<SQLSelectItem> selectList, SQLExpr where, SQLOrderBy orderBy) {
        List<String> columnNames = selectList.stream().map(SQLSelectItem::toString).collect(Collectors.toList());
        int[] projection = getProjection(columnNames);
        Condition<Comparable<T>> condition = handleWhere(where);
        List<List<Object>> result = data.stream().filter(row -> condition.getPredicate().test((Comparable<T>) row.get(condition.getIndex())))
                .map(row -> project(row, projection)).sorted((row1, row2) -> sortHelper(row1, row2, orderBy)).collect(Collectors.toList());
        System.out.println(result);
    }

    private int sortHelper(List<Object> row1, List<Object> row2, SQLOrderBy orderBy) {
        // 无排序条件，保持原有顺序
        if (orderBy == null) {
            return 0;
        }
        List<SQLSelectOrderByItem> sqlSelectOrderByItems = orderBy.getItems();
        for (SQLSelectOrderByItem sqlSelectOrderByItem : sqlSelectOrderByItems) {
            String columnName = sqlSelectOrderByItem.getExpr().toString();
            SQLOrderingSpecification type = sqlSelectOrderByItem.getType();
            int index = getIndex(columnName);
            int compare;
            if (type.equals(SQLOrderingSpecification.ASC)) {
                compare = ((Comparable<Object>) row1.get(index)).compareTo(row2.get(index));
            } else {
                compare = ((Comparable<Object>) row2.get(index)).compareTo(row1.get(index));
            }
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private List<Object> project(List<Object> row, int[] projection) {
        return Arrays.stream(projection).mapToObj(row::get).collect(Collectors.toList());
    }

    // 处理where条件
    private <T> Condition<Comparable<T>> handleWhere(SQLExpr where) {
        // 无where条件
        if (where == null) {
            return new Condition<>(0, t -> true);
        }
        // 仅支持单个二元运算条件
        if (!(where instanceof SQLBinaryOpExpr)
                || !(((SQLBinaryOpExpr) where).getLeft() instanceof SQLIdentifierExpr)
                || !(((SQLBinaryOpExpr) where).getRight() instanceof SQLLiteralExpr)) {
            throw new RuntimeException("unsupported where condition: " + where);
        }
        SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) where;
        SQLIdentifierExpr left = (SQLIdentifierExpr) binaryOpExpr.getLeft();
        SQLLiteralExpr right = (SQLLiteralExpr) binaryOpExpr.getRight();
        String columnName = left.getName();
        SQLBinaryOperator operator = binaryOpExpr.getOperator();
        Object value = ConvertUtils.convertValue(getColumnType(columnName), right.toString());
        return new Condition<>(getIndex(columnName), getPredicate(operator, (T) value));
    }

    private static <T> Predicate<Comparable<T>> getPredicate(SQLBinaryOperator operator, T value) {
        if (operator.equals(SQLBinaryOperator.Equality)) {
            return comparable -> comparable.compareTo(value) == 0;
        } else if (operator.equals(SQLBinaryOperator.NotEqual)) {
            return comparable -> comparable.compareTo(value) != 0;
        } else if (operator.equals(SQLBinaryOperator.GreaterThan)) {
            return comparable -> comparable.compareTo(value) > 0;
        } else if (operator.equals(SQLBinaryOperator.GreaterThanOrEqual)) {
            return comparable -> comparable.compareTo(value) >= 0;
        } else if (operator.equals(SQLBinaryOperator.LessThan)) {
            return comparable -> comparable.compareTo(value) < 0;
        } else if (operator.equals(SQLBinaryOperator.LessThanOrEqual)) {
            return comparable -> comparable.compareTo(value) <= 0;
        } else {
            throw new RuntimeException("unsupported operator: " + operator);
        }
    }

    private String getColumnType(String columnName) {
        Integer index = nameToIndex.get(columnName);
        if (index == null) {
            throw new RuntimeException("no such column named " + columnName + " in table " + tableName);
        }
        return columnDefinitions.get(index).getDataType().getName();
    }

    private int getIndex(String columnName) {
        Integer index = nameToIndex.get(columnName);
        if (index == null) {
            throw new RuntimeException("no such column named " + columnName + " in table " + tableName);
        }
        return index;
    }

    private int[] getProjection(List<String> columnNames) {
        if (columnNames.isEmpty() || "*".equals(columnNames.get(0).trim())) {
            return IntStream.range(0, columnDefinitions.size()).toArray();
        }
        int[] project = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Integer index = nameToIndex.get(columnName);
            if (index == null) {
                throw new RuntimeException("no such column named " + columnName + " in table " + tableName);
            }
            project[i] = index;
        }
        return project;
    }


}
