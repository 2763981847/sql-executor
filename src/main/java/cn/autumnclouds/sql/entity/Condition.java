package cn.autumnclouds.sql.entity;

import java.util.function.Predicate;

/**
 * 表示数据筛选条件的类。
 *
 * @param <T> 条件比较的值的类型
 * @author Oreki
 * @since 2023/5/30
 */
public class Condition<T> {
    private int index;                          // 要比较的列的索引
    private Predicate<T> predicate;             // 表示条件的谓词

    /**
     * 创建一个新的 Condition 对象。
     *
     * @param index     要比较的列的索引
     * @param predicate 表示条件的谓词
     */
    public Condition(int index, Predicate<T> predicate) {
        this.index = index;
        this.predicate = predicate;
    }

    /**
     * 获取要比较的列的索引。
     *
     * @return 要比较的列的索引
     */
    public int getIndex() {
        return index;
    }

    /**
     * 获取表示条件的谓词。
     *
     * @return 表示条件的谓词
     */
    public Predicate<T> getPredicate() {
        return predicate;
    }

}
