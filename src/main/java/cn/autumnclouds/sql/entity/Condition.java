package cn.autumnclouds.sql.entity;

import java.util.PrimitiveIterator;
import java.util.function.Predicate;

/**
 * @author Oreki
 * @since 2023/5/30
 */
public class Condition<T> {
    private int index;
    private Predicate<T> predicate;

    public Condition(int index, Predicate<T> predicate) {
        this.index = index;
        this.predicate = predicate;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Predicate<T> getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate<T> predicate) {
        this.predicate = predicate;
    }
}
