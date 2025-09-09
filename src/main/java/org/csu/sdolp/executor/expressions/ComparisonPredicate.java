package org.csu.sdolp.executor.expressions;

import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;

/**
 * 一个增强的比较谓词，用于检查 "列 op 值" 是否成立。
 */
public class ComparisonPredicate extends AbstractPredicate {
    private final int columnIndex;
    private final Value value;
    private final String operator; // 新增：用于存储运算符类型

    /**
     * @param columnIndex 要比较的列的索引 (从0开始)
     * @param value       要比较的值
     * @param operator    比较运算符 (例如 "EQUAL", "GREATER")
     */
    public ComparisonPredicate(int columnIndex, Value value, String operator) {
        this.columnIndex = columnIndex;
        this.value = value;
        this.operator = operator;
    }

    @Override
    public boolean evaluate(Tuple tuple) {
        Value tupleValue = tuple.getValues().get(columnIndex);

        // 类型不匹配则直接返回 false
        if (tupleValue.getType() != value.getType()) {
            return false;
        }

        Comparable v1 = (Comparable) tupleValue.getValue();
        Comparable v2 = (Comparable) value.getValue();
        int cmp = v1.compareTo(v2);

        // 根据构造时传入的运算符进行判断
        return switch (operator) {
            case "EQUAL" -> cmp == 0;
            case "NOT_EQUAL" -> cmp != 0;
            case "GREATER" -> cmp > 0;
            case "GREATER_EQUAL" -> cmp >= 0;
            case "LESS" -> cmp < 0;
            case "LESS_EQUAL" -> cmp <= 0;
            default -> throw new UnsupportedOperationException("Unsupported operator in ComparisonPredicate: " + operator);
        };
    }
}