package org.csu.sdolp.executor.expressions;

import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;

/**
 * 一个简单的比较谓词，用于检查 "列 = 值" 是否成立。
 */
public class ComparisonPredicate extends AbstractPredicate {
    private final int columnIndex;
    private final Value value;

    /**
     * @param columnIndex 要比较的列的索引 (从0开始)
     * @param value       要比较的值
     */
    public ComparisonPredicate(int columnIndex, Value value) {
        this.columnIndex = columnIndex;
        this.value = value;
    }

    @Override
    public boolean evaluate(Tuple tuple) {
        // 注意：这是一个简化的比较，实际数据库需要处理不同类型间的比较
        return tuple.getValues().get(columnIndex).getValue().equals(value.getValue());
    }
}