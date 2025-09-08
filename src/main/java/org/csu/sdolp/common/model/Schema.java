package org.csu.sdolp.common.model;

import java.util.List;

/**
 * 定义表的模式，包含列的定义。
 */
public class Schema {
    private final List<Column> columns;

    public Schema(List<Column> columns) {
        this.columns = columns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public int getTupleLength() {
        // 对于定长类型，可以直接计算长度，此处简化处理，动态计算
        return columns.size();
    }
}