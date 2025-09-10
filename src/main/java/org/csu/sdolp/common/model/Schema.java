package org.csu.sdolp.common.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public List<String> getColumnNames() {
        // 使用 Java Stream API 可以非常简洁地实现
        return columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeInt(columns.size());
        for (Column col : columns) {
            col.write(out);
        }
    }

    public static Schema read(DataInputStream in) throws IOException {
        int numColumns = in.readInt();
        List<Column> columns = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            columns.add(Column.read(in));
        }
        return new Schema(columns);
    }
}