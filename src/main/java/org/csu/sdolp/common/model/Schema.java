package org.csu.sdolp.common.model;

import lombok.Getter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 定义表的模式，包含列的定义。
 */
@Getter
public class Schema {
    private final List<Column> columns;
    private String primaryKeyColumnName;

    public Schema(List<Column> columns) {
        this.columns = columns;
    }
    public Schema(List<Column> columns, String primaryKeyColumnName) {
        this(columns);
        this.primaryKeyColumnName = primaryKeyColumnName;
    }

    public int getTupleLength() {
        return columns.size();
    }

    public List<String> getColumnNames() {
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
    /**
     * 新增方法：根据列名获取列对象（忽略大小写）。
     * @param columnName 要查找的列名
     * @return 匹配的 Column 对象
     * @throws IllegalArgumentException 如果找不到对应的列
     */
    public Column getColumn(String columnName) {
        return columns.stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Column '" + columnName + "' not found in schema."));
    }
    /**
     * 新增方法：根据列名获取其索引（忽略大小写）。
     * @param columnName 要查找的列名
     * @return 列的索引 (从0开始)
     * @throws IllegalArgumentException 如果找不到对应的列
     */
    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column '" + columnName + "' not found in schema.");
    }

}