package org.csu.sdolp.cli.tool;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 一个可重用的工具类，用于将元组列表格式化为美观的、带边框的控制台表格。
 */
public class QueryResultFormatter {

    /**
     * 将给定的 Schema 和元组列表格式化为字符串表格。
     *
     * @param schema  结果集的模式 (用于打印表头)
     * @param results 结果集元组列表
     * @return 格式化后的表格字符串
     */
    public static String format(Schema schema, List<Tuple> results) {
        if (schema == null || schema.getColumns().isEmpty()) {
            if (results.size() == 1) {
                return results.get(0).getValues().get(0).toString();
            }
            return "Query OK, " + results.size() + " rows affected.";
        }

        if (results.isEmpty()) {
            return "Query finished, 0 rows returned.";
        }

        StringBuilder sb = new StringBuilder();
        List<String> columnNames = schema.getColumnNames();
        List<Integer> columnWidths = new ArrayList<>();

        // 1. 计算每列的最大宽度 (处理 null 值)
        for (int i = 0; i < columnNames.size(); i++) {
            int maxWidth = columnNames.get(i).length();
            for (Tuple tuple : results) {
                // 确保 getValues() 的索引在安全范围内
                if (i < tuple.getValues().size()) {
                    Object value = tuple.getValues().get(i).getValue();
                    String cellValue = (value == null) ? "NULL" : value.toString();
                    maxWidth = Math.max(maxWidth, cellValue.length());
                }
            }
            columnWidths.add(maxWidth);
        }

        // 2. 打印边框和表头
        sb.append(getSeparator(columnWidths)).append("\n");
        sb.append(getRow(columnNames, columnWidths)).append("\n");
        sb.append(getSeparator(columnWidths)).append("\n");

        // 3. 打印数据行 (处理 null 值)
        for (Tuple tuple : results) {
            List<String> values = tuple.getValues().stream()
                    .map(v -> (v.getValue() == null) ? "NULL" : v.getValue().toString())
                    .collect(Collectors.toList());
            sb.append(getRow(values, columnWidths)).append("\n");
        }

        // 4. 打印底部边框和最终消息
        sb.append(getSeparator(columnWidths)).append("\n");
        sb.append("Query finished, ").append(results.size()).append(" rows returned.");

        return sb.toString();
    }

    private static String getRow(List<String> cells, List<Integer> widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < cells.size(); i++) {
            sb.append(String.format(" %-" + widths.get(i) + "s |", cells.get(i)));
        }
        return sb.toString();
    }

    private static String getSeparator(List<Integer> widths) {
        StringBuilder sb = new StringBuilder("+");
        for (Integer width : widths) {
            sb.append("-".repeat(width + 2)).append("+");
        }
        return sb.toString();
    }
}