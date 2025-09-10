package org.csu.sdolp.engine;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import java.util.List;

/**
 * 封装一次查询执行的所有结果信息。
 */
public record QueryResult(
        String message,         // 成功/失败/影响行数等信息
        Schema schema,          // 结果集的模式 (用于打印表头)
        List<Tuple> results     // 结果集元组列表
) {
    // 静态工厂方法，用于非 SELECT 语句的成功返回
    public static QueryResult newSuccessResult(String message) {
        return new QueryResult(message, null, List.of());
    }

    // 静态工厂方法，用于 SELECT 语句的成功返回
    public static QueryResult newSelectResult(Schema schema, List<Tuple> results) {
        String message = results.size() + " rows returned.";
        return new QueryResult(message, schema, results);
    }
}
