package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.parser.ast.expression.AggregateExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.planner.plan.query.AggregatePlanNode;
import org.csu.sdolp.executor.TupleIterator;

import java.io.IOException;
import java.util.*;

/**
 * 聚合执行器 (重构版).
 * 这是一个阻塞执行器，它会拉取所有子节点的元组，在内存中进行哈希聚合，然后再向上层返回。
 */
public class AggregateExecutor implements TupleIterator {

    private final TupleIterator child;
    private final AggregatePlanNode plan;
    private final Schema childSchema;

    private Iterator<Tuple> resultIterator;
    private boolean isInitialized = false;

    // 用于存储 AVG 的中间结果 (sum, count)
    private static class AvgPair {
        int sum = 0;
        int count = 0;
    }

    public AggregateExecutor(TupleIterator child, AggregatePlanNode plan) {
        this.child = child;
        this.plan = plan;
        this.childSchema = child.getOutputSchema();
    }

    private void init() throws IOException {
        if (isInitialized) return;

        // Key: 分组键 (List<Value>), Value: 中间聚合结果 (Object[])
        Map<List<Value>, Object[]> aggregationMap = new HashMap<>();

        while (child.hasNext()) {
            Tuple tuple = child.next();
            List<Value> groupByKey = createGroupByKey(tuple);

            // 如果哈希表中没有这个 key，则初始化聚合值的数组
            aggregationMap.computeIfAbsent(groupByKey, k -> {
                Object[] initialValues = new Object[plan.getAggregates().size()];
                for (int i = 0; i < initialValues.length; i++) {
                    AggregateExpressionNode agg = plan.getAggregates().get(i);
                    switch (agg.functionName().toUpperCase()) {
                        case "AVG" -> initialValues[i] = new AvgPair();
                        case "MIN" -> initialValues[i] = Integer.MAX_VALUE;
                        case "MAX" -> initialValues[i] = Integer.MIN_VALUE;
                        default -> initialValues[i] = 0; // COUNT, SUM
                    }
                }
                return initialValues;
            });

            // 更新聚合值
            Object[] currentAggValues = aggregationMap.get(groupByKey);
            for (int i = 0; i < plan.getAggregates().size(); i++) {
                AggregateExpressionNode agg = plan.getAggregates().get(i);

                // 修复点: 正确获取列名并从 tuple 中取值
                Value tupleVal = agg.isStar() ?
                        new Value(1) :
                        getColumnValue(tuple, ((IdentifierNode) agg.argument()).getName());

                int intValue = (tupleVal != null && tupleVal.getValue() instanceof Integer) ? (Integer) tupleVal.getValue() : 0;

                switch (agg.functionName().toUpperCase()) {
                    case "COUNT" -> currentAggValues[i] = (int) currentAggValues[i] + 1;
                    case "SUM" -> currentAggValues[i] = (int) currentAggValues[i] + intValue;
                    case "AVG" -> {
                        AvgPair pair = (AvgPair) currentAggValues[i];
                        pair.sum += intValue;
                        pair.count++;
                    }
                    case "MIN" -> currentAggValues[i] = Math.min((int) currentAggValues[i], intValue);
                    case "MAX" -> currentAggValues[i] = Math.max((int) currentAggValues[i], intValue);
                }
            }
        }

        // --- 构建最终结果 ---
        List<Tuple> finalResults = new ArrayList<>();
        for (Map.Entry<List<Value>, Object[]> entry : aggregationMap.entrySet()) {
            List<Value> resultValues = new ArrayList<>(entry.getKey()); // 先加入分组键
            Object[] aggRawResults = entry.getValue();

            for (int i=0; i < aggRawResults.length; i++) {
                Object rawResult = aggRawResults[i];
                if (rawResult instanceof AvgPair pair) {
                    resultValues.add(new Value(pair.count == 0 ? 0 : pair.sum / pair.count));
                } else {
                    resultValues.add(new Value((Integer) rawResult));
                }
            }
            finalResults.add(new Tuple(resultValues));
        }

        this.resultIterator = finalResults.iterator();
        this.isInitialized = true;
    }

    @Override
    public Tuple next() throws IOException {
        if (!hasNext()) return null;
        return resultIterator.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        init();
        return resultIterator.hasNext();
    }

    @Override
    public Schema getOutputSchema() {
        return plan.getOutputSchema();
    }

    private List<Value> createGroupByKey(Tuple tuple) {
        List<IdentifierNode> groupBys = plan.getGroupBys();
        if (groupBys == null || groupBys.isEmpty()) {
            return Collections.emptyList(); // 对于没有GROUP BY的聚合（如SELECT SUM(*)），返回一个固定的空列表作为Key
        }

        List<Value> key = new ArrayList<>();
        for (IdentifierNode groupByCol : groupBys) {
            key.add(getColumnValue(tuple, groupByCol.getName()));
        }
        return key;
    }

    private Value getColumnValue(Tuple tuple, String columnName) {
        int colIndex = childSchema.getColumnIndex(columnName);
        return tuple.getValues().get(colIndex);
    }
}