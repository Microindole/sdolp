package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.query.SortPlanNode;
import org.csu.sdolp.executor.TupleIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 排序执行器。
 * 这是一个阻塞执行器，它会先拉取所有子节点的元组，在内存中完成排序，然后再向上层返回。
 */
public class SortExecutor implements TupleIterator {

    private final TupleIterator child;
    private final SortPlanNode plan;
    private List<Tuple> sortedTuples;
    private int cursor = 0;

    public SortExecutor(TupleIterator child, SortPlanNode plan) {
        this.child = child;
        this.plan = plan;
    }

    /**
     * 初始化方法，执行排序
     */
    private void init() throws IOException {
        if (sortedTuples != null) {
            return;
        }

        // 1. 从子执行器拉取所有元组
        sortedTuples = new ArrayList<>();
        while (child.hasNext()) {
            sortedTuples.add(child.next());
        }

        // 2. 确定排序列的索引
        Schema childSchema = plan.getChild().getOutputSchema();
        String colName = plan.getOrderBy().column().getName();
        int colIndex = -1;
        for (int i = 0; i < childSchema.getColumns().size(); i++) {
            if (childSchema.getColumns().get(i).getName().equalsIgnoreCase(colName)) {
                colIndex = i;
                break;
            }
        }
        if (colIndex == -1) {
            throw new IllegalStateException("Column '" + colName + "' not found in schema for sorting.");
        }
        final int finalColIndex = colIndex;

        // 3. 定义比较器并排序
        Comparator<Tuple> comparator = (t1, t2) -> {
            Value v1 = t1.getValues().get(finalColIndex);
            Value v2 = t2.getValues().get(finalColIndex);
            if (v1 == null || v1.getValue() == null) return -1;
            if (v2 == null || v2.getValue() == null) return 1;
            return ((Comparable) v1.getValue()).compareTo(v2.getValue());
        };

        if (!plan.getOrderBy().isAscending()) {
            comparator = comparator.reversed();
        }

        sortedTuples.sort(comparator);
    }


    @Override
    public Tuple next() throws IOException {
        init(); // 确保数据已排序
        if (hasNext()) {
            return sortedTuples.get(cursor++);
        }
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        init(); // 确保数据已排序
        return cursor < sortedTuples.size();
    }
    @Override
    public Schema getOutputSchema() {
        return child.getOutputSchema();
    }
}