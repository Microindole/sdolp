package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hidyouth
 * @description: 投影执行器，根据指定的列索引，从子执行器返回的元组中提取列。
 */
public class ProjectExecutor implements TupleIterator {
    private final TupleIterator child;
    private final List<Integer> columnIndexes;

    /**
     * @param child          子执行器 (如 FilterExecutor 或 SeqScanExecutor)
     * @param columnIndexes  需要保留的列的索引列表
     */
    public ProjectExecutor(TupleIterator child, List<Integer> columnIndexes) {
        this.child = child;
        this.columnIndexes = columnIndexes;
    }

    @Override
    public Tuple next() throws IOException {
        if (!hasNext()) {
            return null;
        }
        
        Tuple originalTuple = child.next();
        if (originalTuple == null) {
            return null;
        }

        List<Value> projectedValues = new ArrayList<>();
        for (Integer index : columnIndexes) {
            projectedValues.add(originalTuple.getValues().get(index));
        }
        
        return new Tuple(projectedValues);
    }

    @Override
    public boolean hasNext() throws IOException {
        return child.hasNext();
    }
}