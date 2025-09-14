package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.executor.TupleIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 投影执行器，根据指定的列索引，从子执行器返回的元组中提取列。
 */
public class ProjectExecutor implements TupleIterator {
    private final TupleIterator child;
    private final List<Integer> columnIndexes;
    private final Schema outputSchema;

    /**
     * @param child          子执行器 (如 FilterExecutor 或 SeqScanExecutor)
     * @param columnIndexes  需要保留的列的索引列表
     */
    public ProjectExecutor(TupleIterator child, List<Integer> columnIndexes) {
        this.child = child;
        this.columnIndexes = columnIndexes;
        Schema inputSchema = child.getOutputSchema();
        List<Column> projectedColumns = new ArrayList<>();
        for (int index : columnIndexes) {
            projectedColumns.add(inputSchema.getColumns().get(index));
        }
        this.outputSchema = new Schema(projectedColumns);
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

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

}