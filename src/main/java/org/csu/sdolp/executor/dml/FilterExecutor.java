package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.executor.expressions.AbstractPredicate;
import java.io.IOException;

/**
 * 过滤执行器，根据谓词条件过滤来自子执行器的元组。
 */
public class FilterExecutor implements TupleIterator {
    private final TupleIterator child;
    private final AbstractPredicate predicate;
    private Tuple nextTuple;

    public FilterExecutor(TupleIterator child, AbstractPredicate predicate) {
        this.child = child;
        this.predicate = predicate;
        this.nextTuple = null;
    }

    @Override
    public Tuple next() throws IOException {
        // 如果 hasNext() 已经预取了元组，则直接返回
        if (this.nextTuple != null) {
            Tuple result = this.nextTuple;
            this.nextTuple = null;
            return result;
        }
        // 调用 hasNext() 来查找下一个匹配的元组
        if (hasNext()) {
            Tuple result = this.nextTuple;
            this.nextTuple = null;
            return result;
        }
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        // 如果已经有一个预取的元组，直接返回 true
        if (this.nextTuple != null) {
            return true;
        }
        // 不断从子执行器获取元组，直到找到一个匹配的或子执行器结束
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if (predicate.evaluate(tuple)) {
                this.nextTuple = tuple; // 预取并存储匹配的元组
                return true;
            }
        }
        return false;
    }

    @Override
    public Schema getOutputSchema() {
        return child.getOutputSchema();
    }
}