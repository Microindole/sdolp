package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.executor.expressions.AbstractPredicate;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;

public class SeqScanExecutor implements TupleIterator {

    private final TableHeap tableHeap;
    //private boolean isInitialized = false;
    private final AbstractPredicate predicate;
    private Tuple nextTuple;

    // *** 构造函数直接接收TableHeap ***
    public SeqScanExecutor(TableHeap tableHeap, Transaction txn, AbstractPredicate predicate) throws IOException {
        this.tableHeap = tableHeap;
        this.predicate = predicate;
        this.tableHeap.initIterator(txn);
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

        // 循环直到找到一个匹配的元组或迭代结束
        while (tableHeap.hasNext()) {
            Tuple tuple = tableHeap.next();
            // 如果没有谓词 (SELECT *) 或者谓词求值为 true，则找到下一个元组
            if (predicate == null || predicate.evaluate(tuple)) {
                this.nextTuple = tuple; // 预取并存储匹配的元组
                return true;
            }
        }
        // 没有找到更多匹配的元组
        return false;
    }
    @Override
    public Schema getOutputSchema() {
        return tableHeap.getOutputSchema();
    }


}