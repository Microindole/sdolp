package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteExecutor implements TupleIterator {
    private final TupleIterator child;
    private final TableHeap tableHeap;
    private boolean done = false;

    public DeleteExecutor(TupleIterator child, TableHeap tableHeap) {
        this.child = child;
        this.tableHeap = tableHeap;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        List<RID> ridsToDelete = new ArrayList<>();
        while (child.hasNext()) {
            ridsToDelete.add(child.next().getRid());
        }
        // 阶段二：根据收集到的 RID 列表执行删除
        int deletedCount = 0;
        for (RID rid : ridsToDelete) {
            if (tableHeap.deleteTuple(rid)) {
                deletedCount++;
            }
        }
        done = true;
        // 返回一个包含删除数量的元组
        return new Tuple(Collections.singletonList(new Value(deletedCount)));
    }

    @Override
    public boolean hasNext() throws IOException {
        return !done;
    }
}