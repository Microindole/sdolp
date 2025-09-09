package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


public class TableHeap implements TupleIterator {

    private final BufferPoolManager bufferPoolManager;
    private final TableInfo tableInfo;
    private final Schema schema;

    // --- 迭代器状态 ---
    private PageId currentPageId;
    private Iterator<Tuple> currentPageTupleIterator;

    public TableHeap(BufferPoolManager bufferPoolManager, TableInfo tableInfo) throws IOException {
        this.bufferPoolManager = bufferPoolManager;
        this.tableInfo = tableInfo;
        this.schema = tableInfo.getSchema();
        
        // 初始化迭代器
        this.currentPageId = tableInfo.getFirstPageId();
        loadPageIterator(currentPageId);
    }

    /**
     * 向表中插入一条元组
     * @param tuple 要插入的元组
     * @return 插入成功返回 true
     */
    public boolean insertTuple(Tuple tuple) throws IOException {
        PageId pid = tableInfo.getFirstPageId(); // 简化实现：总是从第一页尝试插入
        Page page = bufferPoolManager.getPage(pid);
        
        // TODO: 实际应遍历所有页，找到有空间的页，或在所有页都满时分配新页
        boolean success = page.insertTuple(tuple);
        bufferPoolManager.flushPage(pid); // 确保写入持久化
        return success;
    }


    @Override
    public Tuple next() throws IOException {
        if (!hasNext()) {
            return null;
        }
        return currentPageTupleIterator.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (currentPageTupleIterator == null) {
            return false;
        }
        if (currentPageTupleIterator.hasNext()) {
            return true;
        }
        // TODO: 当前页已遍历完，需要加载下一页 (本次简化，只读第一页)
        return false;
    }

    private void loadPageIterator(PageId pageId) throws IOException {
        Page page = bufferPoolManager.getPage(pageId);
        List<Tuple> tuples = page.getAllTuples(schema);
        this.currentPageTupleIterator = tuples.iterator();
    }
}