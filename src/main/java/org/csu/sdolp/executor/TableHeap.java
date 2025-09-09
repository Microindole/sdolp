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
        while (true) {
            Page page = bufferPoolManager.getPage(pid);
            // 尝试在当前页插入
            if (page.insertTuple(tuple)) {
                bufferPoolManager.flushPage(pid); // 确保数据持久化
                return true;
            }

            // 当前页已满，获取下一页ID
            int nextPageNum = page.getNextPageId();
            if (nextPageNum != -1) { // 如果存在下一页
                pid = new PageId(nextPageNum);
            } else { // 如果不存在下一页，说明已到达链表末尾
                // 1. 分配一个新页
                Page newPage = bufferPoolManager.newPage();
                // 2. 将旧页的 next_page_id 指向新页
                page.setNextPageId(newPage.getPageId().getPageNum());
                bufferPoolManager.flushPage(pid); // 将旧页的更新持久化

                // 3. 在新页中插入元组
                pid = newPage.getPageId();
                if(newPage.insertTuple(tuple)) {
                    bufferPoolManager.flushPage(pid);
                    return true;
                } else {
                    // 如果一个全新的空页都无法插入，说明元组本身太大了
                    throw new IllegalStateException("Tuple is too large to fit in a new page.");
                }
            }
        }
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

        // 如果当前页的迭代器还有元素，直接返回 true
        if (currentPageTupleIterator.hasNext()) {
            return true;
        }

        // 当前页已遍历完，尝试加载下一页
        Page currentPage = bufferPoolManager.getPage(currentPageId);
        int nextPageNum = currentPage.getNextPageId();

        if (nextPageNum != -1) {
            currentPageId = new PageId(nextPageNum);
            loadPageIterator(currentPageId);
            // 加载新页后，再次检查新页的迭代器
            return currentPageTupleIterator.hasNext();
        }

        // 没有下一页了
        return false;
    }

    private void loadPageIterator(PageId pageId) throws IOException {
        Page page = bufferPoolManager.getPage(pageId);
        List<Tuple> tuples = page.getAllTuples(schema);
        this.currentPageTupleIterator = tuples.iterator();
    }
}