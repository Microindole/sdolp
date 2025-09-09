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

/**
 * 顺序扫描执行器，真正负责从头到尾迭代一张表的所有元组。
 */
public class SeqScanExecutor implements TupleIterator {

    private final BufferPoolManager bufferPoolManager;
    private final Schema schema;

    // --- 迭代器状态 ---
    private PageId currentPageId;
    private Iterator<Tuple> currentPageTupleIterator;

    public SeqScanExecutor(BufferPoolManager bufferPoolManager, TableInfo tableInfo) throws IOException {
        this.bufferPoolManager = bufferPoolManager;
        this.schema = tableInfo.getSchema();

        // 初始化迭代器，从表的第一页开始
        this.currentPageId = tableInfo.getFirstPageId();
        if (this.currentPageId != null) {
            loadPageIterator(this.currentPageId);
        } else {
            // 如果表是空的，甚至连第一页都没有分配
            this.currentPageTupleIterator = null;
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