package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.IOException;

/**
 * TableHeap 负责一张表的物理存储堆，并提供遍历其所有元组的能力。
 * 它封装了页面链表的迭代、元组的插入、删除和更新等底层操作。
 * 这是所有上层执行器的物理数据源。
 */
public class TableHeap implements TupleIterator {

    private final BufferPoolManager bufferPoolManager;
    private final Schema schema;
    private final PageId firstPageId;

    // --- 迭代器状态 ---
    private PageId currentPageId;
    private Page currentPage;
    private int currentSlotIndex; // 使用槽位索引来代替 Iterator，以便获取RID

    public TableHeap(BufferPoolManager bufferPoolManager, TableInfo tableInfo) throws IOException {
        this.bufferPoolManager = bufferPoolManager;
        this.schema = tableInfo.getSchema();
        this.firstPageId = tableInfo.getFirstPageId();

        // 初始化迭代器状态
        this.currentPageId = this.firstPageId;
        if (this.currentPageId != null && this.currentPageId.getPageNum() != -1) {
            this.currentPage = bufferPoolManager.getPage(this.currentPageId);
        } else {
            this.currentPage = null; // 表是空的，没有任何页面
        }
        this.currentSlotIndex = 0;
    }

    /**
     * 获取表中的下一条有效元组。
     * @return 下一条元组，如果不存在则返回 null。
     */
    @Override
    public Tuple next() throws IOException {
        // hasNext() 方法会预先定位到下一个有效的元组
        if (!hasNext()) {
            return null;
        }

        // 从当前页面的当前槽位获取元组
        Tuple tuple = currentPage.getTuple(currentSlotIndex, schema);
        // 【核心】为元组附加其物理地址 RID
        tuple.setRid(new RID(currentPageId.getPageNum(), currentSlotIndex));

        // 将索引移动到下一个槽位，为下一次调用 next() 做准备
        currentSlotIndex++;
        return tuple;
    }

    /**
     * 检查表中是否还有更多有效元组。
     * 此方法会处理页面跳转和已删除元组的跳过。
     * @return 如果还有元组，则返回 true。
     */
    @Override
    public boolean hasNext() throws IOException {
        if (currentPage == null) {
            return false; // 表中没有任何页面
        }

        // 循环直到找到一个有效的元组或遍历完所有页面
        while (true) {
            // 检查当前页是否还有未扫描的槽位
            if (currentSlotIndex < currentPage.getNumTuples()) {
                // 检查这个槽位的元组是否有效（未被删除）
                if (currentPage.getTuple(currentSlotIndex, schema) != null) {
                    return true; // 找到了一个有效的元组，hasNext()成功
                }
                // 如果是已删除的元组，则跳到下一个槽位继续在本页查找
                currentSlotIndex++;
            } else {
                // 当前页已扫描完毕，尝试移动到下一页
                int nextPageNum = currentPage.getNextPageId();
                if (nextPageNum != -1) { // 检查是否存在下一页
                    currentPageId = new PageId(nextPageNum);
                    currentPage = bufferPoolManager.getPage(currentPageId);
                    currentSlotIndex = 0; // 重置槽位索引，从新页面的第一个槽位开始
                } else {
                    // 没有下一页了，遍历结束
                    return false;
                }
            }
        }
    }

    /**
     * 向表中插入一条元组。
     * 它会自动查找有可用空间的页面，如果所有页面都满了，则会分配新页。
     * @param tuple 要插入的元组
     * @return 插入成功返回 true
     */
    public boolean insertTuple(Tuple tuple) throws IOException {
        PageId pid = this.firstPageId;
        Page lastPage = null;

        while (pid != null && pid.getPageNum() != -1) {
            Page page = bufferPoolManager.getPage(pid);
            lastPage = page; // 记录下最后一页
            // 尝试在当前页插入
            if (page.insertTuple(tuple)) {
                bufferPoolManager.flushPage(pid); // 确保数据持久化
                return true;
            }
            // 当前页已满，移动到下一页
            int nextPageNum = page.getNextPageId();
            pid = (nextPageNum != -1) ? new PageId(nextPageNum) : null;
        }

        // 如果循环结束，说明所有现有页面都满了，需要在 lastPage 后面链接一个新页
        Page newPage = bufferPoolManager.newPage();
        if (newPage == null) {
            return false; // 缓冲池也满了，插入失败
        }
        newPage.init(); // 初始化新页的头部 (在你的 Page 构造函数或新方法里实现)

        // 将新页链接到表的末尾
        if (lastPage != null) {
            lastPage.setNextPageId(newPage.getPageId().getPageNum());
            bufferPoolManager.flushPage(lastPage.getPageId());
        } else {
            // 这是表的第一页，需要更新 Catalog (这部分逻辑通常在 Catalog.createTable 中处理)
            // 此处简化，假设 createTable 时已创建了第一页
        }

        // 在新页中插入元组
        if (newPage.insertTuple(tuple)) {
            bufferPoolManager.flushPage(newPage.getPageId());
            return true;
        } else {
            // 如果一个全新的空页都无法插入，说明元组本身太大了
            throw new IllegalStateException("Tuple is too large to fit in a new page.");
        }
    }

    /**
     * 根据 RID 删除一条元组 (标记删除)
     * @param rid 要删除元组的物理地址
     * @return 删除成功返回 true
     */
    public boolean deleteTuple(RID rid) throws IOException {
        Page page = bufferPoolManager.getPage(new PageId(rid.pageNum()));
        boolean success = page.markTupleAsDeleted(rid.slotIndex());
        if (success) {
            // 将修改后的页面标记为“脏页”，以便写回磁盘
            bufferPoolManager.flushPage(page.getPageId());
        }
        return success;
    }

    /**
     * 根据 RID 更新一条元组 (策略：先删除旧的，再插入新的)
     * @param newTuple 新的元组数据
     * @param rid      旧元组的物理地址
     * @return 更新成功返回 true
     */
    public boolean updateTuple(Tuple newTuple, RID rid) throws IOException {
        // 注意：这个简单的策略可能会改变元组的 RID，并且在并发环境下有问题。
        // 但对于单用户数据库来说，这是最直接的实现方式。
        if (deleteTuple(rid)) {
            // insertTuple 会负责找到空间并插入
            return insertTuple(newTuple);
        }
        return false;
    }
}