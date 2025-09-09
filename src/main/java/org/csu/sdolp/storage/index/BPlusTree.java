package org.csu.sdolp.storage.index;

import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * B+树索引结构。
 * 它负责管理整个树的结构，包括根节点、查找、插入和删除操作。
 */
public class BPlusTree {

    private final BufferPoolManager bufferPoolManager;
    private PageId rootPageId;

    public BPlusTree(BufferPoolManager bufferPoolManager, PageId rootPageId) {
        this.bufferPoolManager = bufferPoolManager;
        this.rootPageId = rootPageId;
    }

    /**
     * 查找给定键对应的Tuple ID。
     * @param key 要查找的键
     * @return 对应的Tuple ID，如果找不到则返回null
     */
    public PageId search(Value key) throws IOException {
        if (rootPageId == null) {
            return null;
        }

        Page currentPage = bufferPoolManager.getPage(rootPageId);
        BPlusTreePage pageNode = createBPlusTreePage(currentPage);

        // 迭代地从根节点向下查找
        while (!pageNode.isLeaf()) {
            BPlusTreeInternalPage internalPage = (BPlusTreeInternalPage) pageNode;
            PageId childPageId = internalPage.lookup(key);
            currentPage = bufferPoolManager.getPage(childPageId);
            pageNode = createBPlusTreePage(currentPage);
        }

        // 到达叶子节点，在叶子节点内部查找
        BPlusTreeLeafPage leafPage = (BPlusTreeLeafPage) pageNode;
        for (int i = 0; i < leafPage.getKeyCount(); i++) {
            // Corrected comparison
            if (((Integer)leafPage.getKey(i).getValue()).equals(key.getValue())) {
                return leafPage.getTupleId(i);
            }
        }

        return null; // 未找到
    }

    /**
     * 向B+树中插入一个键值对。
     * (简化版：暂不处理节点分裂)
     */
    public void insert(Value key, PageId tupleId) throws IOException {
        if (rootPageId == null) {
            // 如果是空树，创建一个新的叶子节点作为根
            Page rootPage = bufferPoolManager.newPage();
            rootPageId = rootPage.getPageId();
            BPlusTreeLeafPage rootNode = new BPlusTreeLeafPage();
            rootNode.init(rootPage.getData(), -1);
            rootNode.insert(key, tupleId);
            // 将更新后的数据写回 Page 的 ByteBuffer
            bufferPoolManager.flushPage(rootPageId);
            return;
        }

        // TODO: 找到合适的叶子节点并插入，如果叶子节点已满则需要分裂。
        // 这是一个复杂的递归过程，我们先聚焦于测试查找功能。
    }

    public void delete(Value key) throws IOException {
        // TODO: 删除逻辑比插入更复杂，通常涉及到节点的合并或重分配
    }

    /**
     * 辅助方法：根据Page的内容创建对应的BPlusTreePage子类实例
     */
    private BPlusTreePage createBPlusTreePage(Page page) {
        ByteBuffer data = page.getData();
        int pageType = data.getInt(BPlusTreePage.PAGE_TYPE_OFFSET);

        BPlusTreePage pageNode;
        if (pageType == 0) { // Leaf Node
            pageNode = new BPlusTreeLeafPage();
        } else { // Internal Node
            pageNode = new BPlusTreeInternalPage();
        }
        pageNode.init(data);
        return pageNode;
    }
}