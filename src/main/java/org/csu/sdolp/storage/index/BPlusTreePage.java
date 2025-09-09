package org.csu.sdolp.storage.index;

import java.nio.ByteBuffer;

/**
 * B+树所有节点页的抽象基类，定义了通用的头部信息。
 */
public abstract class BPlusTreePage {
    // ---- Header Offsets ----
    protected static final int PAGE_TYPE_OFFSET = 0; // 4 bytes: 0 for leaf, 1 for internal
    protected static final int KEY_COUNT_OFFSET = 4; // 4 bytes: a number of keys
    // Header size for BPlusTreePage
    protected static final int B_PLUS_TREE_PAGE_HEADER_SIZE = 8;

    protected ByteBuffer data;

    // 初始化页
    public void init(ByteBuffer buffer) {
        this.data = buffer;
    }

    // 检查当前节点是否为叶子节点。
    public boolean isLeaf() {
        return data.getInt(PAGE_TYPE_OFFSET) == 0;
    }

    /**
     * 获取当前节点中的键数量。
     * @return a number of keys
     */
    public int getKeyCount() {
        return data.getInt(KEY_COUNT_OFFSET);
    }

    /**
     * 设置当前节点中的键数量。
     * @param keyCount a number of keys to set
     */
    public void setKeyCount(int keyCount) {
        data.putInt(KEY_COUNT_OFFSET, keyCount);
    }

    // 新增：获取 ByteBuffer 的方法，方便BPlusTree类直接操作
    public ByteBuffer getData() {
        return data;
    }
}