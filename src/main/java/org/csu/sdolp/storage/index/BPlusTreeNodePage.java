package org.csu.sdolp.storage.index;

import org.csu.sdolp.storage.page.Page;

import java.nio.ByteBuffer;

public abstract class BPlusTreeNodePage {
    // 节点类型枚举
    public enum NodeType {
        LEAF,
        INTERNAL
    }
    // --- 头部字段的偏移量 ---
    protected static final int OFFSET_NODE_TYPE = 0;      // 节点类型 (1 byte)
    protected static final int OFFSET_KEY_COUNT = 1;      // 当前键数量 (4 bytes, int)
    protected static final int HEADER_SIZE = 9;
    protected static final int OFFSET_PARENT_PAGE_ID = 5;

    protected Page page;
    protected ByteBuffer data;

    public BPlusTreeNodePage(Page page) {
        this.page = page;
        this.data = page.getData();
    }

    public NodeType getNodeType() {
        return data.get(OFFSET_NODE_TYPE) == 0 ? NodeType.LEAF : NodeType.INTERNAL;
    }

    protected void setNodeType(NodeType type) {
        data.put(OFFSET_NODE_TYPE, (byte) (type == NodeType.LEAF ? 0 : 1));
    }

    public int getKeyCount() {
        return data.getInt(OFFSET_KEY_COUNT);
    }

    protected void setKeyCount(int keyCount) {
        data.putInt(OFFSET_KEY_COUNT, keyCount);
    }
    public abstract int getMaxSize();

    public int getParentPageId() {
        return data.getInt(OFFSET_PARENT_PAGE_ID);
    }

    public void setParentPageId(int parentId) {
        data.putInt(OFFSET_PARENT_PAGE_ID, parentId);
    }

    public abstract int getMinSize();



}
