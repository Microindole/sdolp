package org.csu.sdolp.storage.index;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.page.Page;

import java.util.Comparator;

/**
 * B+树叶子节点页。
 * 存储 <Key, RID> 对。
 * Header: [NodeType, KeyCount]
 * Data:   [NextLeafPageId, <Key1, RID1>, <Key2, RID2>, ...]
 */
public class BPlusTreeLeafPage extends BPlusTreeNodePage {

    private static final int OFFSET_NEXT_LEAF_PAGE_ID = HEADER_SIZE; // (4 bytes, int)
    private static final int PAYLOAD_OFFSET = OFFSET_NEXT_LEAF_PAGE_ID + 4;

    private static final int KEY_SIZE = 4;
    private static final int RID_SIZE = 8;
    private static final int ENTRY_SIZE = KEY_SIZE + RID_SIZE;

    private final int maxSize;
    private final Comparator<Value> keyComparator;

    public BPlusTreeLeafPage(Page page) {
        super(page);
        this.maxSize = (Page.PAGE_SIZE - PAYLOAD_OFFSET) / ENTRY_SIZE;
        this.keyComparator = Comparator.comparingInt(v -> (Integer) v.getValue());
    }

    public void init(int pageId, int parentId) {
        setNodeType(NodeType.LEAF);
        setKeyCount(0);
        setParentPageId(parentId);
        setNextLeafPageId(-1);
    }

    public int getNextLeafPageId() {
        return data.getInt(OFFSET_NEXT_LEAF_PAGE_ID);
    }

    public void setNextLeafPageId(int pageId) {
        data.putInt(OFFSET_NEXT_LEAF_PAGE_ID, pageId);
    }

    @Override
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * 根据索引获取 RID
     */
    public RID getRid(int index) {
        int offset = getEntryOffset(index) + KEY_SIZE; // 跳过 key
        int pageNum = data.getInt(offset);
        int slotIndex = data.getInt(offset + 4);
        return new RID(pageNum, slotIndex);
    }

    /**
     * 根据索引获取 Key
     */
    public Value getKey(int index) {
        int offset = getEntryOffset(index);
        return new Value(data.getInt(offset));
    }

    /**
     * 获取第 index 个条目的起始偏移量
     */
    private int getEntryOffset(int index) {
        return PAYLOAD_OFFSET + index * ENTRY_SIZE;
    }

    /**
     * 写入一个完整的条目
     */
    private void setEntry(int index, Value key, RID rid) {
        int offset = getEntryOffset(index);
        data.putInt(offset, (Integer) key.getValue());
        data.putInt(offset + KEY_SIZE, rid.pageNum());
        data.putInt(offset + KEY_SIZE + 4, rid.slotIndex());
    }

    /**
     * 使用二分查找在页面中定位键的位置。
     * @param key 要查找的键
     * @return 如果找到，返回键的索引；如果没有找到，返回它应该被插入的位置的索引。
     */
    public int keyIndexLookup(Value key) {
        int low = 0;
        int high = getKeyCount() - 1;
        int mid = 0;
        while (low <= high) {
            mid = low + (high - low) / 2;
            Value midKey = getKey(mid);
            int cmp = keyComparator.compare(midKey, key);
            if (cmp == 0) {
                return mid;
            } else if (cmp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return low; // key 不存在，返回应该插入的位置
    }

    /**
     * 插入一个新的 <Key, RID> 对到叶子节点。
     * 必须保证键的有序性。
     * @param key 要插入的键
     * @param rid 对应的值 (Record ID)
     * @return 如果成功插入，返回 true；如果节点已满，返回 false。
     */
    public boolean insert(Value key, RID rid) {
        int keyCount = getKeyCount();
        if (keyCount >= getMaxSize()) {
            return false;
        }

        int index = keyIndexLookup(key);

        // 将 index 及之后的所有元素向后移动一位
        int srcPos = getEntryOffset(index);
        int destPos = getEntryOffset(index + 1);
        int length = (keyCount - index) * ENTRY_SIZE;
        if (length > 0) {
            System.arraycopy(data.array(), srcPos, data.array(), destPos, length);
        }

        // 在 index 位置插入新条目
        setEntry(index, key, rid);
        setKeyCount(keyCount + 1);
        return true;
    }

    /**
     * 在叶子节点的末尾插入一个键值对。
     */
    public void insertAtEnd(Value key, RID rid) {
        int keyCount = getKeyCount();
        setEntry(keyCount, key, rid);
        setKeyCount(keyCount + 1);
    }

    /**
     * 在叶子节点的开头插入一个键值对。
     */
    public void insertAtFront(Value key, RID rid) {
        int keyCount = getKeyCount();
        // 所有元素向后移动一位
        System.arraycopy(data.array(), getEntryOffset(0), data.array(), getEntryOffset(1), keyCount * ENTRY_SIZE);
        setEntry(0, key, rid);
        setKeyCount(keyCount + 1);
    }

    /**
     * 移除并返回第一个键值对。
     */
    public KeyValuePair removeAndGetFirst() {
        int keyCount = getKeyCount();
        Value key = getKey(0);
        RID rid = getRid(0);
        // 其余元素向前移动一位
        System.arraycopy(data.array(), getEntryOffset(1), data.array(), getEntryOffset(0), (keyCount - 1) * ENTRY_SIZE);
        setKeyCount(keyCount - 1);
        return new KeyValuePair(key, rid);
    }

    /**
     * 移除并返回最后一个键值对。
     */
    public KeyValuePair removeAndGetLast() {
        int keyCount = getKeyCount();
        Value key = getKey(keyCount - 1);
        RID rid = getRid(keyCount - 1);
        setKeyCount(keyCount - 1);
        return new KeyValuePair(key, rid);
    }

    public boolean delete(Value key) {
        int keyCount = getKeyCount();
        int index = keyIndexLookup(key);

        if (index >= keyCount || keyComparator.compare(getKey(index), key) != 0) {
            return false;
        }

        int srcPos = getEntryOffset(index + 1);
        int destPos = getEntryOffset(index);
        int length = (keyCount - index - 1) * ENTRY_SIZE;
        if (length > 0) {
            System.arraycopy(data.array(), srcPos, data.array(), destPos, length);
        }

        setKeyCount(keyCount - 1);
        return true;
    }

    @Override
    public String toString() {
        int keyCount = getKeyCount();
        StringBuilder sb = new StringBuilder();
        sb.append("LeafPage(id=").append(page.getPageId().getPageNum())
                .append(", parent=").append(getParentPageId())
                .append(", count=").append(keyCount)
                .append(", next=").append(getNextLeafPageId())
                .append(") Keys: [");
        for (int i = 0; i < keyCount; i++) {
            sb.append(getKey(i).getValue());
            if (i < keyCount - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public int getMinSize() {
        return getMaxSize() / 2;
    }
}