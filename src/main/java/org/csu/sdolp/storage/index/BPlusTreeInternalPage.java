package org.csu.sdolp.storage.index;

import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.page.Page;

import java.util.Comparator;

public class BPlusTreeInternalPage extends BPlusTreeNodePage {

    private static final int PAYLOAD_OFFSET = HEADER_SIZE;
    private static final int KEY_SIZE = 4;
    private static final int POINTER_SIZE = 4;
    private static final int ENTRY_SIZE = KEY_SIZE + POINTER_SIZE;

    private final int maxSize;
    private final Comparator<Value> keyComparator;

    public BPlusTreeInternalPage(Page page) {
        super(page);
        this.maxSize = (Page.PAGE_SIZE - PAYLOAD_OFFSET - POINTER_SIZE) / ENTRY_SIZE;
        this.keyComparator = Comparator.comparingInt(v -> (Integer) v.getValue());
    }

    public void init(int pageId, int parentId) {
        setNodeType(NodeType.INTERNAL);
        setKeyCount(0);
        setParentPageId(parentId);
    }

    public void populate(int leftChildId, Value key, int rightChildId) {
        setKeyCount(1);
        setChildPageId(0, leftChildId);
        setKey(1, key);
        setChildPageId(1, rightChildId);
    }

    @Override
    public int getMaxSize() {
        return maxSize;
    }

    public Value getKey(int index) {
        if (index <= 0 || index > getKeyCount()) {
            throw new IndexOutOfBoundsException("Key index " + index + " out of bounds for key count " + getKeyCount());
        }
        int offset = PAYLOAD_OFFSET + POINTER_SIZE + (index - 1) * ENTRY_SIZE;
        return new Value(data.getInt(offset));
    }

    public void setKey(int index, Value key) {
        if (index <= 0 || index > getMaxSize()) {
            throw new IndexOutOfBoundsException("SetKey index " + index + " out of bounds for max size " + getMaxSize());
        }
        int offset = PAYLOAD_OFFSET + POINTER_SIZE + (index - 1) * ENTRY_SIZE;
        data.putInt(offset, (Integer) key.getValue());
    }

    public int getChildPageId(int index) {
        if (index < 0 || index > getKeyCount()) {
            throw new IndexOutOfBoundsException("Child index " + index + " out of bounds for key count " + getKeyCount());
        }
        int offset = PAYLOAD_OFFSET + index * ENTRY_SIZE;
        return data.getInt(offset);
    }

    public void setChildPageId(int index, int pageId) {
        if (index < 0 || index > getMaxSize() + 1) {
            throw new IndexOutOfBoundsException("SetChildPageId index " + index + " out of bounds for max size " + getMaxSize());
        }
        int offset = PAYLOAD_OFFSET + index * ENTRY_SIZE;
        data.putInt(offset, pageId);
    }

    public int childIndexLookup(Value key) {
        int low = 1;
        int high = getKeyCount();
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Value midKey = getKey(mid);
            int cmp = keyComparator.compare(key, midKey);
            if (cmp < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return high;
    }

    public void insert(Value key, int rightChildPageId) {
        int keyCount = getKeyCount();
        int index = childIndexLookup(key) + 1;
        int srcPos = PAYLOAD_OFFSET + POINTER_SIZE + (index - 1) * ENTRY_SIZE;
        int destPos = srcPos + ENTRY_SIZE;
        int numMovedEntries = keyCount - index + 1;
        if (numMovedEntries > 0) {
            System.arraycopy(data.array(), srcPos, data.array(), destPos, numMovedEntries * ENTRY_SIZE);
        }
        setKey(index, key);
        setChildPageId(index, rightChildPageId);
        setKeyCount(keyCount + 1);
    }

    public int getChildIndexByPageId(int pageId) {
        for (int i = 0; i <= getKeyCount(); i++) {
            if (getChildPageId(i) == pageId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 【已修正】移除指定索引的键及其右指针，使用 System.arraycopy 保证原子性。
     * @param keyIndex 要移除的键的索引 (1-based)。
     */
    public void remove(int keyIndex) {
        int keyCount = getKeyCount();
        // 源数据块的起始位置：要移除的 key 后面的那个 key 的位置
        int srcPos = PAYLOAD_OFFSET + POINTER_SIZE + (keyIndex) * ENTRY_SIZE;
        // 目标数据块的起始位置：要移除的 key 的位置
        int destPos = PAYLOAD_OFFSET + POINTER_SIZE + (keyIndex - 1) * ENTRY_SIZE;
        // 需要移动的字节数
        int length = (keyCount - keyIndex) * ENTRY_SIZE;

        if (length > 0) {
            System.arraycopy(data.array(), srcPos, data.array(), destPos, length);
        }

        setKeyCount(keyCount - 1);
    }


    /**
     * 移除第一个指针，并返回它。同时，它左边的 key 会被删除。
     */
    public int removeAndGetFirstPointer() {
        int keyCount = getKeyCount();
        int firstPointer = getChildPageId(0);

        // 源数据块的起始位置：第一个 key 的位置
        int srcPos = PAYLOAD_OFFSET + POINTER_SIZE;
        // 目标数据块的起始位置：第一个指针的位置
        int destPos = PAYLOAD_OFFSET;
        // 需要移动的字节数：所有 key 和除了第一个之外的所有 pointer
        int length = keyCount * ENTRY_SIZE;

        System.arraycopy(data.array(), srcPos, data.array(), destPos, length);
        setKeyCount(keyCount - 1);
        return firstPointer;
    }

    public int removeAndGetLastPointer() {
        int keyCount = getKeyCount();
        int lastPointer = getChildPageId(keyCount);
        setKeyCount(keyCount - 1);
        return lastPointer;
    }

    public void insertAtEnd(Value key, int pointer) {
        int keyCount = getKeyCount();
        setKey(keyCount + 1, key);
        setChildPageId(keyCount + 1, pointer);
        setKeyCount(keyCount + 1);
    }

    public void insertAtFront(Value key, int pointer) {
        int keyCount = getKeyCount();
        // 将所有现有条目（P0, K1, P1, ...）向右移动一个 ENTRY_SIZE 的位置
        int srcPos = PAYLOAD_OFFSET;
        int destPos = PAYLOAD_OFFSET + ENTRY_SIZE;
        int length = POINTER_SIZE + keyCount * ENTRY_SIZE;
        System.arraycopy(data.array(), srcPos, data.array(), destPos, length);

        // 在开头插入新的 key 和 pointer
        setKey(1, key);
        setChildPageId(0, pointer);
        setKeyCount(keyCount + 1);
    }

    @Override
    public String toString() {
        int keyCount = getKeyCount();
        StringBuilder sb = new StringBuilder();
        sb.append("InternalPage(id=").append(page.getPageId().getPageNum())
                .append(", parent=").append(getParentPageId())
                .append(", count=").append(keyCount)
                .append(") Pointers: [").append(getChildPageId(0));
        for (int i = 1; i <= keyCount; i++) {
            sb.append(", K:").append(getKey(i).getValue());
            sb.append(", P:").append(getChildPageId(i));
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public int getMinSize() {
        return getMaxSize() / 2;
    }
}