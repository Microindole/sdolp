package org.csu.sdolp.storage.index;

import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.page.PageId;
import java.nio.ByteBuffer;

/**
 * B+树的叶子节点页。
 * 存储结构:
 * [Header(8 bytes)][NextPageId(4 bytes)][(Key, TuplePageId, TupleSlotIndex)...]
 */
public class BPlusTreeLeafPage extends BPlusTreePage {

    private static final int NEXT_PAGE_ID_OFFSET = 8;
    private static final int PAYLOAD_OFFSET = 12;

    // Key(INT) + TuplePageId(INT) + TupleSlotIndex(INT)
    private static final int ENTRY_SIZE = 12;

    /**
     * Formats a given ByteBuffer as a new B+Tree leaf page.
     * @param buffer The ByteBuffer from a Page object to format.
     * @param nextPageId The page ID of the next leaf node.
     */
    public void init(ByteBuffer buffer, int nextPageId) {
        this.data = buffer;
        setKeyCount(0);
        data.putInt(PAGE_TYPE_OFFSET, 0); // 0 indicates a leaf page
        setNextPageId(nextPageId);
    }

    public int getNextPageId() {
        return data.getInt(NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(int nextPageId) {
        data.putInt(NEXT_PAGE_ID_OFFSET, nextPageId);
    }

    public Value getKey(int index) {
        int offset = PAYLOAD_OFFSET + index * ENTRY_SIZE;
        return new Value(data.getInt(offset));
    }

    public PageId getTupleId(int index) {
        int offset = PAYLOAD_OFFSET + index * ENTRY_SIZE + 4;
        return new PageId(data.getInt(offset));
    }

    /**
     * 在叶子节点中插入键值对。
     * @return 插入后新的键数量
     */
    public int insert(Value key, PageId tupleId) {
        int keyCount = getKeyCount();
        int insertIndex = findPositionToInsert(key);

        // 将插入点之后的所有元素向后移动一位
        System.arraycopy(data.array(), PAYLOAD_OFFSET + insertIndex * ENTRY_SIZE,
                data.array(), PAYLOAD_OFFSET + (insertIndex + 1) * ENTRY_SIZE,
                (keyCount - insertIndex) * ENTRY_SIZE);

        // 插入新元素
        int offset = PAYLOAD_OFFSET + insertIndex * ENTRY_SIZE;
        data.putInt(offset, (Integer) key.getValue());
        data.putInt(offset + 4, tupleId.getPageNum());
        data.putInt(offset + 8, 0); // SlotIndex 暂时不用，预留

        setKeyCount(keyCount + 1);
        return keyCount + 1;
    }

    /**
     * 找到给定 key 应该插入的位置。
     */
    private int findPositionToInsert(Value key) {
        int keyCount = getKeyCount();
        int low = 0, high = keyCount - 1;
        int targetIndex = keyCount;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            if ((Integer)getKey(mid).getValue() >= (Integer)key.getValue()) {
                targetIndex = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return targetIndex;
    }
}