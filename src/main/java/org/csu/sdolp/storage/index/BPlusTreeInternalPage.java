package org.csu.sdolp.storage.index;

import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.page.PageId;
import java.nio.ByteBuffer;

/**
 * B+树的内部节点页。
 * [Header(8 bytes)][(Key, ChildPageId)...]
 */
public class BPlusTreeInternalPage extends BPlusTreePage {

    private static final int PAYLOAD_OFFSET = 8;
    // Key(INT) + ChildPageId(INT)
    private static final int ENTRY_SIZE = 8;

    public void init(ByteBuffer buffer) {
        this.data = buffer;
        setKeyCount(0);
        data.putInt(PAGE_TYPE_OFFSET, 1); // 1 indicates an internal page
    }

    public Value getKey(int index) {
        return new Value(data.getInt(PAYLOAD_OFFSET + index * ENTRY_SIZE));
    }

    public PageId getChildId(int index) {
        return new PageId(data.getInt(PAYLOAD_OFFSET + index * ENTRY_SIZE + 4));
    }

    /**
     * 根据 key 查找对应的子节点 PageId。
     */
    public PageId lookup(Value key) {
        int keyCount = getKeyCount();
        int low = 1, high = keyCount - 1;
        int resultIndex = 0;
        // 二分查找第一个大于 key 的位置
        while (low <= high) {
            int mid = low + (high - low) / 2;
            if ((Integer)getKey(mid).getValue() <= (Integer)key.getValue()) {
                resultIndex = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return getChildId(resultIndex);
    }
}