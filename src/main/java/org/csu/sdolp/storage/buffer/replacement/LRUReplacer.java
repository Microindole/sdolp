package org.csu.sdolp.storage.buffer.replacement;


import org.csu.sdolp.storage.page.PageId;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU（最近最少使用）替换策略。
 */
public class LRUReplacer implements BufferPoolReplacer {
    // LinkedHashMap 天然支持 LRU 逻辑
    private final LinkedHashMap<PageId, Boolean> lruMap = new LinkedHashMap<>(16, 0.75f, true);

    @Override
    public synchronized void pin(PageId pageId) {
        // 访问后，元素会自动移到链表尾部
        lruMap.put(pageId, true);
    }

    @Override
    public synchronized PageId unpin() {
        if (lruMap.isEmpty()) {
            return null;
        }
        // 链表头部的就是最久未使用的页
        Map.Entry<PageId, Boolean> firstEntry = lruMap.entrySet().iterator().next();
        lruMap.remove(firstEntry.getKey());
        return firstEntry.getKey();
    }
}