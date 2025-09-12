package org.csu.sdolp.storage.buffer.replacement;


import org.csu.sdolp.storage.page.PageId;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * FIFO（先进先出）替换策略。
 */
public class FIFOReplacer implements BufferPoolReplacer {
    private final Queue<PageId> fifoQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void pin(PageId pageId) {
        // 新页或已存在的页加入队列尾部
        if (!fifoQueue.contains(pageId)) {
            fifoQueue.add(pageId);
        }
    }

    @Override
    public PageId unpin() {
        // 移除并返回队列头部的页
        return fifoQueue.poll();
    }
    @Override
    public void remove(PageId pageId) {
        fifoQueue.remove(pageId);
    }

}