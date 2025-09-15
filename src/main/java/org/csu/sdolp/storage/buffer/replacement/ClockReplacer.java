package org.csu.sdolp.storage.buffer.replacement;

import org.csu.sdolp.storage.page.PageId;

import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Clock (时钟) 页面替换策略，也称为“二次机会”算法。
 * 它使用一个循环队列和“引用位”来近似 LRU 的行为，但开销更低。
 */
public class ClockReplacer implements BufferPoolReplacer {

    private final ConcurrentLinkedDeque<PageId> clockQueue;
    private final ConcurrentHashMap<PageId, Boolean> referenceBits;

    public ClockReplacer() {
        this.clockQueue = new ConcurrentLinkedDeque<>();
        this.referenceBits = new ConcurrentHashMap<>();
    }

    private String getQueueState() {
        StringJoiner sj = new StringJoiner(", ", "[", "]");
        for (PageId pid : clockQueue) {
            sj.add(String.format("P%d(ref=%d)", pid.getPageNum(), referenceBits.getOrDefault(pid, false) ? 1 : 0));
        }
        return sj.toString();
    }

    @Override
    public synchronized void pin(PageId pageId) {
        if (!referenceBits.containsKey(pageId)) {
            clockQueue.add(pageId);
            System.out.printf("[Clock] Page %d is new, added to clock. Queue: %s%n", pageId.getPageNum(), getQueueState());
        }
        referenceBits.put(pageId, true);
        System.out.printf("[Clock] Page %d pinned (accessed). Setting ref bit to 1. Queue: %s%n", pageId.getPageNum(), getQueueState());
    }

    @Override
    public synchronized PageId unpin() {
        if (clockQueue.isEmpty()) {
            return null;
        }

        System.out.println("[Clock] Need to evict a page. Starting scan...");
        while (true) {
            PageId currentPageId = clockQueue.poll();
            if (currentPageId == null) return null;

            System.out.printf("[Clock]  -> Checking Page %d (ref=%d)...%n", currentPageId.getPageNum(), referenceBits.getOrDefault(currentPageId, false) ? 1 : 0);

            if (referenceBits.getOrDefault(currentPageId, false)) {
                referenceBits.put(currentPageId, false);
                clockQueue.add(currentPageId);
                System.out.printf("[Clock]  -> Giving Page %d a second chance. Set ref bit to 0 and move to tail. Queue: %s%n", currentPageId.getPageNum(), getQueueState());
            } else {
                referenceBits.remove(currentPageId);
                System.out.printf("[Clock]  -> Found victim! Page %d has ref bit 0. Evicting it. Final Queue: %s%n", currentPageId.getPageNum(), getQueueState());
                return currentPageId;
            }
        }
    }

    @Override
    public synchronized void remove(PageId pageId) {
        clockQueue.remove(pageId);
        referenceBits.remove(pageId);
        System.out.printf("[Clock] Page %d explicitly removed. Queue: %s%n", pageId.getPageNum(), getQueueState());
    }
}