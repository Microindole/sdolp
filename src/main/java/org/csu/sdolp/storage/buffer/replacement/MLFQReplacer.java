package org.csu.sdolp.storage.buffer.replacement;

import org.csu.sdolp.storage.page.PageId;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MLFQReplacer implements BufferPoolReplacer {

    private final Queue<PageId> highPriorityQueue;
    private final Queue<PageId> mediumPriorityQueue;
    private final Queue<PageId> lowPriorityQueue;
    private final Map<PageId, Integer> pagePriorities;

    public MLFQReplacer() {
        // 使用 ConcurrentLinkedQueue
        this.highPriorityQueue = new ConcurrentLinkedQueue<>();
        this.mediumPriorityQueue = new ConcurrentLinkedQueue<>();
        this.lowPriorityQueue = new ConcurrentLinkedQueue<>();
        this.pagePriorities = new ConcurrentHashMap<>();
    }

    // pin, unpin, remove, demote 方法保持不变
    @Override
    public synchronized void pin(PageId pageId) {
        Integer currentPriority = pagePriorities.get(pageId);

        if (currentPriority == null) {
            mediumPriorityQueue.add(pageId);
            pagePriorities.put(pageId, 2);
        } else {
            if (currentPriority == 2) {
                mediumPriorityQueue.remove(pageId);
                highPriorityQueue.add(pageId);
                pagePriorities.put(pageId, 3);
            } else if (currentPriority == 1) {
                lowPriorityQueue.remove(pageId);
                mediumPriorityQueue.add(pageId);
                pagePriorities.put(pageId, 2);
            } else {
                highPriorityQueue.remove(pageId);
                highPriorityQueue.add(pageId);
            }
        }
    }

    @Override
    public synchronized PageId unpin() {
        if (!lowPriorityQueue.isEmpty()) {
            PageId victim = lowPriorityQueue.poll();
            pagePriorities.remove(victim);
            return victim;
        }
        if (!mediumPriorityQueue.isEmpty()) {
            PageId victim = mediumPriorityQueue.poll();
            pagePriorities.remove(victim);
            return victim;
        }
        if (!highPriorityQueue.isEmpty()) {
            PageId victim = highPriorityQueue.poll();
            pagePriorities.remove(victim);
            return victim;
        }
        return null;
    }

    @Override
    public synchronized void remove(PageId pageId) {
        Integer priority = pagePriorities.remove(pageId);
        if (priority != null) {
            if (priority == 3) {
                highPriorityQueue.remove(pageId);
            } else if (priority == 2) {
                mediumPriorityQueue.remove(pageId);
            } else {
                lowPriorityQueue.remove(pageId);
            }
        }
    }

    public synchronized void demote(PageId pageId) {
        Integer currentPriority = pagePriorities.get(pageId);
        if (currentPriority == null) return;

        if (currentPriority == 3) { // high -> medium
            highPriorityQueue.remove(pageId);
            mediumPriorityQueue.add(pageId);
            pagePriorities.put(pageId, 2);
        } else if (currentPriority == 2) { // medium -> low
            mediumPriorityQueue.remove(pageId);
            lowPriorityQueue.add(pageId);
            pagePriorities.put(pageId, 1);
        }
    }
}