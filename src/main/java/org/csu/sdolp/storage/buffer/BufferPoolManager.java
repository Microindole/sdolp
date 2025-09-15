package org.csu.sdolp.storage.buffer;

import lombok.Getter;
import org.csu.sdolp.storage.buffer.replacement.BufferPoolReplacer;
import org.csu.sdolp.storage.buffer.replacement.FIFOReplacer;
import org.csu.sdolp.storage.buffer.replacement.LRUReplacer;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存池管理器，负责管理内存中的页缓存。
 */
public class BufferPoolManager {
    private final DiskManager diskManager;
    private final int poolSize;
    private final Map<PageId, Page> pageTable;
    private final BufferPoolReplacer replacer;

    @Getter
    private int hitCount = 0;
    @Getter
    private int missCount = 0;

    public BufferPoolManager(int poolSize, DiskManager diskManager, String strategy) {
        this.poolSize = poolSize;
        this.diskManager = diskManager;
        this.pageTable = new ConcurrentHashMap<>();

        if ("LRU".equalsIgnoreCase(strategy)) {
            this.replacer = new LRUReplacer();
        } else if ("FIFO".equalsIgnoreCase(strategy)) {
            this.replacer = new FIFOReplacer();
        } else {
            throw new IllegalArgumentException("Unsupported replacement strategy: " + strategy);
        }
    }

    public Page getPage(PageId pageId) throws IOException {
        if (pageId == null) {
            throw new IllegalArgumentException("PageId cannot be null.");
        }

        if (pageTable.containsKey(pageId)) {
            hitCount++;
            replacer.pin(pageId);
            return pageTable.get(pageId);
        }
        missCount++;

        if (pageTable.size() >= poolSize) {
            PageId victimId = replacer.unpin();
            if (victimId != null) {
                Page victimPage = pageTable.get(victimId);
                // 【健壮性修复】在淘汰前检查 victimPage 是否为 null
                if (victimPage != null) {
                    diskManager.writePage(victimPage);
                    pageTable.remove(victimId);
                }
            } else {
                throw new IllegalStateException("Buffer pool is full and cannot evict any page.");
            }
        }

        Page newPage = diskManager.readPage(pageId);
        pageTable.put(pageId, newPage);
        replacer.pin(pageId);
        return newPage;
    }

    public void flushPage(PageId pageId) throws IOException {
        Page page = pageTable.get(pageId);
        if (page != null) {
            diskManager.writePage(page);
        }
    }

    public Page newPage() throws IOException {
        PageId newPageId = diskManager.allocatePage();
        Page newPage = new Page(newPageId);

        if (pageTable.size() >= poolSize) {
            PageId victimId = replacer.unpin();
            if (victimId != null) {
                Page victimPage = pageTable.get(victimId);
                if (victimPage != null) {
                    diskManager.writePage(victimPage);
                    pageTable.remove(victimId);
                }
            } else {
                throw new IllegalStateException("Buffer pool is full and cannot allocate a new page.");
            }
        }

        pageTable.put(newPageId, newPage);
        replacer.pin(newPageId);
        return newPage;
    }

    /**
     * 【已修正】删除一个页。
     * 现在会同时从 pageTable 和 replacer 中移除。
     */
    public boolean deletePage(PageId pageId) throws IOException {
        // 1. 从缓存页表中移除
        pageTable.remove(pageId);

        // 2. 【关键修复】通知 Replacer 移除这个页，防止它成为“幽灵”
        replacer.remove(pageId);

        // 3. 通知 DiskManager 释放这个页
        diskManager.deallocatePage(pageId);

        return true;
    }

    public void flushAllPages() throws IOException {
        for (PageId pageId : pageTable.keySet()) {
            flushPage(pageId);
        }
    }

    public double getHitRate() {
        int total = hitCount + missCount;
        if (total == 0) {
            return 0.0;
        }
        return (double) hitCount / total;
    }

    public void resetStats() {
        hitCount = 0;
        missCount = 0;
    }

    public void clear() {
        pageTable.clear();
        // replacer.clear(); // 理想情况下 replacer 也应该有 clear 方法
        System.out.println("[BufferPool] All pages have been cleared from the buffer pool.");
    }
}