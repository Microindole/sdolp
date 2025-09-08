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
    private final BufferPoolReplacer replacer; // 使用接口，支持多种替换策略

    @Getter
    private int hitCount = 0;
    @Getter
    private int missCount = 0;

    public BufferPoolManager(int poolSize, DiskManager diskManager, String strategy) {
        this.poolSize = poolSize;
        this.diskManager = diskManager;
        this.pageTable = new ConcurrentHashMap<>();
        
        // 根据传入的策略字符串选择具体的替换器 [cite: 84, 130]
        if ("LRU".equalsIgnoreCase(strategy)) {
            this.replacer = new LRUReplacer();
        } else if ("FIFO".equalsIgnoreCase(strategy)) {
            this.replacer = new FIFOReplacer();
        } else {
            throw new IllegalArgumentException("Unsupported replacement strategy: " + strategy);
        }
    }

    /**
     * 从缓存或磁盘获取一个页。
     * @param pageId 要获取的页ID
     * @return 获取到的页
     */
    public Page getPage(PageId pageId) throws IOException {
        // 1. 检查页是否在缓存中 [cite: 86, 132]
        if (pageTable.containsKey(pageId)) {
            // 缓存命中，更新替换策略的访问记录
            hitCount++;
            replacer.pin(pageId);
            return pageTable.get(pageId);
        }
        missCount++;

        // 2. 缓存未命中，需要从磁盘读取
        if (pageTable.size() >= poolSize) {
            // 缓存已满，根据替换策略淘汰一个页
            PageId victimId = replacer.unpin();
            if (victimId != null) {
                System.out.println("[BufferPool] Buffer is full. Evicting page "
                        + victimId.getPageNum() + " using " +
                        replacer.getClass().getSimpleName());
                // 找到要淘汰的页，先写回磁盘
                Page victimPage = pageTable.get(victimId);
                diskManager.writePage(victimPage);
                pageTable.remove(victimId);
            } else {
                throw new IllegalStateException("Buffer pool is full and cannot evict any page.");
            }
        }
        
        // 3. 从磁盘读取新页并放入缓存
        Page newPage = diskManager.readPage(pageId);
        if (pageTable.containsKey(pageId)) {
            // 如果在从磁盘读取的漫长过程中，其他线程已经把这个页加载进来了，
            // 就用缓存里的，避免数据不一致。
            // 这是一个健壮性修改，虽然不是本次问题的直接原因，但是个好习惯。
            return pageTable.get(pageId);
        }
        pageTable.put(pageId, newPage);
        replacer.pin(pageId);
        return newPage;
    }
    
    /**
     * 将指定页的内容写回磁盘。
     * @param pageId 要刷新的页ID
     */
    public void flushPage(PageId pageId) throws IOException {
        Page page = pageTable.get(pageId);
        if (page != null) {
            diskManager.writePage(page);
        }
    }

    /**
     * 分配一个新页，并将其固定在缓存中。
     * @return 新分配的页
     */
    public Page newPage() throws IOException {
        // 1. 向磁盘管理器请求一个新的页号
        PageId newPageId = diskManager.allocatePage();
        Page newPage = new Page(newPageId);
        
        // 2. 检查缓存是否已满，如果满则淘汰一个页
        if (pageTable.size() >= poolSize) {
             PageId victimId = replacer.unpin();
             if (victimId != null) {
                 Page victimPage = pageTable.get(victimId);
                 diskManager.writePage(victimPage);
                 pageTable.remove(victimId);
             } else {
                 throw new IllegalStateException("Buffer pool is full and cannot allocate a new page.");
             }
        }
        
        // 3. 将新页加入缓存
        pageTable.put(newPageId, newPage);
        replacer.pin(newPageId);
        return newPage;
    }

    /**
     * 删除一个页。
     * 这包括从缓存中移除它，并通知 DiskManager 将其标记为可用。
     * @param pageId 要删除的页ID
     * @return 如果成功删除返回 true
     */
    public boolean deletePage(PageId pageId) throws IOException {
        // 1. 从缓存页表中移除
        Page removedPage = pageTable.remove(pageId);

        // 如果页不在缓存中，它可能只在磁盘上，这也是允许的
        // if (removedPage == null) {
        //     return false; // 或者根据你的设计决定是否报错
        // }

        // 2. 从替换策略中移除（如果你的替换器需要显式移除）
        // 对于我们当前的 LRU/FIFO 实现，当它被淘汰时会自动移除，
        // 但为了健壮性，可以考虑为 Replacer 增加一个 remove 方法。
        // replacer.remove(pageId);

        // 3. 通知 DiskManager 释放这个页
        diskManager.deallocatePage(pageId);

        return true;
    }

    /**
     * 将缓冲池中的所有页刷新到磁盘。
     * 这在关闭数据库或创建检查点时非常重要。
     * @throws IOException
     */
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
}