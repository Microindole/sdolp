package org.csu.sdolp.replacement;

import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.PageId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门用于测试 Clock 页面替换策略的单元测试。
 */
public class ClockReplacerTest {

    private final String TEST_DB_FILE = "clock_test.db";
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;

    @BeforeEach
    void setUp() throws IOException {
        new File(TEST_DB_FILE).delete();
        diskManager = new DiskManager(TEST_DB_FILE);
        diskManager.open();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (diskManager != null) {
            diskManager.close();
        }
        new File(TEST_DB_FILE).delete();
    }

    @Test
    void testClockEvictionLogic() throws IOException {
        // 使用一个容量为 3 的缓冲池
        bufferPoolManager = new BufferPoolManager(3, diskManager, "CLOCK");

        System.out.println("--- Test: Clock Eviction Logic ---");

        // 1. 填满缓冲池 (p0, p1, p2)
        PageId p0 = bufferPoolManager.newPage().getPageId(); // [p0(ref=1)]
        PageId p1 = bufferPoolManager.newPage().getPageId(); // [p0(ref=1), p1(ref=1)]
        PageId p2 = bufferPoolManager.newPage().getPageId(); // [p0(ref=1), p1(ref=1), p2(ref=1)]
        
        System.out.println("Buffer pool filled. Page table size: " + bufferPoolManager.getPageTable().size());
        assertEquals(3, bufferPoolManager.getPageTable().size());


        // 2. 再次访问 p0，它的引用位应该保持为 1
        bufferPoolManager.getPage(p0); // [p0(ref=1), p1(ref=1), p2(ref=1)]

        // 3. 插入 p3，这将触发淘汰
        // 时钟指针开始扫描:
        // - 检查 p0: ref=1 -> ref=0, 移动到队尾. 队列: [p1(ref=1), p2(ref=1), p0(ref=0)]
        // - 检查 p1: ref=1 -> ref=0, 移动到队尾. 队列: [p2(ref=1), p0(ref=0), p1(ref=0)]
        // - 检查 p2: ref=1 -> ref=0, 移动到队尾. 队列: [p0(ref=0), p1(ref=0), p2(ref=0)]
        // - 再次检查 p0: ref=0 -> 淘汰!
        // 最终 p0 被淘汰
        System.out.println("Inserting a new page to trigger eviction...");
        bufferPoolManager.newPage(); // Insert p3

        // 验证 p0 是否已被淘汰
        assertNull(bufferPoolManager.getPageTable().get(p0), "Page 0 should be evicted.");
        assertNotNull(bufferPoolManager.getPageTable().get(p1), "Page 1 should still be in the buffer pool.");
        assertNotNull(bufferPoolManager.getPageTable().get(p2), "Page 2 should still be in the buffer pool.");
        System.out.println("Page 0 was correctly evicted.");

        // 4. 再次插入 p4，触发第二次淘汰
        // 当前队列: [p1(ref=0), p2(ref=0), p3(ref=1)]
        // - 检查 p1: ref=0 -> 淘汰!
        System.out.println("Inserting another page...");
        bufferPoolManager.newPage(); // Insert p4

        assertNull(bufferPoolManager.getPageTable().get(p1), "Page 1 should now be evicted.");
        assertNotNull(bufferPoolManager.getPageTable().get(p2), "Page 2 should still be present.");
        System.out.println("Page 1 was correctly evicted.");
        
        System.out.println("--- Clock Eviction Test PASSED ---");
    }
}