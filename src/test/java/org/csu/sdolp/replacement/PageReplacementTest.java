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
 * 针对不同页面替换策略 (FIFO, LRU, Clock) 的详细测试用例。
 */
public class PageReplacementTest {

    private final String TEST_DB_FILE_PREFIX = "replacement_test_";
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private String testDbFileName;

    @BeforeEach
    void setUp() throws IOException {
        // 每个测试使用不同的文件名，防止冲突
    }

    @AfterEach
    void tearDown() throws IOException {
        if (bufferPoolManager != null) {
            bufferPoolManager.flushAllPages();
        }
        if (diskManager != null) {
            diskManager.close();
        }
        if (testDbFileName != null) {
            new File(testDbFileName).delete();
        }
    }

    private void initializeManagers(String strategy, int poolSize) throws IOException {
        testDbFileName = TEST_DB_FILE_PREFIX + strategy + ".db";
        new File(testDbFileName).delete();
        diskManager = new DiskManager(testDbFileName);
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(poolSize, diskManager, strategy);
    }


    @Test
    void testFifoEvictionLogic() throws IOException {
        initializeManagers("FIFO", 3);
        System.out.println("\n--- [测试] FIFO 淘汰逻辑 (缓冲池大小 = 3) ---");

        // 1. 填满缓冲池
        System.out.println("第 1 步: 填满缓冲池...");
        PageId p0 = bufferPoolManager.newPage().getPageId();
        System.out.println("  - 获取页面 P0。当前缓冲: [P0]");
        PageId p1 = bufferPoolManager.newPage().getPageId();
        System.out.println("  - 获取页面 P1。当前缓冲: [P0, P1]");
        PageId p2 = bufferPoolManager.newPage().getPageId();
        System.out.println("  - 获取页面 P2。当前缓冲: [P0, P1, P2]。缓冲池已满。");
        assertEquals(3, bufferPoolManager.getPageTable().size());

        // 2. 插入一个新页面 (p3)，这将触发淘汰
        System.out.println("\n第 2 步: 插入页面 P3 以触发淘汰...");
        bufferPoolManager.newPage(); // 这将淘汰 P0
        System.out.println("  - 已获取页面 P3。");

        // 3. 验证
        System.out.println("\n第 3 步: 验证缓冲池状态...");
        assertNull(bufferPoolManager.getPageTable().get(p0), "[失败] 页面 P0 应该已被 FIFO 策略淘汰。");
        System.out.println("  - 验证通过: 页面 P0 不在缓冲池中。");
        assertNotNull(bufferPoolManager.getPageTable().get(p1), "[失败] 页面 P1 应该仍在缓冲池中。");
        System.out.println("  - 验证通过: 页面 P1 仍在缓冲池中。");
        assertNotNull(bufferPoolManager.getPageTable().get(p2), "[失败] 页面 P2 应该仍在缓冲池中。");
        System.out.println("  - 验证通过: 页面 P2 仍在缓冲池中。");
        System.out.println("  - 最终缓冲状态: [P1, P2, P3]");

        System.out.println("\n--- FIFO 淘汰测试通过 ---");
    }

    @Test
    void testLruEvictionLogic() throws IOException {
        initializeManagers("LRU", 3);
        System.out.println("\n--- [测试] LRU 淘汰逻辑 (缓冲池大小 = 3) ---");

        // 1. 填满缓冲池
        System.out.println("第 1 步: 填满缓冲池...");
        PageId p0 = bufferPoolManager.newPage().getPageId();
        System.out.println("  - 获取页面 P0。使用顺序: [P0]");
        PageId p1 = bufferPoolManager.newPage().getPageId();
        System.out.println("  - 获取页面 P1。使用顺序: [P0, P1]");
        PageId p2 = bufferPoolManager.newPage().getPageId();
        System.out.println("  - 获取页面 P2。使用顺序: [P0, P1, P2]。缓冲池已满。");
        assertEquals(3, bufferPoolManager.getPageTable().size());

        // 2. 访问 p0，使其成为最近使用的页面
        System.out.println("\n第 2 步: 访问页面 P0，使其变为最近使用...");
        bufferPoolManager.getPage(p0);
        System.out.println("  - 已访问页面 P0。当前使用顺序: [P1, P2, P0]。");

        // 3. 插入一个新页面 (p3)，这将触发淘汰
        // 此时，p1 是最久未使用的页面
        System.out.println("\n第 3 步: 插入页面 P3 以触发淘汰...");
        bufferPoolManager.newPage(); // 应该淘汰 P1
        System.out.println("  - 已获取页面 P3。");

        // 4. 验证
        System.out.println("\n第 4 步: 验证缓冲池状态...");
        assertNotNull(bufferPoolManager.getPageTable().get(p0), "[失败] 页面 P0 不应该被淘汰。");
        System.out.println("  - 验证通过: 页面 P0 仍在缓冲池中。");
        assertNull(bufferPoolManager.getPageTable().get(p1), "[失败] 页面 P1 (最久未使用的页面) 应该已被淘汰。");
        System.out.println("  - 验证通过: 页面 P1 不在缓冲池中。");
        assertNotNull(bufferPoolManager.getPageTable().get(p2), "[失败] 页面 P2 应该仍在缓冲池中。");
        System.out.println("  - 验证通过: 页面 P2 仍在缓冲池中。");
        System.out.println("  - 最终缓冲状态: [P2, P0, P3]");

        System.out.println("\n--- LRU 淘汰测试通过 ---");
    }

    @Test
    void testClockEvictionLogicWithDetailedLogs() throws IOException {
        initializeManagers("CLOCK", 3);
        System.out.println("\n--- [测试] Clock 淘汰逻辑及详细日志 (缓冲池大小 = 3) ---");

        // 1. 填满缓冲池，所有页面的引用位都为 1
        System.out.println("\n第 1 步: 填满缓冲池...");
        PageId p0 = bufferPoolManager.newPage().getPageId();
        PageId p1 = bufferPoolManager.newPage().getPageId();
        PageId p2 = bufferPoolManager.newPage().getPageId();
        assertEquals(3, bufferPoolManager.getPageTable().size());
        System.out.println("缓冲池已满。");

        // 2. 再次访问 p1，以确保它的引用位是 1
        System.out.println("\n第 2 步: 再次访问页面 P1 (其引用位已为1, 此处仅为演示)...");
        bufferPoolManager.getPage(p1);

        // 3. 插入新页面 p3，触发淘汰
        // 预期行为:
        // - 时钟指针检查 P0(ref=1)，给予第二次机会 -> P0(ref=0)，移到队尾
        // - 时钟指针检查 P1(ref=1)，给予第二次机会 -> P1(ref=0)，移到队尾
        // - 时钟指针检查 P2(ref=1)，给予第二次机会 -> P2(ref=0)，移到队尾
        // - 时钟指针再次检查 P0(ref=0)，找到牺牲品并淘汰
        System.out.println("\n第 3 步: 插入页面 P3 以触发淘汰。请观察下方来自 ClockReplacer 的详细日志...");
        bufferPoolManager.newPage();

        // 4. 验证结果
        System.out.println("\n第 4 步: 验证淘汰后的缓冲池状态...");
        assertNull(bufferPoolManager.getPageTable().get(p0), "[失败] 页面 P0 应该已被 Clock 算法淘汰。");
        System.out.println("  - 验证通过: 页面 P0 不在缓冲池中。");
        assertNotNull(bufferPoolManager.getPageTable().get(p1), "[失败] 页面 P1 应该获得了第二次机会而保留。");
        System.out.println("  - 验证通过: 页面 P1 仍在缓冲池中。");
        assertNotNull(bufferPoolManager.getPageTable().get(p2), "[失败] 页面 P2 应该获得了第二次机会而保留。");
        System.out.println("  - 验证通过: 页面 P2 仍在缓冲池中。");

        System.out.println("\n--- Clock 淘汰测试通过 ---");
    }
}