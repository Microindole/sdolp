package org.csu.sdolp;

import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.PageId;
import org.csu.sdolp.storage.buffer.replacement.MLFQReplacer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Queue;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门用于测试 MLFQ (Multi-level Feedback Queue) 缓存替换策略的单元测试。
 * [最终版 - 逻辑修正]
 */
public class MLFQReplacerTest {

    private final String TEST_DB_FILE = "mlfq_test.db";
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

    // 其他测试方法 (testNewPageGoesToMediumQueue, testPagePromotionFromMediumToHigh) 保持不变...

    @Test
    void testNewPageGoesToMediumQueue() throws IOException {
        bufferPoolManager = new BufferPoolManager(5, diskManager, "MLFQ");
        PageId pageId0 = bufferPoolManager.newPage().getPageId();

        System.out.println("--- testNewPageGoesToMediumQueue ---");
        printAllQueuesState();

        try {
            Object replacer = getReplacer();
            Queue<PageId> mediumQueue = getQueue(replacer, "mediumPriorityQueue");
            assertEquals(1, mediumQueue.size(), "新页面应该在中优先级队列中");
            assertTrue(mediumQueue.contains(pageId0), "中优先级队列应包含 pageId0");
        } catch (Exception e) {
            fail("测试失败: " + e.getMessage());
        }
    }

    @Test
    void testPagePromotionFromMediumToHigh() throws IOException {
        bufferPoolManager = new BufferPoolManager(5, diskManager, "MLFQ");
        PageId pageId0 = bufferPoolManager.newPage().getPageId();

        System.out.println("--- testPagePromotionFromMediumToHigh ---");
        System.out.println("Before promotion:");
        printAllQueuesState();

        bufferPoolManager.getPage(pageId0); // 第二次访问，触发晋升

        System.out.println("After promotion:");
        printAllQueuesState();

        try {
            Object replacer = getReplacer();
            Queue<PageId> mediumQueue = getQueue(replacer, "mediumPriorityQueue");
            Queue<PageId> highQueue = getQueue(replacer, "highPriorityQueue");

            assertTrue(mediumQueue.isEmpty(), "页面晋升后，中优先级队列应该为空");
            assertEquals(1, highQueue.size(), "页面应该被晋升到高优先级队列");
            assertTrue(highQueue.contains(pageId0), "高优先级队列应包含 pageId0");
        } catch (Exception e) {
            fail("测试失败: " + e.getMessage());
        }
    }

    /**
     * [最终版 - 逻辑修正] 测试页面淘汰顺序是否严格符合 低 -> 中 -> 高 的原则。
     */
    @Test
    void testEvictionOrderIsCorrect() throws IOException {
        bufferPoolManager = new BufferPoolManager(3, diskManager, "MLFQ");
        MLFQReplacer replacer = (MLFQReplacer) getReplacer();

        // --- 步骤 1: 准备数据 ---
        System.out.println("--- 步骤 1: 准备数据 ---");
        PageId p0 = bufferPoolManager.newPage().getPageId();
        PageId p1 = bufferPoolManager.newPage().getPageId();
        PageId p2 = bufferPoolManager.newPage().getPageId();
        bufferPoolManager.getPage(p0);
        bufferPoolManager.getPage(p1);
        bufferPoolManager.getPage(p2);

        replacer.demote(p1);
        replacer.demote(p2);
        replacer.demote(p2);
        System.out.println("准备完成后的队列状态:");
        printAllQueuesState();

        // --- 步骤 2: 验证淘汰顺序 ---

        // Part A: 验证低优先级队列
        System.out.println("\n--- Part A: 验证淘汰 Low Priority ---");
        bufferPoolManager.newPage(); // 插入 p3, 淘汰 p2
        assertNull(bufferPoolManager.getPageTable().get(p2), "Page 2 (low) 应该被淘汰");
        printAllQueuesState();

        // Part B: 验证中优先级队列
        System.out.println("\n--- Part B: 验证淘汰 Medium Priority ---");
        bufferPoolManager.newPage(); // 插入 p4, 淘汰 p1
        assertNull(bufferPoolManager.getPageTable().get(p1), "Page 1 (medium) 应该被淘汰");
        printAllQueuesState();

        // Part C: 验证高优先级队列
        System.out.println("\n--- Part C: 验证淘汰 High Priority ---");
        // 当前 medium 队列中有 p3, p4。需要将它们全部提升到 high, 从而清空 medium。
        System.out.println("提升 p3 和 p4 到 High 队列，以清空 Medium 队列...");
        bufferPoolManager.getPage(new PageId(3)); // p3 -> high
        bufferPoolManager.getPage(new PageId(4)); // p4 -> high
        printAllQueuesState();

        // 现在 low 和 medium 队列都为空，下一次淘汰必然来自 high 队列
        System.out.println("Medium/Low 队列已清空，准备从 High 队列淘汰...");
        bufferPoolManager.newPage(); // 插入 p5, 此时应该淘汰 p0 (high 队列的队首)
        printAllQueuesState();

        assertNull(bufferPoolManager.getPageTable().get(p0), "Page 0 (high) 应该被淘汰");

        System.out.println("\n淘汰顺序测试通过！");
    }

    private Object getReplacer() {
        try {
            Field replacerField = BufferPoolManager.class.getDeclaredField("replacer");
            replacerField.setAccessible(true);
            return replacerField.get(bufferPoolManager);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("无法获取 Replacer 实例", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Queue<PageId> getQueue(Object replacer, String queueName) {
        try {
            Field queueField = replacer.getClass().getDeclaredField(queueName);
            queueField.setAccessible(true);
            return (Queue<PageId>) queueField.get(replacer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("无法获取队列: " + queueName, e);
        }
    }

    private void printAllQueuesState() {
        if (bufferPoolManager == null) return;
        try {
            Object replacer = getReplacer();
            Queue<PageId> high = getQueue(replacer, "highPriorityQueue");
            Queue<PageId> medium = getQueue(replacer, "mediumPriorityQueue");
            Queue<PageId> low = getQueue(replacer, "lowPriorityQueue");

            System.out.println(String.format("  [State] High: %s, Medium: %s, Low: %s, PageTable Size: %d",
                    formatQueue(high), formatQueue(medium), formatQueue(low), bufferPoolManager.getPageTable().size()));
        } catch (Exception e) {
            System.out.println("  [State]无法打印队列状态: " + e.getMessage());
        }
    }

    private String formatQueue(Queue<PageId> queue) {
        StringJoiner sj = new StringJoiner(", ", "[", "]");
        for (PageId id : queue) {
            sj.add(String.valueOf(id.getPageNum()));
        }
        return sj.toString();
    }
}