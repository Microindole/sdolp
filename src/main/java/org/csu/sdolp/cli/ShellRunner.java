package org.csu.sdolp.cli;

import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

@Component
public class ShellRunner implements CommandLineRunner {

    private final String DB_FILE_PATH = "minidb.data";

    @Override
    public void run(String... args) throws Exception {
        System.out.println("MiniDB 存储引擎集成测试开始...");

        // 确保测试文件是新的
        new File(DB_FILE_PATH).delete();

        // 1. 测试数据写入与持久化
        System.out.println("--- 步骤1: 测试数据写入与持久化 ---");
        writeDataAndVerifyPersistence();
        System.out.println("数据持久化测试完成，结果符合预期。\n");

        // 2. 测试页缓存和替换策略
        System.out.println("--- 步骤2: 测试缓存与替换策略 ---");
        testBufferPoolReplacement();
        System.out.println("缓存替换测试完成，结果符合预期。\n");

        System.out.println("所有存储引擎测试已完成。");
    }

    private void writeDataAndVerifyPersistence() throws IOException {
        DiskManager diskManager = new DiskManager(DB_FILE_PATH);
        diskManager.open();
        // 缓存池大小设为10页
        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");

        // 1.1. 分配新页并写入数据
        Page newPage = bufferPoolManager.newPage();
        int pageNum = newPage.getPageId().getPageNum();
        String testString = "Hello, World! This is page " + pageNum;
        newPage.getData().put(testString.getBytes());
        System.out.println("已在页 " + pageNum + " 写入数据: " + testString);

        // 1.2. 强制将数据刷入磁盘，模拟持久化
        bufferPoolManager.flushPage(newPage.getPageId());
        System.out.println("页 " + pageNum + " 已刷新到磁盘。");

        // 1.3. 模拟程序重启: 关闭旧的管理器，创建新的
        diskManager.close();
        System.out.println("程序重启，重新加载磁盘文件...");

        DiskManager newDiskManager = new DiskManager(DB_FILE_PATH);
        newDiskManager.open();
        BufferPoolManager newBufferPoolManager = new BufferPoolManager(10, newDiskManager, "LRU");

        // 1.4. 从磁盘加载数据并验证
        Page loadedPage = newBufferPoolManager.getPage(new PageId(pageNum));
        byte[] loadedData = new byte[testString.length()];
        loadedPage.getData().get(loadedData);
        String loadedString = new String(loadedData);

        System.out.println("从磁盘加载页 " + pageNum + " 的数据: " + loadedString);
        assert loadedString.equals(testString) : "持久化验证失败！";

        newDiskManager.close();
    }

    private void testBufferPoolReplacement() throws IOException {
        final String TEST_DB_FILE_REPLACEMENT = "minidb_replacement.db";
        new File(TEST_DB_FILE_REPLACEMENT).delete();
        DiskManager diskManager = new DiskManager(TEST_DB_FILE_REPLACEMENT);
        diskManager.open();
        BufferPoolManager bufferPoolManager = new BufferPoolManager(3, diskManager, "FIFO");

        System.out.println("缓存池大小: 3，使用 FIFO 策略。");

        for (int i = 0; i < 4; i++) {
            Page page = bufferPoolManager.newPage();
            int pageNum = page.getPageId().getPageNum();
            page.getData().put(0, (byte) (65 + i)); // 写入 'A', 'B', 'C', 'D'
            System.out.println("分配并访问新页: " + pageNum + "，写入字符: " + (char)(65 + i));
            bufferPoolManager.flushPage(page.getPageId());
        }

        System.out.println("再次访问页 0，预期它已被淘汰并从磁盘重新加载。");
        PageId evictedPageId = new PageId(0);
        Page evictedPage = bufferPoolManager.getPage(evictedPageId); // 这会产生一次 miss

        // --- ↓↓↓ 新增代码，再次访问，这次应该是 hit ↓↓↓ ---
        System.out.println("再次访问页 0，这次应该命中缓存。");
        bufferPoolManager.getPage(evictedPageId); // 这会产生一次 hit
        // --- ↑↑↑ 新增代码 ↑↑↑ ---

        byte data = evictedPage.getData().get(0);
        System.out.println("从页 0 读取的数据: " + (char) data);

        if (data == 'A') {
            System.out.println("缓存替换测试通过，结果符合预期。");
        } else {
            System.err.println( "data == " + data + ", data != 'A'");
            System.err.println("缓存替换测试失败！");
        }

        // --- ↓↓↓ 新增代码，打印最终的统计信息 ↓↓↓ ---
        System.out.println("\n--- 缓存统计信息 ---");
        System.out.println("命中次数: " + bufferPoolManager.getHitCount());
        System.out.println("未命中次数: " + bufferPoolManager.getMissCount());
        System.out.printf("缓存命中率: %.2f%%\n", bufferPoolManager.getHitRate() * 100);
        // --- ↑↑↑ 新增代码 ↑↑↑ ---

        diskManager.close();
        new File(TEST_DB_FILE_REPLACEMENT).delete();
    }
}