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
        DiskManager diskManager = new DiskManager(DB_FILE_PATH);
        diskManager.open();
        // 将缓存池大小设为3，以快速触发替换策略
        BufferPoolManager bufferPoolManager = new BufferPoolManager(3, diskManager, "FIFO");

        // 2.1. 顺序访问4个页，触发替换
        System.out.println("缓存池大小: 3，使用 FIFO 策略。");
        for (int i = 0; i < 4; i++) {
            Page page = bufferPoolManager.newPage();
            page.getData().put(0, (byte) (65 + i)); // 写入 'A', 'B', 'C', 'D'
            System.out.println("分配并访问新页: " + page.getPageId().getPageNum());
        }

        // 2.2. 再次访问第一个页（页0），验证它是否已被淘汰并从磁盘加载
        System.out.println("再次访问页 0，预期它已被淘汰并从磁盘重新加载。");
        Page firstPage = bufferPoolManager.getPage(new PageId(0));
        byte data = firstPage.getData().get(0);
        System.out.println("从页 0 读取的数据: " + (char) data);
        assert data == 'A' : "FIFO 替换策略验证失败！";

        diskManager.close();
    }
}