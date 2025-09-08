package org.csu.sdolp;

import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

public class DiskManagerTest {
    private DiskManager diskManager;
    private final String TEST_FILE = "test.db";

    @BeforeEach
    void setUp() throws IOException {
        // 在每个测试前创建 DiskManager 实例和测试文件
        diskManager = new DiskManager(TEST_FILE);
        diskManager.open();
    }

    @AfterEach
    void tearDown() throws IOException {
        // 在每个测试后关闭并删除测试文件
        diskManager.close();
        new File(TEST_FILE).delete();
    }

    @Test
    void testWriteAndReadPage() throws IOException {
        // 1. 创建一个新页并写入数据
        PageId pageId = new PageId(0);
        Page page = new Page(pageId);
        byte[] testData = "Hello, MiniDB!".getBytes();
        page.getData().put(testData);

        // 2. 将页写入磁盘
        diskManager.writePage(page);

        // 3. 从磁盘读回同一页
        Page readPage = diskManager.readPage(pageId);
        byte[] readData = new byte[testData.length];
        readPage.getData().get(readData);

        // 4. 验证数据是否一致
        assertArrayEquals(testData, readData);
    }
    
    @Test
    void testAllocatePage() throws IOException {
        // 1. 初始文件为空，分配第一个页号
        PageId pageId1 = diskManager.allocatePage();
        assertEquals(0, pageId1.getPageNum());
        
        // 2. 写入一个页，模拟文件增长
        Page page = new Page(new PageId(0));
        diskManager.writePage(page);
        
        // 3. 再次分配，应该得到下一个页号
        PageId pageId2 = diskManager.allocatePage();
        assertEquals(1, pageId2.getPageNum());
    }
}