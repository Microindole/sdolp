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
        new File(TEST_FILE).delete();
        diskManager = new DiskManager(TEST_FILE);
        diskManager.open();
    }

    @AfterEach
    void tearDown() throws IOException {
        diskManager.close();
        new File(TEST_FILE).delete();
    }

    @Test
    void testWriteAndReadPage() throws IOException {
        PageId pageId = new PageId(0);
        Page page = new Page(pageId);
        byte[] testData = "Hello, MiniDB!".getBytes();
        page.getData().put(testData);
        diskManager.writePage(page);
        Page readPage = diskManager.readPage(pageId);
        byte[] readData = new byte[testData.length];
        readPage.getData().get(readData);
        assertArrayEquals(testData, readData);
    }

    @Test
    void testDeallocateAndReallocatePage() throws IOException {
        System.out.println("--- 测试页释放与重分配 ---");

        // 1. 连续分配4个新页 (现在从 page 0 开始)
        PageId pageId0 = diskManager.allocatePage();
        PageId pageId1 = diskManager.allocatePage();
        PageId pageId2 = diskManager.allocatePage();
        PageId pageId3 = diskManager.allocatePage();

        assertEquals(0, pageId0.getPageNum(), "第一个分配的页号应该是 0");
        assertEquals(1, pageId1.getPageNum(), "第二个分配的页号应该是 1");
        assertEquals(2, pageId2.getPageNum(), "第三个分配的页号应该是 2");
        assertEquals(3, pageId3.getPageNum(), "第四个分配的页号应该是 3");

        // 2. 释放中间的两个页：page 2 和 page 1
        diskManager.deallocatePage(pageId2); // 先释放 2
        diskManager.deallocatePage(pageId1); // 再释放 1
        // 空闲链表: head -> 1 -> 2 -> null

        // 3. 再次请求分配两个新页
        PageId reusedPageId1 = diskManager.allocatePage();
        PageId reusedPageId2 = diskManager.allocatePage();

        // 4. 验证重用的页号是否正确 (后释放的先被重用)
        assertEquals(1, reusedPageId1.getPageNum(), "应该首先重用最后被释放的 Page 1");
        assertEquals(2, reusedPageId2.getPageNum(), "其次应该重用 Page 2");

        // 5. 再次请求分配一个新页，此时空闲链表已空
        PageId brandNewPageId = diskManager.allocatePage();
        assertEquals(4, brandNewPageId.getPageNum(), "空闲页用完后，应该分配文件末尾的新 Page 4");
    }

    @Test
    void testDeallocationPersistence() throws IOException {
        System.out.println("\n--- 测试页释放的持久化 ---");

        // --- 第一次会话 ---
        diskManager.allocatePage(); // Page 0
        PageId pageId1 = diskManager.allocatePage(); // Page 1
        diskManager.allocatePage(); // Page 2
        diskManager.deallocatePage(pageId1); // 释放 Page 1
        diskManager.close(); // 关闭，此时 freeListHeadPageId=1 应该被持久化

        // --- 第二次会话 ---
        diskManager = new DiskManager(TEST_FILE);
        diskManager.open(); // 重新打开，应该能读到 freeListHeadPageId=1

        // 请求一个新页，预期重用 Page 1
        PageId reusedPageId = diskManager.allocatePage();
        assertEquals(1, reusedPageId.getPageNum(), "重启后，应该能重用之前被释放的 Page 1");
    }
}