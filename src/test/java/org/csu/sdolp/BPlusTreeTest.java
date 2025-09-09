package org.csu.sdolp;

import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.index.BPlusTree;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.PageId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class BPlusTreeTest {

    private final String TEST_DB_FILE = "test_bplustree.db";
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private BPlusTree bPlusTree;

    @BeforeEach
    void setUp() throws IOException {
        new File(TEST_DB_FILE).delete();
        diskManager = new DiskManager(TEST_DB_FILE);
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        // 假设B+树的根在page 2 (page 0和1被目录占用)
        bPlusTree = new BPlusTree(bufferPoolManager, null);
    }

    @AfterEach
    void tearDown() throws IOException {
        diskManager.close();
        new File(TEST_DB_FILE).delete();
    }

    @Test
    void testInsertAndSearchInEmptyTree() throws IOException {
        System.out.println("--- 测试: 向空树中插入并查找 ---");

        // 定义要插入的数据
        Value key1 = new Value(10);
        PageId tupleId1 = new PageId(100);
        bPlusTree.insert(key1, tupleId1);


        // Verify search
        PageId result1 = bPlusTree.search(key1);
        assertNotNull(result1, "Should find key 10");
        assertEquals(tupleId1.getPageNum(), result1.getPageNum(), "The PageNum should match");
        System.out.println("Successfully found key: 10 -> page: " + result1.getPageNum());

        // Verify searching for a non-existent key
        PageId resultNonExistent = bPlusTree.search(new Value(999));
        assertNull(resultNonExistent, "Should not find a non-existent key");
        System.out.println("Successfully verified that non-existent key 999 is not found.");
    }

    // 后续可以添加更多测试，比如节点分裂、合并等
}