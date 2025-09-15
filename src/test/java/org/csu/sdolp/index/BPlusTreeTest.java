package org.csu.sdolp.index;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.index.BPlusTree;
import org.csu.sdolp.storage.page.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BPlusTree 的最终完整功能测试类。
 * 包含专门针对节点分裂的精确测试，以及大规模随机压力测试。
 */
public class BPlusTreeTest {

    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private BPlusTree bPlusTree;
    private static final String TEST_DB_FILE_PREFIX = "bpt_stress_test_";

    private String testDbFile;

    @BeforeEach
    public void setUp(TestInfo testInfo) throws IOException {
        String testName = testInfo.getTestMethod().orElseThrow().getName();
        testDbFile = TEST_DB_FILE_PREFIX + testName + ".db";

        File dbFile = new File(testDbFile);
        if (dbFile.exists()) {
            dbFile.delete();
        }

        diskManager = new DiskManager(testDbFile);
        diskManager.open();
        // 增加 BufferPool 的大小以适应更大规模的测试
        bufferPoolManager = new BufferPoolManager(200, diskManager, "LRU");

        Page rootPage = bufferPoolManager.newPage();
        bPlusTree = new BPlusTree(bufferPoolManager, rootPage.getPageId().getPageNum());
        System.out.println("\n--- Starting Stress Test: " + testName + " ---");
    }

    @AfterEach
    public void tearDown() throws IOException {
        System.out.println("--- Stress Test Finished ---");
        bufferPoolManager.flushAllPages();
        diskManager.close();
        new File(testDbFile).delete();
    }

    @Test
    public void testMassiveInsertDeleteAndSearch() throws IOException {
        int initialInsertSize = 200000;
        int deleteSize = 100000;
        int secondInsertSize = 50000;

        Random random = new Random();
        Set<Integer> presentKeys = new HashSet<>();
        List<Integer> initialKeys = new ArrayList<>();

        // --- 阶段 1: 大规模随机插入 ---
        System.out.println("\n[Phase 1] Inserting " + initialInsertSize + " random keys...");
        for (int i = 0; i < initialInsertSize; i++) {
            int key = random.nextInt();
            // 确保key的唯一性
            while(presentKeys.contains(key)){
                key = random.nextInt();
            }
            initialKeys.add(key);
            presentKeys.add(key);
            bPlusTree.insert(new Value(key), new RID(key, key));
        }
        System.out.println("[Phase 1] Insertion complete. Total keys: " + presentKeys.size());


        // --- 阶段 2: 验证初次插入 ---
        System.out.println("\n[Phase 2] Verifying all initially inserted keys...");
        Collections.shuffle(initialKeys); // 随机顺序验证
        for (int key : initialKeys) {
            RID foundRid = bPlusTree.search(new Value(key));
            assertNotNull(foundRid, "Key " + key + " should be found after initial insertion, but was not.");
            assertEquals(key, foundRid.pageNum(), "RID data for key " + key + " is incorrect.");
        }
        System.out.println("[Phase 2] Verification successful.");


        // --- 阶段 3: 大规模随机删除 ---
        System.out.println("\n[Phase 3] Deleting " + deleteSize + " random keys...");
        List<Integer> keysToDelete = new ArrayList<>(initialKeys.subList(0, deleteSize));
        for (int key : keysToDelete) {
            assertTrue(bPlusTree.delete(new Value(key)), "Failed to delete key: " + key);
            presentKeys.remove(key);
        }
        System.out.println("[Phase 3] Deletion complete. Remaining keys: " + presentKeys.size());


        // --- 阶段 4: 验证删除结果 ---
        System.out.println("\n[Phase 4] Verifying deletion results...");
        for (int key : keysToDelete) {
            assertNull(bPlusTree.search(new Value(key)), "Deleted key " + key + " was found in the tree.");
        }
        System.out.println("[Phase 4] Verification of deleted keys successful.");


        // --- 阶段 5: 再次插入新数据 ---
        // 这个阶段很重要，用于测试在经历了大量删除和合并操作后，树的插入功能是否正常
        System.out.println("\n[Phase 5] Inserting " + secondInsertSize + " new random keys...");
        for (int i = 0; i < secondInsertSize; i++) {
            int key = random.nextInt();
            while(presentKeys.contains(key)){ // 确保新key的唯一性
                key = random.nextInt();
            }
            presentKeys.add(key);
            bPlusTree.insert(new Value(key), new RID(key, key));
        }
        System.out.println("[Phase 5] Second insertion complete. Total keys now: " + presentKeys.size());


        // --- 阶段 6: 最终全面验证 ---
        System.out.println("\n[Phase 6] Final comprehensive verification...");
        int verifiedCount = 0;
        for (int key : presentKeys) {
            RID foundRid = bPlusTree.search(new Value(key));
            assertNotNull(foundRid, "Key " + key + " should be present in the final tree, but was not found.");
            assertEquals(key, foundRid.pageNum());
            verifiedCount++;
        }
        assertEquals(presentKeys.size(), verifiedCount, "The number of verified keys does not match the expected count.");
        System.out.println("[Phase 6] Successfully verified " + verifiedCount + " keys. The B+ Tree is stable.");
    }
}