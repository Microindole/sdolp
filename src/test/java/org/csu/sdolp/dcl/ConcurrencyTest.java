package org.csu.sdolp.dcl;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.PageId;
import org.csu.sdolp.transaction.LockManager;
import org.csu.sdolp.transaction.Transaction;
import org.csu.sdolp.transaction.TransactionManager;
import org.csu.sdolp.transaction.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 专门用于测试并发控制和锁管理器的集成测试.
 */
public class ConcurrencyTest {

    private final String TEST_DB_NAME = "concurrency_test_db";
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private LogManager logManager;
    private LockManager lockManager;
    private TransactionManager transactionManager;
    private Catalog catalog;
    private TableInfo testTableInfo;

    @BeforeEach
    void setUp() throws IOException {
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        diskManager = new DiskManager("data/" + TEST_DB_NAME + "/minidb.data");
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(100, diskManager, "LRU");
        logManager = new LogManager("data/" + TEST_DB_NAME + "/minidb.data.log");
        lockManager = new LockManager();
        transactionManager = new TransactionManager(lockManager, logManager);
        catalog = new Catalog(bufferPoolManager);
        testTableInfo = catalog.createTable("test_table", new Schema(Arrays.asList(new Column("id", DataType.INT))));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (logManager != null) logManager.close();
        if (diskManager != null) diskManager.close();
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void testWriterBlocksReader() throws InterruptedException {
        System.out.println("--- Test: Writer (Exclusive Lock) should block Reader (Shared Lock) ---");

        TableHeap tableHeap = new TableHeap(bufferPoolManager, testTableInfo, logManager, lockManager);
        PageId pageId = testTableInfo.getFirstPageId();

        CountDownLatch writerStartedLocking = new CountDownLatch(1);
        CountDownLatch readerFinished = new CountDownLatch(1);
        AtomicBoolean readerWasBlocked = new AtomicBoolean(false);

        // 线程A: 写入者 (Writer)
        Thread writerThread = new Thread(() -> {
            try {
                Transaction writerTxn = transactionManager.begin();
                System.out.println("[Writer]  : Tries to acquire EXCLUSIVE lock on page " + pageId.getPageNum());
                lockManager.lockExclusive(writerTxn, pageId);
                System.out.println("[Writer]  : Acquired EXCLUSIVE lock. Notifying reader to start.");
                writerStartedLocking.countDown(); // 通知读者可以开始了

                // 持有锁一段时间，模拟正在进行写操作
                Thread.sleep(500);

                System.out.println("[Writer]  : Releasing EXCLUSIVE lock.");
                lockManager.unlock(writerTxn, pageId);
                transactionManager.commit(writerTxn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // 线程B: 读取者 (Reader)
        Thread readerThread = new Thread(() -> {
            try {
                // 确保写入者已经持有了锁
                writerStartedLocking.await(2, TimeUnit.SECONDS);

                Transaction readerTxn = transactionManager.begin();
                long startTime = System.currentTimeMillis();
                System.out.println("[Reader]  : Tries to acquire SHARED lock on page " + pageId.getPageNum());
                lockManager.lockShared(readerTxn, pageId);
                long endTime = System.currentTimeMillis();
                System.out.println("[Reader]  : Acquired SHARED lock.");

                // 如果获取锁的时间超过了写入者持有锁时间的一半，我们就认为它被阻塞了
                if (endTime - startTime > 250) {
                    readerWasBlocked.set(true);
                }

                lockManager.unlock(readerTxn, pageId);
                transactionManager.commit(readerTxn);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                readerFinished.countDown();
            }
        });

        writerThread.start();
        readerThread.start();

        // 等待读者线程完成
        readerFinished.await(5, TimeUnit.SECONDS);

        assertTrue(readerWasBlocked.get(), "Reader should have been blocked by the writer's exclusive lock.");
        System.out.println("\n[SUCCESS] Verification passed. The reader was correctly blocked.");
    }

    private void deleteDirectory(File directory) {
        if (!directory.exists()) return;
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}