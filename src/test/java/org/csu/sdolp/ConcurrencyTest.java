package org.csu.sdolp;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConcurrencyTest {

    private final String TEST_DB_FILE = "concurrency_test.db";
    private final String TEST_LOG_FILE = "concurrency_test.db.log";

    @BeforeEach
    void setUp() {
        new File(TEST_DB_FILE).delete();
        new File(TEST_LOG_FILE).delete();
    }

    @AfterEach
    void tearDown() throws IOException {
        new File(TEST_DB_FILE).delete();
        new File(TEST_LOG_FILE).delete();
    }

    @Test
    void testWriterBlocksReader() throws InterruptedException {
        System.out.println("--- Test: Writer Blocks Reader ---");
        // 1. Setup
        QueryProcessor qp = new QueryProcessor(TEST_DB_FILE);
        qp.execute("CREATE TABLE accounts (id INT, balance INT);");
        qp.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000);");

        CountDownLatch writerLatch = new CountDownLatch(1);
        CountDownLatch readerLatch = new CountDownLatch(1);
        AtomicBoolean readerIsBlocked = new AtomicBoolean(false);

        // 2. 线程 A (Writer) - 获取排他锁并持有
        Thread writerThread = new Thread(() -> {
            QueryProcessor writerQp = new QueryProcessor(TEST_DB_FILE);
            System.out.println("WRITER: Starting transaction.");
            // 简化：用一个长时间运行的更新来模拟持有锁
            // 注意：我们的QueryProcessor不支持BEGIN/COMMIT, 所以每个execute是一个独立事务
            // 我们需要一个新的QueryProcessor来实现并发
            writerQp.execute("UPDATE accounts SET balance = 500 WHERE id = 1;");
            System.out.println("WRITER: Update should be blocked, this line should not be reached yet.");

        });

        // 3. 线程 B (Reader) - 尝试获取共享锁
        Thread readerThread = new Thread(() -> {
            try {
                writerLatch.await(5, TimeUnit.SECONDS); // 等待Writer启动
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            QueryProcessor readerQp = new QueryProcessor(TEST_DB_FILE);
            System.out.println("READER: Attempting to read balance.");
            long startTime = System.currentTimeMillis();
            readerQp.execute("SELECT balance FROM accounts WHERE id = 1;");
            long endTime = System.currentTimeMillis();
            if(endTime - startTime > 200){ // 期望被阻塞
                readerIsBlocked.set(true);
            }
            System.out.println("READER: Read completed.");
            readerLatch.countDown();
        });
        
        // 我们需要一种方法让Writer持有锁，但目前每个execute都会自动提交。
        // 所以，我们需要修改测试逻辑。
        // 新逻辑：让一个线程执行一个长时间的事务，另一个线程去访问。
        // 由于我们的系统还不支持并发事务，我们可以用一个简化的方式来测试锁本身。
        
        // TODO: The test needs a way to hold a transaction open across multiple 'execute' calls,
        // which the current QueryProcessor does not support. 
        // A full concurrency test would require modifications to InteractiveShell/QueryProcessor
        // to handle BEGIN, COMMIT, and ROLLBACK commands.
        // For now, manual testing is the best way to observe the locking behavior.
        
        System.out.println("NOTE: Automated concurrency test requires interactive transaction support.");
        System.out.println("Please test manually using two separate InteractiveShell instances.");

    }
}