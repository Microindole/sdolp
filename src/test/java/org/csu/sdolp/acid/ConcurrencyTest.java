package org.csu.sdolp.acid;

import org.csu.sdolp.cli.server.Session;
import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证事务并发控制和锁机制的集成测试。
 */
public class ConcurrencyTest {

    private final String TEST_DB_NAME = "concurrency_test_db";
    private QueryProcessor qp;

    // --- 新增：用于生成带时间戳和线程名的日志 ---
    private final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    private synchronized void log(String message) {
        System.out.printf("[%s] [%s] %s%n",
                sdf.format(new Date()),
                Thread.currentThread().getName(),
                message);
    }
    // -----------------------------------------

    @BeforeEach
    void setUp() {
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        qp = new QueryProcessor(TEST_DB_NAME);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (qp != null) {
            qp.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    /**
     * 测试核心场景：写-读冲突。
     * 一个事务正在写入（持有排他锁），另一个事务尝试读取，应该被阻塞。
     */
    @Test
    void testWriterBlocksReader() throws InterruptedException {
        log("--- [测试开始] 写-读冲突：验证写入者会阻塞读取者 ---");

        // 步骤 1: 环境准备
        log("环境准备：由 root 用户创建表、用户并授权...");
        qp.execute("CREATE TABLE accounts (id INT, balance INT);");
        qp.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000);");
        qp.execute("CREATE USER 'writer' IDENTIFIED BY 'w_pass';");
        qp.execute("CREATE USER 'reader' IDENTIFIED BY 'r_pass';");
        qp.execute("GRANT UPDATE ON accounts TO 'writer';");
        qp.execute("GRANT SELECT ON accounts TO 'reader';");
        log("环境准备完毕。");

        // 用于线程同步的工具
        CountDownLatch writerAcquiredLock = new CountDownLatch(1);
        CountDownLatch testFinished = new CountDownLatch(2); // 等待两个线程都结束
        AtomicBoolean readerWasBlocked = new AtomicBoolean(false);

        // --- 线程 A: 写入者 ---
        Thread writerThread = new Thread(() -> {
            Session writerSession = Session.createAuthenticatedSession(1, "writer");
            try {
                log("开启事务...");
                qp.executeAndGetResult("BEGIN;", writerSession);

                log("执行 UPDATE, 准备请求排他锁...");
                qp.executeAndGetResult("UPDATE accounts SET balance = 500 WHERE id = 1;", writerSession);
                log("...成功获取排他锁！");

                // 通知读取者线程：我已经拿到锁了，你可以开始了
                writerAcquiredLock.countDown();

                log("持有锁并休眠 300 毫秒，以模拟业务处理...");
                Thread.sleep(300);

                log("休眠结束，准备提交事务...");
                qp.executeAndGetResult("COMMIT;", writerSession);
                log("事务已提交，锁已释放。");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                testFinished.countDown();
            }
        }, "写入者线程-A");

        // --- 线程 B: 读取者 ---
        Thread readerThread = new Thread(() -> {
            Session readerSession = Session.createAuthenticatedSession(2, "reader");
            try {
                // 等待，直到写入者确实拿到了锁
                writerAcquiredLock.await();

                log("检测到写入者已持有锁，我将发起 SELECT 请求...");
                log("如果锁机制生效，本线程将在此处【阻塞】...");
                long startTime = System.currentTimeMillis();
                String result = qp.executeAndGetResult("SELECT balance FROM accounts WHERE id = 1;", readerSession);
                long duration = System.currentTimeMillis() - startTime;
                log("【阻塞结束】！读取操作完成，耗时: " + duration + " ms。");

                if (result.startsWith("ERROR:")) {
                    fail("读取者收到了一个错误: " + result);
                }

                log("读取到的数据: " + result.split("\n")[3].trim());

                if (duration > 200) { // 阻塞时间应约等于写入者的休眠时间
                    readerWasBlocked.set(true);
                    log("验证：耗时 > 200ms，可以判定读取者确实被阻塞了。");
                }

                assertTrue(result.contains("500"), "读取者应该读到事务提交后的新值(500)！");

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                testFinished.countDown();
            }
        }, "读取者线程-B");

        // 启动线程
        writerThread.start();
        readerThread.start();

        // 等待两个线程都执行完毕，最多等待5秒
        boolean finishedInTime = testFinished.await(5, TimeUnit.SECONDS);

        // 最终验证
        assertTrue(finishedInTime, "测试超时！可能发生了死锁或线程未正常结束。");
        assertTrue(readerWasBlocked.get(), "读取者没有被写入者有效阻塞，锁机制或测试逻辑存在问题！");
        log("--- [测试通过] 成功验证了并发场景下的写-读阻塞！ ---");
    }

    /**
     * 测试核心场景：写-写冲突。
     * 两个事务同时尝试更新同一行数据，后来的事务应被阻塞，直到第一个事务结束。
     */
    @Test
    void testWriterBlocksWriter() throws InterruptedException {
        log("--- [测试开始] 写-写冲突：验证写入者会阻塞另一个写入者 ---");

        // 步骤 1: 环境准备
        log("环境准备：创建表、用户并授权...");
        qp.execute("CREATE TABLE products (id INT, stock INT);");
        qp.execute("INSERT INTO products (id, stock) VALUES (101, 10);");
        qp.execute("CREATE USER 'updater_A' IDENTIFIED BY 'pass_a';");
        qp.execute("CREATE USER 'updater_B' IDENTIFIED BY 'pass_b';");
        qp.execute("GRANT UPDATE ON products TO 'updater_A';");
        qp.execute("GRANT UPDATE ON products TO 'updater_B';");
        log("环境准备完毕。");

        // 用于线程同步
        CountDownLatch writerAAcquiredLock = new CountDownLatch(1);
        CountDownLatch testFinished = new CountDownLatch(2);
        AtomicBoolean writerBWasBlocked = new AtomicBoolean(false);

        // --- 线程 A: 第一个写入者 ---
        Thread writerAThread = new Thread(() -> {
            Session sessionA = Session.createAuthenticatedSession(1, "updater_A");
            try {
                log("开启事务，准备将库存从 10 减到 9...");
                qp.executeAndGetResult("BEGIN;", sessionA);
                qp.executeAndGetResult("UPDATE products SET stock = 9 WHERE id = 101;", sessionA);
                log("...成功获取排他锁！");

                writerAAcquiredLock.countDown(); // 通知线程B可以开始了

                log("持有锁并休眠 300 毫秒...");
                Thread.sleep(300);

                log("准备提交事务...");
                qp.executeAndGetResult("COMMIT;", sessionA);
                log("事务已提交，锁已释放。最终库存应为 9。");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                testFinished.countDown();
            }
        }, "写入者线程-A");

        // --- 线程 B: 第二个写入者 ---
        Thread writerBThread = new Thread(() -> {
            Session sessionB = Session.createAuthenticatedSession(2, "updater_B");
            try {
                writerAAcquiredLock.await(); // 等待线程A拿到锁

                log("检测到线程A已持有锁，我将发起另一个 UPDATE 请求...");
                log("如果锁机制生效，本线程将在此处【阻塞】...");
                long startTime = System.currentTimeMillis();
                qp.executeAndGetResult("UPDATE products SET stock = 8 WHERE id = 101;", sessionB);
                long duration = System.currentTimeMillis() - startTime;
                log("【阻塞结束】！我的 UPDATE 操作完成，耗时: " + duration + " ms。");

                if (duration > 200) {
                    writerBWasBlocked.set(true);
                    log("验证：耗时 > 200ms，可以判定本线程确实被阻塞了。");
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                testFinished.countDown();
            }
        }, "写入者线程-B");

        // 启动并等待线程
        writerAThread.start();
        writerBThread.start();
        boolean finishedInTime = testFinished.await(5, TimeUnit.SECONDS);

        // 最终验证
        assertTrue(finishedInTime, "测试超时！可能发生了死锁。");
        assertTrue(writerBWasBlocked.get(), "写入者 B 没有被写入者 A 阻塞，锁机制可能未生效！");

        log("最终一致性验证：检查数据库中的最终库存值...");
        String finalResult = qp.executeAndGetResult("SELECT stock FROM products WHERE id = 101;");
        assertTrue(finalResult.contains("8"), "最终库存值应该是 8，但查询结果并非如此！这可能意味着发生了'丢失更新'。");
        log("验证通过：最终库存为 8，符合事务的串行化执行结果。");

        log("--- [测试通过] 成功验证了并发场景下的写-写阻塞和数据一致性！ ---");
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