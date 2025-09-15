package org.csu.sdolp.acid;

import org.csu.sdolp.distributed.Coordinator;
import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证分布式事务（两阶段提交）的集成测试。
 */
public class DistributedTransactionTest {

    private QueryProcessor participant1;
    private QueryProcessor participant2;
    private Coordinator coordinator;

    private final String DB1_NAME = "dist_db1";
    private final String DB2_NAME = "dist_db2";

    @BeforeEach
    void setUp() {
        // 清理环境
        deleteDirectory(new File("data/" + DB1_NAME));
        deleteDirectory(new File("data/" + DB2_NAME));

        // 初始化两个数据库实例（参与者）
        participant1 = new QueryProcessor(DB1_NAME);
        participant2 = new QueryProcessor(DB2_NAME);

        // 初始化协调者
        coordinator = new Coordinator(List.of(participant1, participant2));

        // 在每个数据库中创建初始数据
        participant1.execute("CREATE TABLE accounts (user_id INT PRIMARY KEY, balance INT);");
        participant1.execute("INSERT INTO accounts (user_id, balance) VALUES (1, 1000);");

        participant2.execute("CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount INT);");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (participant1 != null) participant1.close();
        if (participant2 != null) participant2.close();
        deleteDirectory(new File("data/" + DB1_NAME));
        deleteDirectory(new File("data/" + DB2_NAME));
    }

    /**
     * 测试一个成功的分布式事务：用户A扣款，并成功创建订单。
     */
    @Test
    void testSuccessfulDistributedTransaction() {
        System.out.println("\n--- [测试场景] 成功的分布式事务 ---");
        System.out.println("目标: 参与者A扣款100，参与者B创建对应订单。预期两个操作都成功。");

        Map<QueryProcessor, String> operations = new HashMap<>();
        operations.put(participant1, "UPDATE accounts SET balance = balance - 100 WHERE user_id = 1;");
        operations.put(participant2, "INSERT INTO orders (order_id, user_id, amount) VALUES (101, 1, 100);");

        boolean success = coordinator.executeDistributedTransaction(operations);

        System.out.println("\n--- [最终验证] ---");
        assertTrue(success, "分布式事务应该成功提交！");

        String balanceResult = participant1.executeAndGetResult("SELECT balance FROM accounts WHERE user_id = 1;");
        assertTrue(balanceResult.contains("900"), "验证失败: 账户余额应为 900。");
        System.out.println("  - 参与者 A (accounts) 状态正确: balance = 900");

        String orderResult = participant2.executeAndGetResult("SELECT COUNT(*) FROM orders WHERE order_id = 101;");
        assertTrue(orderResult.contains("1"), "验证失败: 新订单未成功插入。");
        System.out.println("  - 参与者 B (orders) 状态正确: 新订单已创建");

        System.out.println("\n[测试通过] 数据在所有参与者之间保持了一致性。");
    }

    /**
     * 测试一个失败的分布式事务：用户A扣款，但订单创建失败（模拟主键冲突）。
     * 最终两个操作都应该被回滚。
     */
    @Test
    void testFailedDistributedTransaction() {
        System.out.println("\n--- [测试场景] 失败的分布式事务 (一方准备失败) ---");
        System.out.println("目标: 参与者A扣款，参与者B创建订单时因主键冲突失败。预期两个操作都被回滚。");

        participant2.execute("INSERT INTO orders (order_id, user_id, amount) VALUES (101, 1, 50);"); // 制造冲突
        System.out.println("[环境准备] 已在 orders 表中预先插入冲突数据 (order_id = 101)。");

        Map<QueryProcessor, String> operations = new HashMap<>();
        operations.put(participant1, "UPDATE accounts SET balance = balance - 100 WHERE user_id = 1;");
        operations.put(participant2, "INSERT INTO orders (order_id, user_id, amount) VALUES (101, 1, 100);");

        boolean success = coordinator.executeDistributedTransaction(operations);

        System.out.println("\n--- [最终验证] ---");
        assertFalse(success, "分布式事务应该失败并回滚！");

        String balanceResult = participant1.executeAndGetResult("SELECT balance FROM accounts WHERE user_id = 1;");
        assertTrue(balanceResult.contains("1000"), "验证失败: 账户余额应已回滚至 1000。");
        System.out.println("  - 参与者 A (accounts) 状态正确: balance 已回滚为 1000");

        String orderResult = participant2.executeAndGetResult("SELECT COUNT(*) FROM orders WHERE amount = 100;");
        assertTrue(orderResult.contains("0"), "验证失败: 冲突的订单不应被插入。");
        System.out.println("  - 参与者 B (orders) 状态正确: 冲突订单未插入");

        System.out.println("\n[测试通过] 事务回滚成功，数据在所有参与者之间保持了一致性。");
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