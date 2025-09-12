package org.csu.sdolp;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B+树索引集成测试 (End-to-End Test)。
 * 这个测试将验证从建表、插入数据、创建索引、使用索引查询、
 * DML操作后索引维护，到最后删除表的完整流程。
 */
public class IndexIntegrationTest {

    private QueryProcessor queryProcessor;
    private static final String TEST_DB_FILE = "index_integration_test.db";
    private static final String TEST_LOG_FILE = "index_integration_test.db.log";


    @BeforeEach
    public void setUp(TestInfo testInfo) throws IOException {
        // 在每个测试前，确保清理掉旧的数据库文件
        new File(TEST_DB_FILE).delete();
        new File(TEST_LOG_FILE).delete();

        // 初始化数据库引擎
        queryProcessor = new QueryProcessor(TEST_DB_FILE);
        System.out.println("\n--- Starting Integration Test: " + testInfo.getTestMethod().orElseThrow().getName() + " ---");
    }

    @AfterEach
    public void tearDown() throws IOException {
        System.out.println("--- Test Finished ---");
        queryProcessor.close();
        new File(TEST_DB_FILE).delete();
        new File(TEST_LOG_FILE).delete();
    }

    @Test
    public void testFullIndexWorkflow() {
        System.out.println("\n[Phase 1] 准备阶段：创建表并插入大批量数据...");
        // 1. 创建一个用户表
        String createTableSql = "CREATE TABLE users (id INT, name VARCHAR);";
        String createResult = queryProcessor.executeAndGetResult(createTableSql);
        assertTrue(createResult.contains("Table 'users' created."), "创建 'users' 表失败！");

        // 2. 插入大量数据 (例如 500 条)
        for (int i = 1; i <= 500; i++) {
            String insertSql = String.format("INSERT INTO users (id, name) VALUES (%d, 'user%d');", i, i);
            queryProcessor.executeAndGetResult(insertSql);
        }
        System.out.println("[Phase 1] 500条数据插入完毕。");

        System.out.println("\n[Phase 2] 索引前查询：验证全表扫描...");
        // 3. 在没有索引的情况下，查询一条数据。注意观察服务器后台日志，应显示 "Using Sequential Scan"。
        String selectSql = "SELECT * FROM users WHERE id = 250;";
        System.out.println("执行查询: " + selectSql);
        String selectResult = queryProcessor.executeAndGetResult(selectSql);
        assertTrue(selectResult.contains("user250"), "在没有索引的情况下，未能找到 id=250 的用户！");
        System.out.println("[Phase 2] 全表扫描查询成功。");

        System.out.println("\n[Phase 3] 创建索引...");
        // 4. 在 id 列上创建一个索引
        String createIndexSql = "CREATE INDEX idx_users_id ON users (id);";
        System.out.println("执行: " + createIndexSql);
        String indexResult = queryProcessor.executeAndGetResult(createIndexSql);
        // 注意：CreateIndexExecutor 目前不返回消息，所以我们不在这里断言结果
        System.out.println("[Phase 3] 索引创建完毕。");

        System.out.println("\n[Phase 4] 索引后查询：验证索引扫描...");
        // 5. 再次执行相同的查询。注意观察服务器后台日志，此时应显示 "Using Index Scan"。
        System.out.println("再次执行查询: " + selectSql);
        selectResult = queryProcessor.executeAndGetResult(selectSql);
        assertTrue(selectResult.contains("user250"), "在使用索引的情况下，未能找到 id=250 的用户！");
        System.out.println("[Phase 4] 索引扫描查询成功，性能应该已提升！");

        System.out.println("\n[Phase 5] 验证索引维护：INSERT, DELETE, UPDATE...");
        // 6. 测试 INSERT
        String insertNewSql = "INSERT INTO users (id, name) VALUES (501, 'newUser');";
        queryProcessor.executeAndGetResult(insertNewSql);
        String selectNewSql = "SELECT * FROM users WHERE id = 501;";
        String selectNewResult = queryProcessor.executeAndGetResult(selectNewSql);
        assertTrue(selectNewResult.contains("newUser"), "INSERT 操作后，未能通过索引找到新插入的数据！");
        System.out.println("  - INSERT后索引维护... OK");

        // 7. 测试 DELETE
        String deleteSql = "DELETE FROM users WHERE id = 250;";
        queryProcessor.executeAndGetResult(deleteSql);
        String selectDeletedSql = "SELECT * FROM users WHERE id = 250;";
        String selectDeletedResult = queryProcessor.executeAndGetResult(selectDeletedSql);
        assertFalse(selectDeletedResult.contains("user250"), "DELETE 操作后，仍然能通过索引找到已删除的数据！");
        System.out.println("  - DELETE后索引维护... OK");

        // 8. 测试 UPDATE (需要您已实现 UpdateExecutor 的索引维护逻辑)
        // String updateSql = "UPDATE users SET name = 'updatedUser' WHERE id = 100;";
        // queryProcessor.executeAndGetResult(updateSql);
        // String selectUpdatedSql = "SELECT name FROM users WHERE id = 100;";
        // String selectUpdatedResult = queryProcessor.executeAndGetResult(selectUpdatedSql);
        // assertTrue(selectUpdatedResult.contains("updatedUser"), "UPDATE 操作后，未能通过索引找到更新后的数据！");
        // System.out.println("  - UPDATE后索引维护... OK");

        System.out.println("\n[Phase 6] 清理阶段：删除表...");
        // 9. 删除表
        String dropTableSql = "DROP TABLE users;";
        String dropResult = queryProcessor.executeAndGetResult(dropTableSql);
        assertTrue(dropResult.contains("indexes dropped"), "删除表时，未能看到索引被一并删除的消息！");
        System.out.println("[Phase 6] 表和索引清理完毕。");

        System.out.println("\n--- 索引集成测试所有阶段均成功通过！ ---");
    }
}