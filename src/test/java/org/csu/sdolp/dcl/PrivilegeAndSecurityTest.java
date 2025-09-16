package org.csu.sdolp.dcl;

import org.csu.sdolp.cli.server.Session;
import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 专门用于测试用户权限管理和安全访问控制的集成测试.
 */
public class PrivilegeAndSecurityTest {

    private final String TEST_DB_NAME = "security_test_db";
    private QueryProcessor queryProcessor;

    // 为不同用户创建独立的 Session
    private final Session rootSession = Session.createAuthenticatedSession(1, "root");
    private Session testUserSession;

    @BeforeEach
    void setUp() {
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // 准备环境：root用户创建表和测试用户
        queryProcessor.executeAndGetResult("CREATE TABLE secret_data (id INT, data VARCHAR);", rootSession);
        queryProcessor.executeAndGetResult("INSERT INTO secret_data (id, data) VALUES (1, 'top secret');", rootSession);
        queryProcessor.executeAndGetResult("CREATE USER 'testuser' IDENTIFIED BY 'password';", rootSession);

        // 为 testuser 创建一个已认证的会话
        testUserSession = Session.createAuthenticatedSession(2, "testuser");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void testSelectPermissionDenied() {
        System.out.println("--- Test: SELECT Permission Denied ---");
        String sql = "SELECT * FROM secret_data;";
        // 以 testuser 的身份执行
        String result = queryProcessor.executeAndGetResult(sql, testUserSession);

        System.out.println("Result for testuser: " + result);
        assertTrue(result.contains("Access denied"), "testuser should be denied SELECT access initially.");
    }

    @Test
    void testGrantAndSelectPermission() {
        System.out.println("--- Test: Grant SELECT and Verify Access ---");
        // 1. Root 用户授予 SELECT 权限
        String grantSql = "GRANT SELECT ON secret_data TO 'testuser';";
        String grantResult = queryProcessor.executeAndGetResult(grantSql, rootSession);
        assertTrue(grantResult.contains("Grants successful"), "GRANT command should succeed.");

        // 2. testuser 再次尝试 SELECT
        String selectSql = "SELECT * FROM secret_data;";
        String selectResult = queryProcessor.executeAndGetResult(selectSql, testUserSession);

        System.out.println("Result for testuser after GRANT: " + selectResult);
        assertTrue(selectResult.contains("top secret"), "testuser should be able to SELECT data after being granted permission.");
        assertFalse(selectResult.contains("Access denied"), "Access should not be denied after GRANT.");
    }

    @Test
    void testInsertAndUpdatePermissionFlow() {
        System.out.println("--- Test: DML Permission Flow (INSERT/UPDATE) ---");
        String insertSql = "INSERT INTO secret_data (id, data) VALUES (2, 'new data');";
        String updateSql = "UPDATE secret_data SET data = 'updated' WHERE id = 1;";

        // 1. 验证初始状态下 INSERT 和 UPDATE 都被拒绝
        String insertResult1 = queryProcessor.executeAndGetResult(insertSql, testUserSession);
        assertTrue(insertResult1.contains("Access denied"), "testuser should be denied INSERT access initially.");
        String updateResult1 = queryProcessor.executeAndGetResult(updateSql, testUserSession);
        assertTrue(updateResult1.contains("Access denied"), "testuser should be denied UPDATE access initially.");

        // 2. Root 授予 INSERT 权限
        queryProcessor.executeAndGetResult("GRANT INSERT ON secret_data TO 'testuser';", rootSession);

        // 3. 验证 INSERT 成功，但 UPDATE 仍然失败
        String insertResult2 = queryProcessor.executeAndGetResult(insertSql, testUserSession);
        assertFalse(insertResult2.contains("Access denied"), "INSERT should be allowed after GRANT.");
        String updateResult2 = queryProcessor.executeAndGetResult(updateSql, testUserSession);
        assertTrue(updateResult2.contains("Access denied"), "UPDATE should still be denied.");
    }

    @Test
    void testRootUserPrivileges() {
        System.out.println("--- Test: Root User Privileges ---");
        // Root 用户应该可以执行高权限操作
        String createUserSql = "CREATE USER 'anotheruser' IDENTIFIED BY 'pass';";
        String dropTableSql = "DROP TABLE secret_data;";

        String createUserResult = queryProcessor.executeAndGetResult(createUserSql, rootSession);
        assertFalse(createUserResult.contains("Access denied"), "Root user should be able to create users.");

        String dropTableResult = queryProcessor.executeAndGetResult(dropTableSql, rootSession);
        assertFalse(dropTableResult.contains("Access denied"), "Root user should be able to drop tables.");
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