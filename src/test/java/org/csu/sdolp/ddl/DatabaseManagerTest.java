package org.csu.sdolp.ddl;

import org.csu.sdolp.DatabaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

/**
 * DatabaseManager 类的单元测试.
 */
public class DatabaseManagerTest {

    private DatabaseManager dbManager;
    private final String TEST_DB_NAME = "db_manager_test_db";
    private final String DB_ROOT_DIR = "data";

    @BeforeEach
    void setUp() {
        dbManager = new DatabaseManager();
        // 确保测试前环境是干净的
        File dbDir = new File(DB_ROOT_DIR, TEST_DB_NAME);
        if (dbDir.exists()) {
            deleteDirectory(dbDir);
        }
    }

    @AfterEach
    void tearDown() {
        // 清理测试创建的数据库
        File dbDir = new File(DB_ROOT_DIR, TEST_DB_NAME);
        if (dbDir.exists()) {
            deleteDirectory(dbDir);
        }
    }

    @Test
    void testCreateDatabase() {
        System.out.println("--- Test: Create Database ---");
        dbManager.createDatabase(TEST_DB_NAME);
        File dbDir = new File(DB_ROOT_DIR, TEST_DB_NAME);
        assertTrue(dbDir.exists(), "Database directory should be created.");
        assertTrue(dbDir.isDirectory(), "Created path should be a directory.");
    }

    @Test
    void testCreateExistingDatabaseThrowsException() {
        System.out.println("--- Test: Create Existing Database ---");
        dbManager.createDatabase(TEST_DB_NAME); // 第一次创建
        // 第二次创建同名数据库，应该抛出异常
        Exception exception = assertThrows(RuntimeException.class, () -> {
            dbManager.createDatabase(TEST_DB_NAME);
        });
        assertTrue(exception.getMessage().contains("already exists"), "Exception message should indicate that the database already exists.");
    }

    @Test
    void testListDatabases() {
        System.out.println("--- Test: List Databases ---");
        dbManager.createDatabase(TEST_DB_NAME);
        List<String> databases = dbManager.listDatabases();
        assertTrue(databases.contains(TEST_DB_NAME), "List of databases should include the newly created one.");
    }

    @Test
    void testDropDatabase() {
        System.out.println("--- Test: Drop Database ---");
        dbManager.createDatabase(TEST_DB_NAME);
        File dbDir = new File(DB_ROOT_DIR, TEST_DB_NAME);
        assertTrue(dbDir.exists(), "Database directory should exist before dropping.");

        dbManager.dropDatabase(TEST_DB_NAME);
        assertFalse(dbDir.exists(), "Database directory should be deleted after dropping.");
    }

    @Test
    void testDropNonExistentDatabaseThrowsException() {
        System.out.println("--- Test: Drop Non-Existent Database ---");
        Exception exception = assertThrows(RuntimeException.class, () -> {
            dbManager.dropDatabase("non_existent_db_12345");
        });
        assertTrue(exception.getMessage().contains("does not exist"), "Exception message should indicate that the database does not exist.");
    }

    @Test
    void testGetDbFilePath() {
        System.out.println("--- Test: Get DB File Path ---");
        String expectedPath = "data" + File.separator + TEST_DB_NAME + File.separator + "minidb.data";
        assertEquals(expectedPath, DatabaseManager.getDbFilePath(TEST_DB_NAME), "The generated DB file path is incorrect.");
    }

    // 辅助方法，用于递归删除目录
    private void deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}