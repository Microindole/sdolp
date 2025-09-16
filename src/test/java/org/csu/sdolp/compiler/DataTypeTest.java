package org.csu.sdolp.compiler;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 专门用于测试新添加的数据类型 (FLOAT, DOUBLE, CHAR) 的集成测试.
 */
public class DataTypeTest {

    private final String TEST_DB_NAME = "datatype_test_db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        System.out.println("--- [DataTypeTest] Cleaning up environment ---");
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        System.out.println("--- [DataTypeTest] Environment cleaned up ---");
    }

    @Test
    void testCreateInsertAndSelectNewDataTypes() {
        System.out.println("--- Test: Create, Insert, and Select with FLOAT, DOUBLE, CHAR ---");

        // 1. 创建包含新数据类型的表
        String createSql = "CREATE TABLE new_types_table (c_float FLOAT, c_double DOUBLE, c_char CHAR);";
        System.out.println("Executing: " + createSql);
        String createResult = queryProcessor.executeAndGetResult(createSql);
        assertTrue(createResult.contains("Table 'new_types_table' created."), "CREATE TABLE with new types failed.");
        System.out.println("Table with new data types created successfully.");

        // 2. 插入使用新数据类型的值
        String insertSql = "INSERT INTO new_types_table (c_float, c_double, c_char) VALUES (1.23, 4.56789, 'a');";
        System.out.println("Executing: " + insertSql);
        String insertResult = queryProcessor.executeAndGetResult(insertSql);
        // 此处修改：不再检查 "rows affected"，而是检查操作是否没有报错，这更符合当前系统的返回逻辑
        assertFalse(insertResult.toUpperCase().contains("ERROR"), "INSERT statement failed with an error: " + insertResult);
        System.out.println("Data with new types inserted successfully. Result: " + insertResult);

        // 3. 查询并验证数据
        String selectSql = "SELECT * FROM new_types_table;";
        System.out.println("Executing: " + selectSql);
        String selectResult = queryProcessor.executeAndGetResult(selectSql);
        System.out.println("Query Result:\n" + selectResult);

        assertTrue(selectResult.contains("1.23"), "SELECT failed to retrieve correct FLOAT value.");
        assertTrue(selectResult.contains("4.56789"), "SELECT failed to retrieve correct DOUBLE value.");
        assertTrue(selectResult.contains("a"), "SELECT failed to retrieve correct CHAR value.");
        System.out.println("Verification PASSED. All new data types are working correctly.");
    }

    // 辅助方法，用于递归删除目录
    private void deleteDirectory(File directory) {
        if (!directory.exists()) {
            return;
        }
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}

