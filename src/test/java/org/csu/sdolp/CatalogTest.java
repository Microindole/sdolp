package org.csu.sdolp;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class CatalogTest {

    private final String TEST_DB_FILE = "test_catalog.db";
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private Catalog catalog;

    @BeforeEach
    void setUp() throws IOException {
        new File(TEST_DB_FILE).delete();
        diskManager = new DiskManager(TEST_DB_FILE);
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        catalog = new Catalog(bufferPoolManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        diskManager.close();
        new File(TEST_DB_FILE).delete();
    }

    @Test
    void testCreateTableAndGetTable() throws IOException {
        // 1. 定义一个新表的 Schema
        Schema studentSchema = new Schema(Arrays.asList(
                new Column("id", DataType.INT),
                new Column("name", DataType.VARCHAR)
        ));

        // 2. 创建表
        String tableName = "student";
        catalog.createTable(tableName, studentSchema);

        // 3. 从 Catalog 中获取表信息并验证
        TableInfo retrievedTable = catalog.getTable(tableName);
        assertNotNull(retrievedTable, "应该能获取到创建的表");
        assertEquals(tableName, retrievedTable.getTableName());
        assertEquals(2, retrievedTable.getSchema().getColumns().size());
        assertEquals("id", retrievedTable.getSchema().getColumns().get(0).getName());
        assertEquals(DataType.VARCHAR, retrievedTable.getSchema().getColumns().get(1).getType());

        System.out.println("表 " + tableName + " 创建并验证成功。");
    }
    
    @Test
    void testCatalogPersistence() throws IOException {
        // --- 第一次会话 ---
        // 1. 创建一个表
        Schema schema = new Schema(Arrays.asList(new Column("data", DataType.VARCHAR)));
        catalog.createTable("my_table", schema);
        
        // 2. 关闭并保存（通过 BufferPoolManager 的 flush 机制隐式完成）
        diskManager.close();
        System.out.println("数据库关闭，数据已持久化。");
        
        // --- 第二次会话 ---
        // 3. 重新打开数据库，创建新的管理器和 Catalog 实例
        System.out.println("重新打开数据库...");
        diskManager = new DiskManager(TEST_DB_FILE);
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        catalog = new Catalog(bufferPoolManager);
        
        // 4. 验证之前创建的表是否存在
        TableInfo tableInfo = catalog.getTable("my_table");
        assertNotNull(tableInfo, "重启后应该能加载到 my_table");
        assertEquals("my_table", tableInfo.getTableName());
        assertEquals(1, tableInfo.getSchema().getColumns().size());
        assertEquals("data", tableInfo.getSchema().getColumns().get(0).getName());
        
        System.out.println("目录持久化测试成功！");
    }
}