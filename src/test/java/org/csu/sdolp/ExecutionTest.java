package org.csu.sdolp;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.executor.SeqScanExecutor;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ExecutionTest {
    private final String TEST_DB_FILE = "test_execution.db";
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
    void testInsertAndSequentialScan() throws IOException {
        // 1. 手动定义并创建一张表
        String tableName = "student";
        Schema schema = new Schema(Arrays.asList(
                new Column("id", DataType.INT),
                new Column("name", DataType.VARCHAR)
        ));
        catalog.createTable(tableName, schema);
        System.out.println("表 " + tableName + " 已创建。");

        // 2. 获取表信息，并构造 TableHeap 来执行插入
        TableInfo tableInfo = catalog.getTable(tableName);
        TableHeap tableHeap = new TableHeap(bufferPoolManager, tableInfo);

        // 3. 插入两条记录
        Tuple tuple1 = new Tuple(Arrays.asList(new Value(1), new Value("Alice")));
        Tuple tuple2 = new Tuple(Arrays.asList(new Value(2), new Value("Bob")));
        assertTrue(tableHeap.insertTuple(tuple1), "插入 Alice 应该成功");
        assertTrue(tableHeap.insertTuple(tuple2), "插入 Bob 应该成功");
        System.out.println("两条记录已插入。");

        // 4. 创建顺序扫描执行器来读取数据
        SeqScanExecutor seqScan = new SeqScanExecutor(bufferPoolManager, tableInfo);
        
        // 5. 验证读取的数据是否正确
        assertTrue(seqScan.hasNext());
        Tuple retrievedTuple1 = seqScan.next();
        assertNotNull(retrievedTuple1);
        assertEquals(1, retrievedTuple1.getValues().get(0).getValue());
        assertEquals("Alice", retrievedTuple1.getValues().get(1).getValue());
        System.out.println("读取到元组: " + retrievedTuple1);

        assertTrue(seqScan.hasNext());
        Tuple retrievedTuple2 = seqScan.next();
        assertNotNull(retrievedTuple2);
        assertEquals(2, retrievedTuple2.getValues().get(0).getValue());
        assertEquals("Bob", retrievedTuple2.getValues().get(1).getValue());
        System.out.println("读取到元组: " + retrievedTuple2);
        
        assertFalse(seqScan.hasNext(), "不应该再有更多元组了");
        System.out.println("顺序扫描测试成功！");
    }
}