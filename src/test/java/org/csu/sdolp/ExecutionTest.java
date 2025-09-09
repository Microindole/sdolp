package org.csu.sdolp;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.executor.FilterExecutor;
import org.csu.sdolp.executor.ProjectExecutor;
import org.csu.sdolp.executor.SeqScanExecutor;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.expressions.ComparisonPredicate;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

    @Test
    void testFilterAndMultiPageScan() throws IOException {
        // 1. 创建表
        String tableName = "users";
        Schema schema = new Schema(Arrays.asList(
                new Column("id", DataType.INT),
                new Column("name", DataType.VARCHAR)
        ));
        catalog.createTable(tableName, schema);
        TableInfo tableInfo = catalog.getTable(tableName);
        TableHeap tableHeap = new TableHeap(bufferPoolManager, tableInfo);

        // 2. 插入大量数据，确保会跨页
        // 一个简单的元组 (int, 10字节varchar) 大约需要 4 + (4+10) = 18字节
        // 一页大约能放 4000 / (18+8) ~= 150 个元组。我们插入200个。
        Tuple targetTuple = null;
        for (int i = 0; i < 200; i++) {
            Tuple tuple = new Tuple(Arrays.asList(new Value(i), new Value("user" + i)));
            tableHeap.insertTuple(tuple);
            if (i == 170) {
                targetTuple = tuple; // 我们要查找的目标元组，它很可能在第二页
            }
        }
        System.out.println("200条记录已插入，应该已跨页。");

        // 3. 测试全表扫描 (不带过滤)
        SeqScanExecutor seqScanAll = new SeqScanExecutor(bufferPoolManager, tableInfo);
        int count = 0;
        while(seqScanAll.hasNext()){
            seqScanAll.next();
            count++;
        }
        assertEquals(200, count, "全表扫描应该能读取所有200条记录");
        System.out.println("跨页全表扫描成功！");


        // 4. 使用 FilterExecutor 查找 id = 170 的记录
        SeqScanExecutor seqScanFilter = new SeqScanExecutor(bufferPoolManager, tableInfo);
        ComparisonPredicate predicate = new ComparisonPredicate(0, new Value(170),"EQUAL"); // id 在第0列
        FilterExecutor filterExecutor = new FilterExecutor(seqScanFilter, predicate);

        // 5. 验证过滤结果
        assertTrue(filterExecutor.hasNext(), "应该能找到 id=170 的记录");
        Tuple result = filterExecutor.next();
        assertNotNull(result);
        assertEquals(170, result.getValues().get(0).getValue());
        assertEquals("user170", result.getValues().get(1).getValue());
        System.out.println("通过FilterExecutor成功找到元组: " + result);

        assertFalse(filterExecutor.hasNext(), "只应该有一条 id=170 的记录");
        System.out.println("过滤查询测试成功！");
    }

    @Test
    void testProjection() throws IOException {
        // 1. 准备数据 (与之前的测试类似)
        String tableName = "employees";
        Schema schema = new Schema(Arrays.asList(
                new Column("id", DataType.INT),
                new Column("name", DataType.VARCHAR),
                new Column("salary", DataType.INT)
        ));
        catalog.createTable(tableName, schema);
        TableInfo tableInfo = catalog.getTable(tableName);
        TableHeap tableHeap = new TableHeap(bufferPoolManager, tableInfo);
        tableHeap.insertTuple(new Tuple(Arrays.asList(new Value(1), new Value("Alice"), new Value(8000))));
        tableHeap.insertTuple(new Tuple(Arrays.asList(new Value(2), new Value("Bob"), new Value(9000))));

        // 2. 构造执行器链: SeqScan -> Project
        // 相当于执行: SELECT name, salary FROM employees;
        SeqScanExecutor seqScan = new SeqScanExecutor(bufferPoolManager, tableInfo);
        // "name" 是第1列 (索引1), "salary" 是第2列 (索引2)
        ProjectExecutor projectExecutor = new ProjectExecutor(seqScan, List.of(1, 2));

        // 3. 验证投影结果
        assertTrue(projectExecutor.hasNext());
        Tuple result1 = projectExecutor.next();
        assertEquals(2, result1.getValues().size(), "投影后的元组应该只有2个值");
        assertEquals("Alice", result1.getValues().get(0).getValue());
        assertEquals(8000, result1.getValues().get(1).getValue());
        System.out.println("投影后的元组 1: " + result1);

        assertTrue(projectExecutor.hasNext());
        Tuple result2 = projectExecutor.next();
        assertEquals(2, result2.getValues().size());
        assertEquals("Bob", result2.getValues().get(0).getValue());
        assertEquals(9000, result2.getValues().get(1).getValue());
        System.out.println("投影后的元组 2: " + result2);

        assertFalse(projectExecutor.hasNext());
        System.out.println("投影测试成功！");
    }
}
