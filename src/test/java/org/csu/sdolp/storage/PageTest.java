package org.csu.sdolp.storage;

import org.csu.sdolp.common.model.*;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PageTest {

    @Test
    void testInsertAndGetTuple() {
        // 1. 定义 Schema
        Schema schema = new Schema(Arrays.asList(
                new Column("id", DataType.INT),
                new Column("name", DataType.VARCHAR),
                new Column("age", DataType.INT)
        ));

        // 2. 创建一个新 Page
        PageId pageId = new PageId(0);
        Page page = new Page(pageId);

        // 3. 创建两条 Tuple
        Tuple tuple1 = new Tuple(Arrays.asList(
                new Value(1),
                new Value("Alice"),
                new Value(20)
        ));
        Tuple tuple2 = new Tuple(Arrays.asList(
                new Value(2),
                new Value("Bob"),
                new Value(25)
        ));

        // 4. 插入 Tuple
        assertTrue(page.insertTuple(tuple1), "第一次插入应该成功");
        assertTrue(page.insertTuple(tuple2), "第二次插入应该成功");

        // 5. 读取并验证 Tuple
        Tuple retrievedTuple1 = page.getTuple(0, schema);
        assertNotNull(retrievedTuple1);
        assertEquals(1, retrievedTuple1.getValues().get(0).getValue());
        assertEquals("Alice", retrievedTuple1.getValues().get(1).getValue());
        assertEquals(20, retrievedTuple1.getValues().get(2).getValue());
        System.out.println("Retrieved Tuple 1: " + retrievedTuple1.toString());

        Tuple retrievedTuple2 = page.getTuple(1, schema);
        assertNotNull(retrievedTuple2);
        assertEquals(2, retrievedTuple2.getValues().get(0).getValue());
        assertEquals("Bob", retrievedTuple2.getValues().get(1).getValue());
        assertEquals(25, retrievedTuple2.getValues().get(2).getValue());
        System.out.println("Retrieved Tuple 2: " + retrievedTuple2.toString());

        // 6. 验证 getAllTuples
        List<Tuple> allTuples = page.getAllTuples(schema);
        assertEquals(2, allTuples.size());
        System.out.println("All tuples retrieved successfully.");
    }

    @Test
    void testPageFull() {
        // 定义一个只包含一个长字符串的 Schema
        Schema schema = new Schema(Arrays.asList(
                new Column("data", DataType.VARCHAR)
        ));

        Page page = new Page(new PageId(1));

        // 创建一个很大的 Tuple，精确计算使其几乎占满整个页面
        // 页面可用空间: 4096 - 8(header) = 4088
        // smallTuple需要空间: (4+5)(data) + 8(slot) = 17 bytes
        // 因此 largeTuple 插入后，剩余空间必须 < 17 bytes
        // 剩余空间 = 4088 - largeTupleDataSize - 8(slot) < 17
        // 4063 < largeTupleDataSize
        // largeTupleDataSize = 4 + string.length() => 4059 < string.length()
        // 我们选择 4060
        String largeString = "a".repeat(4060);
        Tuple largeTuple = new Tuple(List.of(new Value(largeString)));

        assertTrue(page.insertTuple(largeTuple), "插入大 Tuple 应该成功");

        // 创建另一个小 Tuple
        Tuple smallTuple = new Tuple(List.of(new Value("small")));

        // 此时页面应该满了，插入失败
        assertFalse(page.insertTuple(smallTuple), "页面已满，插入应该失败");
        System.out.println("Page full test passed.");
    }
}