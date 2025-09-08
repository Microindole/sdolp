package org.csu.sdolp.storage.page;

import org.yaml.snakeyaml.util.Tuple;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Page类，封装了4KB的二进制数据，并提供了读写记录的方法。
 * 一页包含一个页头和一个数据区域。
 */
public class Page {
    public static final int PAGE_SIZE = 4096; // 4KB [cite: 78, 124]
    private final PageId pageId;
    private final ByteBuffer data;

    public Page(PageId pageId) {
        this.pageId = pageId;
        this.data = ByteBuffer.allocate(PAGE_SIZE);
    }

    public Page(PageId pageId, byte[] rawData) {
        this.pageId = pageId;
        this.data = ByteBuffer.wrap(rawData);
    }
    
    public PageId getPageId() {
        return pageId;
    }
    
    public ByteBuffer getData() {
        return data;
    }
    
    // TODO: 实现读写记录的方法，将Tuple对象转换为字节流，并写入页中。
    // 这部分需要与record/Tuple.java和record/Schema.java结合。
    public void writeTuple(Tuple tuple, int offset) {
        // 示例：将字符串写入页中
        byte[] tupleBytes = tuple.toString().getBytes(StandardCharsets.UTF_8);
        data.position(offset);
        data.put(tupleBytes);
    }
    
    public Tuple readTuple(int offset) {
        // 示例：从页中读取字符串，并转换为Tuple
        // 这部分实现依赖于你如何定义 Tuple 的序列化格式。
        return null; 
    }
}