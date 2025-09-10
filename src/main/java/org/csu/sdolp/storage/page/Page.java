package org.csu.sdolp.storage.page;

import lombok.Getter;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Page类，采用 Slotted Page 布局来管理和存储 Tuple。
 */
@Getter
public class Page {
    public static final int PAGE_SIZE = 4096;//  [cite_start] // 4KB [cite: 78, 124]
    private static final int HEADER_NUM_TUPLES_OFFSET = 0;
    private static final int HEADER_FREE_SPACE_POINTER_OFFSET = 4;
    private static final int HEADER_NEXT_PAGE_ID_OFFSET = 8;
    private static final int HEADER_SIZE = 12;
    private static final int SLOT_SIZE = 8; // 4 bytes for offset, 4 bytes for length

    private final PageId pageId;
    private final ByteBuffer data;

    public Page(PageId pageId) {
        this.pageId = pageId;
        this.data = ByteBuffer.allocate(PAGE_SIZE);
        // 初始化页头
        setNumTuples(0);
        setFreeSpacePointer(PAGE_SIZE);
        setNextPageId(-1);
    }

    public Page(PageId pageId, byte[] rawData) {
        this.pageId = pageId;
        this.data = ByteBuffer.wrap(rawData);
    }

    public void init() {
        setNumTuples(0);
        setFreeSpacePointer(PAGE_SIZE);
        setNextPageId(-1);
    }


    // --- 页头操作 ---
    public int getNumTuples() {
        return data.getInt(HEADER_NUM_TUPLES_OFFSET);
    }

    private void setNumTuples(int numTuples) {
        data.putInt(HEADER_NUM_TUPLES_OFFSET, numTuples);
    }

    private int getFreeSpacePointer() {
        return data.getInt(HEADER_FREE_SPACE_POINTER_OFFSET);
    }

    private void setFreeSpacePointer(int pointer) {
        data.putInt(HEADER_FREE_SPACE_POINTER_OFFSET, pointer);
    }

    // --- 槽操作 ---
    private int getTupleOffset(int slotIndex) {
        return data.getInt(HEADER_SIZE + slotIndex * SLOT_SIZE);
    }

    private void setTupleOffset(int slotIndex, int offset) {
        data.putInt(HEADER_SIZE + slotIndex * SLOT_SIZE, offset);
    }

    private int getTupleLength(int slotIndex) {
        return data.getInt(HEADER_SIZE + slotIndex * SLOT_SIZE + 4);
    }

    private void setTupleLength(int slotIndex, int length) {
        data.putInt(HEADER_SIZE + slotIndex * SLOT_SIZE + 4, length);
    }

    /**
     * 计算剩余可用空间。
     * 可用空间 = 空闲空间指针 - (页头大小 + 已有槽位大小)
     * @return 剩余字节数
     */
    public int getFreeSpace() {
        return getFreeSpacePointer() - (HEADER_SIZE + getNumTuples() * SLOT_SIZE);
    }

    /**
     * 向页面中插入一条记录。
     * @param tuple 要插入的记录
     * @return 如果成功返回 true，如果空间不足返回 false。
     */
    public boolean insertTuple(Tuple tuple) {
        byte[] tupleBytes = tuple.toBytes();
        int tupleLength = tupleBytes.length;

        // 检查是否有足够空间 (元组数据 + 1个新槽位)
        if (getFreeSpace() < tupleLength + SLOT_SIZE) {
            return false;
        }

        // 1. 计算新元组的存储位置，并写入数据
        int newFreeSpacePointer = getFreeSpacePointer() - tupleLength;
        setFreeSpacePointer(newFreeSpacePointer);
        data.position(newFreeSpacePointer);
        data.put(tupleBytes);

        // 2. 在槽数组中为新元组分配一个槽
        int numTuples = getNumTuples();
        setTupleOffset(numTuples, newFreeSpacePointer);
        setTupleLength(numTuples, tupleLength);

        // 3. 更新页头中的元组数量
        setNumTuples(numTuples + 1);

        return true;
    }

    /**
     * 根据槽位索引读取一条记录。
     * @param slotIndex 槽位号
     * @param schema    表的模式
     * @return 读取到的 Tuple 对象，如果槽位无效则返回 null。
     */
    public Tuple getTuple(int slotIndex, Schema schema) {
        if (slotIndex >= getNumTuples()) {
            return null;
        }
        int offset = getTupleOffset(slotIndex);
        int length = getTupleLength(slotIndex);

        if (length == 0) { // 空元组或无效长度
            return null;
        }
        if (length < 0) { // 已被标记为删除的元组
            return null;
        }
        // 检查offset和length是否会导致访问越界
        if (offset < HEADER_SIZE || offset + length > PAGE_SIZE) {
            System.err.println("WARNING: Corrupted tuple slot found in Page " + pageId.getPageNum() +
                    " at slot " + slotIndex + ". Invalid offset=" + offset + ", length=" + length + ". Skipping.");
            return null; // 将其视为无效元组，跳过
        }

        // 使用 System.arraycopy 直接从页面的底层数组复制字节。
        // 这种方法不依赖也不修改 ByteBuffer 的内部状态（如 position），
        // 因此更加健壮，能避免共享状态带来的潜在问题。
        byte[] tupleBytes = new byte[length];
        System.arraycopy(data.array(), offset, tupleBytes, 0, length);

        return Tuple.fromBytes(tupleBytes, schema);
    }

    /**
     * 获取页面中所有的元组。
     * @param schema 表的模式
     * @return 包含所有元组的列表
     */
    public List<Tuple> getAllTuples(Schema schema) {
        List<Tuple> tuples = new ArrayList<>();
        int numTuples = getNumTuples();
        for (int i = 0; i < numTuples; i++) {
            Tuple tuple = getTuple(i, schema);
            if (tuple != null) {
                tuples.add(tuple);
            }
        }
        return tuples;
    }

    //标记删除的方法
    public boolean markTupleAsDeleted(int slotIndex) {
        if (slotIndex >= getNumTuples()) {
            return false;
        }
        int length = getTupleLength(slotIndex);
        if (length < 0) {
            return false;
        }
        // 将长度标记为负数来表示删除
        setTupleLength(slotIndex, -length);
        return true;
    }




    public int getNextPageId() {
        return data.getInt(HEADER_NEXT_PAGE_ID_OFFSET);
    }

    public void setNextPageId(int nextPageId) {
        data.putInt(HEADER_NEXT_PAGE_ID_OFFSET, nextPageId);
    }
}