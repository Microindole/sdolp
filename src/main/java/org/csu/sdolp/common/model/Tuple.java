package org.csu.sdolp.common.model;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 表示一条记录（行），由多个 Value 组成。
 */
public class Tuple {
    private final List<Value> values;

    public Tuple(List<Value> values) {
        this.values = values;
    }

    public List<Value> getValues() {
        return values;
    }

    /**
     * 将整条记录序列化为字节数组。
     * @return 序列化后的字节数组
     */
    public byte[] toBytes() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            for (Value value : values) {
                value.serialize(dos);
            }
            dos.close();
        } catch (IOException e) {
            // 这在内存操作中基本不会发生
            e.printStackTrace();
        }
        return baos.toByteArray();
    }

    /**
     * 从字节数组反序列化为一条记录。
     * @param data   字节数组
     * @param schema 表的模式
     * @return 反序列化后的 Tuple 对象
     */
    public static Tuple fromBytes(byte[] data, Schema schema) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        List<Value> values = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            values.add(Value.deserialize(buffer, column.getType()));
        }
        return new Tuple(values);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Tuple{");
        for (int i = 0; i < values.size(); i++) {
            sb.append(values.get(i).toString());
            if (i < values.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}