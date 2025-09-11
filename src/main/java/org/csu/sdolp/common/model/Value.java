package org.csu.sdolp.common.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 表示一个具体的值，可以是不同数据类型。
 */
public class Value {
    private final DataType type;
    private final Object value;

    public Value(Integer value) {
        this.type = DataType.INT;
        this.value = value;
    }

    public Value(String value) {
        this.type = DataType.VARCHAR;
        this.value = value;
    }
    // ======(Phase 4 Bug Fix) ======
    // 私有构造函数，用于特殊情况，如 AVG 的中间值
    private Value(DataType type, Object value) {
        this.type = type;
        this.value = value;
    }
    public DataType getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

    /**
     * 将值序列化为字节数组。
     * @param out 输出流
     * @throws IOException
     */
    public void serialize(DataOutputStream out) throws IOException {
        switch (type) {
            case INT:
                out.writeInt((Integer) value);
                break;
            case VARCHAR:
                byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                out.writeInt(bytes.length); // 先写入字符串长度
                out.write(bytes);          // 再写入字符串内容
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data type for serialization: " + type);
        }
    }

    /**
     * 从字节缓冲中反序列化一个值。
     * @param buffer 字节缓冲
     * @param type   要读取的值的类型
     * @return 读取到的 Value 对象
     */
    public static Value deserialize(ByteBuffer buffer, DataType type) {
        switch (type) {
            case INT:
                return new Value(buffer.getInt());
            case VARCHAR:
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                return new Value(new String(bytes, StandardCharsets.UTF_8));
            default:
                throw new UnsupportedOperationException("Unsupported data type for deserialization: " + type);
        }
    }

    @Override
    public String toString() {
        return value.toString();
    }

    // ====== 核心修复点 (Phase 4 Bug Fix) ======
    // 重写 equals 和 hashCode 是让其在 HashMap 中作为 Key 的关键
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Value value1 = (Value) o;
        return type == value1.type && Objects.equals(value, value1.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }
}