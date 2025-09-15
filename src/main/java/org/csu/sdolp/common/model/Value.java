package org.csu.sdolp.common.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
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
    public Value(BigDecimal value) {
        this.type = DataType.DECIMAL;
        this.value = value;
    }

    public Value(LocalDate value) {
        this.type = DataType.DATE;
        this.value = value;
    }

    public Value(Boolean value) {
        this.type = DataType.BOOLEAN;
        this.value = value;
    }
    public Value(Float value) {
        this.type = DataType.FLOAT;
        this.value = value;
    }

    public Value(Double value) {
        this.type = DataType.DOUBLE;
        this.value = value;
    }

    public Value(DataType type, Object value) {
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
            case CHAR://CHAR和VARCHAR使用相同的序列化方式
                byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                out.writeInt(bytes.length); // 先写入字符串长度
                out.write(bytes);          // 再写入字符串内容
                break;
            case DECIMAL:
                // 将 BigDecimal 转换为字符串进行存储
                String decimalStr = ((BigDecimal) value).toPlainString();
                byte[] decimalBytes = decimalStr.getBytes(StandardCharsets.UTF_8);
                out.writeInt(decimalBytes.length);
                out.write(decimalBytes);
                break;
            case DATE:
                // 将 LocalDate 转换为 ISO 格式的字符串 (e.g., "2025-09-13")
                String dateStr = ((LocalDate) value).toString();
                byte[] dateBytes = dateStr.getBytes(StandardCharsets.UTF_8);
                out.writeInt(dateBytes.length);
                out.write(dateBytes);
                break;
            case BOOLEAN:
                out.writeBoolean((Boolean) value);
                break;
            case FLOAT: //
                out.writeFloat((Float) value);
                break;
            case DOUBLE: //
                out.writeDouble((Double) value);
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
            case CHAR:
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                return new Value(new String(bytes, StandardCharsets.UTF_8));
            case DECIMAL:
                int decimalLen = buffer.getInt();
                byte[] decimalBytes = new byte[decimalLen];
                buffer.get(decimalBytes);
                return new Value(new BigDecimal(new String(decimalBytes, StandardCharsets.UTF_8)));
            case DATE:
                int dateLen = buffer.getInt();
                byte[] dateBytes = new byte[dateLen];
                buffer.get(dateBytes);
                return new Value(LocalDate.parse(new String(dateBytes, StandardCharsets.UTF_8)));
            case BOOLEAN:
                return new Value(buffer.get() == 1);
            case FLOAT: //
                return new Value(buffer.getFloat());
            case DOUBLE: //
                return new Value(buffer.getDouble());
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