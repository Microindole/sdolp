package org.csu.sdolp.common.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Column {
    private final String name;
    private final DataType type;

    public Column(String name, DataType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type.name());
    }

    public static Column read(DataInputStream in) throws IOException {
        String name = in.readUTF();
        DataType type = DataType.valueOf(in.readUTF());
        return new Column(name, type);
    }
}