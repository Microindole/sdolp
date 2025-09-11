package org.csu.sdolp.compiler.parser.ast;

import java.util.Objects;

/**
 * AST 节点: 表示一个标识符，如表名或列名。
 * 新版本支持可选的表限定符，如 "users.id"。
 */
public final class IdentifierNode implements ExpressionNode {
    private final String tableQualifier; // 表限定符, e.g., "users" in "users.id". Can be null.
    private final String name;           // 标识符名称, e.g., "id" or "users"

    // 构造函数 for simple identifier (e.g., "id", "users")
    public IdentifierNode(String name) {
        this(null, name);
    }

    // 构造函数 for qualified identifier (e.g., "users.id")
    public IdentifierNode(String tableQualifier, String name) {
        this.tableQualifier = tableQualifier;
        this.name = name;
    }

    public String getTableQualifier() {
        return tableQualifier;
    }

    public String getName() {
        return name;
    }

    public String getFullName() {
        return (tableQualifier != null ? tableQualifier + "." : "") + name;
    }

    @Override
    public String toString() {
        return "IdentifierNode[" + getFullName() + "]";
    }

    // equals and hashCode are important for records and classes used in collections
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdentifierNode that = (IdentifierNode) o;
        return Objects.equals(tableQualifier, that.tableQualifier) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableQualifier, name);
    }
}