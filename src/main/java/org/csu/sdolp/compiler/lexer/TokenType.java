package org.csu.sdolp.compiler.lexer;

/**
 * @author hidyouth
 * @description: 定义词法单元（Token）的类型，即“种别码”
 *
 * 这是我们SQL语言中所有可能出现的“单词”的分类。
 */
public enum TokenType {
    // ---- 关键字 (Keywords) ----
    SELECT,     // "SELECT"
    FROM,       // "FROM"
    WHERE,      // "WHERE"
    CREATE,     // "CREATE"
    TABLE,      // "TABLE"
    DATABASE,   // "DATABASE"
    DATABASES,  // "DATABASES"
    INDEX,      // "INDEX"
    INSERT,     // "INSERT"
    INTO,       // "INTO"
    VALUES,     // "VALUES"
    USE,        // "USE"
    DELETE,     // "DELETE"
    UPDATE,     // "UPDATE"
    SET,        // "SET"
    INT,        // "INT" 数据类型
    VARCHAR,    // "VARCHAR" 数据类型
    DECIMAL,
    DATE,
    BOOLEAN,
    TRUE,
    FALSE,

    // ====== 权限管理关键字 ======
    USER,       // "USER"
    IDENTIFIED, // "IDENTIFIED"
    GRANT,      // "GRANT"
    TO,         // "TO"

    // ====== 查询子句关键字 ======
    ORDER,      // "ORDER"
    BY,         // "BY"
    ASC,        // "ASC"
    DESC,       // "DESC"
    LIMIT,      // "LIMIT"
    AND,        // "AND"
    OR,         // "OR"
    JOIN,       // “JOIN”
    ON,         // "ON"

    DROP,       // "DROP"
    ALTER,      // "ALTER"
    ADD,        // "ADD"
    COLUMN,     // "COLUMN"
    COLUMNS,    // "COLUMNS
    // 关键字 (Phase 4)
    GROUP,      // "GROUP"
    COUNT,      // "COUNT"
    SUM,        // "SUM"
    AVG,        // "AVG"
    MIN,        // "MIN"
    MAX,        // "MAX"

    SHOW,       // "SHOW"
    TABLES,     // "TABLES"
    FULL,       // "FULL"

    // ---- 标识符 (Identifier) ----
    IDENTIFIER, // 表名、列名等

    // ---- 常量 (Constants) ----
    INTEGER_CONST,    // 整数常量, e.g., 123
    DECIMAL_CONST,    // 小数常量, e.g., 123.45
    STRING_CONST,     // 字符串常量, e.g., 'hello'

    // ---- 运算符 (Operators) ----
    EQUAL,      // =
    NOT_EQUAL,  // != 或 <>
    GREATER,    // >
    LESS,       // <

    // ====== 符号和运算符 ======
    ASTERISK,       // *
    LESS_EQUAL,     // <=
    GREATER_EQUAL,  // >=
    // ============================

    // ---- 分隔符 (Delimiters) ----
    COMMA,      // ,
    SEMICOLON,  // ;
    LPAREN,     // (
    RPAREN,     // )
    DOT,        // .

    // ---- 特殊 Token ----
    EOF,        // End-Of-File，表示输入流结束
    ILLEGAL     // 非法字符，用于错误处理
}