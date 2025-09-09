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
    INSERT,     // "INSERT"
    INTO,       // "INTO"
    VALUES,     // "VALUES"
    DELETE,     // "DELETE"
    UPDATE,     // "UPDATE"
    SET,        // "SET"
    INT,        // "INT" 数据类型
    VARCHAR,    // "VARCHAR" 数据类型

    // ---- 标识符 (Identifier) ----
    IDENTIFIER, // 表名、列名等

    // ---- 常量 (Constants) ----
    INTEGER_CONST,    // 整数常量, e.g., 123
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

    // ---- 特殊 Token ----
    EOF,        // End-Of-File，表示输入流结束
    ILLEGAL     // 非法字符，用于错误处理
}
