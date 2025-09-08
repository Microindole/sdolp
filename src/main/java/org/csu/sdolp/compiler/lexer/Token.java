package org.csu.sdolp.compiler.lexer;

/**
 * @author hidyouth
 * @description: 代表一个词法单元（Token），是词法分析器的最小输出单位
 *
 * 使用Java 17的record特性可以非常简洁地定义一个不可变的数据类。
 * 如果您的项目JDK版本较低，可以将其改写为传统的class。
 *
 * @param type 词法单元的类型 (种别码)
 * @param lexeme 词法单元的原始文本 (词素值)
 * @param line 所在的行号
 * @param column 所在的列号
 */
public record Token(TokenType type, String lexeme, int line, int column) {

    @Override
    public String toString() {
        // 重写toString方法，方便调试和打印
        return String.format("Token[Type=%-15s, Lexeme='%s', Position=%d:%d]",
                type, lexeme, line, column);
    }
}
