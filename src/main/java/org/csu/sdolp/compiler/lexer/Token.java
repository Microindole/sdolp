package org.csu.sdolp.compiler.lexer;

/**
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
