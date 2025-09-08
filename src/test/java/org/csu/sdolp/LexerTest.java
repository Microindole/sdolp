package org.csu.sdolp;

import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.lexer.TokenType;
import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

/**
 * @author hidyouth
 * @description: Lexer 类的单元测试 (使用 JUnit 4)
 *
 * 这是一个标准的测试类，用于验证词法分析器的各种功能是否正确。
 */
public class LexerTest {

    @Test
    public void testSimpleSelectStatement() {
        // 测试一个简单的 SELECT 语句
        String sql = "SELECT id, name FROM student;";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        // 期望的Token类型序列
        TokenType[] expectedTypes = {
                TokenType.SELECT, TokenType.IDENTIFIER, TokenType.COMMA, TokenType.IDENTIFIER,
                TokenType.FROM, TokenType.IDENTIFIER, TokenType.SEMICOLON, TokenType.EOF
        };

        // 断言：检查Token数量是否正确 (包括EOF)
        assertEquals("Token数量不匹配", expectedTypes.length, tokens.size());

        // 断言：逐一检查每个Token的类型是否正确
        for (int i = 0; i < expectedTypes.length; i++) {
            assertEquals("Token类型不匹配 at index " + i, expectedTypes[i], tokens.get(i).type());
        }

        // 也可以具体检查某个Token的词素值
        assertEquals("id", tokens.get(1).lexeme());
        assertEquals("student", tokens.get(5).lexeme());
    }

    @Test
    public void testComplexStatementWithAllFeatures() {
        // 测试一个更复杂的语句，包含所有类型的Token
        String sql = "INSERT INTO users (id, name) VALUES (101, 'Alice');";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        TokenType[] expectedTypes = {
                TokenType.INSERT, TokenType.INTO, TokenType.IDENTIFIER, TokenType.LPAREN,
                TokenType.IDENTIFIER, TokenType.COMMA, TokenType.IDENTIFIER, TokenType.RPAREN,
                TokenType.VALUES, TokenType.LPAREN, TokenType.INTEGER_CONST, TokenType.COMMA,
                TokenType.STRING_CONST, TokenType.RPAREN, TokenType.SEMICOLON, TokenType.EOF
        };

        assertEquals(expectedTypes.length, tokens.size());
        for (int i = 0; i < expectedTypes.length; i++) {
            assertEquals(expectedTypes[i], tokens.get(i).type());
        }

        // 验证常量的值
        assertEquals("101", tokens.get(10).lexeme());
        assertEquals("Alice", tokens.get(12).lexeme());
    }

    @Test
    public void testIllegalCharacters() {
        // 测试包含非法字符的情况
        String sql = "SELECT # FROM users;";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        // 期望在 '#' 处得到一个 ILLEGAL Token
        assertEquals(TokenType.SELECT, tokens.get(0).type());
        assertEquals(TokenType.ILLEGAL, tokens.get(1).type());
        assertEquals("#", tokens.get(1).lexeme());
        assertEquals(TokenType.FROM, tokens.get(2).type());
    }
}