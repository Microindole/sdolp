package org.csu.sdolp;

import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.CreateTableStatementNode;
import org.csu.sdolp.compiler.parser.ast.SelectStatementNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author hidyouth
 * @description: Parser 类的单元测试 (使用 JUnit 4)
 */
public class ParserTest {

    private StatementNode parseSql(String sql) {
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        return parser.parse();
    }

    @Test
    public void testParseCreateTable() {
        String sql = "CREATE TABLE users (id INT, name VARCHAR);";
        StatementNode node = parseSql(sql);

        // 检查根节点类型
        assertTrue("AST node should be an instance of CreateTableStatementNode", node instanceof CreateTableStatementNode);

        CreateTableStatementNode createTableNode = (CreateTableStatementNode) node;

        // 检查表名
        assertEquals("users", createTableNode.tableName().name());

        // 检查列定义
        assertEquals(2, createTableNode.columns().size());
        assertEquals("id", createTableNode.columns().get(0).columnName().name());
        assertEquals("INT", createTableNode.columns().get(0).dataType().name());
        assertEquals("name", createTableNode.columns().get(1).columnName().name());
        assertEquals("VARCHAR", createTableNode.columns().get(1).dataType().name());
    }

    @Test
    public void testParseSimpleSelect() {
        String sql = "SELECT id, name FROM customers;";
        StatementNode node = parseSql(sql);

        assertTrue("AST node should be an instance of SelectStatementNode", node instanceof SelectStatementNode);

        SelectStatementNode selectNode = (SelectStatementNode) node;

        // 检查 FROM 表
        assertEquals("customers", selectNode.fromTable().name());

        // 检查 SELECT 列表
        assertEquals(2, selectNode.selectList().size());
        // 注意：这里需要根据你的AST设计来断言，我们设计的是IdentifierNode
        assertEquals("id", ((org.csu.sdolp.compiler.parser.ast.IdentifierNode) selectNode.selectList().get(0)).name());
        assertEquals("name", ((org.csu.sdolp.compiler.parser.ast.IdentifierNode) selectNode.selectList().get(1)).name());
    }

    @Test(expected = ParseException.class)
    public void testInvalidSyntax_MissingSemicolon() {
        // 缺少分号应该抛出异常
        String sql = "SELECT name FROM products";
        parseSql(sql);
    }

    @Test(expected = ParseException.class)
    public void testInvalidSyntax_CreateTableMissingParen() {
        // CREATE TABLE 缺少右括号
        String sql = "CREATE TABLE test (id INT;";
        parseSql(sql);
    }
}
