package org.csu.sdolp;

import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.*;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author hidyouth
 * @description: Parser 类的单元测试 (使用 JUnit 4)
 */
public class ParserTest {

    private StatementNode parseSql(String sql) {
        System.out.println("Input SQL: " + sql); // [日志] 打印每次解析的SQL

        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();
        System.out.println("Tokens: " + tokens); // [日志] 打印词法分析结果

        Parser parser = new Parser(tokens);
        StatementNode ast = parser.parse();
        System.out.println("Generated AST: " + ast); // [日志] 打印生成的AST

        return ast;
    }

    @Test
    public void testParseCreateTable() {
        System.out.println("--- Running test: testParseCreateTable ---");
        String sql = "CREATE TABLE users (id INT, name VARCHAR);";
        StatementNode node = parseSql(sql);
        assertTrue("AST node should be an instance of CreateTableStatementNode", node instanceof CreateTableStatementNode);
        CreateTableStatementNode createTableNode = (CreateTableStatementNode) node;
        assertEquals("users", createTableNode.tableName().name());
        assertEquals(2, createTableNode.columns().size());
        assertEquals("id", createTableNode.columns().get(0).columnName().name());
        assertEquals("INT", createTableNode.columns().get(0).dataType().name());
        assertEquals("name", createTableNode.columns().get(1).columnName().name());
        assertEquals("VARCHAR", createTableNode.columns().get(1).dataType().name());
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testParseSimpleSelect() {
        System.out.println("--- Running test: testParseSimpleSelect ---");
        String sql = "SELECT id, name FROM customers;";
        StatementNode node = parseSql(sql);
        assertTrue("AST node should be an instance of SelectStatementNode", node instanceof SelectStatementNode);
        SelectStatementNode selectNode = (SelectStatementNode) node;
        assertEquals("customers", selectNode.fromTable().name());
        assertEquals(2, selectNode.selectList().size());
        assertEquals("id", ((IdentifierNode) selectNode.selectList().get(0)).name());
        assertEquals("name", ((IdentifierNode) selectNode.selectList().get(1)).name());
        System.out.println("Result: Test PASSED.\n");
    }

    // ====== 新增：INSERT 语句测试 ======
    @Test
    public void testParseInsert() {
        System.out.println("--- Running test: testParseInsert ---");
        String sql = "INSERT INTO users (id, name) VALUES (1, 'hidyouths');";
        StatementNode node = parseSql(sql);
        assertTrue(node instanceof InsertStatementNode);

        InsertStatementNode insertNode = (InsertStatementNode) node;
        assertEquals("users", insertNode.tableName().name());
        assertEquals(2, insertNode.columns().size());
        assertEquals("id", insertNode.columns().get(0).name());
        assertEquals("name", insertNode.columns().get(1).name());
        assertEquals(2, insertNode.values().size());
        assertTrue(insertNode.values().get(0) instanceof LiteralNode);
        assertTrue(insertNode.values().get(1) instanceof LiteralNode);
        System.out.println("Result: Test PASSED.\n");
    }
    // ======================================

    // ====== 新增：DELETE 语句测试 ======
    @Test
    public void testParseDeleteWithWhere() {
        System.out.println("--- Running test: testParseDeleteWithWhere ---");
        String sql = "DELETE FROM users WHERE id = 10;";
        StatementNode node = parseSql(sql);
        assertTrue(node instanceof DeleteStatementNode);

        DeleteStatementNode deleteNode = (DeleteStatementNode) node;
        assertEquals("users", deleteNode.tableName().name());
        assertNotNull("WHERE clause should not be null", deleteNode.whereClause());
        assertTrue(deleteNode.whereClause() instanceof BinaryExpressionNode);
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testParseDeleteWithoutWhere() {
        System.out.println("--- Running test: testParseDeleteWithoutWhere ---");
        String sql = "DELETE FROM users;";
        StatementNode node = parseSql(sql);
        assertTrue(node instanceof DeleteStatementNode);

        DeleteStatementNode deleteNode = (DeleteStatementNode) node;
        assertEquals("users", deleteNode.tableName().name());
        assertNull("WHERE clause should be null", deleteNode.whereClause());
        System.out.println("Result: Test PASSED.\n");
    }
    // ======================================

    // ====== 新增：增强的 SELECT 语句测试 ======
    @Test
    public void testParseSelectWithWhere() {
        System.out.println("--- Running test: testParseSelectWithWhere ---");
        String sql = "SELECT name FROM students WHERE age > 20;";
        StatementNode node = parseSql(sql);
        assertTrue(node instanceof SelectStatementNode);

        SelectStatementNode selectNode = (SelectStatementNode) node;
        assertEquals("students", selectNode.fromTable().name());
        assertNotNull("WHERE clause should not be null", selectNode.whereClause());
        assertFalse(selectNode.isSelectAll());
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testParseSelectStar() {
        System.out.println("--- Running test: testParseSelectStar ---");
        String sql = "SELECT * FROM products;";
        StatementNode node = parseSql(sql);
        assertTrue(node instanceof SelectStatementNode);

        SelectStatementNode selectNode = (SelectStatementNode) node;
        assertTrue("isSelectAll should be true", selectNode.isSelectAll());
        assertEquals("products", selectNode.fromTable().name());
        assertNull("WHERE clause should be null", selectNode.whereClause());
        System.out.println("Result: Test PASSED.\n");
    }
    // ==========================================

    @Test(expected = ParseException.class)
    public void testInvalidSyntax_MissingSemicolon() {
//        String sql = "SELECT name FROM products";
//        parseSql(sql);
        System.out.println("--- Running test: testInvalidSyntax_MissingSemicolon ---");
        System.out.println("Goal: Expect a ParseException for missing semicolon.");
        String sql = "SELECT name FROM products";
        try {
            parseSql(sql);
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }

    @Test(expected = ParseException.class)
    public void testInvalidSyntax_CreateTableMissingParen() {
//        String sql = "CREATE TABLE test (id INT;";
//        parseSql(sql);
        System.out.println("--- Running test: testInvalidSyntax_CreateTableMissingParen ---");
        System.out.println("Goal: Expect a ParseException for missing parenthesis.");
        String sql = "CREATE TABLE test (id INT;";
        try {
            parseSql(sql);
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }
}
