package org.csu.sdolp;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.StatementNode;
import org.csu.sdolp.compiler.semantic.SemanticAnalyzer;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author hidyouth
 * @description: SemanticAnalyzer 类的单元测试 (JUnit 4)
 */
public class SemanticAnalyzerTest {

    private final String TEST_DB_FILE = "test_semantic.db";
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private Catalog catalog;
    private SemanticAnalyzer semanticAnalyzer;

    @Before
    public void setUp() throws IOException {
        new File(TEST_DB_FILE).delete();
        diskManager = new DiskManager(TEST_DB_FILE);
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        catalog = new Catalog(bufferPoolManager);
        semanticAnalyzer = new SemanticAnalyzer(catalog);

        // 预先创建一张表用于测试
        Schema schema = new Schema(Arrays.asList(
                new Column("id", DataType.INT),
                new Column("name", DataType.VARCHAR)
        ));
        catalog.createTable("users", schema);
    }

    @After
    public void tearDown() throws IOException {
        diskManager.close();
        new File(TEST_DB_FILE).delete();
    }

    private StatementNode parseSql(String sql) {
        Lexer lexer = new Lexer(sql);
        Parser parser = new Parser(lexer.tokenize());
        return parser.parse();
    }

    @Test
    public void testAnalyzeValidStatements() {
        System.out.println("--- Running test: testAnalyzeValidStatements ---");
        System.out.println("Goal: Verify that legally correct statements do not throw exceptions.");
        // 这些都是合法的，不应该抛出异常
        semanticAnalyzer.analyze(parseSql("SELECT id, name FROM users;"));
        semanticAnalyzer.analyze(parseSql("SELECT * FROM users WHERE id = 1;"));
        semanticAnalyzer.analyze(parseSql("INSERT INTO users (id, name) VALUES (100, 'test');"));
        semanticAnalyzer.analyze(parseSql("DELETE FROM users WHERE name = 'test';"));
        System.out.println("Result: Test PASSED.\n");
    }

    @Test(expected = SemanticException.class)
    public void testAnalyzeCreateTable_TableExists() {
        System.out.println("--- Running test: testAnalyzeCreateTable_TableExists ---");
        System.out.println("Goal: Expect a SemanticException for creating an existing table.");
        try {
            // 错误：表已存在
            semanticAnalyzer.analyze(parseSql("CREATE TABLE users (id INT);"));
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }

    @Test(expected = SemanticException.class)
    public void testAnalyzeSelect_TableNotFound() {
        System.out.println("--- Running test: testAnalyzeSelect_TableNotFound ---");
        System.out.println("Goal: Expect a SemanticException for selecting from a non-existent table.");
        try {
            // 错误：查询一个不存在的表
            semanticAnalyzer.analyze(parseSql("SELECT * FROM non_existent_table;"));
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }

    @Test(expected = SemanticException.class)
    public void testAnalyzeSelect_ColumnNotFound() {
        System.out.println("--- Running test: testAnalyzeSelect_ColumnNotFound ---");
        System.out.println("Goal: Expect a SemanticException for selecting a non-existent column.");
        try {
            // 错误：查询一个不存在的列
            semanticAnalyzer.analyze(parseSql("SELECT age FROM users;"));
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }

    @Test(expected = SemanticException.class)
    public void testAnalyzeInsert_ColumnNotFound() {
        System.out.println("--- Running test: testAnalyzeInsert_ColumnNotFound ---");
        System.out.println("Goal: Expect a SemanticException for inserting into a non-existent column.");
        try {
            // 错误：插入一个不存在的列
            semanticAnalyzer.analyze(parseSql("INSERT INTO users (age) VALUES (30);"));
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }

    @Test(expected = SemanticException.class)
    public void testAnalyzeInsert_TypeMismatch() {
        System.out.println("--- Running test: testAnalyzeInsert_TypeMismatch ---");
        System.out.println("Goal: Expect a SemanticException for inserting a value with mismatched type.");
        try {
            // 错误：插入的值类型不匹配
            semanticAnalyzer.analyze(parseSql("INSERT INTO users (id) VALUES ('a string');"));
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }

    @Test(expected = SemanticException.class)
    public void testAnalyzeWhere_TypeMismatch() {
        System.out.println("--- Running test: testAnalyzeWhere_TypeMismatch ---");
        System.out.println("Goal: Expect a SemanticException for a type mismatch in the WHERE clause.");
        try {
            // 错误：WHERE子句中类型不匹配
            semanticAnalyzer.analyze(parseSql("SELECT * FROM users WHERE name > 123;"));
        } finally {
            System.out.println("Result: Test threw expected exception.\n");
        }
    }
}

