package org.csu.sdolp;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.cli.server.Session;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.StatementNode;
import org.csu.sdolp.compiler.planner.Planner;
import org.csu.sdolp.compiler.planner.plan.*;
import org.csu.sdolp.compiler.planner.plan.ddl.CreateTablePlanNode;
import org.csu.sdolp.compiler.planner.plan.dml.InsertPlanNode;
import org.csu.sdolp.compiler.planner.plan.query.FilterPlanNode;
import org.csu.sdolp.compiler.planner.plan.query.ProjectPlanNode;
import org.csu.sdolp.compiler.planner.plan.query.SeqScanPlanNode;
import org.csu.sdolp.compiler.semantic.SemanticAnalyzer;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author hidyouth
 * @description: Planner 类的单元测试 (JUnit 4)
 */
public class PlannerTest {

    private final String TEST_DB_FILE = "test_planner.db";
    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private Catalog catalog;
    private Planner planner;

    @Before
    public void setUp() throws IOException {
        new File(TEST_DB_FILE).delete();
        diskManager = new DiskManager(TEST_DB_FILE);
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        catalog = new Catalog(bufferPoolManager);
        planner = new Planner(catalog);

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

    private PlanNode createPlanForSql(String sql) {
        Lexer lexer = new Lexer(sql);
        Parser parser = new Parser(lexer.tokenize());
        StatementNode ast = parser.parse();

        // 必须先经过语义分析
        SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
        Session mockRootSession = Session.createAuthenticatedSession(-1, "root");
        semanticAnalyzer.analyze(ast,mockRootSession);

        return planner.createPlan(ast);
    }

    @Test
    public void testCreateTablePlan() {
        System.out.println("--- Running test: testCreateTablePlan ---");
        PlanNode plan = createPlanForSql("CREATE TABLE products (pid INT, pname VARCHAR);");
        assertTrue(plan instanceof CreateTablePlanNode);
        CreateTablePlanNode createPlan = (CreateTablePlanNode) plan;
        assertEquals("products", createPlan.getTableName());
        assertEquals(2, createPlan.getOutputSchema().getColumns().size());
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testInsertPlan() {
        System.out.println("--- Running test: testInsertPlan ---");
        PlanNode plan = createPlanForSql("INSERT INTO users (id, name) VALUES (1, 'test');");
        assertTrue(plan instanceof InsertPlanNode);
        InsertPlanNode insertPlan = (InsertPlanNode) plan;
        assertEquals("users", insertPlan.getTableInfo().getTableName());
        assertEquals(1, insertPlan.getRawTuples().size());
        assertEquals(1, insertPlan.getRawTuples().get(0).getValues().get(0).getValue());
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testSimpleSelectPlan() {
        System.out.println("--- Running test: testSimpleSelectPlan ---");
        PlanNode plan = createPlanForSql("SELECT * FROM users;");
        // 期望: SeqScan
        assertTrue(plan instanceof SeqScanPlanNode);
        assertEquals(2, plan.getOutputSchema().getColumns().size());
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testSelectWithProjectionPlan() {
        System.out.println("--- Running test: testSelectWithProjectionPlan ---");
        PlanNode plan = createPlanForSql("SELECT id FROM users;");
        // 期望: Project -> SeqScan
        assertTrue(plan instanceof ProjectPlanNode);
        assertEquals(1, plan.getOutputSchema().getColumns().size());
        assertEquals("id", plan.getOutputSchema().getColumns().get(0).getName());

        ProjectPlanNode projectPlan = (ProjectPlanNode) plan;
        assertTrue(projectPlan.getChild() instanceof SeqScanPlanNode);
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testSelectWithFilterPlan() {
        System.out.println("--- Running test: testSelectWithFilterPlan ---");
        PlanNode plan = createPlanForSql("SELECT * FROM users WHERE id = 1;");
        // 期望: Filter -> SeqScan
        assertTrue(plan instanceof FilterPlanNode);
        assertEquals(2, plan.getOutputSchema().getColumns().size());

        FilterPlanNode filterPlan = (FilterPlanNode) plan;
        assertTrue(filterPlan.getChild() instanceof SeqScanPlanNode);
        assertNotNull(filterPlan.getPredicate());
        System.out.println("Result: Test PASSED.\n");
    }

    @Test
    public void testSelectWithFilterAndProjectionPlan() {
        System.out.println("--- Running test: testSelectWithFilterAndProjectionPlan ---");
        PlanNode plan = createPlanForSql("SELECT name FROM users WHERE id > 10;");
        // 期望: Project -> Filter -> SeqScan
        assertTrue(plan instanceof ProjectPlanNode);
        assertEquals(1, plan.getOutputSchema().getColumns().size());
        assertEquals("name", plan.getOutputSchema().getColumns().get(0).getName());

        ProjectPlanNode projectPlan = (ProjectPlanNode) plan;
        assertTrue(projectPlan.getChild() instanceof FilterPlanNode);

        FilterPlanNode filterPlan = (FilterPlanNode) projectPlan.getChild();
        assertTrue(filterPlan.getChild() instanceof SeqScanPlanNode);
        System.out.println("Result: Test PASSED.\n");
    }
}
