package org.csu.sdolp.engine;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.*;
import org.csu.sdolp.compiler.semantic.SemanticAnalyzer;
import org.csu.sdolp.executor.FilterExecutor;
import org.csu.sdolp.executor.SeqScanExecutor;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.executor.expressions.AbstractPredicate;
import org.csu.sdolp.executor.expressions.ComparisonPredicate;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 数据库查询处理器核心引擎。
 * 负责接收SQL字符串，并协调编译器、执行器和存储层来完成整个查询处理流程。
 * 这是连接所有数据库组件的中央枢纽。
 */
public class QueryProcessor {

    private final DiskManager diskManager;
    private final BufferPoolManager bufferPoolManager;
    private final Catalog catalog;

    /**
     * 构造函数，初始化数据库引擎的底层组件。
     * @param dbFilePath 数据库文件在磁盘上的路径
     */
    public QueryProcessor(String dbFilePath) {
        try {
            this.diskManager = new DiskManager(dbFilePath);
            diskManager.open();
            // 定义缓冲池大小，例如 100 个页
            final int bufferPoolSize = 100;
            this.bufferPoolManager = new BufferPoolManager(bufferPoolSize, diskManager, "LRU");
            this.catalog = new Catalog(bufferPoolManager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database engine", e);
        }
    }

    /**
     * 执行一条SQL语句的主入口方法。
     * @param sql 用户输入的SQL字符串
     */
    public void execute(String sql) {
        try {
            // --- 1. 编译阶段 (Compilation) ---
            System.out.println("Executing: " + sql);
            // 1.1 词法分析: String -> List<Token>
            Lexer lexer = new Lexer(sql);
            List<Token> tokens = lexer.tokenize();

            // 1.2 语法分析: List<Token> -> Abstract Syntax Tree (AST)
            Parser parser = new Parser(tokens);
            StatementNode ast = parser.parse();

            // 1.3 语义分析: 检查AST的逻辑正确性
            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast);
            System.out.println("Semantic analysis passed.");

            // --- 2. 计划与执行阶段 (Planning & Execution) ---
            // 2.1 创建执行计划
            TupleIterator executionPlan = createExecutionPlan(ast);

            // 2.2 执行计划
            // 对于SELECT语句，执行并打印结果。对于其他语句，执行计划可能为null，但在planning阶段就已经生效了。
            if (executionPlan != null) {
                printResults(executionPlan);
            }

        } catch (ParseException | SemanticException | IOException e) {
            // 捕获并打印所有预期的异常
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            // 捕获意外的运行时异常
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 根据抽象语法树(AST)创建物理执行计划（一个执行器树）。
     * @param ast 经过语义分析的AST节点
     * @return 一个可执行的元组迭代器
     */
    private TupleIterator createExecutionPlan(StatementNode ast) throws IOException {
        if (ast instanceof CreateTableStatementNode node) {
            return planCreateTable(node);
        }
        if (ast instanceof InsertStatementNode node) {
            return planInsert(node);
        }
        if (ast instanceof SelectStatementNode node) {
            return planSelect(node);
        }
        if (ast instanceof DeleteStatementNode node) {
            // TODO: 实现 Delete 语句的计划
            System.out.println("DELETE statement execution is not yet implemented.");
            return null;
        }
        throw new UnsupportedOperationException("Unsupported statement type: " + ast.getClass().getSimpleName());
    }

    // --- 各类语句的计划生成方法 ---

    private TupleIterator planCreateTable(CreateTableStatementNode node) throws IOException {
        String tableName = node.tableName().name();
        List<Column> columns = new ArrayList<>();
        for (ColumnDefinitionNode colDef : node.columns()) {
            String colName = colDef.columnName().name();
            // 注意：DataType需要从字符串转换
            DataType colType = DataType.valueOf(colDef.dataType().name().toUpperCase());
            columns.add(new Column(colName, colType));
        }
        Schema schema = new Schema(columns);
        catalog.createTable(tableName, schema);
        System.out.println("Table '" + tableName + "' created successfully.");
        return null; // DDL语句不返回元组
    }

    private TupleIterator planInsert(InsertStatementNode node) throws IOException {
        String tableName = node.tableName().name();
        TableInfo tableInfo = catalog.getTable(tableName);
        Schema schema = tableInfo.getSchema();

        // 将AST中的字面量转换为数据库内部的Value对象
        List<Value> values = new ArrayList<>();
        for (ExpressionNode expr : node.values()) {
            if (expr instanceof LiteralNode literalNode) {
                String lexeme = literalNode.literal().lexeme();
                switch (literalNode.literal().type()) {
                    case INTEGER_CONST -> values.add(new Value(Integer.parseInt(lexeme)));
                    case STRING_CONST -> values.add(new Value(lexeme));
                    default -> throw new IllegalStateException("Unsupported literal type during insert planning.");
                }
            }
        }
        Tuple tupleToInsert = new Tuple(values);

        // 使用TableHeap直接插入 (这是简化版，未来会由InsertExecutor处理)
        TableHeap tableHeap = new TableHeap(bufferPoolManager, tableInfo);
        boolean success = tableHeap.insertTuple(tupleToInsert);

        if (success) {
            System.out.println("1 row inserted.");
        } else {
            System.out.println("Insert failed. The page might be full.");
        }
        return null; // DML语句不返回元组
    }

    private TupleIterator planSelect(SelectStatementNode node) throws IOException {
        String tableName = node.fromTable().name();
        TableInfo tableInfo = catalog.getTable(tableName);

        // 基础执行器：顺序扫描
        TupleIterator plan = new SeqScanExecutor(bufferPoolManager, tableInfo);
        if (node.whereClause() != null) {
            AbstractPredicate predicate = createPredicateFromAst(node.whereClause(), tableInfo.getSchema());
            plan = new FilterExecutor(plan, predicate);
        }

        // TODO: 增强2 - 添加ProjectionExecutor
        // if (!node.isSelectAll()) {
        //     plan = new ProjectionExecutor(plan, node.selectList());
        // }

        return plan;
    }


    private AbstractPredicate createPredicateFromAst(ExpressionNode expression, Schema schema) {
        if (expression instanceof BinaryExpressionNode node) {
            if (!(node.left() instanceof IdentifierNode) || !(node.right() instanceof LiteralNode)) {
                throw new UnsupportedOperationException("WHERE clause only supports 'column_name op literal' format.");
            }

            // --- 修改开始 ---
            String columnName = ((IdentifierNode) node.left()).name();
            // 1. 从 Schema 中查找列名对应的索引
            int columnIndex = getColumnIndex(schema, columnName);

            // 2. 获取运算符和字面量值
            String operator = node.operator().type().name();
            Value literalValue = getLiteralValue((LiteralNode) node.right());

            // 3. 使用新的构造函数创建 Predicate
            return new ComparisonPredicate(columnIndex, literalValue, operator);
            // --- 修改结束 ---
        }
        throw new UnsupportedOperationException("Unsupported expression type in WHERE clause.");
    }
    private int getColumnIndex(Schema schema, String columnName) {
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (schema.getColumns().get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        // 此异常理论上应在语义分析阶段被捕获
        throw new IllegalStateException("Column '" + columnName + "' not found in schema during planning.");
    }

    private Value getLiteralValue(LiteralNode literalNode) {
        String lexeme = literalNode.literal().lexeme();
        return switch (literalNode.literal().type()) {
            case INTEGER_CONST -> new Value(Integer.parseInt(lexeme));
            case STRING_CONST -> new Value(lexeme);
            default -> throw new IllegalStateException("Unsupported literal type in expression.");
        };
    }

    /**
     * 优雅地打印查询结果
     * @param iterator 包含结果元组的迭代器
     */
    private void printResults(TupleIterator iterator) throws IOException {
        List<Tuple> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }

        if (results.isEmpty()) {
            System.out.println("Query finished, 0 rows returned.");
            return;
        }

        // 简单打印
        for (Tuple tuple : results) {
            System.out.println(tuple);
        }
        System.out.println("Query finished, " + results.size() + " rows returned.");
    }
}