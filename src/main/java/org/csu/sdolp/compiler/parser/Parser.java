package org.csu.sdolp.compiler.parser;

import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.lexer.TokenType;
import org.csu.sdolp.compiler.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hidyouth
 * @description: 语法分析器
 * 采用递归下降法，将Token流转换为抽象语法树(AST)
 */
public class Parser {

    private final List<Token> tokens;
    private int position = 0;

    public Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    /**
     * 解析入口，解析单个SQL语句
     * @return AST的根节点 (一个StatementNode)
     */
    public StatementNode parse() {
        if (isAtEnd()) {
            return null;
        }
        StatementNode statement = parseStatement();


        consume(TokenType.SEMICOLON, "';' at the end of statement");


        return statement;
    }

    private StatementNode parseStatement() {
        if (match(TokenType.CREATE)) {
            return parseCreateTableStatement();
        }
        if (match(TokenType.SELECT)) {
            return parseSelectStatement();
        }
        // ====== 修改：新增 INSERT 和 DELETE 解析分支 ======
        if (match(TokenType.INSERT)) {
            return parseInsertStatement();
        }
        if (match(TokenType.DELETE)) {
            return parseDeleteStatement();
        }
        if (match(TokenType.UPDATE)) { // <-- 新增分支
            return parseUpdateStatement();
        }
        // ===============================================
        // 在此可以扩展支持 INSERT, DELETE 等语句
        throw new ParseException(peek(), "a valid statement (CREATE, SELECT, INSERT, DELETE, etc.)");
    }

    /**
     * 解析 CREATE TABLE 语句
     * 语法: CREATE TABLE table_name ( col1 type1, col2 type2, ... );
     */
    private CreateTableStatementNode parseCreateTableStatement() {
        consume(TokenType.TABLE, "'TABLE' keyword");
        Token tableNameToken = consume(TokenType.IDENTIFIER, "table name");
        IdentifierNode tableName = new IdentifierNode(tableNameToken.lexeme());

        consume(TokenType.LPAREN, "'(' after table name");

        List<ColumnDefinitionNode> columns = new ArrayList<>();
        if (!check(TokenType.RPAREN)) {
            do {
                columns.add(parseColumnDefinition());
            } while (match(TokenType.COMMA));
        }

        consume(TokenType.RPAREN, "')' after column definitions");

        return new CreateTableStatementNode(tableName, columns);
    }

//    private ColumnDefinitionNode parseColumnDefinition() {
//        Token columnNameToken = consume(TokenType.IDENTIFIER, "column name");
//        IdentifierNode columnName = new IdentifierNode(columnNameToken.lexeme());
//
//        Token dataTypeToken = consume(TokenType.IDENTIFIER, "data type (e.g., INT, VARCHAR)");
//        IdentifierNode dataType = new IdentifierNode(dataTypeToken.lexeme());
//        // 注意：这里我们简单地将类型也视为标识符，语义分析阶段会检查其合法性
//
//        return new ColumnDefinitionNode(columnName, dataType);
//    }
    private ColumnDefinitionNode parseColumnDefinition() {
        Token columnNameToken = consume(TokenType.IDENTIFIER, "column name");
        IdentifierNode columnName = new IdentifierNode(columnNameToken.lexeme());

        // --- FIX START ---
        // 原来的代码 rigid 地要求 IDENTIFIER 类型
        // Token dataTypeToken = consume(TokenType.IDENTIFIER, "data type (e.g., INT, VARCHAR)");

        // 修正后的代码：检查下一个token是否为已知的数据类型关键字或通用标识符
        Token dataTypeToken = peek();
        if (dataTypeToken.type() == TokenType.INT ||
                dataTypeToken.type() == TokenType.VARCHAR ||
                dataTypeToken.type() == TokenType.IDENTIFIER) {
            advance(); // 如果是合法类型，就消费掉这个token
        } else {
            // 否则，抛出异常
            throw new ParseException(peek(), "data type (e.g., INT, VARCHAR)");
        }
        // --- FIX END ---

        IdentifierNode dataType = new IdentifierNode(dataTypeToken.lexeme());
        return new ColumnDefinitionNode(columnName, dataType);
    }

    // ====== 修改：重写 SELECT 解析以支持 * 和 WHERE ======
    private SelectStatementNode parseSelectStatement() {
        List<ExpressionNode> selectList = new ArrayList<>();
        boolean isSelectAll = false;
        if (match(TokenType.ASTERISK)) {
            isSelectAll = true;
        } else {
            if (!check(TokenType.FROM)) {
                do {
                    // select list 也需要支持 table.column ***
                    selectList.add(parsePrimaryExpression());
                } while (match(TokenType.COMMA));
            }
        }
        consume(TokenType.FROM, "'FROM' keyword");
        Token tableNameToken = consume(TokenType.IDENTIFIER, "table name");
        IdentifierNode fromTable = new IdentifierNode(tableNameToken.lexeme());
        ExpressionNode whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }
        OrderByClauseNode orderByClause = null;
        if (match(TokenType.ORDER)) {
            orderByClause = parseOrderByClause();
        }
        LimitClauseNode limitClause = null;
        if (match(TokenType.LIMIT)) {
            limitClause = parseLimitClause();
        }
        return new SelectStatementNode(selectList, fromTable, whereClause, isSelectAll, orderByClause, limitClause);
    }
    // ====== INSERT 语句解析方法 ======
    private InsertStatementNode parseInsertStatement() {
        consume(TokenType.INTO, "'INTO' keyword");
        Token tableNameToken = consume(TokenType.IDENTIFIER, "table name");
        IdentifierNode tableName = new IdentifierNode(tableNameToken.lexeme());

        consume(TokenType.LPAREN, "'(' after table name");
        List<IdentifierNode> columns = new ArrayList<>();
        if (!check(TokenType.RPAREN)) {
            do {
                Token colToken = consume(TokenType.IDENTIFIER, "column name");
                columns.add(new IdentifierNode(colToken.lexeme()));
            } while (match(TokenType.COMMA));
        }
        consume(TokenType.RPAREN, "')' after column list");

        consume(TokenType.VALUES, "'VALUES' keyword");
        consume(TokenType.LPAREN, "'(' before value list");
        List<ExpressionNode> values = new ArrayList<>();
        if (!check(TokenType.RPAREN)) {
            do {
                values.add(parsePrimaryExpression());
            } while (match(TokenType.COMMA));
        }
        consume(TokenType.RPAREN, "')' after value list");

        return new InsertStatementNode(tableName, columns, values);
    }
    // ======================================


    private DeleteStatementNode parseDeleteStatement() {
        consume(TokenType.FROM, "'FROM' keyword");
        Token tableNameToken = consume(TokenType.IDENTIFIER, "table name");
        IdentifierNode tableName = new IdentifierNode(tableNameToken.lexeme());

        ExpressionNode whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }

        return new DeleteStatementNode(tableName, whereClause);
    }
    // ======================================


    private UpdateStatementNode parseUpdateStatement() {
        Token tableNameToken = consume(TokenType.IDENTIFIER, "table name after UPDATE");
        IdentifierNode tableName = new IdentifierNode(tableNameToken.lexeme());

        consume(TokenType.SET, "'SET' keyword");

        List<SetClauseNode> setClauses = new ArrayList<>();
        do {
            Token colToken = consume(TokenType.IDENTIFIER, "column name in SET clause");
            IdentifierNode column = new IdentifierNode(colToken.lexeme());
            consume(TokenType.EQUAL, "'=' after column name");
            ExpressionNode value = parsePrimaryExpression();
            setClauses.add(new SetClauseNode(column, value));
        } while (match(TokenType.COMMA));

        ExpressionNode whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }

        return new UpdateStatementNode(tableName, setClauses, whereClause);
    }
    // ====== 解析方法 (Phase 1) ======
    private OrderByClauseNode parseOrderByClause() {
        consume(TokenType.BY, "'BY' after 'ORDER'");
        // *** 修改点: ORDER BY 的列也可能是 table.column ***
        ExpressionNode columnExpr = parsePrimaryExpression();
        if (!(columnExpr instanceof IdentifierNode)) {
            throw new ParseException("Expected a column identifier for ORDER BY clause.");
        }
        IdentifierNode column = (IdentifierNode) columnExpr;
        boolean isAscending = true;
        if (match(TokenType.ASC)) {
            isAscending = true;
        } else if (match(TokenType.DESC)) {
            isAscending = false;
        }
        return new OrderByClauseNode(column, isAscending);
    }
    private LimitClauseNode parseLimitClause() {
        Token limitToken = consume(TokenType.INTEGER_CONST, "integer value for LIMIT");
        try {
            int limit = Integer.parseInt(limitToken.lexeme());
            return new LimitClauseNode(limit);
        } catch (NumberFormatException e) {
            throw new ParseException("Invalid number for LIMIT: " + limitToken.lexeme());
        }
    }
    // ====== 新增：表达式解析相关方法 ======
    // ====== 表达式解析逻辑重构 (Phase 2) ======
    // parseExpression 是新的入口
    private ExpressionNode parseExpression() {
        return parseOrExpression();
    }

    // OR 的优先级最低
    private ExpressionNode parseOrExpression() {
        ExpressionNode left = parseAndExpression();
        while (match(TokenType.OR)) {
            Token operator = previous();
            ExpressionNode right = parseAndExpression();
            left = new BinaryExpressionNode(left, operator, right);
        }
        return left;
    }

    // AND 的优先级高于 OR
    private ExpressionNode parseAndExpression() {
        ExpressionNode left = parseComparison();
        while (match(TokenType.AND)) {
            Token operator = previous();
            ExpressionNode right = parseComparison();
            left = new BinaryExpressionNode(left, operator, right);
        }
        return left;
    }

    private ExpressionNode parseComparison() {
        ExpressionNode left = parsePrimaryExpression();
        if (match(TokenType.GREATER, TokenType.GREATER_EQUAL, TokenType.LESS, TokenType.LESS_EQUAL, TokenType.EQUAL, TokenType.NOT_EQUAL)) {
            Token operator = previous();
            ExpressionNode right = parsePrimaryExpression();
            left = new BinaryExpressionNode(left, operator, right);
        }
        return left;
    }
    // ====== 增强主表达式解析以支持列名 ======
    private ExpressionNode parsePrimaryExpression() {
        if (match(TokenType.INTEGER_CONST, TokenType.STRING_CONST)) {
            return new LiteralNode(previous());
        }

        if (check(TokenType.IDENTIFIER)) {
            Token firstIdentifier = advance();
            // 预读 (lookahead): 检查下一个 token 是否是点号
            if (match(TokenType.DOT)) {
                Token secondIdentifier = consume(TokenType.IDENTIFIER, "column name after '.'");
                // 解析为 table.column 格式
                return new IdentifierNode(firstIdentifier.lexeme(), secondIdentifier.lexeme());
            }
            // 如果不是点号，则是一个简单的标识符
            return new IdentifierNode(firstIdentifier.lexeme());
        }

        if (match(TokenType.LPAREN)) {
            ExpressionNode expr = parseExpression();
            consume(TokenType.RPAREN, "')' after expression.");
            return expr;
        }

        throw new ParseException(peek(), "an expression (a literal or an identifier)");
    }
    // --- 辅助方法 ---
    private boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) {
                advance();
                return true;
            }
        }
        return false;
    }

    private Token consume(TokenType type, String message) {
        if (check(type)) return advance();
        throw new ParseException(peek(), message);
    }

    private boolean check(TokenType type) {
        if (isAtEnd()) return false;
        return peek().type() == type;
    }

    private Token advance() {
        if (!isAtEnd()) position++;
        return previous();
    }

    private boolean isAtEnd() {
        return peek().type() == TokenType.EOF;
    }

    private Token peek() {
        return tokens.get(position);
    }

    private Token previous() {
        return tokens.get(position - 1);
    }
}
