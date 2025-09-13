package org.csu.sdolp.compiler.parser;

import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.lexer.TokenType;
import org.csu.sdolp.compiler.parser.ast.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author hidyouth
 * @description: 语法分析器
 * 采用递归下降法，将Token流转换为抽象语法树(AST)
 */
public class Parser {

    private final List<Token> tokens;
    private int position = 0;

    private static final Set<TokenType> AGGREGATE_FUNCTIONS = Set.of(
            TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MIN, TokenType.MAX
    );

    public Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    public StatementNode parse() {
        if (peek().type() == TokenType.EOF) {
            return null;
        }

        StatementNode statement = parseStatement();

        consume(TokenType.SEMICOLON, "Expected ';' at the end of the statement");
        if (!isAtEnd()) {
            throw new ParseException(peek(), "Unexpected tokens after semicolon");
        }
        return statement;
    }
    private StatementNode parseStatement() {
        if (check(TokenType.CREATE)) {
            Token nextToken = tokens.get(position + 1);
            if (nextToken.type() == TokenType.TABLE) {
                consume(TokenType.CREATE, "'CREATE' keyword");
                return parseCreateTableStatement();
            }
            if (nextToken.type() == TokenType.DATABASE) {
                consume(TokenType.CREATE, "'CREATE' keyword");
                return parseCreateDatabaseStatement();
            }
            if (nextToken.type() == TokenType.INDEX) {
                consume(TokenType.CREATE, "'CREATE' keyword");
                return parseCreateIndexStatement();
            }
            if (nextToken.type() == TokenType.USER) {
                consume(TokenType.CREATE, "'CREATE' keyword");
                return parseCreateUserStatement();
            }
            throw new ParseException(nextToken, "Expected 'TABLE', 'DATABASE', or 'INDEX' after 'CREATE'");
        }
        if (match(TokenType.GRANT)) {
            return parseGrantStatement();
        }
        if (match(TokenType.SHOW)) {
            return parseShowStatement();
        }
        if (match(TokenType.SELECT)) {
            return parseSelectStatement();
        }
        if (match(TokenType.INSERT)) {
            return parseInsertStatement();
        }
        if (match(TokenType.DELETE)) {
            return parseDeleteStatement();
        }
        if (match(TokenType.UPDATE)) {
            return parseUpdateStatement();
        }
        if (match(TokenType.DROP)) {
            if (peek().type() == TokenType.DATABASE) {
                return parseDropDatabaseStatement();
            }
            return parseDropTableStatement();
        }
        if (match(TokenType.USE)) {
            return parseUseDatabaseStatement();
        }

        throw new ParseException(peek(), "a valid statement (CREATE, SELECT, INSERT, DELETE, DROP, etc.)");
    }

    private StatementNode parseShowStatement() {
        if (peek().type() == TokenType.DATABASES) {
            consume(TokenType.DATABASES, "Expected 'DATABASES' after 'SHOW'");
            return new ShowDatabasesStatementNode();
        }

        match(TokenType.FULL);

        if (peek().type() == TokenType.COLUMNS) {
            consume(TokenType.COLUMNS, "Expected 'COLUMNS' after 'SHOW'");
            consume(TokenType.FROM, "Expected 'FROM' after 'COLUMNS'");
            IdentifierNode tableName = parseTableName();
            return new ShowColumnsStatementNode(tableName);
        }

        if (peek().type() == TokenType.TABLES) {
            consume(TokenType.TABLES, "Expected 'TABLES' after 'SHOW'");
            if (match(TokenType.FROM)) {
                consume(TokenType.IDENTIFIER, "Expected database name after 'FROM'");
            }
            if (match(TokenType.WHERE)) {
                while (peek().type() != TokenType.SEMICOLON && peek().type() != TokenType.EOF) {
                    advance();
                }
            }
            return new ShowTablesStatementNode();
        }
        if (peek().type() == TokenType.CREATE) {
            consume(TokenType.CREATE, "Expected 'CREATE' after 'SHOW'");
            consume(TokenType.TABLE, "Expected 'TABLE' after 'CREATE'");
            IdentifierNode tableName = parseTableName();
            return new ShowCreateTableStatementNode(tableName);
        }

        throw new ParseException(peek(), "Expected DATABASES, COLUMNS, or TABLES after 'SHOW'");
    }

    private StatementNode parseUseDatabaseStatement() {
        IdentifierNode dbName = new IdentifierNode(consume(TokenType.IDENTIFIER, "database name").lexeme());
        return new UseDatabaseStatementNode(dbName);
    }

    private StatementNode parseDropDatabaseStatement() {
        consume(TokenType.DATABASE, "Expected 'DATABASE' after 'DROP'");
        IdentifierNode dbName = new IdentifierNode(consume(TokenType.IDENTIFIER, "database name").lexeme());
        return new DropDatabaseStatementNode(dbName);
    }

    private StatementNode parseCreateDatabaseStatement() {
        consume(TokenType.DATABASE, "Expected 'DATABASE' after 'CREATE'");
        Token dbNameToken = consume(TokenType.IDENTIFIER, "database name");
        return new CreateDatabaseStatementNode(new IdentifierNode(dbNameToken.lexeme()));
    }

    private CreateUserStatementNode parseCreateUserStatement() {
        consume(TokenType.USER, "'USER' keyword after 'CREATE'");
        IdentifierNode username = new IdentifierNode(consume(TokenType.STRING_CONST, "username as a string literal").lexeme());
        consume(TokenType.IDENTIFIED, "'IDENTIFIED' keyword");
        consume(TokenType.BY, "'BY' keyword");
        LiteralNode password = new LiteralNode(consume(TokenType.STRING_CONST, "password as a string literal"));
        return new CreateUserStatementNode(username, password);
    }

    private GrantStatementNode parseGrantStatement() {
        List<IdentifierNode> privileges = new ArrayList<>();
        do {
            Token privilegeToken = peek();
            TokenType type = privilegeToken.type();

            if (type == TokenType.SELECT || type == TokenType.INSERT || type == TokenType.UPDATE || type == TokenType.DELETE || type == TokenType.IDENTIFIER) {
                advance();
                privileges.add(new IdentifierNode(privilegeToken.lexeme()));
            } else {
                throw new ParseException(privilegeToken, "a valid privilege type (e.g., SELECT, INSERT, ALL)");
            }
        } while (match(TokenType.COMMA));

        consume(TokenType.ON, "'ON' keyword");
        IdentifierNode tableName = new IdentifierNode(consume(TokenType.IDENTIFIER, "table name").lexeme());
        consume(TokenType.TO, "'TO' keyword");
        IdentifierNode username = new IdentifierNode(consume(TokenType.STRING_CONST, "username as a string literal").lexeme());
        return new GrantStatementNode(privileges, tableName, username);
    }

    private DropTableStatementNode parseDropTableStatement() {
        consume(TokenType.TABLE, "'TABLE' keyword after 'DROP'");
        IdentifierNode tableName = new IdentifierNode(consume(TokenType.IDENTIFIER, "table name").lexeme());
        return new DropTableStatementNode(tableName);
    }


    private CreateTableStatementNode parseCreateTableStatement() {
        consume(TokenType.TABLE, "'TABLE' keyword after 'CREATE'");
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

    private CreateIndexStatementNode parseCreateIndexStatement() {
        consume(TokenType.INDEX, "'INDEX' keyword after 'CREATE'");
        IdentifierNode indexName = new IdentifierNode(consume(TokenType.IDENTIFIER, "index name").lexeme());
        consume(TokenType.ON, "'ON' keyword after index name");
        IdentifierNode tableName = new IdentifierNode(consume(TokenType.IDENTIFIER, "table name").lexeme());
        consume(TokenType.LPAREN, "'(' before column name");
        List<IdentifierNode> columns = new ArrayList<>();
        columns.add(new IdentifierNode(consume(TokenType.IDENTIFIER, "column name").lexeme()));
        while (match(TokenType.COMMA)) {
            columns.add(new IdentifierNode(consume(TokenType.IDENTIFIER, "column name").lexeme()));
        }
        consume(TokenType.RPAREN, "')' after column name(s)");
        return new CreateIndexStatementNode(indexName, tableName, columns);
    }


    private ColumnDefinitionNode parseColumnDefinition() {
        Token columnNameToken = consume(TokenType.IDENTIFIER, "column name");
        IdentifierNode columnName = new IdentifierNode(columnNameToken.lexeme());
        Token dataTypeToken = peek();
        if (dataTypeToken.type() == TokenType.INT ||
                dataTypeToken.type() == TokenType.VARCHAR ||
                dataTypeToken.type() == TokenType.DECIMAL ||
                dataTypeToken.type() == TokenType.DATE ||
                dataTypeToken.type() == TokenType.BOOLEAN ||
                dataTypeToken.type() == TokenType.IDENTIFIER) {
            advance();
        } else {
            throw new ParseException(peek(), "a valid data type (e.g., INT, VARCHAR, DECIMAL, etc.)");
        }
        IdentifierNode dataType = new IdentifierNode(dataTypeToken.lexeme());
        return new ColumnDefinitionNode(columnName, dataType);
    }

    private SelectStatementNode parseSelectStatement() {
        List<ExpressionNode> selectList = new ArrayList<>();
        boolean isSelectAll = false;
        if (match(TokenType.ASTERISK)) {
            isSelectAll = true;
        } else {
            if (!check(TokenType.FROM)) {
                do {
                    selectList.add(parseExpression());
                } while (match(TokenType.COMMA));
            }
        }
        consume(TokenType.FROM, "'FROM' keyword");

        IdentifierNode fromTable = parseTableName();

        IdentifierNode joinTable = null;
        ExpressionNode joinCondition = null;
        if (match(TokenType.JOIN)) {
            joinTable = parseTableName();
            consume(TokenType.ON, "'ON' keyword after JOIN table");
            joinCondition = parseExpression();
        }

        ExpressionNode whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }
        List<IdentifierNode> groupByClause = null;
        if (match(TokenType.GROUP)) {
            consume(TokenType.BY, "'BY' after 'GROUP'");
            groupByClause = new ArrayList<>();
            do {
                ExpressionNode groupByExpr = parsePrimaryExpression();
                if (!(groupByExpr instanceof IdentifierNode)) {
                    throw new ParseException(peek(), "GROUP BY clause only supports column identifiers.");
                }
                groupByClause.add((IdentifierNode) groupByExpr);
            } while (match(TokenType.COMMA));
        }
        OrderByClauseNode orderByClause = null;
        if (match(TokenType.ORDER)) {
            orderByClause = parseOrderByClause();
        }
        LimitClauseNode limitClause = null;
        if (match(TokenType.LIMIT)) {
            limitClause = parseLimitClause();
        }
        if (peek().type() == TokenType.COMMA) {
            match(TokenType.COMMA);
            consume(TokenType.INTEGER_CONST, "integer value for LIMIT count");
        }

        return new SelectStatementNode(selectList, fromTable, joinTable, joinCondition,whereClause, isSelectAll, groupByClause, orderByClause, limitClause);
    }

    private IdentifierNode parseTableName() {
        Token firstPart = consume(TokenType.IDENTIFIER, "database or table name");
        if (match(TokenType.DOT)) {
            Token tableNameToken = consume(TokenType.IDENTIFIER, "table name after '.'");
            return new IdentifierNode(tableNameToken.lexeme());
        } else {
            return new IdentifierNode(firstPart.lexeme());
        }
    }

    private InsertStatementNode parseInsertStatement() {
        consume(TokenType.INTO, "'INTO' keyword after 'INSERT'");
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

    private DeleteStatementNode parseDeleteStatement() {
        consume(TokenType.FROM, "'FROM' keyword after 'DELETE'");
        Token tableNameToken = consume(TokenType.IDENTIFIER, "table name");
        IdentifierNode tableName = new IdentifierNode(tableNameToken.lexeme());
        ExpressionNode whereClause = null;
        if (match(TokenType.WHERE)) {
            whereClause = parseExpression();
        }
        return new DeleteStatementNode(tableName, whereClause);
    }

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

    private OrderByClauseNode parseOrderByClause() {
        consume(TokenType.BY, "'BY' after 'ORDER'");
        ExpressionNode columnExpr = parsePrimaryExpression();
        if (!(columnExpr instanceof IdentifierNode)) {
            throw new ParseException(peek(), "Expected a column identifier for ORDER BY clause.");
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
            throw new ParseException(peek(), "Invalid number for LIMIT: " + limitToken.lexeme());
        }
    }

    private ExpressionNode parseExpression() {
        return parseOrExpression();
    }

    private ExpressionNode parseOrExpression() {
        ExpressionNode left = parseAndExpression();
        while (match(TokenType.OR)) {
            Token operator = previous();
            ExpressionNode right = parseAndExpression();
            left = new BinaryExpressionNode(left, operator, right);
        }
        return left;
    }

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

    private ExpressionNode parsePrimaryExpression() {
        if (match(TokenType.INTEGER_CONST, TokenType.DECIMAL_CONST, TokenType.STRING_CONST, TokenType.TRUE, TokenType.FALSE)) {
            return new LiteralNode(previous());
        }
        if (AGGREGATE_FUNCTIONS.contains(peek().type())) {
            return parseAggregateExpression();
        }
        if (check(TokenType.IDENTIFIER)) {
            Token firstIdentifier = advance();
            if (match(TokenType.DOT)) {
                Token secondIdentifier = consume(TokenType.IDENTIFIER, "column name after '.'");
                return new IdentifierNode(firstIdentifier.lexeme(), secondIdentifier.lexeme());
            }
            return new IdentifierNode(firstIdentifier.lexeme());
        }
        if (match(TokenType.LPAREN)) {
            ExpressionNode expr = parseExpression();
            consume(TokenType.RPAREN, "')' after expression.");
            return expr;
        }
        throw new ParseException(peek(), "an expression (a literal, an identifier, or an aggregate function)");
    }

    private AggregateExpressionNode parseAggregateExpression() {
        Token functionToken = advance();
        String functionName = functionToken.lexeme().toUpperCase();
        consume(TokenType.LPAREN, "'(' after aggregate function name");
        boolean isStar = false;
        ExpressionNode argument = null;
        if (match(TokenType.ASTERISK)) {
            isStar = true;
        } else {
            argument = parsePrimaryExpression();
        }
        consume(TokenType.RPAREN, "')' after aggregate function argument");
        return new AggregateExpressionNode(functionName, argument, isStar);
    }

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