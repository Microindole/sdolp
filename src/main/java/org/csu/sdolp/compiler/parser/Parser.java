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

        // --- 新代码 ---
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
        // 在此可以扩展支持 INSERT, DELETE 等语句
        throw new ParseException(peek(), "a valid statement (CREATE, SELECT, etc.)");
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

    /**
     * 解析 SELECT 语句
     * 语法: SELECT col1, col2, ... FROM table_name;
     */
    private SelectStatementNode parseSelectStatement() {
        List<ExpressionNode> selectList = new ArrayList<>();
        if (!check(TokenType.FROM)) {
            do {
                Token columnToken = consume(TokenType.IDENTIFIER, "column name or '*' in select list");
                selectList.add(new IdentifierNode(columnToken.lexeme()));
            } while (match(TokenType.COMMA));
        }

        consume(TokenType.FROM, "'FROM' keyword");

        Token tableNameToken = consume(TokenType.IDENTIFIER, "table name");
        IdentifierNode fromTable = new IdentifierNode(tableNameToken.lexeme());

        return new SelectStatementNode(selectList, fromTable);
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
