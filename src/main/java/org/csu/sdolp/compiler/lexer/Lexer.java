package org.csu.sdolp.compiler.lexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hidyouth
 * @description: 词法分析器 (Lexer/Scanner)
 *
 * 负责将输入的SQL字符串分解为一系列的Token。
 */
public class Lexer {

    private final String input;
    private int position = 0; // 当前读取的位置
    private int line = 1;     // 当前行号
    private int column = 1;   // 当前列号

    // 关键字映射表
    private static final Map<String, TokenType> keywords;

    static {
        keywords = new HashMap<>();
        keywords.put("select", TokenType.SELECT);
        keywords.put("from", TokenType.FROM);
        keywords.put("where", TokenType.WHERE);
        keywords.put("create", TokenType.CREATE);
        keywords.put("table", TokenType.TABLE);
        keywords.put("database", TokenType.DATABASE);
        keywords.put("databases", TokenType.DATABASES);
        keywords.put("index", TokenType.INDEX);
        keywords.put("insert", TokenType.INSERT);
        keywords.put("into", TokenType.INTO);
        keywords.put("values", TokenType.VALUES);
        keywords.put("use", TokenType.USE);
        keywords.put("delete", TokenType.DELETE);
        keywords.put("update", TokenType.UPDATE);
        keywords.put("set", TokenType.SET);
        keywords.put("int", TokenType.INT);
        keywords.put("varchar", TokenType.VARCHAR);
        keywords.put("decimal", TokenType.DECIMAL);
        keywords.put("date", TokenType.DATE);
        keywords.put("boolean", TokenType.BOOLEAN);
        keywords.put("true", TokenType.TRUE);
        keywords.put("false", TokenType.FALSE);
        keywords.put("order", TokenType.ORDER);
        keywords.put("by", TokenType.BY);
        keywords.put("asc", TokenType.ASC);
        keywords.put("desc", TokenType.DESC);
        keywords.put("limit", TokenType.LIMIT);
        keywords.put("and", TokenType.AND);
        keywords.put("or", TokenType.OR);
        keywords.put("join", TokenType.JOIN);
        keywords.put("on", TokenType.ON);
        keywords.put("drop", TokenType.DROP);
        keywords.put("alter", TokenType.ALTER);
        keywords.put("add", TokenType.ADD);
        keywords.put("column", TokenType.COLUMN);
        keywords.put("columns", TokenType.COLUMNS);
        keywords.put("group", TokenType.GROUP);
        keywords.put("count", TokenType.COUNT);
        keywords.put("sum", TokenType.SUM);
        keywords.put("avg", TokenType.AVG);
        keywords.put("min", TokenType.MIN);
        keywords.put("max", TokenType.MAX);
        keywords.put("show", TokenType.SHOW); // show
        keywords.put("tables", TokenType.TABLES); // tables
        keywords.put("full", TokenType.FULL);
        keywords.put("user", TokenType.USER);
        keywords.put("identified", TokenType.IDENTIFIED);
        keywords.put("grant", TokenType.GRANT);
        keywords.put("to", TokenType.TO);
    }

    public Lexer(String input) {
        this.input = input;
    }

    /**
     * 主方法，执行词法分析并返回所有Token
     * @return Token列表
     */
    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token token;
        do {
            token = nextToken();
            tokens.add(token);
        } while (token.type() != TokenType.EOF);
        return tokens;
    }

    /**
     * 获取下一个Token
     * @return 解析出的下一个Token
     */
    private Token nextToken() {
        skipWhitespace();

        if (position >= input.length()) {
            return new Token(TokenType.EOF, "", line, column);
        }

        char currentChar = peek();

        // 对反引号 (`) 的处理
        if (currentChar == '`') {
            return readQuotedIdentifier();
        }

        // 识别标识符或关键字
        if (isLetter(currentChar)) {
            return readIdentifierOrKeyword();
        }

        // 识别数字
        if (isDigit(currentChar)) {
            return readNumber();
        }

        // 识别字符串
        if (currentChar == '\'') {
            return readString();
        }


        // 识别运算符和分隔符
        switch (currentChar) {
            case '=':
                return consumeAndReturn(TokenType.EQUAL, "=");
            case ',':
                return consumeAndReturn(TokenType.COMMA, ",");
            case ';':
                return consumeAndReturn(TokenType.SEMICOLON, ";");
            case '(':
                return consumeAndReturn(TokenType.LPAREN, "(");
            case ')':
                return consumeAndReturn(TokenType.RPAREN, ")");
            case '*':
                return consumeAndReturn(TokenType.ASTERISK, "*");
            case '.': return consumeAndReturn(TokenType.DOT, ".");
            case '>':
                if (peekNext() == '=') {
                    advance(); // consume '='
                    return consumeAndReturn(TokenType.GREATER_EQUAL, ">=");
                }
                return consumeAndReturn(TokenType.GREATER, ">");
            case '<':
                if (peekNext() == '>') {
                    advance(); // consume '>'
                    return consumeAndReturn(TokenType.NOT_EQUAL, "<>");
                }
                if (peekNext() == '=') {
                    advance(); // consume '='
                    return consumeAndReturn(TokenType.LESS_EQUAL, "<=");
                }
                return consumeAndReturn(TokenType.LESS, "<");
            case '!':
                if (peekNext() == '=') {
                    advance(); // consume '='
                    return consumeAndReturn(TokenType.NOT_EQUAL, "!=");
                }
                return consumeAndReturn(TokenType.ILLEGAL, String.valueOf(currentChar));
            default:
                return consumeAndReturn(TokenType.ILLEGAL, String.valueOf(currentChar));
        }
    }

    private Token readQuotedIdentifier() {
        int startCol = column;
        advance(); // 跳过起始的反引号
        int startPos = position;
        while (position < input.length() && peek() != '`') {
            advance();
        }
        String text = input.substring(startPos, position);
        if (position >= input.length() || peek() != '`') {
            // 如果没有找到结束的反引号，这是一个语法错误
            return new Token(TokenType.ILLEGAL, text, line, startCol);
        }
        advance(); // 跳过结束的反引号
        // 被反引号包裹的内容强制视为标识符，即使它是关键字
        return new Token(TokenType.IDENTIFIER, text, line, startCol);
    }

    private Token readIdentifierOrKeyword() {
        int startPos = position;
        int startCol = column;
        while (position < input.length() && isLetterOrDigit(peek())) {
            advance();
        }
        String text = input.substring(startPos, position);
        // 检查是否是关键字，忽略大小写
        TokenType type = keywords.getOrDefault(text.toLowerCase(), TokenType.IDENTIFIER);
        return new Token(type, text, line, startCol);
    }

    private Token readNumber() {
        int startPos = position;
        int startCol = column;
        while (position < input.length() && isDigit(peek())) {
            advance();
        }

        // --- 核心修改：检查并处理小数点 ---
        if (position < input.length() && peek() == '.') {
            // 确认小数点后面还有数字，以区分 `table.column` 语法
            if (isDigit(peekNext())) {
                advance(); // 消耗掉 '.'
                while (position < input.length() && isDigit(peek())) {
                    advance();
                }
                // 如果包含小数点，则识别为 DECIMAL_CONST
                String number = input.substring(startPos, position);
                return new Token(TokenType.DECIMAL_CONST, number, line, startCol);
            }
        }
        String number = input.substring(startPos, position);
        return new Token(TokenType.INTEGER_CONST, number, line, startCol);
    }

    private Token readString() {
        int startCol = column;
        advance(); // 跳过起始的单引号
        int startPos = position;
        while (position < input.length() && peek() != '\'') {
            advance();
        }
        String text = input.substring(startPos, position);
        if (position >= input.length() || peek() != '\'') {
            return new Token(TokenType.ILLEGAL, text, line, startCol); // 未闭合的字符串
        }
        advance(); // 跳过结束的单引号
        return new Token(TokenType.STRING_CONST, text, line, startCol);
    }

    // --- 辅助方法 ---

    private void skipWhitespace() {
        while (position < input.length()) {
            char ch = peek();
            if (ch == ' ' || ch == '\t' || ch == '\r') {
                advance();
            } else if (ch == '\n') {
                line++;
                column = 0; // advance会加1，所以这里设为0
                advance();
            } else {
                break;
            }
        }
    }

    private char peek() {
        if (position >= input.length()) return '\0'; // 文件结束符
        return input.charAt(position);
    }

    private char peekNext() {
        if (position + 1 >= input.length()) return '\0';
        return input.charAt(position + 1);
    }

    private void advance() {
        position++;
        column++;
    }

    private Token consumeAndReturn(TokenType type, String lexeme) {
        Token token = new Token(type, lexeme, line, column);
        advance();
        return token;
    }

    private boolean isLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    private boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private boolean isLetterOrDigit(char c) {
        return isLetter(c) || isDigit(c);
    }
}