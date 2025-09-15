package org.csu.sdolp.common.exception;

import org.csu.sdolp.compiler.lexer.Token;

/**
 * @author hidyouth
 */
public class ParseException extends RuntimeException {

    public ParseException(String message) {
        super(message);
    }

    public ParseException(Token token, String expected) {
        super(String.format("Syntax Error at line %d, column %d: Expected %s, but found '%s' (%s)",
                token.line(),
                token.column(),
                expected,
                token.lexeme(),
                token.type()));
    }
}
