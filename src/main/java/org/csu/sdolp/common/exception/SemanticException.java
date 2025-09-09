package org.csu.sdolp.common.exception;

/**
 * @author hidyouth
 * @description: 语义分析阶段的自定义异常
 */
public class SemanticException extends RuntimeException {
    public SemanticException(String message) {
        super(message);
    }
}
