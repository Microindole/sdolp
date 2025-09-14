package org.csu.sdolp.engine;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.parser.ast.expression.BinaryExpressionNode;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.expression.LiteralNode;

/**
 * 表达式求值器。
 * 负责计算 WHERE 子句中的表达式对于给定的元组是否为真。
 */
public class ExpressionEvaluator {

    /**
     * 对给定的元组计算表达式的值。
     * @param expression 表达式的AST节点 (e.g., id = 1)
     * @param schema     元组对应的模式
     * @param tuple      需要被判断的元组
     * @return 表达式计算结果为 true 或 false
     */
    public static boolean evaluate(ExpressionNode expression, Schema schema, Tuple tuple) {
        if (expression instanceof BinaryExpressionNode node) {
            if (node.left() instanceof IdentifierNode leftCol && node.right() instanceof IdentifierNode rightCol) {
                Value leftValue = getColumnValue(tuple, schema, leftCol);
                Value rightValue = getColumnValue(tuple, schema, rightCol);
                return compareValues(leftValue, rightValue, node.operator().type().name());
            }

            if (node.left() instanceof IdentifierNode columnNode && node.right() instanceof LiteralNode literalNode) {
                Value tupleValue = getColumnValue(tuple, schema, columnNode);
                Value literalValue = getLiteralValue(literalNode);
                return compareValues(tupleValue, literalValue, node.operator().type().name());
            }

            throw new UnsupportedOperationException("Expression format not supported. Must be 'column op literal' or 'column1 op column2'.");
        }
        throw new UnsupportedOperationException("Unsupported expression type in WHERE or ON clause.");
    }

    private static Value getColumnValue(Tuple tuple, Schema schema, IdentifierNode columnNode) {
        String columnName = columnNode.getName();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (schema.getColumns().get(i).getName().equalsIgnoreCase(columnName)) {
                return tuple.getValues().get(i);
            }
        }
        throw new IllegalStateException("Column '" + columnName + "' not found in tuple schema. This should have been caught during semantic analysis.");
    }
    private static Value getLiteralValue(LiteralNode literalNode) {
        String lexeme = literalNode.literal().lexeme();
        return switch (literalNode.literal().type()) {
            case INTEGER_CONST -> new Value(Integer.parseInt(lexeme));
            case STRING_CONST -> new Value(lexeme);
            default -> throw new IllegalStateException("Unsupported literal type in expression.");
        };
    }
    private static boolean compareValues(Value val1, Value val2, String operator) {
        if (val1 == null || val1.getValue() == null || val2 == null || val2.getValue() == null) {
            // SQL 中，任何与 NULL 的比较结果都是 UNKNOWN，在这里我们将其视为 false
            return false;
        }
        if (val1.getType() != val2.getType()) {
            return false;
        }

        Comparable v1 = (Comparable) val1.getValue();
        Comparable v2 = (Comparable) val2.getValue();
        int cmp = v1.compareTo(v2);

        return switch (operator) {
            case "EQUAL" -> cmp == 0;
            case "NOT_EQUAL" -> cmp != 0;
            case "GREATER" -> cmp > 0;
            case "GREATER_EQUAL" -> cmp >= 0;
            case "LESS" -> cmp < 0;
            case "LESS_EQUAL" -> cmp <= 0;
            default -> throw new UnsupportedOperationException("Unsupported operator: " + operator);
        };
    }
}