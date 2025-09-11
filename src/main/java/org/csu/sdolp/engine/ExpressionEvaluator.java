package org.csu.sdolp.engine;

import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.parser.ast.BinaryExpressionNode;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.LiteralNode;

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
            // 目前我们只处理 "列名 op 字面量" 形式的二元表达式
            if (!(node.left() instanceof IdentifierNode) || !(node.right() instanceof LiteralNode)) {
                throw new UnsupportedOperationException("WHERE clause only supports 'column_name op literal' format.");
            }

            IdentifierNode columnNode = (IdentifierNode) node.left();
            LiteralNode literalNode = (LiteralNode) node.right();

            // 1. 从元组中获取列的实际值
            Value tupleValue = getColumnValue(tuple, schema, columnNode.getName());

            // 2. 从AST中获取字面量的值
            Value literalValue = getLiteralValue(literalNode);

            // 3. 比较两个值
            return compareValues(tupleValue, literalValue, node.operator().type().name());
        }
        throw new UnsupportedOperationException("Unsupported expression type in WHERE clause.");
    }

    private static Value getColumnValue(Tuple tuple, Schema schema, String columnName) {
        int columnIndex = -1;
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (schema.getColumns().get(i).getName().equalsIgnoreCase(columnName)) {
                columnIndex = i;
                break;
            }
        }
        if (columnIndex == -1) {
            // 这在语义分析阶段就应该被捕获，但作为防御性编程，我们还是检查一下
            throw new IllegalStateException("Column '" + columnName + "' not found in schema.");
        }
        return tuple.getValues().get(columnIndex);
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
        // 类型检查
        if (val1.getType() != val2.getType()) {
            // 允许不同数值类型比较，但此处简化，要求类型完全一致
            return false;
        }

        // 使用 Comparable 接口进行比较
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