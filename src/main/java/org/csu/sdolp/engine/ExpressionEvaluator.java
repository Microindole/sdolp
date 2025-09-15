package org.csu.sdolp.engine;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.parser.ast.expression.BinaryExpressionNode;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.AggregateExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.expression.LiteralNode;

/**
 * 表达式求值器。
 * 负责计算 WHERE 子句和 UPDATE SET 子句中的表达式。
 */
public class ExpressionEvaluator {

    // --- 这个方法用于 WHERE/HAVING 子句，返回布尔值，保持不变 ---
    public static boolean evaluate(ExpressionNode expression, Schema schema, Tuple tuple, TableInfo leftTable, TableInfo rightTable) {
        if (expression instanceof BinaryExpressionNode node) {
            // ... (你现有的比较逻辑保持不变)
            if (node.left() instanceof AggregateExpressionNode leftAgg && node.right() instanceof LiteralNode rightLiteral) {
                String aggColumnName = leftAgg.toString();
                int colIndex = schema.getColumnIndex(aggColumnName);
                if (colIndex == -1) throw new IllegalStateException("Aggregate column '" + aggColumnName + "' not found in post-aggregation schema.");
                Value leftValue = tuple.getValues().get(colIndex);
                Value rightValue = getLiteralValue(rightLiteral);
                return compareValues(leftValue, rightValue, node.operator().type().name());
            } else if (node.left() instanceof LiteralNode leftLiteral && node.right() instanceof AggregateExpressionNode rightAgg) {
                String aggColumnName = rightAgg.toString();
                int colIndex = schema.getColumnIndex(aggColumnName);
                if (colIndex == -1) throw new IllegalStateException("Aggregate column '" + aggColumnName + "' not found in post-aggregation schema.");
                Value leftValue = getLiteralValue(leftLiteral);
                Value rightValue = tuple.getValues().get(colIndex);
                return compareValues(leftValue, rightValue, node.operator().type().name());
            }

            Value leftValue = getValue(node.left(), schema, tuple, leftTable, rightTable);
            Value rightValue = getValue(node.right(), schema, tuple, leftTable, rightTable);

            return compareValues(leftValue, rightValue, node.operator().type().name());
        }
        throw new UnsupportedOperationException("Unsupported expression type in WHERE or HAVING clause: " + expression.getClass().getSimpleName());
    }

    // --- 新增的核心方法：用于计算表达式的值，例如在 UPDATE SET 中 ---
    public static Value evaluateToValue(ExpressionNode expression, Schema schema, Tuple tuple) {
        if (expression instanceof LiteralNode literalNode) {
            return getLiteralValue(literalNode);
        }
        if (expression instanceof IdentifierNode idNode) {
            int colIndex = schema.getColumnIndex(idNode.getName());
            return tuple.getValues().get(colIndex);
        }
        if (expression instanceof BinaryExpressionNode node) {
            Value leftValue = evaluateToValue(node.left(), schema, tuple);
            Value rightValue = evaluateToValue(node.right(), schema, tuple);

            // 目前只为整数类型实现加减法
            // TODO: 未来可以扩展以支持更多类型 (FLOAT, DECIMAL) 和更多操作 (*, /)
            if (leftValue.getValue() instanceof Number && rightValue.getValue() instanceof Number) {
                int leftInt = ((Number) leftValue.getValue()).intValue();
                int rightInt = ((Number) rightValue.getValue()).intValue();

                return switch (node.operator().type()) {
                    case PLUS -> new Value(leftInt + rightInt);
                    case MINUS -> new Value(leftInt - rightInt);
                    default -> throw new UnsupportedOperationException("Unsupported arithmetic operator in expression: " + node.operator().type());
                };
            }
            throw new UnsupportedOperationException("Arithmetic operations only supported for numeric types.");
        }
        throw new UnsupportedOperationException("Unsupported expression type for value evaluation: " + expression.getClass().getSimpleName());
    }

    // --- 其他所有辅助方法保持不变 ---
    // ... (此处省略 compareValues, getValue, getLiteralValue 等你已有的方法)
    public static boolean evaluate(ExpressionNode expression, Schema schema, Tuple tuple) {
        // 调用主方法，但左右表上下文为null
        return evaluate(expression, schema, tuple, null, null);
    }
    // 此处修改：新增的辅助方法，用于从任何类型的表达式节点中获取值
    private static Value getValue(ExpressionNode node, Schema schema, Tuple tuple, TableInfo leftTable, TableInfo rightTable) {
        if (node instanceof IdentifierNode idNode) {
            int colIndex = getColumnIndex(schema, idNode, leftTable, rightTable);
            return tuple.getValues().get(colIndex);
        }
        if (node instanceof LiteralNode literalNode) {
            return getLiteralValue(literalNode);
        }
        throw new IllegalStateException("Unsupported node type in expression: " + node.getClass().getSimpleName());
    }
    // 此处修改：修复后的、能感知表限定符的列索引查找方法
    private static int getColumnIndex(Schema combinedSchema, IdentifierNode columnNode, TableInfo leftTable, TableInfo rightTable) {
        String columnName = columnNode.getName();
        String tableQualifier = columnNode.getTableQualifier();

        // 场景 1: 单表上下文 (例如 WHERE 子句)，此时 leftTable 和 rightTable 为 null
        if (leftTable == null || rightTable == null) {
            return combinedSchema.getColumnIndex(columnName);
        }

        // 场景 2: JOIN 上下文 (例如 ON 子句)，必须处理表限定符
        int leftSchemaSize = leftTable.getSchema().getColumns().size();

        if (tableQualifier != null) {
            if (tableQualifier.equalsIgnoreCase(leftTable.getTableName())) {
                return leftTable.getSchema().getColumnIndex(columnName);
            } else if (tableQualifier.equalsIgnoreCase(rightTable.getTableName())) {
                // 关键点：为右表的列索引加上左表的大小作为偏移量
                return leftSchemaSize + rightTable.getSchema().getColumnIndex(columnName);
            }
        }
        return combinedSchema.getColumnIndex(columnName);
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
            // 增加对DECIMAL_CONST的支持，以处理浮点数字面量
            case DECIMAL_CONST -> new Value(Double.parseDouble(lexeme));
            default -> throw new IllegalStateException("Unsupported literal type in expression.");
        };
    }
    private static boolean compareValues(Value val1, Value val2, String operator) {
        if (val1 == null || val1.getValue() == null || val2 == null || val2.getValue() == null) {
            return false;
        }
        if (val1.getValue() instanceof Number && val2.getValue() instanceof Number) {
            double v1 = ((Number) val1.getValue()).doubleValue();
            double v2 = ((Number) val2.getValue()).doubleValue();
            return switch (operator) {
                case "EQUAL" -> v1 == v2;
                case "NOT_EQUAL" -> v1 != v2;
                case "GREATER" -> v1 > v2;
                case "GREATER_EQUAL" -> v1 >= v2;
                case "LESS" -> v1 < v2;
                case "LESS_EQUAL" -> v1 <= v2;
                default -> false;
            };
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