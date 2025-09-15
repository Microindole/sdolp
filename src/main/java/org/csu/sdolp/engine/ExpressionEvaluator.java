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
 * 负责计算 WHERE 子句中的表达式对于给定的元组是否为真。
 */
public class ExpressionEvaluator {
    // 为单表上下文（如 WHERE 子句）提供的重载方法
    public static boolean evaluate(ExpressionNode expression, Schema schema, Tuple tuple) {
        // 调用主方法，但左右表上下文为null
        return evaluate(expression, schema, tuple, null, null);
    }
    // 此处修改：为 JOIN 提供上下文的主求值方法
    public static boolean evaluate(ExpressionNode expression, Schema schema, Tuple tuple, TableInfo leftTable, TableInfo rightTable) {
        if (expression instanceof BinaryExpressionNode node) {
            // 处理 HAVING 子句中的聚合函数
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

            // 此处修改：从表达式的左右两边获取值，传入表上下文
            Value leftValue = getValue(node.left(), schema, tuple, leftTable, rightTable);
            Value rightValue = getValue(node.right(), schema, tuple, leftTable, rightTable);

            return compareValues(leftValue, rightValue, node.operator().type().name());
        }
        throw new UnsupportedOperationException("Unsupported expression type in WHERE or HAVING clause: " + expression.getClass().getSimpleName());
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

        // 如果没有限定符，语义分析器应该已经捕获了歧义。
        // 为了健壮性，我们仍然在这里做一个简单的查找。
        return combinedSchema.getColumnIndex(columnName);
    }
//    /**
//     * 对给定的元组计算表达式的值。
//     * @param expression 表达式的AST节点 (e.g., id = 1)
//     * @param schema     元组对应的模式
//     * @param tuple      需要被判断的元组
//     * @return 表达式计算结果为 true 或 false
//     */
//    public static boolean evaluate(ExpressionNode expression, Schema schema, Tuple tuple) {
//        if (expression instanceof BinaryExpressionNode node) {
//            // 此处修改: 新增逻辑块，用于处理 HAVING 子句中的聚合函数
//            // Case 1: 对应 HAVING AVG(salary) > 70000
//            if (node.left() instanceof AggregateExpressionNode leftAgg && node.right() instanceof LiteralNode rightLiteral) {
//                // 在聚合后的Schema中，聚合函数本身（如"AVG(salary)"）就是列名
//                String aggColumnName = leftAgg.toString();
//                int colIndex = schema.getColumnIndex(aggColumnName);
//                if (colIndex == -1) {
//                    throw new IllegalStateException("Aggregate column '" + aggColumnName + "' not found in post-aggregation schema.");
//                }
//                Value leftValue = tuple.getValues().get(colIndex);
//                Value rightValue = getLiteralValue(rightLiteral);
//                return compareValues(leftValue, rightValue, node.operator().type().name());
//            }
//            // Case 2: 对应 HAVING 70000 < AVG(salary)
//            else if (node.left() instanceof LiteralNode leftLiteral && node.right() instanceof AggregateExpressionNode rightAgg) {
//                String aggColumnName = rightAgg.toString();
//                int colIndex = schema.getColumnIndex(aggColumnName);
//                if (colIndex == -1) {
//                    throw new IllegalStateException("Aggregate column '" + aggColumnName + "' not found in post-aggregation schema.");
//                }
//                Value leftValue = getLiteralValue(leftLiteral);
//                Value rightValue = tuple.getValues().get(colIndex);
//                return compareValues(leftValue, rightValue, node.operator().type().name());
//            }
//            // 此处修改结束
//            //
//            if (node.left() instanceof IdentifierNode leftCol && node.right() instanceof IdentifierNode rightCol) {
//                Value leftValue = getColumnValue(tuple, schema, leftCol);
//                Value rightValue = getColumnValue(tuple, schema, rightCol);
//                return compareValues(leftValue, rightValue, node.operator().type().name());
//            }
//
//            if (node.left() instanceof IdentifierNode columnNode && node.right() instanceof LiteralNode literalNode) {
//                Value tupleValue = getColumnValue(tuple, schema, columnNode);
//                Value literalValue = getLiteralValue(literalNode);
//                return compareValues(tupleValue, literalValue, node.operator().type().name());
//            }
//
//            throw new UnsupportedOperationException("Expression format not supported. Must be 'column op literal' or 'column1 op column2'.");
//        }
//        // 此处修改: 更新异常信息
//        throw new UnsupportedOperationException("Unsupported expression type in WHERE or HAVING clause: " + expression.getClass().getSimpleName());
//    }

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
            // SQL 中，任何与 NULL 的比较结果都是 UNKNOWN，在这里我们将其视为 false
            return false;
        }
        // 类型转换以支持更广泛的比较
        // 允许整数和浮点数等数字类型之间进行比较
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