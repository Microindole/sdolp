package org.csu.sdolp.compiler.planner.plan.dcl;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.expression.LiteralNode;
import org.csu.sdolp.compiler.planner.plan.PlanNode;

/**
 * 创建用户的执行计划节点
 */
public class CreateUserPlanNode extends PlanNode {
    private final String username;
    private final String password;

    public CreateUserPlanNode(IdentifierNode username, LiteralNode password) {
        super(null); // DDL/DCL 操作不向上层返回元组
        this.username = username.getName();
        this.password = password.literal().lexeme();
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}