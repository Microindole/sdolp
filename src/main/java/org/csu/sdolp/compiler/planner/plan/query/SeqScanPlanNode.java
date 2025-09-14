package org.csu.sdolp.compiler.planner.plan.query;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.compiler.planner.plan.PlanNode;

/**
 * @author hidyouth
 * @description: 顺序扫描执行计划节点
 */
public class SeqScanPlanNode extends PlanNode {
    private final TableInfo tableInfo;

    public SeqScanPlanNode(TableInfo tableInfo) {
        super(tableInfo.getSchema()); // 顺序扫描输出的是整张表的原始 Schema
        this.tableInfo = tableInfo;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }
}
