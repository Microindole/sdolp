package org.csu.sdolp.compiler.planner.plan.dml;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.compiler.planner.plan.PlanNode;

import java.util.List;

/**
 * @author hidyouth
 * @description: 插入操作的执行计划节点
 */
public class InsertPlanNode extends PlanNode {
    private final TableInfo tableInfo;
    private final List<Tuple> rawTuples;

    public InsertPlanNode(TableInfo tableInfo, List<Tuple> rawTuples) {
        super(null); // INSERT 不向上层返回元组，所以输出 Schema 为 null
        this.tableInfo = tableInfo;
        this.rawTuples = rawTuples;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public List<Tuple> getRawTuples() {
        return rawTuples;
    }
}
