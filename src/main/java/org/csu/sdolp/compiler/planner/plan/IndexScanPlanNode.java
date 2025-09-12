package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.catalog.IndexInfo;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Value;

public class IndexScanPlanNode extends PlanNode {
    private final TableInfo tableInfo;
    private final IndexInfo indexInfo;
    private final Value searchKey; // 要在索引中查找的键

    public IndexScanPlanNode(TableInfo tableInfo, IndexInfo indexInfo, Value searchKey) {
        super(tableInfo.getSchema());
        this.tableInfo = tableInfo;
        this.indexInfo = indexInfo;
        this.searchKey = searchKey;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public IndexInfo getIndexInfo() {
        return indexInfo;
    }

    public Value getSearchKey() {
        return searchKey;
    }
}