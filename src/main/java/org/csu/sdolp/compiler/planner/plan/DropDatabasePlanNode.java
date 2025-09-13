package org.csu.sdolp.compiler.planner.plan;

public class DropDatabasePlanNode extends PlanNode {
    private final String dbName;

    public DropDatabasePlanNode(String dbName) {
        super(null); // DDL operations don't return tuples.
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }
}