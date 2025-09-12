package org.csu.sdolp.catalog;
import java.io.Serializable;

/**
 * 封装索引的元数据。
 */
public class IndexInfo implements Serializable {
    private final String indexName;
    private final String tableName;
    private final String columnName;
    private int rootPageId; // B+树的根页面ID

    public IndexInfo(String indexName, String tableName, String columnName, int rootPageId) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.rootPageId = rootPageId;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getRootPageId() {
        return rootPageId;
    }
    public void setRootPageId(int rootPageId) {
        this.rootPageId = rootPageId;
    }
}