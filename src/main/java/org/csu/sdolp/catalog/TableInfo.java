package org.csu.sdolp.catalog;

import lombok.Getter;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.storage.page.PageId;

/**
 * 在内存中表示一个表的信息。
 */
@Getter
public class TableInfo {
    private final String tableName;
    private final Schema schema;
    private final PageId firstPageId;

    public TableInfo(String tableName, Schema schema, PageId firstPageId) {
        this.tableName = tableName;
        this.schema = schema;
        this.firstPageId = firstPageId;
    }

}