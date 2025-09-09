package org.csu.sdolp.catalog;

import org.csu.sdolp.common.model.*;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 系统目录，负责管理数据库的所有元数据（表、列等）。
 * 目录本身也作为特殊的表存储在磁盘上。
 */
public class Catalog {
    private final BufferPoolManager bufferPoolManager;
    // 内存中的缓存
    private final Map<String, TableInfo> tables;
    private final Map<String, Integer> tableIds;
    private final AtomicInteger nextTableId;

    // --- 元数据表的特殊定义 ---
    // 存储所有表的信息 (table_id, table_name, first_page_id)
    public static final String CATALOG_TABLES_TABLE_NAME = "_catalog_tables";
    private final Schema tablesTableSchema;
    private PageId tablesTableFirstPageId;

    // 存储所有列的信息 (table_id, column_name, column_type, column_index)
    public static final String CATALOG_COLUMNS_TABLE_NAME = "_catalog_columns";
    private final Schema columnsTableSchema;
    private PageId columnsTableFirstPageId;


    public Catalog(BufferPoolManager bufferPoolManager) throws IOException {
        this.bufferPoolManager = bufferPoolManager;
        this.tables = new ConcurrentHashMap<>();
        this.tableIds = new ConcurrentHashMap<>();
        this.nextTableId = new AtomicInteger(0);

        // 定义元数据表的 Schema
        this.tablesTableSchema = new Schema(Arrays.asList(
                new Column("table_id", DataType.INT),
                new Column("table_name", DataType.VARCHAR),
                new Column("first_page_id", DataType.INT)
        ));
        this.columnsTableSchema = new Schema(Arrays.asList(
                new Column("table_id", DataType.INT),
                new Column("column_name", DataType.VARCHAR),
                new Column("column_type", DataType.VARCHAR), // DataType.toString()
                new Column("column_index", DataType.INT)
        ));

        // 加载或初始化目录
        loadCatalog();
    }

    private void loadCatalog() throws IOException {
        // Page 0 固定为存储 Catalog Tables Table 的第一页
        tablesTableFirstPageId = new PageId(0);
        Page tablesPage = bufferPoolManager.getPage(tablesTableFirstPageId);

        // 通过第一条记录判断数据库是否为全新创建
        if (tablesPage.getAllTuples(tablesTableSchema).isEmpty()) {
            // 是一个全新的数据库，需要自举（Bootstrap）
            bootstrap();
        } else {
            // 加载现有元数据
            int maxTableId = -1;

            // 1. 加载所有表
            List<Tuple> tableMetaTuples = tablesPage.getAllTuples(tablesTableSchema);
            for (Tuple tuple : tableMetaTuples) {
                int tableId = (int) tuple.getValues().get(0).getValue();
                String tableName = (String) tuple.getValues().get(1).getValue();
                int firstPageId = (int) tuple.getValues().get(2).getValue();

                if (tableId > maxTableId) {
                    maxTableId = tableId;
                }
                tableIds.put(tableName, tableId);
                if (tableName.equals(CATALOG_COLUMNS_TABLE_NAME)) {
                    columnsTableFirstPageId = new PageId(firstPageId);
                }
            }
            nextTableId.set(maxTableId + 1);

            // 2. 加载所有列并重建 Schema
            Page columnsPage = bufferPoolManager.getPage(columnsTableFirstPageId);
            List<Tuple> columnMetaTuples = columnsPage.getAllTuples(columnsTableSchema);

            // 重新构建内存中的 TableInfo
            for (String tableName : tableIds.keySet()) {
                int currentTableId = tableIds.get(tableName);
                List<Column> columns = new ArrayList<>();
                for (Tuple colTuple : columnMetaTuples) {
                    if ((int) colTuple.getValues().get(0).getValue() == currentTableId) {
                        String colName = (String) colTuple.getValues().get(1).getValue();
                        DataType colType = DataType.valueOf((String) colTuple.getValues().get(2).getValue());
                        columns.add(new Column(colName, colType));
                    }
                }

                Schema schema = new Schema(columns);
                int firstPageId = (int) getTableTuple(tableName).getValues().get(2).getValue();
                tables.put(tableName, new TableInfo(tableName, schema, new PageId(firstPageId)));
            }
        }
    }

    private void bootstrap() throws IOException {
        bufferPoolManager.newPage();

        columnsTableFirstPageId = bufferPoolManager.newPage().getPageId();
        if (columnsTableFirstPageId.getPageNum() != 1) {
            throw new IllegalStateException("Catalog bootstrap failed: _catalog_columns table was not allocated to Page 1.");
        }
        int tablesTableId = nextTableId.getAndIncrement();
        int columnsTableId = nextTableId.getAndIncrement();

        // 3. 将元数据表的元数据写入 Catalog Tables Table (Page 0)
        Tuple tablesTableMeta = new Tuple(Arrays.asList(
                new Value(tablesTableId), new Value(CATALOG_TABLES_TABLE_NAME), new Value(tablesTableFirstPageId.getPageNum())
        ));
        Tuple columnsTableMeta = new Tuple(Arrays.asList(
                new Value(columnsTableId), new Value(CATALOG_COLUMNS_TABLE_NAME), new Value(columnsTableFirstPageId.getPageNum())
        ));

        Page tablesPage = bufferPoolManager.getPage(tablesTableFirstPageId);
        tablesPage.insertTuple(tablesTableMeta);
        tablesPage.insertTuple(columnsTableMeta);
        bufferPoolManager.flushPage(tablesTableFirstPageId);

        // 4. 将两个元数据表的 Schema 信息写入 Catalog Columns Table
        Page columnsPage = bufferPoolManager.getPage(columnsTableFirstPageId);

        int colIdx = 0;
        for (Column col : tablesTableSchema.getColumns()) {
            columnsPage.insertTuple(new Tuple(Arrays.asList(new Value(tablesTableId), new Value(col.getName()), new Value(col.getType().toString()), new Value(colIdx++))));
        }

        colIdx = 0;
        for (Column col : columnsTableSchema.getColumns()) {
            columnsPage.insertTuple(new Tuple(Arrays.asList(new Value(columnsTableId), new Value(col.getName()), new Value(col.getType().toString()), new Value(colIdx++))));
        }
        bufferPoolManager.flushPage(columnsTableFirstPageId);

        // 5. 将元数据表信息加载到内存
        tables.put(CATALOG_TABLES_TABLE_NAME, new TableInfo(CATALOG_TABLES_TABLE_NAME, tablesTableSchema, tablesTableFirstPageId));
        tables.put(CATALOG_COLUMNS_TABLE_NAME, new TableInfo(CATALOG_COLUMNS_TABLE_NAME, columnsTableSchema, columnsTableFirstPageId));
        tableIds.put(CATALOG_TABLES_TABLE_NAME, tablesTableId);
        tableIds.put(CATALOG_COLUMNS_TABLE_NAME, columnsTableId);
    }

    public TableInfo createTable(String tableName, Schema schema) throws IOException {
        if (tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " already exists.");
        }
        // 1. 为新表分配一个数据页
        PageId firstPageId = bufferPoolManager.newPage().getPageId();

        // 2. 将新表的元数据持久化到目录表中
        int newTableId = nextTableId.getAndIncrement();
        // 2.1 写入 _catalog_tables
        Tuple tableMeta = new Tuple(Arrays.asList(
                new Value(newTableId), new Value(tableName), new Value(firstPageId.getPageNum())
        ));
        Page tablesPage = bufferPoolManager.getPage(tablesTableFirstPageId);
        tablesPage.insertTuple(tableMeta);
        bufferPoolManager.flushPage(tablesTableFirstPageId);

        // 2.2 写入 _catalog_columns
        Page columnsPage = bufferPoolManager.getPage(columnsTableFirstPageId);
        int colIdx = 0;
        for (Column col : schema.getColumns()) {
            columnsPage.insertTuple(new Tuple(Arrays.asList(
                    new Value(newTableId), new Value(col.getName()), new Value(col.getType().toString()), new Value(colIdx++)
            )));
        }
        bufferPoolManager.flushPage(columnsTableFirstPageId);

        // 3. 更新内存缓存
        TableInfo tableInfo = new TableInfo(tableName, schema, firstPageId);
        tables.put(tableName, tableInfo);
        tableIds.put(tableName, newTableId);

        return tableInfo;
    }

    public TableInfo getTable(String tableName) {
        return tables.get(tableName);
    }

    // 辅助函数，用于从磁盘读取指定表的元组
    private Tuple getTableTuple(String tableName) throws IOException {
        int tableId = tableIds.get(tableName);
        Page page = bufferPoolManager.getPage(tablesTableFirstPageId);
        List<Tuple> tuples = page.getAllTuples(tablesTableSchema);
        for(Tuple t : tuples){
            if((int) t.getValues().get(0).getValue() == tableId){
                return t;
            }
        }
        return null;
    }
}