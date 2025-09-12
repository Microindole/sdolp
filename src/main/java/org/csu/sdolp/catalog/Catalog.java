package org.csu.sdolp.catalog;

import org.csu.sdolp.common.model.*;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    private final Map<String, IndexInfo> indices;

    // 用户和权限的内存缓存
    private final Map<String, byte[]> users; // key: username, value: password hash
    private final Map<String, List<PrivilegeInfo>> userPrivileges; // key: username

    // --- 元数据表的特殊定义 ---
    // 存储所有表的信息 (table_id, table_name, first_page_id)
    public static final String CATALOG_TABLES_TABLE_NAME = "_catalog_tables";
    private final Schema tablesTableSchema;
    private PageId tablesTableFirstPageId;

    // 存储所有列的信息 (table_id, column_name, column_type, column_index)
    public static final String CATALOG_COLUMNS_TABLE_NAME = "_catalog_columns";
    private final Schema columnsTableSchema;
    private PageId columnsTableFirstPageId;

    // 用户和权限表的定义
    public static final String CATALOG_USERS_TABLE_NAME = "_catalog_users";
    private final Schema usersTableSchema;
    private PageId usersTableFirstPageId;

    public static final String CATALOG_PRIVILEGES_TABLE_NAME = "_catalog_privileges";
    private final Schema privilegesTableSchema;
    private PageId privilegesTableFirstPageId;


    public Catalog(BufferPoolManager bufferPoolManager) throws IOException {
        this.bufferPoolManager = bufferPoolManager;
        this.tables = new ConcurrentHashMap<>();
        this.tableIds = new ConcurrentHashMap<>();
        this.nextTableId = new AtomicInteger(0);
        this.indices = new ConcurrentHashMap<>();
        this.users = new ConcurrentHashMap<>();
        this.userPrivileges = new ConcurrentHashMap<>();

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
        this.usersTableSchema = new Schema(Arrays.asList(
                new Column("user_id", DataType.INT),
                new Column("user_name", DataType.VARCHAR),
                new Column("password_hash", DataType.VARCHAR) // Store hash instead of plain text
        ));
        this.privilegesTableSchema = new Schema(Arrays.asList(
                new Column("privilege_id", DataType.INT),
                new Column("user_id", DataType.INT),
                new Column("table_name", DataType.VARCHAR),
                new Column("privilege_type", DataType.VARCHAR) // e.g., "SELECT", "INSERT", "ALL"
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
                switch (tableName) {
                    case CATALOG_COLUMNS_TABLE_NAME -> columnsTableFirstPageId = new PageId(firstPageId);
                    case CATALOG_USERS_TABLE_NAME -> usersTableFirstPageId = new PageId(firstPageId);
                    case CATALOG_PRIVILEGES_TABLE_NAME -> privilegesTableFirstPageId = new PageId(firstPageId);
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

            Page usersPage = bufferPoolManager.getPage(usersTableFirstPageId);
            List<Tuple> userTuples = usersPage.getAllTuples(usersTableSchema);
            for (Tuple userTuple : userTuples) {
                String userName = (String) userTuple.getValues().get(1).getValue();
                String passwordHash = (String) userTuple.getValues().get(2).getValue();
                users.put(userName, passwordHash.getBytes(StandardCharsets.UTF_8));
            }

            Page privilegesPage = bufferPoolManager.getPage(privilegesTableFirstPageId);
            List<Tuple> privilegeTuples = privilegesPage.getAllTuples(privilegesTableSchema);
            for (Tuple privilegeTuple : privilegeTuples) {
                int userId = (int) privilegeTuple.getValues().get(1).getValue();
                String tableName = (String) privilegeTuple.getValues().get(2).getValue();
                String privilegeType = (String) privilegeTuple.getValues().get(3).getValue();
                // Find username by userId (this is inefficient, a map would be better in a real system)
                String userName = findUserNameById(userId, userTuples);
                if (userName != null) {
                    userPrivileges.computeIfAbsent(userName, k -> new ArrayList<>())
                            .add(new PrivilegeInfo(tableName, privilegeType));
                }
            }
        }
    }

    private String findUserNameById(int userId, List<Tuple> userTuples) {
        for (Tuple userTuple : userTuples) {
            if ((int) userTuple.getValues().get(0).getValue() == userId) {
                return (String) userTuple.getValues().get(1).getValue();
            }
        }
        return null;
    }

    private void bootstrap() throws IOException {
        // 1. 分配 Page 0，预留给 _catalog_tables
        PageId pageZero = bufferPoolManager.newPage().getPageId();
        if (pageZero.getPageNum() != 0) {
            // 如果第一个分配的不是0号页，说明分配器有更严重的问题
            throw new IllegalStateException("Bootstrap failed: Expected to allocate Page 0, but got Page " + pageZero.getPageNum());
        }
        // 2. 分配 Page 1, 给 _catalog_columns
        columnsTableFirstPageId = bufferPoolManager.newPage().getPageId();
        if (columnsTableFirstPageId.getPageNum() != 1) {
            // 经过上面的修复，这个异常理论上不会再被触发
            throw new IllegalStateException("Catalog bootstrap failed: _catalog_columns was not on Page 1, got " + columnsTableFirstPageId.getPageNum());
        }
        usersTableFirstPageId = bufferPoolManager.newPage().getPageId();
        privilegesTableFirstPageId = bufferPoolManager.newPage().getPageId();

        int tablesTableId = nextTableId.getAndIncrement();
        int columnsTableId = nextTableId.getAndIncrement();
        int usersTableId = nextTableId.getAndIncrement();
        int privilegesTableId = nextTableId.getAndIncrement();

        Page tablesPage = bufferPoolManager.getPage(tablesTableFirstPageId);
        tablesPage.insertTuple(new Tuple(Arrays.asList(new Value(tablesTableId), new Value(CATALOG_TABLES_TABLE_NAME), new Value(tablesTableFirstPageId.getPageNum()))));
        tablesPage.insertTuple(new Tuple(Arrays.asList(new Value(columnsTableId), new Value(CATALOG_COLUMNS_TABLE_NAME), new Value(columnsTableFirstPageId.getPageNum()))));
        tablesPage.insertTuple(new Tuple(Arrays.asList(new Value(usersTableId), new Value(CATALOG_USERS_TABLE_NAME), new Value(usersTableFirstPageId.getPageNum()))));
        tablesPage.insertTuple(new Tuple(Arrays.asList(new Value(privilegesTableId), new Value(CATALOG_PRIVILEGES_TABLE_NAME), new Value(privilegesTableFirstPageId.getPageNum()))));
        bufferPoolManager.flushPage(tablesTableFirstPageId);

        Page columnsPage = bufferPoolManager.getPage(columnsTableFirstPageId);
        writeSchemaToColumnsTable(columnsPage, tablesTableId, tablesTableSchema);
        writeSchemaToColumnsTable(columnsPage, columnsTableId, columnsTableSchema);
        writeSchemaToColumnsTable(columnsPage, usersTableId, usersTableSchema);
        writeSchemaToColumnsTable(columnsPage, privilegesTableId, privilegesTableSchema);
        bufferPoolManager.flushPage(columnsTableFirstPageId);

        Page usersPage = bufferPoolManager.getPage(usersTableFirstPageId);
        int rootUserId = 0;
        String rootPassword = "root_password";
        String rootPasswordHash = hashPassword(rootPassword);
        usersPage.insertTuple(new Tuple(Arrays.asList(new Value(rootUserId), new Value("root"), new Value(rootPasswordHash))));
        bufferPoolManager.flushPage(usersTableFirstPageId);

        Page privilegesPage = bufferPoolManager.getPage(privilegesTableFirstPageId);
        int privilegeId = 0;
        privilegesPage.insertTuple(new Tuple(Arrays.asList(new Value(privilegeId), new Value(rootUserId), new Value("*"), new Value("ALL"))));
        bufferPoolManager.flushPage(privilegesTableFirstPageId);

        tables.put(CATALOG_TABLES_TABLE_NAME, new TableInfo(CATALOG_TABLES_TABLE_NAME, tablesTableSchema, tablesTableFirstPageId));
        tables.put(CATALOG_COLUMNS_TABLE_NAME, new TableInfo(CATALOG_COLUMNS_TABLE_NAME, columnsTableSchema, columnsTableFirstPageId));
        tables.put(CATALOG_USERS_TABLE_NAME, new TableInfo(CATALOG_USERS_TABLE_NAME, usersTableSchema, usersTableFirstPageId));
        tables.put(CATALOG_PRIVILEGES_TABLE_NAME, new TableInfo(CATALOG_PRIVILEGES_TABLE_NAME, privilegesTableSchema, privilegesTableFirstPageId));

        tableIds.put(CATALOG_TABLES_TABLE_NAME, tablesTableId);
        tableIds.put(CATALOG_COLUMNS_TABLE_NAME, columnsTableId);
        tableIds.put(CATALOG_USERS_TABLE_NAME, usersTableId);
        tableIds.put(CATALOG_PRIVILEGES_TABLE_NAME, privilegesTableId);

        users.put("root", rootPasswordHash.getBytes(StandardCharsets.UTF_8));
        userPrivileges.put("root", List.of(new PrivilegeInfo("*", "ALL")));




        // --- 手动添加测试用户和权限 ---
        // 1. 创建 testuser
        int testUserId = 1;
        String testPassword = "123";
        String testPasswordHash = hashPassword(testPassword);
        usersPage.insertTuple(new Tuple(Arrays.asList(new Value(testUserId), new Value("testuser"), new Value(testPasswordHash))));

        // 2. 创建一个测试表
        createTable("test_table", new Schema(List.of(new Column("id", DataType.INT))));

        // 3. 授予 testuser 对 test_table 的 SELECT 权限
        privilegesPage.insertTuple(new Tuple(Arrays.asList(
                new Value(2), // privilege_id
                new Value(testUserId),
                new Value("test_table"),
                new Value("SELECT")
        )));

        bufferPoolManager.flushPage(usersPage.getPageId());
        bufferPoolManager.flushPage(privilegesPage.getPageId());

        // --- 核心修复：将 testuser 的信息也加载到内存缓存中 ---
        users.put("testuser", testPasswordHash.getBytes(StandardCharsets.UTF_8));
        userPrivileges.put("testuser", List.of(new PrivilegeInfo("test_table", "SELECT")));
        System.out.println("[Bootstrap] Manually created 'testuser' and loaded into memory cache.");
    }

    private void writeSchemaToColumnsTable(Page columnsPage, int tableId, Schema schema) {
        int colIdx = 0;
        for (Column col : schema.getColumns()) {
            columnsPage.insertTuple(new Tuple(Arrays.asList(
                    new Value(tableId),
                    new Value(col.getName()),
                    new Value(col.getType().toString()),
                    new Value(colIdx++)
            )));
        }
    }

    private String hashPassword(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encodedhash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
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
    /**
     * 从目录中删除一个表
     * @param tableName 要删除的表名
     */
    public void dropTable(String tableName) throws IOException {
        Integer tableId = tableIds.get(tableName);
        if (tableId == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
        }

        // 1. 从内存缓存中移除
        tables.remove(tableName);
        tableIds.remove(tableName);

        // 2. 从 _catalog_tables 元数据页中删除表的条目
        deleteTupleFromMetaDataPage(tablesTableFirstPageId, tablesTableSchema, 0, new Value(tableId));

        // 3. 从 _catalog_columns 元数据页中删除该表的所有列条目
        deleteTupleFromMetaDataPage(columnsTableFirstPageId, columnsTableSchema, 0, new Value(tableId));

        // 注意：我们没有删除表的数据页，这在真实系统中需要一个复杂的空闲空间管理机制来回收。
        // 在此简化模型中，我们仅删除元数据，数据页将变为不可访问的“孤儿页”。
    }

    /**
     * 向现有表添加一个新列
     * @param tableName 要修改的表名
     * @param newColumn 新增的列
     */
    public void addColumn(String tableName, Column newColumn) throws IOException {
        TableInfo tableInfo = getTable(tableName);
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
        }
        int tableId = tableIds.get(tableName);

        // 1. 更新内存中的 Schema
        List<Column> updatedColumns = new ArrayList<>(tableInfo.getSchema().getColumns());
        updatedColumns.add(newColumn);
        Schema newSchema = new Schema(updatedColumns);

        // 2. 将新列的元数据持久化到 _catalog_columns
        Page columnsPage = bufferPoolManager.getPage(columnsTableFirstPageId);
        Tuple columnMeta = new Tuple(Arrays.asList(
                new Value(tableId),
                new Value(newColumn.getName()),
                new Value(newColumn.getType().toString()),
                new Value(tableInfo.getSchema().getColumns().size()) // 新列的索引
        ));
        columnsPage.insertTuple(columnMeta);
        bufferPoolManager.flushPage(columnsTableFirstPageId);

        // 3. 更新内存缓存中的 TableInfo
        TableInfo newTableInfo = new TableInfo(tableName, newSchema, tableInfo.getFirstPageId());
        tables.put(tableName, newTableInfo);
    }

    /**
     * 辅助方法：从元数据页中删除满足条件的元组
     */
    private void deleteTupleFromMetaDataPage(PageId pageId, Schema schema, int columnIndex, Value value) throws IOException {
        Page page = bufferPoolManager.getPage(pageId);
        List<Tuple> tuples = page.getAllTuples(schema);

        // 找到所有需要删除的元组的槽位索引
        List<Integer> slotsToDelete = new ArrayList<>();
        for (int i = 0; i < tuples.size(); i++) {
            Tuple t = tuples.get(i);
            if (t != null && t.getValues().get(columnIndex).getValue().equals(value.getValue())) {
                slotsToDelete.add(i);
            }
        }

        // 标记删除
        for (Integer slotIndex : slotsToDelete) {
            page.markTupleAsDeleted(slotIndex);
        }
        bufferPoolManager.flushPage(pageId);
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
    public TableInfo getTableByTuple(Tuple tuple) {
        if (tuple == null) return null;
        // 这是一个简化的、低效的查找，仅用于恢复
        for (TableInfo tableInfo : tables.values()) {
            Schema schema = tableInfo.getSchema();
            if (schema.getColumns().size() == tuple.getValues().size()) {
                // 【新增】同时比较每一列的数据类型
                boolean typesMatch = true;
                for (int i = 0; i < schema.getColumns().size(); i++) {
                    if (schema.getColumns().get(i).getType() != tuple.getValues().get(i).getType()) {
                        typesMatch = false;
                        break;
                    }
                }
                if (typesMatch) {
                    return tableInfo; // 只有列数和所有类型都匹配，才认为是同一张表
                }
            }
        }
        return null;
    }

    /**
     * 创建并注册一个新的索引。
     */
    public void createIndex(String indexName, String tableName, String columnName, int rootPageId) {
        if (indices.containsKey(indexName)) {
            throw new IllegalStateException("Index '" + indexName + "' already exists.");
        }
        IndexInfo indexInfo = new IndexInfo(indexName, tableName, columnName, rootPageId);
        indices.put(indexName, indexInfo);
        // 您需要添加将索引信息持久化到磁盘的逻辑
    }

    /**
     * *** 新增方法：更新索引的根页面ID ***
     */
    public void updateIndexRootPageId(String indexName, int newRootPageId) {
        IndexInfo indexInfo = indices.get(indexName);
        if (indexInfo != null) {
            indexInfo.setRootPageId(newRootPageId);
            // 在一个更完整的系统中，这里也需要将此变更持久化到磁盘的元数据表中。
            System.out.println("[Catalog] Updated root page ID for index '" + indexName + "' to " + newRootPageId);
        } else {
            throw new IllegalStateException("Cannot update root page for non-existent index '" + indexName + "'.");
        }
    }

    /**
     * 根据表名和列名查找索引。
     */
    public IndexInfo getIndex(String tableName, String columnName) {
        for (IndexInfo indexInfo : indices.values()) {
            if (indexInfo.getTableName().equalsIgnoreCase(tableName) && indexInfo.getColumnName().equalsIgnoreCase(columnName)) {
                return indexInfo;
            }
        }
        return null;
    }

    /**
     * 根据索引名称获取索引信息。
     */
    public IndexInfo getIndex(String indexName) {
        return indices.get(indexName);
    }

    public List<IndexInfo> getIndexesForTable(String tableName) {
        return indices.values().stream()
                .filter(indexInfo -> indexInfo.getTableName().equalsIgnoreCase(tableName))
                .collect(Collectors.toList());
    }

    /**
     * 删除一个表上的所有索引元数据。
     * @param tableName 表名
     */
    public void dropIndexesForTable(String tableName) {
        List<String> indexesToRemove = indices.values().stream()
                .filter(indexInfo -> indexInfo.getTableName().equalsIgnoreCase(tableName))
                .map(IndexInfo::getIndexName)
                .collect(Collectors.toList());

        for (String indexName : indexesToRemove) {
            indices.remove(indexName);
            System.out.println("[Catalog] Dropped index metadata '" + indexName + "' for table '" + tableName + "'.");
        }
    }

    // 内部类用于在内存中表示权限
    public static class PrivilegeInfo {
        String tableName;
        String privilegeType;

        PrivilegeInfo(String tableName, String privilegeType) {
            this.tableName = tableName;
            this.privilegeType = privilegeType;
        }
    }

    /**
     * @param username 用户名
     * @return 密码的哈希字节数组，如果用户不存在则返回 null。
     */
    public byte[] getPasswordHash(String username) {
        return users.get(username);
    }

    /**
     * @param username       用户名
     * @param tableName      要操作的表名
     * @param privilegeType  请求的权限类型 (e.g., "SELECT", "INSERT")
     * @return 如果有权限则返回 true，否则返回 false。
     */
    public boolean hasPermission(String username, String tableName, String privilegeType) {
        if (username == null) {
            return false; // 未登录用户没有任何权限
        }

        List<PrivilegeInfo> privileges = userPrivileges.get(username);
        if (privileges == null) {
            return false; // 该用户没有任何权限记录
        }
        // 检查用户是否对所有表有 ALL 权限 (超级用户)
        if (privileges.stream().anyMatch(p -> p.tableName.equals("*") && p.privilegeType.equalsIgnoreCase("ALL"))) {
            return true;
        }
        // 检查用户是否对该特定表有 ALL 权限
        if (privileges.stream().anyMatch(p -> p.tableName.equalsIgnoreCase(tableName) && p.privilegeType.equalsIgnoreCase("ALL"))) {
            return true;
        }
        // 检查用户是否对该特定表有具体的操作权限
        return privileges.stream().anyMatch(p -> p.tableName.equalsIgnoreCase(tableName) && p.privilegeType.equalsIgnoreCase(privilegeType));
    }
}

