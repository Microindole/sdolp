# 模块五详解 · 元数据管理 (Catalog)

## 1. 模块概述

元数据管理模块（通常称为**系统目录**或**Catalog**）是 MiniDB 数据库的“户籍系统”和“字典”。它存储、管理并提供关于数据库内部所有对象的描述性信息——即**元数据（Metadata）**，或称“关于数据的数据”。如果没有系统目录，数据库将无法理解SQL查询中的表名和列名，也无法进行任何有效的操作。

本模块的核心思想是**“用数据库管理数据库”**。系统目录本身并不是一个独立于数据库之外的特殊文件，而是被实现为一组存储在数据库内部的**特殊系统表**。这种自举（Bootstrap）的设计使得元数据的增、删、改、查可以复用数据库自身成熟的存储和事务机制，极大地简化了设计并增强了可靠性。

**核心职责**:

- **存储和管理 Schema 信息**: 包括所有数据库、表、列及其数据类型的定义。
- **管理索引信息**: 记录每个索引的名称、所属表、相关列以及其B+树的根页面ID。
- **管理用户与权限**: 存储用户信息（如密码哈希）和访问控制列表（ACL），定义哪个用户可以对哪个表执行何种操作。
- **提供查询接口**: 为上层模块（尤其是编译器）提供快速查询元数据的接口。

## 2. 总体架构与实现

Catalog 模块通过将元数据持久化到预定义的系统表中，并在内存中维护这些信息的缓存来实现其功能。

**架构图**:

```
+--------------------------------+
|      上层模块 (编译器, 执行引擎)    |
+--------------------------------+
    |
    | 1. getTable(), hasPermission(), ...
    V
+--------------------------------+
|  元数据管理器 (catalog.Catalog)  |
|                                |
|   +--------------------------+   |
|   |   内存缓存 (In-Memory Caches) |   |  <-- 2. 快速查找
|   | - tables, indices, users |   |
|   +--------------------------+   |
+--------------------------------+
    |         ^
    | 3. DDL/ | 4. 启动时加载/
    | DCL操作 |   权限检查时刷新
    V         |
+--------------------------------+
|       存储引擎 (Storage)        |
+--------------------------------+
    |         ^
    |         |
+--------------------------------+
|  物理数据文件 (minidb.data)      |
|                                |
|   +--------------------------+   |
|   |     系统表 (System Tables)   |   |
|   | - _catalog_tables        |   |
|   | - _catalog_columns       |   |
|   | - _catalog_users         |   |
|   | - _catalog_privileges    |   |
|   +--------------------------+   |
+--------------------------------+
```

**实现流程**:

1. **自举 (Bootstrap)**: 当 `Catalog` 在一个全新的、空的数据库文件中被初始化时，它会执行一个“自举”过程。这个过程会创建一系列用于存储元数据的系统表（如 `_catalog_tables`），并将这些系统表自身的元数据写入其中。同时，会创建一个默认的`root`用户并授予所有权限。
2. **启动时加载**: 当 `Catalog` 在一个已存在的数据库上初始化时，它会读取这些系统表的内容，并将所有元数据加载到内存中的**缓存**里，以便快速访问。
3. **运行时查询**: 当编译器需要验证一个表是否存在时，`Catalog` 会首先检查其内存缓存。
4. **DDL/DCL 操作**: 当用户执行 `CREATE TABLE` 或 `GRANT` 等命令时，对应的执行器会调用 `Catalog` 的方法。`Catalog` 会： a.  将新的元数据写入到磁盘上的系统表中。 b.  同步更新其内存缓存，以保证后续查询的一致性。

## 3. 核心组件详解

### 3.1 核心管理类 (`catalog.Catalog`)

这是整个模块的入口和核心。它封装了所有对元数据的操作，并管理着内存缓存。

- **核心类**: `catalog.Catalog`
- **内存缓存**: 为了避免每次查询元数据都去扫描磁盘上的系统表，`Catalog` 内部维护了多个 `Map` 作为缓存：
  - `tables`: 缓存 `TableInfo` 对象，提供表名到表结构（Schema）的快速映射。
  - `indices`: 缓存 `IndexInfo` 对象。
  - `users`: 缓存用户名到密码哈希的映射。
  - `userPrivileges`: 缓存用户的权限列表。

### 3.2 系统表 (System Tables)

这些是以 `_catalog_` 为前缀的特殊表，是元数据的持久化载体。

- `_catalog_tables`:
  - **结构**: `(table_id INT, table_name VARCHAR, first_page_id INT)`
  - **用途**: 记录数据库中每一张表的核心信息。`first_page_id` 是指向该表数据存储的第一个数据页的指针。
- `_catalog_columns`:
  - **结构**: `(table_id INT, column_name VARCHAR, column_type VARCHAR, column_index INT)`
  - **用途**: 记录每个表包含的所有列及其属性。通过 `table_id` 与 `_catalog_tables` 表关联。
- `_catalog_users`:
  - **结构**: `(user_id INT, user_name VARCHAR, password_hash VARCHAR)`
  - **用途**: 存储用户信息。为了安全，密码是以 **SHA-256 哈希** 的形式存储的。
- `_catalog_privileges`:
  - **结构**: `(privilege_id INT, user_id INT, table_name VARCHAR, privilege_type VARCHAR)`
  - **用途**: 存储权限信息，定义了哪个用户 (`user_id`) 可以在哪个表 (`table_name`) 上执行哪种操作 (`privilege_type`)。

### 3.3 权限管理 (`hasPermission`)

MiniDB 实现了一个集中式的权限管理模型。所有用户和权限信息都存储在**`default`数据库**的系统表中。

- **检查逻辑**: 当在**任何**数据库中进行权限检查时 (`hasPermission`)：
  1. 首先检查当前 `Catalog` 实例的**内存缓存**。
  2. 如果缓存检查失败（可能因为权限刚刚在另一个连接中被授予，缓存尚未更新），系统会触发一次**强制权限重载 (`forceReloadPermissions`)**。
  3. `forceReloadPermissions` 会**直接访问 `default` 数据库的物理文件**，重新读取 `_catalog_users` 和 `_catalog_privileges` 表，并用最新的数据刷新内存缓存。
  4. 再次基于刷新后的缓存进行权限检查。
- **设计考量**: 这种设计确保了权限的**全局一致性**。无论用户连接到哪个数据库，其权限都由 `default` 数据库统一管理和验证，避免了权限信息的分散和不一致。

## 4. 与其他模块的交互

- **与编译器 (Compiler)**:
  - `SemanticAnalyzer` 严重依赖 `Catalog` 来获取表和列的 `Schema` 信息，以验证SQL语句的合法性（例如，表是否存在、列是否存在、数据类型是否匹配）。
  - `Planner` 同样需要从 `Catalog` 获取 `TableInfo` 和 `IndexInfo` 来创建高效的执行计划。
- **与执行引擎 (Execution Engine)**:
  - 所有DDL和DCL执行器（如 `CreateTableExecutor`, `CreateIndexExecutor`, `GrantExecutor`）是 `Catalog` 的主要**写入方**。它们在执行时会调用 `catalog.createTable()`, `catalog.grantPrivilege()` 等方法来持久化元数据的变更。
- **与存储引擎 (Storage Engine)**:
  - `Catalog` 是存储引擎的**用户**。它通过 `BufferPoolManager` 来读取和写入其底层的系统表数据页，完全复用了数据库的存储能力。
- **与事务模块 (Transaction & Concurrency)**:
  - 所有对 `Catalog` 系统表的修改操作（如 `CREATE TABLE`）同样被包裹在事务中，并会生成对应的 `LogRecord` 写入WAL日志。这确保了元数据的修改也是**原子**和**持久**的，可以在系统崩溃后被正确恢复。