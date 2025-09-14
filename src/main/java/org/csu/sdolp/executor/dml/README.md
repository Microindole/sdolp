# MiniDB: A Relational Database Engine in Java

欢迎来到 MiniDB！这是一个用 Java 从零开始构建的、功能丰富的关系型数据库管理系统。本项目旨在深入探索数据库内核的实现原理，涵盖了从 SQL 解析、查询优化、执行引擎到存储管理、事务控制和崩溃恢复的全过程。

## ✨ 核心特性

* **标准 SQL 支持**: 实现了 SQL-92 标准的一个子集，支持 DDL, DML, DCL 和 DQL 操作。
    * **DDL**: `CREATE/DROP/ALTER TABLE`, `CREATE/DROP DATABASE`, `CREATE INDEX`
    * **DML**: `INSERT`, `UPDATE`, `DELETE`, `SELECT` (支持 `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`)
    * **DCL**: `CREATE USER`, `GRANT`
    * **辅助命令**: `SHOW TABLES/DATABASES`, `USE`
* **客户端/服务器架构**:
    * **原生TCP服务器** (`ServerHost`):
      为自定义的 `InteractiveShell` 客户端提供服务。
    * **MySQL 协议兼容** (`ServerRemote`):
      允许使用标准的数据库工具（如 Navicat, DBeaver, MySQL Workbench）进行连接和操作。
* **分层式编译器**: 包含一个经典的四阶段查询编译器：
    1.  **词法分析器 (Lexer)**: 将 SQL 字符串分解为 Token 流。
    2.  **语法分析器 (Parser)**:
        使用递归下降法将 Token 流构建为抽象语法树 (AST)。
    3.  **语义分析器 (Semantic Analyzer)**:
        检查 AST 的逻辑正确性，如表名、列名是否存在，数据类型是否匹配等。
    4.  **计划生成器 (Planner)**: 将 AST 转换为可执行的物理计划树。
* **火山模型执行引擎**:
  采用标准的火山迭代模型（TupleIterator），支持多种执行算子（如顺序扫描, 索引扫描, 过滤, 投影, 连接, 排序, 聚合等）。
* **ACID 事务支持**:
    * **预写日志 (Write-Ahead Logging, WAL)**:
      通过 `LogManager` 记录所有数据变更操作，确保原子性和持久性。
    * **ARIES 崩溃恢复**:
      实现了 ARIES 协议的核心思想（Analysis, Redo, Undo），确保数据库在意外崩溃后能恢复到一致的状态。
    * **并发控制**:
      通过 `LockManager` 实现基于页级别的共享锁（S-Lock）和排他锁（X-Lock），支持基本的并发控制（*注：当前为基础实现*）。
* **可插拔存储引擎**:
    * **缓冲池管理器 (`BufferPoolManager`)**:
      管理内存与磁盘之间的数据页交换，支持 LRU 和 FIFO 替换策略。
    * **Slotted Page 布局**: 采用高效的页内结构来存储元组，支持变长记录。
    * **B+树索引**: 实现了 B+树用于主键和二级索引，支持快速的点查询。
* **完善的辅助工具**:
    * `DataReader`:
      一个交互式命令行工具，用于查看表数据或将整个数据库导出为 `.sql` 备份文件。
    * `LogReader`: 一个用于解析和可读化展示 `.log` 文件的调试工具。

## 🚀 如何运行

### 编译项目

本项目使用 Maven 进行管理。首先，在项目根目录下编译所有代码：

```bash
mvn clean install
```

### 1\. 启动数据库服务器

你有两种选择来启动服务器：

* **兼容 MySQL 协议的服务器 (推荐)**:
  允许使用 Navicat 等标准工具连接。

  ```bash
  java -cp target/classes org.csu.sdolp.cli.server.ServerRemote
  ```

  服务器将启动并监听 `9999` 端口。

* **原生 TCP 服务器**:
  仅供项目自带的 `InteractiveShell` 使用。

  ```bash
  java -cp target/classes org.csu.sdolp.cli.server.ServerHost
  ```

  服务器将启动并监听 `8848` 端口。

### 2\. 连接数据库

* **使用 Navicat / DBeaver**:
  新建一个 MySQL 连接，主机填 `localhost`，端口填 `9999`，用户名 `root`，密码 `root_password` (或你在代码中设置的其他用户)。

* **使用项目自带的交互式 Shell**:

  ```bash
  java -cp target/classes org.csu.sdolp.cli.client.InteractiveShell
  ```

  按照提示输入用户名即可开始执行 SQL。

### 3\. 使用辅助工具

* **数据读取与导出工具**:

  ```bash
  java -cp target/classes org.csu.sdolp.cli.tool.DataReader
  ```

  程序会列出所有数据库，并引导你进行查看或导出操作。

* **日志分析工具**:

  ```bash
  java -cp target/classes org.csu.sdolp.cli.tool.LogReader
  ```

  程序会引导你选择要分析的数据库日志。

## 📁 项目目录结构

经过重构，项目现在拥有一个清晰、职责分离的目录结构：

```
src/main/java/org/csu/sdolp/
├── catalog/            # 元数据管理模块 (系统目录)
├── cli/                # 命令行接口 (CLI)
│   ├── client/         # 客户端程序
│   ├── server/         # 服务端程序
│   └── tool/           # 辅助开发工具
├── common/             # 通用模块
│   ├── exception/      # 自定义异常
│   └── model/          #核心数据模型 (Tuple, Schema, Value等)
├── compiler/           # SQL 编译器
│   ├── lexer/          # 词法分析器
│   ├── parser/         # 语法分析器
│   │   └── ast/        #   -> 抽象语法树节点 (按DDL/DML/DCL等分类)
│   ├── planner/        # 计划生成器
│   │   └── plan/       #   -> 执行计划节点 (按DDL/DML/Query等分类)
│   └── semantic/       # 语义分析器
├── engine/             # 执行引擎核心
├── executor/           # 执行器 (算子)
│   ├── ddl/            # DDL 操作执行器
│   ├── dml/            # DML 操作执行器
│   ├── dcl/            # DCL 操作执行器
│   ├── show/           # SHOW 命令执行器
│   └── expressions/    # WHERE/ON 子句的谓词表达式
├── storage/            # 存储引擎
│   ├── buffer/         # 缓冲池管理器
│   ├── disk/           # 磁盘管理器
│   ├── index/          # B+树索引实现
│   └── page/           # 页面布局管理
└── transaction/        # 事务与恢复
    ├── log/            # 日志管理器
    └── ...             # 锁管理器, 恢复管理器等
```

### 各模块职责详解

* `catalog`:
  负责数据库元数据的管理，如表结构、列信息、索引、用户和权限。它本身也作为特殊的表持久化在磁盘上。
* `cli`:
  提供了用户与数据库交互的所有接口，被清晰地划分为 `server`, `client`, 和 `tool`。
* `common`:
  存放整个项目共享的核心数据结构（如 `Tuple`, `Schema`）和自定义异常。
* `compiler`:
  数据库的大脑。它将用户输入的 SQL 字符串一步步转化为数据库可以理解和执行的物理计划。其下的子包严格遵循编译原理的分层设计。
* `engine`:
  连接编译器和执行器的桥梁，负责整个查询处理流程的调度。
* `executor`:
  数据库的“四肢”。包含了实现火山模型的所有执行算子，每个算子执行一个特定的物理操作。同样按 DDL/DML 等职责进行了清晰的划分。
* `storage`:
  数据库的基石。负责与磁盘进行交互，管理数据在内存和磁盘之间的流动，并提供高效的数据组织方式（如B+树）。
* `transaction`:
  保证数据库ACID特性的关键模块。实现了 WAL、锁机制和崩溃恢复，是数据库稳定性的核心。