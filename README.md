
# MiniDB: 一个用 Java 从零打造的关系型数据库内核

![Java](https://img.shields.io/badge/Java-17+-orange.svg)
![Maven](https://img.shields.io/badge/Maven-3.5+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

欢迎来到 MiniDB！

**MiniDB** 是一个由小组合作开发、用 Java 从零开始构建的、功能丰富的关系型数据库管理系统。本项目旨在通过从零开始构建，深入探索数据库内核的实现原理，涵盖了从SQL解析、查询优化、执行引擎到存储管理、事务控制和崩溃恢复的全过程。

> **Note**
> 这是一个数据库系统课程的小组项目。完整的源代码和项目历史，请访问我们的原始 Gitee 仓库：
> **https://gitee.com/cxzhang0508/sdolp**



## ✨ 核心特性

- **标准 SQL 支持**: 实现了 SQL-92 的一个子集，支持完整的 DDL, DML, DCL 和查询操作。
  - **DDL**: `CREATE/DROP/ALTER TABLE`, `CREATE/DROP DATABASE`, `CREATE INDEX`
  - **DML**: `INSERT`, `UPDATE`, `DELETE`, `SELECT` (支持 `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`)
  - **DCL**: `CREATE USER`, `GRANT`
  - **辅助命令**: `SHOW TABLES/DATABASES`, `USE`
- **客户端/服务器架构**:
  - **原生TCP服务器** (`ServerHost`): 为项目自带的 `InteractiveShell` 客户端提供服务。
  - **MySQL 协议兼容** (`ServerRemote`): 允许使用 Navicat, DBeaver, MySQL Workbench 等标准工具连接。
- **分层式编译器**: 包含一个经典的四阶段查询编译器：
  1.  **词法分析器 (Lexer)**
  2.  **语法分析器 (Parser)** -> 抽象语法树 (AST)
  3.  **语义分析器 (Semantic Analyzer)**
  4.  **计划生成器 (Planner)** -> 物理执行计划
- **火山模型执行引擎**: 采用标准的火山迭代模型（`TupleIterator`），支持多种执行算子（扫描, 过滤, 投影, 连接, 排序, 聚合等）。
- **ACID 事务支持**:
  - **预写日志 (WAL)**: 确保操作的原子性和持久性。
  - **ARIES 崩溃恢复**: 保证数据库在意外崩溃后能恢复到一致的状态。
  - **页级并发控制**: 通过共享锁（S-Lock）和排他锁（X-Lock）实现并发控制。
- **可插拔存储引擎**:
  - **缓冲池管理器 (`BufferPoolManager`)**: 支持 LRU/FIFO 替换策略，高效管理内存与磁盘的数据页交换。
  - **B+树索引**: 实现了高性能的 B+树索引结构，支持快速查询。
- **完善的辅助工具**:
  - `DataReader`: 交互式工具，用于查看表数据或将数据库**导出为 `.sql` 备份文件**。
  - `LogReader`: 用于解析和可读化展示 WAL 日志的调试工具。



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

<details>
<summary><strong>📁 点击展开完整目录结构</strong></summary>

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

</details>



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



## 👥 作者与贡献

本项目最初由一个课程设计小组共同完成。出于隐私考虑，此处仅列出本 GitHub 仓库的维护者。同样感谢所有未在此列出的、对项目做出贡献的原始成员。

- **[Microindole](https://github.com/Microindole)**



## ❓ 提问与交流

如果你对项目有任何疑问、建议，或者发现了 Bug，欢迎随时通过 **[GitHub Issues](https://github.com/Microindole/sdolp/issues)** 与我交流。

我会尽我所能回答你的问题！作者有时间也会在这里更新的 ( ´ ▽ ` )ﾉ



## 📞 联系作者

- **GitHub**: **[@Microindole](https://github.com/Microindole/sdolp)**
- **Email:**
  - **项目交流:**  microindole@gmail.com
  - **个人/其他:**  1513979779@qq.com

<div align="center">


## ⭐ 项目支持

如果这个项目对你的学习或工作有所帮助，请别忘了给它点一个 **Star**！

这对我来说是莫大的鼓励，也是我继续维护和改进这个项目的动力。谢谢！

[![GitHub Stars](https://img.shields.io/github/stars/Microindole/sdolp?style=flat-square&logo=github)](https://github.com/Microindole/sdolp)
[![GitHub Forks](https://img.shields.io/github/forks/Microindole/sdolp?style=flat-square&logo=github)](https://github.com/Microindole/sdolp)

### 📈 项目统计

<div align="center">

![GitHub Stats](https://github-readme-stats.vercel.app/api?username=Microindole&show_icons=true&theme=radical&hide_border=true&count_private=true)

</div>

</div>



## 📄 许可证

本项目采用 MIT 许可证。详情请见 [LICENSE](LICENSE) 文件。

Copyright (c) 2025 The sdolp Project Authors




