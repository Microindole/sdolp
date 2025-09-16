# 模块六详解 · 网络服务与客户端 (Networking & Client)

## 1. 模块概述

网络服务与客户端模块是 MiniDB 对外提供服务的门户。它实现了标准的客户端/服务器（C/S）架构，使得用户和外部应用程序能够连接到数据库内核，并执行SQL命令。为了兼顾易用性、兼容性和开发调试的便利性，MiniDB 创新地提供了两种独立的服务器模式和多种客户端工具。

本模块的设计目标是提供一个稳定、可靠且对用户友好的交互层，将底层数据库内核的复杂性封装起来，为最终用户提供一个直观的操作界面。

**核心职责**:

- **监听网络端口**: 接收来自客户端的TCP连接请求。
- **并发处理**: 为每个客户端连接创建一个独立的会话和处理线程，实现多用户并发访问。
- **协议解析与处理**:
  - 实现一个简单的原生TCP协议，用于与项目自带的客户端通信。
  - 模拟**MySQL通信协议**，以兼容主流的第三方数据库管理工具（如 Navicat, DBeaver）。
- **会话管理**: 跟踪每个连接的状态，包括认证用户、当前所在的数据库等。
- **提供用户交互界面**: 提供从基础命令行到高级图形化界面的多种客户端工具。

## 2. 总体架构与模式

MiniDB 提供了两种服务端实现，分别服务于不同类型的客户端，用户可以根据需求选择启动。

**架构图**:

```
+---------------------------+      +---------------------------+
|   第三方工具 (Navicat)     |      |  MiniDB 自带客户端         |
|                           |      | (AdvancedShell, etc.)     |
+---------------------------+      +---------------------------+
             |                                |
 (MySQL Protocol over TCP)      (Simple Protocol over TCP)
             |                                |
             V                                V
+---------------------------+      +---------------------------+
| ServerRemote (端口: 9999) |      |   ServerHost (端口: 8848)   |
| + MysqlProtocolHandler    |      |                           |
+---------------------------+      +---------------------------+
             |                                |
             |                                |
             +--------------+-----------------+
                            |
                            V
               +---------------------------+
               |   QueryProcessor (数据库实例)  |
               +---------------------------+
                            |
                            V
               +---------------------------+
               |      数据库内核 (其他模块)   |
               +---------------------------+
```

- **模式一: 原生TCP服务 (`ServerHost`)**:
  - 监听 `8848` 端口，采用一个简单的“一问一答”式文本协议。
  - 主要为项目自带的 `InteractiveShell` 和 `AdvancedShell` 客户端服务。
  - 优点是协议简单，便于快速开发和调试。
- **模式二: MySQL协议兼容服务 (`ServerRemote`)**:
  - 监听 `9999` 端口，通过 `MysqlProtocolHandler` 模拟了MySQL服务器的握手、认证和查询协议。
  - 允许用户使用功能强大的第三方GUI工具（如Navicat, DBeaver, MySQL Workbench）来连接和管理MiniDB。
  - 优点是极大地提升了易用性和用户体验，让用户可以在熟悉的工具中操作MiniDB。

## 3. 核心组件详解

### 3.1 服务端实现

#### 3.1.1 `ServerHost` (原生TCP服务器)

- **核心类**: `cli.server.ServerHost`
- **工作模式**:
  - 主线程在一个循环中监听 `8848` 端口，等待客户端连接 (`serverSocket.accept()`)。
  - 每当有新客户端连接时，为其创建一个新的专用线程来处理所有后续通信。
  - 每个线程独立地管理一个 `QueryProcessor` 实例映射，实现了**库级隔离**。`USE database` 命令会切换到或创建对应数据库的 `QueryProcessor` 实例。

#### 3.1.2 `ServerRemote` & `MysqlProtocolHandler` (MySQL协议兼容服务器)

- **核心类**: `cli.server.ServerRemote`, `cli.server.MysqlProtocolHandler`
- **工作模式**: `ServerRemote` 的结构与 `ServerHost` 类似，但它为每个连接启动的是一个 `MysqlProtocolHandler` 实例。
- **`MysqlProtocolHandler` 详解**: 这是实现MySQL兼容性的关键。它负责处理与客户端之间复杂的二进制协议交互。
  1. **握手 (Handshake)**: 当客户端首次连接时，服务器发送一个“握手包”，包含服务器版本、连接ID、盐值（用于密码加密）等信息。
  2. **认证 (Authentication)**: 客户端回送一个“认证包”，包含用户名和加密后的密码。`MysqlProtocolHandler` 会从 `default` 数据库的 `_catalog_users` 表中查询用户密码哈希进行比对。
  3. **命令处理 (`COM_QUERY`, `COM_INIT_DB` 等)**:
     - 认证成功后，处理器进入一个循环，等待客户端发送命令包。
     - 对于 `COM_QUERY` (执行SQL)，它会解析出SQL字符串，交给 `QueryProcessor` 执行。
     - 执行结果（无论是数据集还是状态信息）会被格式化成MySQL协议规定的**结果集包 (Result Set Packet)**、**OK包**或**Error包**，然后回送给客户端。
  4. **兼容性模拟**: 为了让Navicat等工具正常工作，`MysqlProtocolHandler` “欺骗”了客户端，模拟实现了一些它们依赖的查询，例如：
     - `SHOW VARIABLES`, `SHOW ENGINES`
     - 对 `information_schema` 数据库的查询
     - `SHOW CREATE TABLE`, `SHOW FULL COLUMNS` 这些查询会返回预设的、符合格式的模拟数据，而不是通过数据库内核真正执行。

### 3.2 会话管理 (`Session`)

- **职责**: 封装和跟踪单个客户端连接的状态。
- **核心类**: `cli.server.Session`
- **实现细节**:
  - 每个 `Session` 对象都包含 `connectionId`、`username`、`isAuthenticated` 标志以及 `currentDatabase`。
  - `Session` 对象在服务器端的处理线程中创建，并会在调用 `SemanticAnalyzer` 进行权限检查时被传递下去，为权限验证提供了必要的上下文。

### 3.3 客户端实现

#### 3.3.1 `InteractiveShell` (交互式命令行客户端)

- **核心类**: `cli.client.InteractiveShell`
- **特点**:
  - 一个纯文本的、基于控制台的客户端。
  - 支持多行SQL输入（以分号`;`结束）。
  - 能够显示当前用户名和数据库名作为提示符。
  - 支持通过 `source` 命令或 `import;` 命令执行 `.sql` 脚本文件。

#### 3.3.2 `AdvancedShell` (高级图形化客户端)

- **核心类**: `cli.client.AdvancedShell`
- **技术栈**: Java Swing, FlatLaf (UI美化), RSyntaxTextArea (SQL编辑器)
- **核心功能**:
  - **连接管理器**: 提供图形化界面来输入服务器地址、端口和用户名。
  - **高级SQL编辑器**:
    - 基于 `RSyntaxTextArea` 实现，支持**SQL语法高亮**。
    - 实现了基于数据库关键字的**代码自动补全**。
    - 支持 **F5** 快捷键执行查询。
    - 支持**历史命令**追溯（上下箭头）。
  - **多视图结果展示**:
    - **表格视图**: 将 `SELECT` 查询返回的数据集美观地展示在 `JTable` 中，支持排序。
    - **控制台视图**: 显示原始的服务器响应、执行信息和错误消息。
  - **工具集成**: 集成了对 `DataReader`（导出数据库）和 `LogReader`（查看日志）的图形化调用入口，形成了一个小型IDE。

## 4. 与其他模块的交互

- **与执行引擎 (QueryProcessor)**: 服务器端的处理线程（无论是 `ServerHost` 的原生处理器还是 `MysqlProtocolHandler`）是 `QueryProcessor` 的主要调用方。它们将从客户端接收到的SQL字符串传递给 `QueryProcessor.executeAndGetResult()`，并等待其返回结果。
- **与元数据管理器 (Catalog)**: `MysqlProtocolHandler` 在**认证阶段**需要访问 `default` 数据库的 `Catalog` 来验证用户名和密码。在处理 `SHOW COLUMNS` 等模拟查询时，也需要从 `Catalog` 获取表的 Schema 信息。
- **与事务模块 (Transaction & Concurrency)**: `Session` 对象被传递给 `SemanticAnalyzer`，后者利用其中的 `username` 来进行权限检查。这是连接网络层身份与数据库内部权限控制的桥梁。