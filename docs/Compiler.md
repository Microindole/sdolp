# 模块详解：SQL 编译器 (Compiler)

## 1. 模块概述

SQL 编译器是 MiniDB 数据库的“大脑”，扮演着将用户输入的、人类可读的 SQL 查询语句翻译成数据库内部可以高效执行的物理操作计划的核心角色。它位于查询处理流程的最前端，其输出直接决定了后续执行引擎的工作方式。

本模块严格遵循经典的数据库编译器设计，采用分层架构，将复杂的转换过程分解为四个独立且职责分明的阶段。这种设计确保了代码的高内聚、低耦合，使得每一阶段的逻辑都清晰可控，并为未来的功能扩展（如更复杂的查询优化）打下了坚实的基础。

**核心职责**: 接收原始SQL字符串，通过**词法分析**、**语法分析**、**语义分析**和**计划生成**四个步骤，最终输出一棵供**执行引擎**使用的**物理执行计划树 (Plan Tree)**。

## 2. 总体架构与数据流

编译器模块内部的数据处理流程是一个线性的、逐级转换的过程。每个阶段都以上一阶段的输出作为输入，并生成更结构化、更接近底层执行逻辑的数据结构。

**数据流图**:

```
[SQL 字符串] -> [ 1. 词法分析器 (Lexer) ] -> [Token 流]
      |
      V
[物理执行计划树] <- [ 4. 计划生成器 (Planner) ] <- [已验证的 AST]
      |
      V
  (送往执行引擎)
```

**流程详解**:

1. **输入**: 接收来自客户端的原始 SQL 查询字符串，例如 `SELECT name FROM users WHERE id = 1;`。
2. **词法分析 (Lexing)**: `Lexer` 扫描字符串，将其分解成一个个有意义的最小单元，称为 **词法单元 (Token)**。例如，`SELECT`、`name`、`FROM` 都是独立的Token。
3. **语法分析 (Parsing)**: `Parser` 接收 Token 流，并根据预定义的 SQL 语法规则（采用递归下降法实现），将其组织成一个层次化的树形结构——**抽象语法树 (Abstract Syntax Tree, AST)**。AST 精确地表达了原始SQL的语法结构。
4. **语义分析 (Semantic Analysis)**: `SemanticAnalyzer` 遍历 AST，检查其在逻辑上是否成立。它会查询**元数据管理器 (Catalog)** 来验证表和列是否存在、检查数据类型是否匹配、操作权限是否满足等。
5. **计划生成 (Planning)**: `Planner` 接收通过语义检查的 AST，并将其转换为一棵**物理执行计划树 (Plan Tree)**。这棵树的每个节点 (`PlanNode`) 都对应一个具体的物理操作（如全表扫描、索引扫描、过滤等）。此阶段还会执行一些基础的查询优化，例如**谓词下推 (Predicate Pushdown)** 和**索引选择**。
6. **输出**: 最终生成的物理执行计划树被传递给**执行引擎 (Execution Engine)** 进行处理。

## 3. 核心组件详解

### 3.1 词法分析器 (Lexer)

- **职责**: 将SQL字符串转换为Token流。
- **核心类**:
  - `compiler.lexer.Lexer`: 词法分析器主类。
  - `compiler.lexer.Token`: 表示一个词法单元，包含类型、原始文本（词素）、行列号。
  - `compiler.lexer.TokenType`: `enum` 类型，定义了所有可能的Token类型（如关键字 `SELECT`、标识符 `IDENTIFIER`、操作符 `EQUAL` 等）。
- **实现细节**:
  - 内部维护一个关键字 `Map` (`keywords`)，用于区分关键字和普通标识符。
  - 通过向前查看 (`peek()`) 和步进 (`advance()`) 的方式逐字符扫描输入字符串。
  - 能够正确识别和处理各种SQL元素，包括：
    - **关键字**: 不区分大小写匹配。
    - **标识符**: 包括带反引号 ``` 的标识符。
    - **常量**: 整数、小数字符串。
    - **字符串**: 由单引号 `'` 包裹。
    - **运算符**: 如 `=`, `>`, `<`, `!=`, `AND`, `OR` 等。
    - **分隔符**: 如 `(`, `)`, `,`, `;`, `.`。
  - 能够跳过空格、换行符等空白字符。

### 3.2 语法分析器 (Parser)

- **职责**: 基于Token流构建抽象语法树 (AST)。
- **核心类**:
  - `compiler.parser.Parser`: 语法分析器主类。
  - `compiler.parser.ast.AstNode`: 所有AST节点的基类接口。
  - `compiler.parser.ast.*`: AST节点的具体实现，按 DDL, DML, DCL 等职责清晰地划分在不同包下。例如 `SelectStatementNode`, `CreateTableStatementNode`。
- **实现细节**:
  - 采用**递归下降 (Recursive Descent)** 的方法进行语法分析，为每一种语法结构（如`statement`, `expression`）编写一个独立的解析方法。
  - 支持丰富的SQL语法，包括：
    - **DDL**: `CREATE/DROP TABLE/DATABASE/INDEX`, `ALTER TABLE`。
    - **DML**: `INSERT`, `UPDATE`, `DELETE`, `SELECT`。
    - **DCL**: `CREATE USER`, `GRANT`。
    - **查询子句**: `JOIN...ON`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`。
  - AST节点的设计与SQL结构高度对应。例如，`SelectStatementNode` 包含了 `selectList`, `fromTable`, `whereClause` 等属性，直观地反映了 `SELECT` 语句的各个部分。
  - 支持点分表示法 (`table.column`)，并在 `IdentifierNode` 中存储了表限定符信息。

### 3.3 语义分析器 (Semantic Analyzer)

- **职责**: 验证AST的逻辑正确性，确保查询在当前数据库模式下是有意义的。
- **核心类**:
  - `compiler.semantic.SemanticAnalyzer`: 语义分析器主类。
- **实现细节**:
  - **依赖注入**: 构造时接收一个 `Catalog` 实例，作为元数据的唯一来源。
  - **核心检查项**:
    - **存在性检查**: 验证 `FROM` 或 `JOIN` 子句中引用的表是否存在。
    - **列有效性检查**: 验证 `SELECT`, `WHERE`, `ON`, `GROUP BY` 等子句中引用的列是否存在于对应的表中。
    - **歧义性检查**: 当 `JOIN` 操作中出现未指定表限定符的同名列时，抛出“列名不明确”的错误。
    - **类型检查**: 验证 `WHERE` 子句和 `INSERT`/`UPDATE` 语句中的值与列定义的数据类型是否兼容。
    - **权限检查**: 基于传入的 `Session` 对象，查询 `Catalog` 以验证当前用户是否具有执行该操作的权限（如 `SELECT`, `INSERT`, `CREATE TABLE` 等）。
  - **错误处理**: 当发现语义错误时，抛出 `SemanticException`，并提供清晰的错误信息。

### 3.4 计划生成器 (Planner)

- **职责**: 将通过语义验证的AST转换为物理执行计划树，并执行基础的查询优化。
- **核心类**:
  - `compiler.planner.Planner`: 计划生成器主类。
  - `compiler.planner.plan.PlanNode`: 所有物理计划节点的抽象基类。
  - `compiler.planner.plan.*`: 物理计划节点的具体实现，如 `SeqScanPlanNode`, `IndexScanPlanNode`, `FilterPlanNode`, `JoinPlanNode` 等。
- **实现细节**:
  - **AST 遍历**: 递归地遍历AST，为每个AST节点生成一个或多个对应的 `PlanNode`。
  - **基础查询优化**:
    - **索引选择**: 对于 `SELECT` 语句，`Planner` 会检查 `WHERE` 子句中的条件。如果发现形如 `indexed_column = value` 的等值查询，并且该列上存在索引，它会生成一个 `IndexScanPlanNode` 来替代低效的 `SeqScanPlanNode`。
    - **谓词下推 (Predicate Pushdown)**: `Planner` 将 `WHERE` 子句中的过滤条件（谓词）直接下推到扫描节点（`SeqScanPlanNode`）中。这使得数据在从磁盘读取的最初阶段就能被过滤，极大地减少了上层执行器需要处理的数据量。
  - **计划树构建**:
    - 对于 `SELECT name FROM users WHERE id > 10;` 这样的查询，`Planner` 会构建一棵 `ProjectPlanNode(SeqScanPlanNode)` 的计划树。
    - `SeqScanPlanNode` 负责扫描 `users` 表，并根据下推的谓词 `id > 10` 进行过滤。
    - `ProjectPlanNode` 则负责从扫描结果中只提取出 `name` 这一列。

## 4. 与其他模块的交互

- **与执行引擎 (Execution Engine)**:
  - **输出**: 编译器是执行引擎的上游。`Planner` 生成的物理执行计划树是 `ExecutionEngine` 的唯一输入。
- **与元数据管理器 (Catalog)**:
  - **依赖**: `SemanticAnalyzer` 和 `Planner` 严重依赖 `Catalog` 来获取表、列和索引的元数据，以进行语义验证和查询优化。
- **与客户端/服务器 (CLI)**:
  - **输入**: 编译器直接消费来自服务器层转发的SQL字符串。