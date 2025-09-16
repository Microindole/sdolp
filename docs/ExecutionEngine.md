# 模块二详解· 执行引擎 (Execution Engine)

## 1. 模块概述

如果说SQL编译器是MiniDB的“大脑”，那么执行引擎就是其强健有力的“四肢”。它负责将编译器精心策划的**物理执行计划 (Plan Tree)** 转化为对数据的实际操作。本模块是连接逻辑查询计划与物理数据存储的桥梁，其执行效率直接决定了数据库的整体性能。

MiniDB的执行引擎采用了经典的**火山模型（Volcano Model）**，也被称为**迭代器模型（Iterator Model）**。在这个模型中，查询计划树中的每一个节点都被实现为一个“执行器”或“算子”（Executor/Operator）。数据以元组（Tuple）为单位，像火山喷发一样，从底层的执行器（数据源）被“拉取”到上层的执行器，经过层层处理，最终在根节点汇集并输出给客户端。

**核心职责**: 接收来自编译器的物理执行计划树，构建一个与之对应的**执行器树 (Executor Tree)**，并通过标准的迭代器接口 (`TupleIterator`) 从上至下地拉取并处理数据，最终完成整个查询。

## 2. 总体架构与数据流

执行引擎的架构是其设计的精髓所在。它通过一个统一的接口和递归的构建方式，将复杂的查询优雅地分解为一系列简单、可组合的操作。

**架构图与数据流**:

```
[物理执行计划树 (PlanNode Tree)]
      |
      V
[ 1. ExecutionEngine.execute() ]
      |
      V
[ 2. 递归构建执行器树 (Executor Tree) ]
      |
      |   +-------------------+
      |   | ProjectExecutor   | <--- 4. 根节点调用 next() 开始拉取数据
      |   +-------------------+
      |             ^
      |             | next()
      |   +-------------------+
      |   | FilterExecutor    |
      |   +-------------------+
      |             ^
      |             | next()
      |   +-------------------+
      |   | SeqScanExecutor   | <--- 3. 叶子节点从存储引擎读取物理数据
      |   +-------------------+
      |             ^
      |             | read_tuple()
      +-------------------------+
      |      存储引擎 (Storage)    |
      +-------------------------+
```

**流程详解**:

1. **输入**: `engine.ExecutionEngine` 接收一个由 `Planner` 生成的 `PlanNode` 树的根节点。
2. **递归构建**: `ExecutionEngine` 从根节点开始，递归地遍历计划树。对于每一个 `PlanNode` 节点，它都会实例化一个对应的 `Executor`。例如，`ProjectPlanNode` 会被实例化为 `ProjectExecutor`。如果一个计划节点有子节点，`ExecutionEngine` 会先为子节点构建执行器，然后将子执行器作为参数传递给父执行器的构造函数，从而自底向上地构建出一棵完整的执行器树。
3. **数据拉取**: 执行器树构建完成后，查询处理器会调用树的根执行器的 `next()` 方法。
4. **火山模型**:
   - 根执行器（如 `ProjectExecutor`）为了产出一个元组，会调用其子执行器（如 `FilterExecutor`）的 `next()` 方法。
   - 这个调用链会一直向下传递，直到叶子节点执行器（如 `SeqScanExecutor`）。
   - 叶子节点执行器与**存储引擎**交互，从磁盘读取物理数据页，并将其解析为一个元组返回。
   - 元组被返回给其父执行器（`FilterExecutor`），后者对其应用过滤条件。如果元组满足条件，则将其传递给更上层的父执行器（`ProjectExecutor`），否则继续向子节点请求下一个元组。
   - 这个“拉取-处理-向上传递”的过程不断重复，直到整个查询完成。

## 3. 核心组件详解

### 3.1 统一迭代器接口 (`TupleIterator`)

这是火山模型的基石，所有执行器都必须实现这个接口。

- **核心接口**: `executor.TupleIterator`
- **核心方法**:
  - `boolean hasNext()`: 检查是否还有更多的元组可以处理。
  - `Tuple next()`: 获取并返回下一个处理完毕的元组。
  - `Schema getOutputSchema()`: 描述该执行器输出的元组的结构。

### 3.2 执行引擎调度器 (`ExecutionEngine`)

- **职责**: 将 `PlanNode` 树转换为 `Executor` 树的工厂和调度中心。
- **核心类**: `engine.ExecutionEngine`
- **实现细节**:
  - 其核心是一个巨大的 `execute()` 方法，内部使用 `instanceof` 来判断 `PlanNode` 的具体类型，并据此创建对应的 `Executor` 实例。
  - 通过依赖注入的方式，持有对 `BufferPoolManager`, `Catalog`, `LogManager`, `LockManager` 等核心模块的引用，并将它们传递给需要这些依赖的执行器。

### 3.3 执行器 (Executors / Operators)

执行器是物理操作的具体实现者，位于 `executor` 包下，并按 DDL/DML/DCL 等职责进行了清晰的划分。

#### 3.3.1 数据访问执行器 (叶子节点)

它们是执行器树的叶子，负责从数据源（表或索引）产生元组。

- `SeqScanExecutor`:
  - **职责**: 执行全表扫描。
  - **实现**: 内部持有一个 `TableHeap` 实例，通过调用 `tableHeap.next()` 来逐一迭代表中的所有元组。如果接收到了下推的谓词（`AbstractPredicate`），则在返回元组前会先进行一次过滤。
- `IndexScanExecutor`:
  - **职责**: 执行索引扫描。
  - **实现**: 接收一个 `IndexInfo` 和一个 `searchKey`。它使用 `BPlusTree.search()` 方法直接定位到符合条件的记录标识符（`RID`），然后通过 `TableHeap.getTuple(rid)` 获取完整的元组。这极大地减少了需要扫描的数据量。

#### 3.3.2 数据修改执行器

这些执行器负责处理 DML 语句，它们通常不向上层返回数据流，而是返回一个表示“影响行数”的单一元组。

- `InsertExecutor`:
  - **职责**: 执行 `INSERT` 语句。
  - **实现**: 接收一个包含待插入数据的 `InsertPlanNode`。对于每一条待插入的元组，它会：
    1. 检查主键唯一性（如果存在主键索引）。
    2. 调用 `TableHeap.insertTuple()` 将元组写入数据页。
    3. 更新该表上的所有索引。
- `DeleteExecutor`:
  - **职责**: 执行 `DELETE` 语句。
  - **实现**: 它的子执行器（通常是 `SeqScanExecutor`）会提供所有待删除的元组。它遍历这些元组，对每一个元组：
    1. 调用 `TableHeap.deleteTuple()` 将元组从数据页中删除。
    2. 更新该表上的所有索引。
- `UpdateExecutor`:
  - **职责**: 执行 `UPDATE` 语句。
  - **实现**: 与 `DeleteExecutor` 类似，它从子执行器获取所有待更新的旧元组。对每一个元组：
    1. 根据 `SET` 子句构建新元组。
    2. 调用 `TableHeap.updateTuple()`，该操作在逻辑上是“删除旧元组+插入新元组”。
    3. 更新所有相关索引。

#### 3.3.3 关系代数执行器 (中间节点)

这些执行器是构成复杂查询的核心，它们接收来自子执行器的数据流，进行处理后，再向上层提供新的数据流。

- `ProjectExecutor`:
  - **职责**: 实现 `SELECT` 子句的投影操作。
  - **实现**: 接收一个待保留列的索引列表。对于从子节点获取的每一个元组，它会根据索引列表构建一个只包含指定列的新元组。
- `FilterExecutor`:
  - **职责**: 实现 `WHERE` 子句的过滤操作。
  - **实现**: 接收一个谓词表达式 (`AbstractPredicate`)。它不断从子节点获取元组，并对每个元组应用谓词。只有当 `predicate.evaluate(tuple)` 返回 `true` 时，该元组才会被向上传递。
- `JoinExecutor`:
  - **职责**: 实现 `JOIN...ON` 操作。
  - **实现**: 采用**嵌套循环连接 (Nested Loop Join)** 算法。它会将右侧（内层）子执行器的所有元组一次性加载到内存中，然后遍历左侧（外层）子执行器的每一个元组，并将其与内存中的所有右侧元组进行匹配。只有满足 `ON` 条件的组合元组才会被向上返回。
- `AggregateExecutor`:
  - **职责**: 实现 `GROUP BY` 和聚合函数（`COUNT`, `SUM` 等）。
  - **实现**: 这是一个**阻塞式 (blocking)** 执行器。它会先拉取子执行器的所有元组，然后在内存中的一个 `HashMap` 中进行**哈希聚合**。`Map` 的键是分组键，值是中间聚合结果。所有子元组处理完毕后，它才开始向上层返回最终的聚合结果。
- `SortExecutor` 和 `LimitExecutor`:
  - `SortExecutor` 也是一个阻塞式执行器，它会拉取所有数据到内存中，使用 `Collections.sort()` 进行排序，然后再逐条返回。
  - `LimitExecutor` 则是一个简单的流式执行器，它只向上层返回指定数量的元组便停止。

## 4. 与其他模块的交互

- **与编译器 (Compiler)**:
  - **输入**: 执行引擎是编译器的下游。`ExecutionEngine` 的 `execute()` 方法直接接收 `Planner` 生成的 `PlanNode` 树。
- **与存储引擎 (Storage Engine)**:
  - **依赖**: 叶子节点执行器（`SeqScanExecutor`, `IndexScanExecutor`）和DML执行器都通过 `TableHeap` 抽象层与存储引擎交互，间接使用了 `BufferPoolManager` 和 `DiskManager`。
- **与事务与并发控制 (Transaction & Concurrency)**:
  - **依赖**: 所有需要访问或修改数据的执行器都在一个**事务 (`Transaction`)** 的上下文中运行。它们通过 `TableHeap` 间接与 `LockManager` 和 `LogManager` 交互，以确保操作的隔离性和持久性。
- **与元数据管理器 (Catalog)**:
  - **依赖**: DDL和DCL执行器（如 `CreateTableExecutor`, `GrantExecutor`）会直接调用 `Catalog` 的方法来修改元数据。