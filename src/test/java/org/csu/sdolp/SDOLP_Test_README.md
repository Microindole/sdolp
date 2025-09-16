# SDOLP_Test_README

## 1. 简介

本文档旨在提供一份关于 `comprehensive_test.sql` 脚本的逐行执行指南。它详细说明了每条SQL语句的测试目的和预期的系统行为，旨在对 MiniDB 数据库系统的核心功能进行一次全面的、端到端的验证。

**建议环境:**

- **客户端:** `AdvancedShell` GUI 客户端或兼容的交互式 SQL 客户端。
- **执行方式:** 为达到最佳测试效果，建议逐条或分阶段复制并执行SQL命令，以便仔细观察每个步骤的输出结果。

## 2. 详细测试步骤与预期结果

### **阶段一：数据库和表的基本操作 (DDL & DML)**

此阶段验证数据库系统最基础的数据定义和数据操作能力。

**1. 创建数据库**

- **语句:**

  ```sql
  CREATE DATABASE test_sdolp;
  ```

- **目的:** 测试数据库的创建功能。

- **预期结果:** 命令成功执行。系统在指定数据目录下成功创建一个名为 `test_sdolp` 的数据库。

**2. 切换当前数据库上下文**

- **语句:**

  ```sql
  USE test_sdolp;
  ```

- **目的:** 测试切换数据库上下文的功能。

- **预期结果:** 命令成功执行。客户端的会话状态切换到 `test_sdolp` 数据库。

**3. 创建表并定义主键**

- **语句:**

  ```sql
  CREATE TABLE sales_regions (id INT PRIMARY KEY,region_name VARCHAR(50));
  
  CREATE TABLE sales_records (id INT PRIMARY KEY,product_id INT,region_id INT,amount INT);
  ```

- **目的:** 测试表的创建以及主键（PRIMARY KEY）约束的定义。

- **预期结果:** `sales_regions` 和 `sales_records` 两张表被成功创建。

**4. 主键约束验证**

- **语句:**

  ```sql
  CREATE TABLE pk_test (id INT PRIMARY KEY,description VARCHAR(100));
  
  INSERT INTO pk_test (id, description) VALUES (1, 'Initial record');
  ```

- **目的:** 为主键唯一性测试准备环境。

- **预期结果:** `pk_test` 表被创建，第一条记录成功插入。

- **语句:**

  ```sql
  INSERT INTO pk_test (id, description) VALUES (1, 'Attempt to insert duplicate PK');
  ```

- **目的:** **【核心测试点】** 验证主键的唯一性约束是否生效。

- **预期结果:** 此命令 **必须失败**。数据库应返回一个明确的错误信息，指出违反了主键约束 (例如: "Primary key constraint violation")。

**5. 插入测试数据**

- **语句:**

  ```sql
  INSERT INTO sales_regions (id, region_name) VALUES (1, 'North');
  INSERT INTO sales_regions (id, region_name) VALUES (2, 'South');
  INSERT INTO sales_regions (id, region_name) VALUES (3, 'East');
  
  INSERT INTO sales_records (id, product_id, region_id, amount) VALUES (101, 1, 1, 500);
  INSERT INTO sales_records (id, product_id, region_id, amount) VALUES (102, 2, 1, 1500);
  INSERT INTO sales_records (id, product_id, region_id, amount) VALUES (103, 3, 1, 200);
  INSERT INTO sales_records (id, product_id, region_id, amount) VALUES (201, 4, 2, 3000);
  INSERT INTO sales_records (id, product_id, region_id, amount) VALUES (202, 5, 2, 4000);
  INSERT INTO sales_records (id, product_id, region_id, amount) VALUES (301, 6, 3, 100);
  ```

- **目的:** 测试向表中批量插入数据的能力。

- **预期结果:** 所有 `INSERT` 语句成功执行。

**6. 验证数据插入**

- **语句:**

  ```sql
  SELECT * FROM sales_regions;
  SELECT * FROM sales_records;
  ```

- **目的:** 验证数据已按预期写入表中。

- **预期结果:** 查询结果以表格形式正确显示了所有刚刚插入的数据。

### **阶段二：用户与权限管理 (DCL)**

此阶段验证数据库的访问控制机制。**此阶段需要管理员（如'root'）权限，并需要手动切换用户身份进行验证。**

**1. 创建新用户**

- **语句:**

  ```sql
  CREATE USER 'testuser' IDENTIFIED BY 'password';
  ```

- **目的:** 测试用户创建功能。

- **预期结果:** 系统成功创建一个新用户 `testuser`。

**2. 权限授予前后对比**

- **授权前 (需手动验证):**

  - **操作:** 重新连接数据库，使用用户名 `testuser` 和密码 `password` 登录。
  - **执行语句:** `SELECT * FROM sales_regions;`
  - **预期结果:** 操作失败，返回权限不足的错误 (例如: "Access Denied")。

- **授予权限 (需`root`权限):**

  - **语句:**

    ```sql
    GRANT SELECT ON sales_regions TO 'testuser';
    ```

  - **目的:** 测试权限授予功能。

  - **预期结果:** 命令成功执行。

- **授权后 (需手动验证):**

  - **操作:** 切换回 `testuser` 的会话。
  - **执行语句:** `SELECT * FROM sales_regions;`
  - **预期结果:** 查询成功执行，并返回 `sales_regions` 表中的数据。

### **阶段三：索引功能测试**

此阶段验证索引的创建及其对查询的优化效果。

**1. 创建索引**

- **语句:**

  ```sql
  CREATE INDEX idx_region_id ON sales_records (region_id);
  ```

- **目的:** 测试在表的特定列上创建索引的功能。

- **预期结果:** 命令成功执行。

**2. 验证索引使用**

- **语句:**

  ```sql
  SELECT * FROM sales_records WHERE region_id = 1;
  ```

- **目的:** 验证查询优化器是否会利用新创建的索引。

- **验证方式:** 查看数据库服务器的后台日志。

- **预期结果:** 日志中应显示查询计划正在使用 "Index Scan" 或类似提示，而不是 "Sequential Scan"（全表扫描）。

### **阶段四：SQL关键字与复杂查询**

此阶段测试数据库对标准SQL功能的实现程度。

**1. 单独关键字测试**

- **ORDER BY:**

  - **语句:** `SELECT * FROM sales_records ORDER BY amount DESC;`
  - **目的:** 测试降序排序功能。
  - **预期结果:** 返回所有销售记录，并按 `amount` 字段从高到低排列。

- **LIMIT:**

  - **语句:** `SELECT * FROM sales_records ORDER BY amount DESC LIMIT 2;`
  - **目的:** 测试限制返回行数的功能。
  - **预期结果:** 只返回金额最高的2条销售记录。

- **WHERE (AND/OR):**

  - **语句:** `SELECT * FROM sales_records WHERE (amount > 1000 AND region_id = 1) OR region_id = 3;`
  - **目的:** 测试复合逻辑条件的过滤能力。
  - **预期结果:** 返回 region_id 为 1 且 amount 大于 1000 的记录，以及所有 region_id 为 3 的记录。

- **GROUP BY (SUM, COUNT):**

  - **语句:** `SELECT region_id, SUM(amount), COUNT(id) FROM sales_records GROUP BY region_id;`
  - **目的:** 测试分组和聚合函数。
  - **预期结果:** 返回按 `region_id` 分组的统计结果，包含每个区域的总销售额和订单数。

- **SHOW & DROP TABLE:**

  - **语句:**

    ```sql
    SHOW TABLES;
    DROP TABLE pk_test;
    SHOW TABLES;
    ```

  - **目的:** 测试 `SHOW TABLES` 和 `DROP TABLE` 命令。

  - **预期结果:** 第一次 `SHOW TABLES` 显示包含 `pk_test` 在内的所有表。`DROP TABLE` 成功后，第二次 `SHOW TABLES` 的结果中不再包含 `pk_test`。

**2. 究极复杂组合查询**

- **语句:**

  ```sql
  SELECT sales_regions.region_name, SUM(sales_records.amount) 
  FROM sales_records 
  JOIN sales_regions ON sales_records.region_id = sales_regions.id 
  WHERE sales_records.amount > 250 
  GROUP BY sales_regions.region_name 
  HAVING COUNT(sales_records.id) >= 2 
  ORDER BY sales_regions.region_name ASC 
  LIMIT 1;
  ```

- **目的:** 对数据库的查询处理器进行压力测试，验证其在多重复杂条件下的正确性。

- **预期结果:** 查询应成功执行，并返回一个精确的单行结果。该结果是：销售额大于250、订单数不少于2个的区域中，按区域名称排序后的第一个区域及其总销售额。

### **阶段五：事务、锁与恢复 (手动测试指南)**

- 此部分在 Junit 中测试，目前是自动回复，自动提交事务且带有日志

此阶段验证数据库的ACID特性，需要手动模拟。

**1. 事务回滚 (手动测试)**

- **流程:**
  1. 执行 `BEGIN TRANSACTION;` (或系统支持的开启事务命令)。
  2. 执行 `UPDATE sales_regions SET region_name = 'Far East' WHERE id = 3;`
  3. 执行 `SELECT * FROM sales_regions;`，确认在事务内数据已被修改为 'Far East'。
  4. 执行 `ROLLBACK;`。
  5. 再次执行 `SELECT * FROM sales_regions;`。
- **预期结果:** `ROLLBACK` 之后，id 为 3 的区域名称应恢复为 'East'，体现事务的原子性。

**2. 崩溃恢复 (手动测试)**

- **流程:**
  1. 开启一个事务。
  2. 执行 `UPDATE sales_regions SET region_name = 'CRASH_TEST' WHERE id = 1;`
  3. **不提交事务**，直接强行关闭数据库服务器进程 (模拟崩溃)。
  4. 重启数据库服务器。
- **预期结果:**
  - 服务器重启时，恢复管理器应自动运行。
  - 由于之前的事务未提交，其所有操作将被撤销 (UNDO)。
  - 登录后查询 `sales_regions` 表，id 为 1 的记录应仍然是 'North'，而不是 'CRASH_TEST'。

## 3. 总结

成功完成此测试脚本中的所有步骤并观察到所有预期的结果，将有力地证明您的数据库系统在数据操作、用户管理、查询优化和事务处理等核心方面具备了基本的正确性和健壮性。