package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor; // 引入核心引擎

import java.util.Scanner;

public class InteractiveShell {

    public static void main(String[] args) {
        // 1. 初始化数据库核心引擎
        // 引擎负责加载 Catalog, BufferPoolManager 等所有底层服务
        QueryProcessor queryProcessor = new QueryProcessor("minidb.data");
        System.out.println("Welcome to MiniDB. Type 'exit;' to quit.");

        // 2. 启动一个循环来读取用户输入
        Scanner scanner = new Scanner(System.in);
        StringBuilder commandBuilder = new StringBuilder();

        while (true) {
            // 提示符
            System.out.print("minidb> ");
            String line = scanner.nextLine();

            // 简单的多行指令处理，直到遇到分号
            commandBuilder.append(line.trim()).append(" ");
            if (!line.trim().endsWith(";")) {
                continue;
            }

            String sql = commandBuilder.toString();
            commandBuilder.setLength(0); // 清空

            if (sql.equalsIgnoreCase("exit; ")) {
                System.out.println("Bye!");
                break;
            }

            try {
                // 3. 将完整的SQL指令交给引擎执行
                queryProcessor.execute(sql);
            } catch (Exception e) {
                // 统一的异常处理
                System.err.println("Error: " + e.getMessage());
            }
        }
        scanner.close();
    }
}