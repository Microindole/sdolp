package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;
import java.io.IOException; // 1. 引入 IOException
import java.util.Scanner;

public class InteractiveShell {

    public static void main(String[] args) {
        QueryProcessor queryProcessor = new QueryProcessor("minidb.data");
        System.out.println("Welcome to MiniDB. Type 'exit;' to quit.");

        Scanner scanner = new Scanner(System.in);
        StringBuilder commandBuilder = new StringBuilder();

        while (true) {
            System.out.print("minidb> ");
            String line = scanner.nextLine();

            commandBuilder.append(line.trim()).append(" ");
            if (!line.trim().endsWith(";")) {
                continue;
            }

            String sql = commandBuilder.toString();
            commandBuilder.setLength(0);

            if (sql.equalsIgnoreCase("exit; ")) {
                System.out.println("Bye!");
                break; // 跳出循环
            }

            try {
                queryProcessor.execute(sql);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
        scanner.close();

        // --- 2. 在循环结束后，关闭 queryProcessor ---
        try {
            System.out.println("Shutting down database...");
            queryProcessor.close();
            System.out.println("Database shut down successfully.");
        } catch (IOException e) {
            System.err.println("Error during database shutdown: " + e.getMessage());
            e.printStackTrace();
        }
    }
}