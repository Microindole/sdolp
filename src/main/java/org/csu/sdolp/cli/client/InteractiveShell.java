package org.csu.sdolp.cli.client;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Scanner;

public class InteractiveShell {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 8848;

        System.out.println("Attempting to connect to MiniDB server at " + host + ":" + port + "...");

        try (
                Socket socket = new Socket(host, port);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                Scanner consoleScanner = new Scanner(System.in)
        ) {
            // 1. 读取服务器的欢迎信息
            String serverWelcome = in.readLine();
            System.out.println("Server: " + serverWelcome);

            // 2. 提示用户输入用户名
            System.out.print("Enter username: ");
            String username = consoleScanner.nextLine();
            out.println(username); // 将用户名发送给服务器

            // 3. 读取服务器的登录确认信息
            String loginResponse = in.readLine();
            System.out.println("Server: " + loginResponse);

            System.out.println("Type 'exit;' to quit.");

            String currentDb = "default";

            StringBuilder commandBuilder = new StringBuilder();
            while (true) {
                // 根据登录的用户名显示不同的提示符
                System.out.print(username + "@" + currentDb + "> ");
                String line = consoleScanner.nextLine();
                //自动导入
                if (line.equalsIgnoreCase("import;")) {
                    handleGraphicalImport(consoleScanner, out, in);
                    continue;
                }
                //手动导入
                if (line.trim().toLowerCase().startsWith("source ")) {
                    executeSqlFile(line.trim(), out, in);
                    continue; // 执行完文件后，继续下一次循环
                }


                commandBuilder.append(line.trim()).append(" ");
                if (!line.trim().endsWith(";")) {
                    System.out.print("           -> ");
                    continue;
                }

                String allSqlCommands = commandBuilder.toString();
                commandBuilder.setLength(0);

                if (allSqlCommands.equalsIgnoreCase("exit; ")) {
                    break;
                }

                String[] sqlStatements = allSqlCommands.split(";");

                for (String sql : sqlStatements) {
                    String singleSql = sql.trim();
                    if (singleSql.isEmpty()) {
                        continue;
                    }
                    out.println(singleSql + ";");

                    String serverResponse = in.readLine();
                    if (serverResponse != null) {
                        if (serverResponse.toLowerCase().startsWith("database changed to")) {
                            currentDb = singleSql.split("\\s+")[1].replace(";", "");
                        }
                        System.out.println(serverResponse.replace("<br>", "\n"));
                    } else {
                        System.out.println("Connection to server lost.");
                        return;
                    }
                }
                System.out.println();
            }

        } catch (Exception e) {
            System.err.println("Could not connect to server: " + e.getMessage());
        }
        System.out.println("Bye!");
    }
    private static void executeSqlFile(String sourceCommand, PrintWriter out, BufferedReader in) {
        String filePath = sourceCommand.substring("source".length()).trim();
        if (filePath.endsWith(";")) {
            filePath = filePath.substring(0, filePath.length() - 1);
        }

        File sqlFile = new File(filePath);
        if (!sqlFile.exists()) {
            System.err.println("ERROR: File not found: " + sqlFile.getAbsolutePath());
            return;
        }

        System.out.println("Executing SQL script from: " + filePath);
        try {
            String content = new String(Files.readAllBytes(sqlFile.toPath()));

            content = content.replaceAll("(?m)^--.*$", "");


            String[] statements = content.split(";");

            for (String statement : statements) {

                String trimmedStatement = statement.trim().replaceAll("\\s+", " ");

                if (!trimmedStatement.isEmpty()) {
                    System.out.println("-> " + trimmedStatement);

                    sendCommand(trimmedStatement + ";", out, in);
                }
            }
            System.out.println("Finished executing script.");
        } catch (IOException e) {
            System.err.println("Error reading or executing file: " + e.getMessage());
        }
    }

    private static void sendCommand(String command, PrintWriter out, BufferedReader in) throws IOException {
        out.println(command);
        String serverResponse = in.readLine();
        if (serverResponse != null) {
            System.out.println(serverResponse.replace("<br>", "\n"));
        } else {
            System.err.println("Connection lost during script execution.");
            throw new IOException("Connection lost");
        }
    }

    private static void handleGraphicalImport(Scanner scanner, PrintWriter out, BufferedReader in) {
        System.out.println("Opening file chooser...");
        String filePath = selectSqlFile(scanner);

        if (filePath != null && !filePath.isEmpty()) {
            executeSqlFile("source " + filePath + ";", out, in);
        } else {
            System.out.println("File selection cancelled.");
        }
    }

    private static String selectSqlFile(Scanner scanner) {
        try {
            JFileChooser chooser = new JFileChooser();
            chooser.setDialogTitle("请选择要导入的 SQL 文件");
            // 设置文件过滤器，只显示 .sql 文件
            FileNameExtensionFilter filter = new FileNameExtensionFilter("SQL Scripts (*.sql)", "sql");
            chooser.setFileFilter(filter);
            chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);

            int returnValue = chooser.showOpenDialog(null);
            if (returnValue == JFileChooser.APPROVE_OPTION) {
                return chooser.getSelectedFile().getAbsolutePath();
            } else {
                return null; // 用户取消了选择
            }
        } catch (HeadlessException e) {
            // 如果在没有图形界面的环境中运行，则降级为手动输入
            System.out.println("\n检测到非图形界面环境，请手动输入 SQL 文件的完整路径：");
            System.out.print("File path: ");
            return scanner.nextLine().trim();
        }
    }
}


