package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;

import java.io.BufferedReader;
import java.io.IOException; // 1. 引入 IOException
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class InteractiveShell {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9999;

        System.out.println("Attempting to connect to MiniDB server at " + host + ":" + port + "...");

        try (
                Socket socket = new Socket(host, port);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                Scanner consoleScanner = new Scanner(System.in)
        ) {
            System.out.println("Successfully connected to MiniDB server. Type 'exit;' to quit.");

            StringBuilder commandBuilder = new StringBuilder();

            while (true) {
                System.out.print("MiniDB-client> ");
                String line = consoleScanner.nextLine();

                commandBuilder.append(line.trim()).append(" ");
                if (!line.trim().endsWith(";")) {
                    continue;
                }

                String sql = commandBuilder.toString();
                commandBuilder.setLength(0);

                if (sql.equalsIgnoreCase("exit; ")) {
                    break;
                }

                // 将SQL语句发送到服务器
                out.println(sql);

                // 从服务器读取结果并打印
                String serverResponse = in.readLine();
                if (serverResponse != null) {
                    // 将服务器返回的特殊标记替换回换行符
                    System.out.println(serverResponse.replace("<br>", "\n"));
                } else {
                    System.out.println("Connection to server lost.");
                    break;
                }
            }

        } catch (Exception e) {
            System.err.println("Could not connect to server: " + e.getMessage());
        }
        System.out.println("Bye!");
    }
}