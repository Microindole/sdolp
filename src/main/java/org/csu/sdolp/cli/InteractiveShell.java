package org.csu.sdolp.cli;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
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
            System.out.println("Successfully connected to MiniDB server. Type 'exit;' to quit.");

            StringBuilder commandBuilder = new StringBuilder();

            while (true) {
                System.out.print("miniDB-client> ");
                String line = consoleScanner.nextLine();

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
}