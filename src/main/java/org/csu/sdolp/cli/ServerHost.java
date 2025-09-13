package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServerHost {
    public static void main(String[] args) throws Exception {

        // 使用线程安全的 Map 来存储已加载的数据库实例
        final Map<String, QueryProcessor> queryProcessorMap = new ConcurrentHashMap<>();

        int port = 8848;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("MiniDB server started. Listening on port " + port + "...");

        // 预先加载默认数据库，这将触发它的恢复流程
        System.out.println("Pre-loading default database...");
        queryProcessorMap.put("default", new QueryProcessor("default"));
        System.out.println("Default database loaded.");


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down database engine...");
            queryProcessorMap.values().forEach(qp -> {
                try {
                    qp.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("Database engine shut down successfully.");
        }));

        while (true) {
            Socket clientSocket = serverSocket.accept();
            new Thread(() -> {
                System.out.println("Client connected: " + clientSocket.getInetAddress());

                String currentDbName = "default";
                QueryProcessor currentQueryProcessor = queryProcessorMap.get(currentDbName);

                try (
                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
                ) {
                    out.println("Welcome! Please enter your username:");
                    String username = in.readLine();
                    if (username == null || username.trim().isEmpty()) {
                        clientSocket.close();
                        return;
                    }
                    Session session = Session.createAuthenticatedSession(clientSocket.hashCode(), username.trim());
                    out.println("Login successful as " + session.getUsername() + ". You can now execute commands.");

                    String sql;
                    while ((sql = in.readLine()) != null) {
                        if ("exit;".equalsIgnoreCase(sql.trim())) {
                            break;
                        }

                        if (sql.trim().toLowerCase().startsWith("use ")) {
                            try {
                                String[] parts = sql.trim().split("\\s+");
                                String dbName = parts[1].replace(";", "");

                                // 使用 computeIfAbsent 来原子性地检查和创建实例
                                // 这会确保每个数据库的 QueryProcessor (包括其恢复流程) 只被创建一次
                                currentQueryProcessor = queryProcessorMap.computeIfAbsent(dbName, k -> new QueryProcessor(k));
                                currentDbName = dbName;

                                out.println("Database changed to " + dbName);
                            } catch (Exception e) {
                                out.println("ERROR: " + e.getMessage());
                            }
                        } else {
                            String result = currentQueryProcessor.executeAndGetResult(sql, session);
                            out.println(result.replace("\n", "<br>"));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error handling client: " + e.getMessage());
                } finally {
                    try {
                        clientSocket.close();
                        System.out.println("Client disconnected: " + clientSocket.getInetAddress());
                    } catch (IOException e) {}
                }
            }).start();
        }
    }
}