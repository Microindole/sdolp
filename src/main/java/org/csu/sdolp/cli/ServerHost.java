package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.transaction.RecoveryManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerHost {
    public static void main(String[] args) throws Exception {
        final QueryProcessor queryProcessor = new QueryProcessor("minidb.data");

        RecoveryManager recoveryManager = new RecoveryManager(
                queryProcessor.getLogManager(),
                queryProcessor.getBufferPoolManager(),
                queryProcessor.getCatalog(),
                queryProcessor.getLockManager()
        );
        recoveryManager.recover();

        int port = 8848;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("MiniDB server started. Listening on port " + port + "...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Shutting down database engine...");
                queryProcessor.close();
                System.out.println("Database engine shut down successfully.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        while (true) {
            Socket clientSocket = serverSocket.accept();

            new Thread(() -> {
                System.out.println("Client connected: " + clientSocket.getInetAddress());
                try (
                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
                ) {
                    // --- 核心修改：在这里接收用户名并创建Session ---
                    out.println("Welcome! Please enter your username:"); // 向客户端发送提示
                    String username = in.readLine(); // 读取客户端发来的第一行作为用户名
                    if (username == null || username.trim().isEmpty()) {
                        System.out.println("Client failed to provide a username. Closing connection.");
                        clientSocket.close();
                        return;
                    }

                    // 使用收到的用户名创建一个已认证的Session
                    Session session = Session.createAuthenticatedSession(clientSocket.hashCode(), username.trim());
                    System.out.println("User '" + session.getUsername() + "' logged in from local shell.");
                    out.println("Login successful as " + session.getUsername() + ". You can now execute commands."); // 向客户端确认登录成功

                    String sql;
                    while ((sql = in.readLine()) != null) {
                        if ("exit;".equalsIgnoreCase(sql.trim())) {
                            break;
                        }
                        // --- 核心修改：调用需要Session参数的executeAndGetResult方法 ---
                        String result = queryProcessor.executeAndGetResult(sql, session);
                        out.println(result.replace("\n", "<br>"));
                    }
                } catch (Exception e) {
                    System.err.println("Error handling client: " + e.getMessage());
                } finally {
                    try {
                        clientSocket.close();
                        System.out.println("Client disconnected: " + clientSocket.getInetAddress());
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }).start();
        }
    }
}