package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) throws Exception {
        // 1. 在服务器启动时，创建唯一的数据库引擎实例
        final QueryProcessor queryProcessor = new QueryProcessor("minidb.data");

        // 2. 监听一个端口 (例如 9999)
        int port = 9999;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("MiniDB server started. Listening on port " + port + "...");

        // 添加一个钩子，在服务器关闭时安全地关闭数据库
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
            // 3. 接受客户端连接，这是一个阻塞操作
            Socket clientSocket = serverSocket.accept();

            // 4. 为每个客户端创建一个新的线程来处理它的请求
            new Thread(() -> {
                System.out.println("Client connected: " + clientSocket.getInetAddress());
                try (
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
                ) {
                    String sql;
                    while ((sql = in.readLine()) != null) {
                        if ("exit;".equalsIgnoreCase(sql.trim())) {
                            break;
                        }
                        // 从客户端读取SQL，交给唯一的引擎执行，并获取结果字符串
                        String result = queryProcessor.executeAndGetResult(sql);
                        // 将结果字符串中的换行符替换为特殊标记，以便客户端正确解析
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