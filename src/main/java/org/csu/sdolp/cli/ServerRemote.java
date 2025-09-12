package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.transaction.RecoveryManager;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerRemote {
    // ... main 方法和其他内容保持不变 ...
    public static void main(String[] args) throws Exception {
        // 1. 启动数据库引擎，执行恢复
        final QueryProcessor queryProcessor = new QueryProcessor("minidb.data");
        RecoveryManager recoveryManager = new RecoveryManager(
                queryProcessor.getLogManager(),
                queryProcessor.getBufferPoolManager(),
                queryProcessor.getCatalog(),
                queryProcessor.getLockManager()
        );
        recoveryManager.recover();

        int port = 9999;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("MiniDB server started. Listening on port " + port + " for MySQL protocol connections...");

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Shutting down database engine...");
                queryProcessor.close();
                System.out.println("Database engine shut down successfully.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        AtomicInteger connectionCounter = new AtomicInteger(0);

        while (true) {
            Socket clientSocket = serverSocket.accept();

            // 2. **核心修改**: 为每个客户端连接启动一个新的 MysqlProtocolHandler 线程
            int connectionId = connectionCounter.incrementAndGet();
            System.out.println("Client connected: " + clientSocket.getInetAddress() + " (Connection ID: " + connectionId + ")");

            MysqlProtocolHandler handler = new MysqlProtocolHandler(clientSocket, queryProcessor, connectionId);
            new Thread(handler).start();
        }
    }
}