package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.transaction.RecoveryManager;
import org.csu.sdolp.catalog.Catalog;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerRemote {
    public static void main(String[] args) throws Exception {
        // --- 核心修复：使用一个明确的数据库名，而不是文件名 ---
        final String DEFAULT_DB_NAME = "default";
        // 1. 启动数据库引擎，执行恢复
        final QueryProcessor queryProcessor = new QueryProcessor(DEFAULT_DB_NAME);
        RecoveryManager recoveryManager = new RecoveryManager(
                queryProcessor.getLogManager(),
                queryProcessor.getBufferPoolManager(),
                queryProcessor.getCatalog(),
                queryProcessor.getLockManager()
        );
        recoveryManager.recover();

        int port = 9999;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("MiniDB server started on database '"+ DEFAULT_DB_NAME +"'. Listening on port " + port + " for MySQL protocol connections...");

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
            int connectionId = connectionCounter.incrementAndGet();
            System.out.println("Client connected: " + clientSocket.getInetAddress() + " (Connection ID: " + connectionId + ")");
            Catalog catalog = queryProcessor.getCatalog();
            MysqlProtocolHandler handler = new MysqlProtocolHandler(clientSocket, queryProcessor, catalog, connectionId);
            new Thread(handler).start();
        }
    }
}