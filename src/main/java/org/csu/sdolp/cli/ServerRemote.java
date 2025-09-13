package org.csu.sdolp.cli;

import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.transaction.RecoveryManager;
import org.csu.sdolp.catalog.Catalog;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerRemote {
    public static void main(String[] args) throws Exception {

        // 我们不再在启动时初始化一个全局的 QueryProcessor 和 RecoveryManager
        // 恢复操作将在首次加载数据库时在 QueryProcessor 构造函数内部隐式完成。
        System.out.println("MiniDB server template started. Ready to create database handlers per connection.");

        int port = 9999;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Listening on port " + port + " for MySQL protocol connections...");

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            // 如果有全局资源需要清理，可以在这里添加
        }));

        AtomicInteger connectionCounter = new AtomicInteger(0);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            int connectionId = connectionCounter.incrementAndGet();
            System.out.println("Client connected: " + clientSocket.getInetAddress() + " (Connection ID: " + connectionId + ")");

            // 注意：我们不再传递 catalog，因为每个 handler 将管理自己的 QueryProcessor 实例
            MysqlProtocolHandler handler = new MysqlProtocolHandler(clientSocket, connectionId);
            new Thread(handler).start();
        }
    }
}