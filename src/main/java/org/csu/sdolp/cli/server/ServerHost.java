package org.csu.sdolp.cli.server;

import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.StatementNode;
import org.csu.sdolp.compiler.parser.ast.dcl.GrantStatementNode;
import org.csu.sdolp.engine.QueryProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
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

                        String trimmedSql = sql.trim().toLowerCase();

                        if (trimmedSql.startsWith("use ")) {
                            try {
                                String[] parts = sql.trim().split("\\s+");
                                String dbName = parts[1].replace(";", "");
                                currentQueryProcessor = queryProcessorMap.computeIfAbsent(dbName, k -> new QueryProcessor(k));
                                currentDbName = dbName;
                                out.println("Database changed to " + dbName);
                            } catch (Exception e) {
                                out.println("ERROR: " + e.getMessage());
                            }
                        } else {
                            // --- START: 这是需要替换的核心逻辑 ---
                            String processorToUseSql = sql.trim(); // 使用原始大小写的SQL

                            // 检查是否是DCL命令
                            if (trimmedSql.startsWith("create user") || trimmedSql.startsWith("grant")) {
                                QueryProcessor defaultProcessor = queryProcessorMap.get("default");

                                // 为GRANT命令增加一个预检查步骤
                                if (trimmedSql.startsWith("grant")) {
                                    try {
                                        // 解析SQL以提取表名
                                        Lexer lexer = new Lexer(processorToUseSql);
                                        Parser parser = new Parser(lexer.tokenize());
                                        StatementNode ast = parser.parse();

                                        if (ast instanceof GrantStatementNode grantNode) {
                                            String tableName = grantNode.tableName().getName();
                                            // 在“当前”数据库的上下文中检查表是否存在
                                            if (!tableName.equals("*") && currentQueryProcessor.getCatalog().getTable(tableName) == null) {
                                                // 如果表不存在，立即返回错误，不继续执行
                                                out.println("ERROR: Table '" + tableName + "' not found in current database ('" + currentDbName + "').<br>");
                                                continue; // 结束本次循环，等待下一条命令
                                            }
                                        }
                                    } catch (Exception e) {
                                        // 如果解析失败，让后续的 executeAndGetResult 统一处理语法错误
                                    }
                                }
                                // 如果预检查通过（或无需检查），则使用default处理器执行
                                String result = defaultProcessor.executeAndGetResult(processorToUseSql, session);
                                out.println(result.replace("\n", "<br>"));

                            } else {
                                // 对于所有其他命令，使用当前处理器
                                String result = currentQueryProcessor.executeAndGetResult(processorToUseSql, session);
                                out.println(result.replace("\n", "<br>"));
                            }
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