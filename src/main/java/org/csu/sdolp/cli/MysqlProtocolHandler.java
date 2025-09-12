package org.csu.sdolp.cli;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.transaction.Transaction;
import org.csu.sdolp.transaction.TransactionManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 负责处理与 MySQL 客户端（如 Navicat）的底层网络协议交互。
 */
public class MysqlProtocolHandler implements Runnable {

    private final Socket clientSocket;
    private final QueryProcessor queryProcessor;
    private final int connectionId;
    private final Catalog catalog;
    private Session session;

    // MySQL Protocol Constants
    private static final int CLIENT_PROTOCOL_41 = 0x00000200;
    private static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    private static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    private static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    private static final int CLIENT_DEPRECATE_EOF = 0x01000000;

    private static class Packet {
        final int sequenceId;
        final byte[] payload;
        Packet(int sequenceId, byte[] payload) {
            this.sequenceId = sequenceId;
            this.payload = payload;
        }
    }

    public MysqlProtocolHandler(Socket clientSocket, QueryProcessor queryProcessor, Catalog catalog, int connectionId) {
        this.clientSocket = clientSocket;
        this.queryProcessor = queryProcessor;
        this.catalog = catalog; // 初始化 Catalog
        this.connectionId = connectionId;
        this.session = new Session(connectionId); // 初始化一个未认证的 Session
    }

    @Override
    public void run() {
        System.out.println("MySQL Protocol Handler started for connection ID: " + connectionId);
        try (InputStream in = clientSocket.getInputStream();
             OutputStream out = clientSocket.getOutputStream()) {

            // 1. 发送握手包
            byte[] salt = sendHandshake(out, 0);
            System.out.println("Sent handshake packet to client.");

            // 2. 读取客户端认证响应
            Packet authPacket = readPacket(in);
            if (authPacket == null) {
                System.err.println("Client disconnected after handshake.");
                return;
            }

            // *** 3. 核心认证逻辑 ***
            if (!authenticate(authPacket, salt, out)) {
                System.err.println("Authentication failed for connection ID: " + connectionId);
                // authenticate 方法内部已经发送了错误信息，这里直接返回即可
                return;
            }

            // 如果认证成功，session 已经被更新
            System.out.println("User '" + session.getUsername() + "' authenticated successfully for connection ID: " + connectionId);

            // 4. 发送 OK 包表示认证成功
            sendOkPacket(out, authPacket.sequenceId + 1, 0, 0);

            // 5. 主命令循环
            while (!clientSocket.isClosed()) {
                Packet commandPacket = readPacket(in);
                if (commandPacket == null) break;

                // 在处理命令之前，检查是否已认证
                if (!session.isAuthenticated()) {
                    sendErrorPacket(out, 1, 1045, "28000", "Access denied. Please log in.");
                    continue;
                }
                handleCommand(commandPacket, out);
            }

        } catch (IOException e) {
            if (!e.getMessage().contains("Connection reset") && !e.getMessage().contains("Socket closed")) {
                System.err.println("Error handling client connection " + connectionId + ": " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Unexpected error in protocol handler " + connectionId + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (!clientSocket.isClosed()) clientSocket.close();
                System.out.println("Client disconnected: " + session.getUsername() + "@" + clientSocket.getInetAddress());
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    private boolean authenticate(Packet authPacket, byte[] salt, OutputStream out) throws IOException, NoSuchAlgorithmException {
        // 解析用户名
        int pos = 4 + 4 + 1 + 23;
        int userStart = -1;
        for (int i = pos; i < authPacket.payload.length; i++) {
            if (authPacket.payload[i] != 0) {
                userStart = i;
                break;
            }
        }
        if (userStart == -1) return false;

        int userEnd = -1;
        for (int i = userStart; i < authPacket.payload.length; i++) {
            if (authPacket.payload[i] == 0) {
                userEnd = i;
                break;
            }
        }
        if (userEnd == -1) return false;

        String username = new String(authPacket.payload, userStart, userEnd - userStart, StandardCharsets.UTF_8);

        // --- 核心修复 ---
        // 从 Catalog 获取该用户的密码哈希，以此判断用户是否存在
        byte[] storedPasswordHash = catalog.getPasswordHash(username);

        if (storedPasswordHash != null) {
            // 在我们的简化模型中，只要用户存在于 Catalog 中，我们就认为认证成功。
            // 一个真正的数据库在这里会进行复杂的密码哈希比对。

            // 认证成功，更新 Session
            this.session = Session.createAuthenticatedSession(this.connectionId, username);
            System.out.println("Simplified authentication successful for user: '" + username + "'");
            return true;
        } else {
            // 如果在 Catalog 中找不到用户，则认证失败
            System.err.println("Authentication failed: User '" + username + "' not found in catalog.");
            sendErrorPacket(out, authPacket.sequenceId + 1, 1045, "28000", "Access denied for user '" + username + "'");
            return false;
        }
    }

    private byte[] sendHandshake(OutputStream out, int sequenceId) throws IOException {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.write(10);
        String serverVersion = "8.0.28-minidb";
        packet.write(serverVersion.getBytes(StandardCharsets.UTF_8));
        packet.write(0);
        packet.write(writeInt(connectionId, 4));

        byte[] salt1 = new byte[8];
        new Random().nextBytes(salt1);
        packet.write(salt1);
        packet.write(0);

        int capabilityFlags = 0xffffff7f;
        packet.write(capabilityFlags & 0xFF);
        packet.write((capabilityFlags >> 8) & 0xFF);
        packet.write(0xff);
        packet.write(0x02);
        packet.write(0x00);
        packet.write((capabilityFlags >> 16) & 0xFF);
        packet.write((capabilityFlags >> 24) & 0xFF);
        packet.write(21);
        packet.write(new byte[10]);
        byte[] salt2 = new byte[12];
        new Random().nextBytes(salt2);
        packet.write(salt2);
        packet.write(0);
        String authPlugin = "mysql_native_password";
        packet.write(authPlugin.getBytes(StandardCharsets.UTF_8));
        packet.write(0);

        writePacket(out, packet.toByteArray(), sequenceId);

        // 返回完整的 salt
        byte[] fullSalt = new byte[salt1.length + salt2.length];
        System.arraycopy(salt1, 0, fullSalt, 0, salt1.length);
        System.arraycopy(salt2, 0, fullSalt, salt1.length, salt2.length);
        return fullSalt;
    }

    private void handleCommand(Packet commandPacket, OutputStream out) throws Exception {
        if (commandPacket.payload.length == 0) {
            return;
        }

        int commandType = commandPacket.payload[0] & 0xFF;
        int serverSequenceId = commandPacket.sequenceId + 1;

        System.out.println("Received command type: 0x" + Integer.toHexString(commandType));

        switch (commandType) {
            case 0x00: // COM_SLEEP
                // Do nothing
                break;

            case 0x01: // COM_QUIT
                System.out.println("Client sent QUIT command");
                clientSocket.close();
                break;

            case 0x02: // COM_INIT_DB
                String dbName = new String(commandPacket.payload, 1, commandPacket.payload.length - 1, StandardCharsets.UTF_8);
                System.out.println("Client wants to use database: " + dbName);
                sendOkPacket(out, serverSequenceId, 0, 0);
                break;

            case 0x03: // COM_QUERY
                String sql = new String(commandPacket.payload, 1, commandPacket.payload.length - 1, StandardCharsets.UTF_8);
                System.out.println("Received SQL query: " + sql);

                // Handle special MySQL client queries
                if (handleSpecialQuery(sql, out, serverSequenceId)) {
                    return;
                }

                Transaction txn = null;
                TransactionManager transactionManager = queryProcessor.getTransactionManager(); // 获取事务管理器


                try {
                    // 1. 在处理查询前开始事务
                    txn = transactionManager.begin();

                    // 2. 调用新的方法，在当前事务中执行查询
                    TupleIterator iterator = queryProcessor.createExecutorForQuery(sql, txn, this.session);

                    // 3. 判断是返回结果集 (SELECT) 还是只返回OK (INSERT/UPDATE/CREATE...)
                    if (iterator == null || iterator.getOutputSchema() == null ||
                            "message".equals(iterator.getOutputSchema().getColumns().get(0).getName()) ||
                            "inserted_rows".equals(iterator.getOutputSchema().getColumns().get(0).getName()) ||
                            "deleted_rows".equals(iterator.getOutputSchema().getColumns().get(0).getName()) ||
                            "updated_rows".equals(iterator.getOutputSchema().getColumns().get(0).getName()))
                    {
                        // 对于 DML/DDL，消耗掉迭代器以确保操作执行
                        if (iterator != null && iterator.hasNext()) {
                            iterator.next();
                        }
                        sendOkPacket(out, serverSequenceId, 0, 0);
                    } else {
                        // 对于 SELECT 查询，获取所有结果
                        Schema schema = iterator.getOutputSchema();
                        List<Tuple> results = new ArrayList<>();
                        while (iterator.hasNext()) {
                            results.add(iterator.next());
                        }

                        // 按照MySQL协议发送结果集
                        serverSequenceId = sendResultSetHeader(out, serverSequenceId, schema.getColumns().size());
                        serverSequenceId = sendFieldPackets(out, serverSequenceId, schema);
                        serverSequenceId = sendEofPacket(out, serverSequenceId);

                        if (!results.isEmpty()) {
                            serverSequenceId = sendRowPackets(out, serverSequenceId, results);
                        }
                        sendEofPacket(out, serverSequenceId);
                    }

                    // 4. 如果所有操作都成功，提交事务
                    transactionManager.commit(txn);

                } catch (Exception e) {
                    System.err.println("Error executing query: " + e.getMessage());
                    // 1142 (ER_COMMAND_DENIED_ERROR) 是权限错误的专用代码
                    if (e instanceof SemanticException && e.getMessage().toLowerCase().contains("access denied")) {
                        sendErrorPacket(out, serverSequenceId, 1142, "42000", e.getMessage());
                    } else {
                        // 其他错误使用通用错误码
                        sendErrorPacket(out, serverSequenceId, 1064, "42000", "Error: " + e.getMessage());
                    }

                    if (txn != null && txn.getState() == Transaction.State.ACTIVE) {
                        transactionManager.abort(txn);
                    }
                }
                break; // COM_QUERY case 结束

            case 0x04: // COM_FIELD_LIST
                // Return empty field list
                sendEofPacket(out, serverSequenceId);
                break;

            case 0x05: // COM_CREATE_DB
            case 0x06: // COM_DROP_DB
            case 0x07: // COM_REFRESH
            case 0x08: // COM_SHUTDOWN
            case 0x09: // COM_STATISTICS
            case 0x0a: // COM_PROCESS_INFO
            case 0x0b: // COM_CONNECT
            case 0x0c: // COM_PROCESS_KILL
            case 0x0d: // COM_DEBUG
            case 0x0e: // COM_PING
                sendOkPacket(out, serverSequenceId, 0, 0);
                break;

            default:
                System.err.println("Unsupported command: 0x" + Integer.toHexString(commandType));
                sendErrorPacket(out, serverSequenceId, 1047, "08S01",
                        "Unknown command: 0x" + Integer.toHexString(commandType));
                break;
        }
    }

    private boolean handleSpecialQuery(String sql, OutputStream out, int sequenceId) throws IOException {
        String sqlLower = sql.toLowerCase().trim();

        // Handle SET statements
        if (sqlLower.startsWith("set ")) {
            sendOkPacket(out, sequenceId, 0, 0);
            return true;
        }

        // Handle SHOW statements
        if (sqlLower.startsWith("show ")) {
            if (sqlLower.contains("databases")) {
                // Return empty database list
                sendResultSetHeader(out, sequenceId, 1);
                sendSimpleFieldPacket(out, sequenceId + 1, "Database");
                sendEofPacket(out, sequenceId + 2);
                sendEofPacket(out, sequenceId + 3);
                return true;
            } else if (sqlLower.contains("variables")) {
                // Handle SHOW VARIABLES queries
                return handleShowVariables(sql, out, sequenceId);
            } else if (sqlLower.contains("engines")) {
                // Handle SHOW ENGINES or queries about engines
                return handleShowEngines(out, sequenceId);
            } else if (sqlLower.contains("status")) {
                // Handle SHOW STATUS queries
                return handleShowStatus(out, sequenceId);
            }
        }

        if (sqlLower.contains("@@") || sqlLower.contains("information_schema")) {
            return handleSystemVariables(sql, out, sequenceId);
        }

        return false;
    }

    // 添加处理SHOW STATUS的方法
    private boolean handleShowStatus(OutputStream out, int sequenceId) throws IOException {
        sendResultSetHeader(out, sequenceId, 2);
        sendSimpleFieldPacket(out, sequenceId + 1, "Variable_name");
        sendSimpleFieldPacket(out, sequenceId + 2, "Value");
        sendEofPacket(out, sequenceId + 3);

        // Send some dummy status variables
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(writeLengthEncodedString("Connections"));
        bos.write(writeLengthEncodedString("1"));
        writePacket(out, bos.toByteArray(), sequenceId + 4);

        sendEofPacket(out, sequenceId + 5);
        return true;
    }


    private int sendSimpleFieldPacket(OutputStream out, int sequenceId, String fieldName) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(writeLengthEncodedString("def"));
        bos.write(writeLengthEncodedString(""));
        bos.write(writeLengthEncodedString(""));
        bos.write(writeLengthEncodedString(""));
        bos.write(writeLengthEncodedString(fieldName));
        bos.write(writeLengthEncodedString(fieldName));
        bos.write(0x0c);
        bos.write(writeInt(33, 2));
        bos.write(writeInt(255, 4));
        bos.write((byte) 0xfd);
        bos.write(writeInt(0, 2));
        bos.write((byte) 0x00);
        bos.write(new byte[2]);
        return writePacket(out, bos.toByteArray(), sequenceId);
    }

    private int sendResultSetHeader(OutputStream out, int sequenceId, int fieldCount) throws IOException {
        return writePacket(out, writeLengthEncodedInt(fieldCount), sequenceId);
    }

    private int sendFieldPackets(OutputStream out, int sequenceId, Schema schema) throws IOException {
        for (org.csu.sdolp.common.model.Column col : schema.getColumns()) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString("def"));           // catalog
            bos.write(writeLengthEncodedString("minidb"));        // database
            bos.write(writeLengthEncodedString(""));              // table
            bos.write(writeLengthEncodedString(""));              // org_table
            bos.write(writeLengthEncodedString(col.getName()));   // name
            bos.write(writeLengthEncodedString(col.getName()));   // org_name
            bos.write(0x0c);                                      // length of fixed fields
            bos.write(writeInt(33, 2));                          // character set
            bos.write(writeInt(1024, 4));                        // column length
            bos.write((byte) 0xfd);                              // type (VARCHAR)
            bos.write(writeInt(0, 2));                           // flags
            bos.write((byte) 0x00);                              // decimals
            bos.write(new byte[2]);                              // filler
            sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
        }
        return sequenceId;
    }

    private int sendRowPackets(OutputStream out, int sequenceId, List<Tuple> tuples) throws IOException {
        for (Tuple tuple : tuples) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (org.csu.sdolp.common.model.Value val : tuple.getValues()) {
                if (val.getValue() == null) {
                    bos.write(0xfb); // NULL value
                } else {
                    bos.write(writeLengthEncodedString(val.getValue().toString()));
                }
            }
            sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
        }
        return sequenceId;
    }

    private int sendEofPacket(OutputStream out, int sequenceId) throws IOException {
        byte[] eofPacket = {(byte) 0xfe, 0x00, 0x00, 0x02, 0x00};
        return writePacket(out, eofPacket, sequenceId);
    }

    private Packet readPacket(InputStream in) throws IOException {
        byte[] header = new byte[4];
        int n = in.read(header);
        if (n < 4) {
            return null;
        }

        int payloadLength = (header[0] & 0xFF) |
                ((header[1] & 0xFF) << 8) |
                ((header[2] & 0xFF) << 16);
        int sequenceId = header[3] & 0xFF;

        if (payloadLength == 0) {
            return new Packet(sequenceId, new byte[0]);
        }

        byte[] payload = new byte[payloadLength];
        int bytesRead = 0;
        while (bytesRead < payloadLength) {
            int read = in.read(payload, bytesRead, payloadLength - bytesRead);
            if (read == -1) {
                throw new IOException("Incomplete packet read");
            }
            bytesRead += read;
        }

        return new Packet(sequenceId, payload);
    }

    private int writePacket(OutputStream out, byte[] payload, int sequenceId) throws IOException {
        int payloadLength = payload.length;
        out.write(payloadLength & 0xFF);
        out.write((payloadLength >> 8) & 0xFF);
        out.write((payloadLength >> 16) & 0xFF);
        out.write(sequenceId & 0xFF);
        out.write(payload);
        out.flush();
        return sequenceId + 1;
    }

    private int sendOkPacket(OutputStream out, int sequenceId, int affectedRows, int lastInsertId) throws IOException {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.write(0x00); // OK header
        packet.write(writeLengthEncodedInt(affectedRows));
        packet.write(writeLengthEncodedInt(lastInsertId));
        packet.write(0x02); // SERVER_STATUS_AUTOCOMMIT
        packet.write(0x00);
        packet.write(0x00); // warnings
        packet.write(0x00);
        return writePacket(out, packet.toByteArray(), sequenceId);
    }

    private int sendErrorPacket(OutputStream out, int sequenceId, int errorCode, String sqlState, String message) throws IOException {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.write(0xFF); // Error header
        packet.write(writeInt(errorCode, 2));
        packet.write('#');
        packet.write(sqlState.getBytes(StandardCharsets.UTF_8));
        packet.write(message.getBytes(StandardCharsets.UTF_8));
        return writePacket(out, packet.toByteArray(), sequenceId);
    }

    private byte[] writeInt(int value, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) ((value >> (i * 8)) & 0xFF);
        }
        return bytes;
    }

    private byte[] writeLengthEncodedInt(long n) {
        if (n < 251) {
            return new byte[]{(byte) n};
        }
        if (n < 65536) {
            return new byte[]{(byte) 0xfc, (byte) n, (byte) (n >> 8)};
        }
        if (n < 16777216) {
            return new byte[]{(byte) 0xfd, (byte) n, (byte) (n >> 8), (byte) (n >> 16)};
        }
        return new byte[]{(byte) 0xfe, (byte) n, (byte) (n >> 8), (byte) (n >> 16), (byte) (n >> 24),
                (byte) (n >> 32), (byte) (n >> 40), (byte) (n >> 48), (byte) (n >> 56)};
    }

    private byte[] writeLengthEncodedString(String s) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        bos.write(writeLengthEncodedInt(data.length));
        bos.write(data);
        return bos.toByteArray();
    }

    private boolean handleShowVariables(String sql, OutputStream out, int sequenceId) throws IOException {
        // Return dummy variables
        sendResultSetHeader(out, sequenceId, 2);
        sendSimpleFieldPacket(out, sequenceId + 1, "Variable_name");
        sendSimpleFieldPacket(out, sequenceId + 2, "Value");
        sendEofPacket(out, sequenceId + 3);

        // Send some dummy variable rows if needed
        if (sql.toLowerCase().contains("lower_case")) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString("lower_case_table_names"));
            bos.write(writeLengthEncodedString("0"));
            writePacket(out, bos.toByteArray(), sequenceId + 4);
        } else if (sql.toLowerCase().contains("sql_mode")) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString("sql_mode"));
            bos.write(writeLengthEncodedString("STRICT_TRANS_TABLES"));
            writePacket(out, bos.toByteArray(), sequenceId + 4);
        }

        sendEofPacket(out, sequenceId + 5);
        return true;
    }

    private boolean handleShowEngines(OutputStream out, int sequenceId) throws IOException {
        sendResultSetHeader(out, sequenceId, 6);
        sendSimpleFieldPacket(out, sequenceId + 1, "Engine");
        sendSimpleFieldPacket(out, sequenceId + 2, "Support");
        sendSimpleFieldPacket(out, sequenceId + 3, "Comment");
        sendSimpleFieldPacket(out, sequenceId + 4, "Transactions");
        sendSimpleFieldPacket(out, sequenceId + 5, "XA");
        sendSimpleFieldPacket(out, sequenceId + 6, "Savepoints");
        sendEofPacket(out, sequenceId + 7);

        // Return dummy engine info
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(writeLengthEncodedString("InnoDB"));
        bos.write(writeLengthEncodedString("DEFAULT"));
        bos.write(writeLengthEncodedString("Supports transactions, row-level locking, and foreign keys"));
        bos.write(writeLengthEncodedString("YES"));
        bos.write(writeLengthEncodedString("YES"));
        bos.write(writeLengthEncodedString("YES"));
        writePacket(out, bos.toByteArray(), sequenceId + 8);

        sendEofPacket(out, sequenceId + 9);
        return true;
    }

    private boolean handleSystemVariables(String sql, OutputStream out, int sequenceId) throws IOException {
        // Handle SELECT @@variable queries
        sendResultSetHeader(out, sequenceId, 1);
        sendSimpleFieldPacket(out, sequenceId + 1, "Value");
        sendEofPacket(out, sequenceId + 2);

        // Return dummy value
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(writeLengthEncodedString("dummy_value"));
        writePacket(out, bos.toByteArray(), sequenceId + 3);

        sendEofPacket(out, sequenceId + 4);
        return true;
    }

    private boolean handleInformationSchema(String sql, OutputStream out, int sequenceId) throws IOException {
        String sqlLower = sql.toLowerCase();

        if (sqlLower.contains("engines")) {
            // Handle queries like: SELECT COUNT(*) AS support_ndb FROM information_schema.ENGINES WHERE Engine = 'ndbcluster'
            sendResultSetHeader(out, sequenceId, 1);
            if (sqlLower.contains("count")) {
                sendSimpleFieldPacket(out, sequenceId + 1, "support_ndb");
            } else {
                sendSimpleFieldPacket(out, sequenceId + 1, "Result");
            }
            sendEofPacket(out, sequenceId + 2);

            // Return count of 0 for ndbcluster
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString("0"));
            writePacket(out, bos.toByteArray(), sequenceId + 3);

            sendEofPacket(out, sequenceId + 4);
            return true;
        }

        // For other information_schema queries, return empty result
        sendResultSetHeader(out, sequenceId, 1);
        sendSimpleFieldPacket(out, sequenceId + 1, "Result");
        sendEofPacket(out, sequenceId + 2);
        sendEofPacket(out, sequenceId + 3);
        return true;
    }
}