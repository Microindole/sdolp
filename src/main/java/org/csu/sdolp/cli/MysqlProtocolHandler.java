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
    private QueryProcessor queryProcessor;
    private final int connectionId;
    private Catalog catalog;
    private Session session;
    private String currentDb;

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

    public MysqlProtocolHandler(Socket clientSocket, int connectionId) {
        this.clientSocket = clientSocket;
        this.connectionId = connectionId;
        this.session = new Session(connectionId);
        this.queryProcessor = null;
        this.catalog = null;
        this.currentDb = null;
    }

    public MysqlProtocolHandler(Socket clientSocket, QueryProcessor queryProcessor, Catalog catalog, int connectionId) {
        this.clientSocket = clientSocket;
        this.queryProcessor = queryProcessor;
        this.catalog = catalog;
        this.connectionId = connectionId;
        this.session = new Session(connectionId);
    }

    private void switchDatabase(String dbName) throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        System.out.println("[ConnID: " + connectionId + "] Switching to database: " + dbName);
        queryProcessor = new QueryProcessor(dbName);
        catalog = queryProcessor.getCatalog(); // 更新 catalog 引用
        currentDb = dbName;
    }

    @Override
    public void run() {
        System.out.println("MySQL Protocol Handler started for connection ID: " + connectionId);
        try (InputStream in = clientSocket.getInputStream();
             OutputStream out = clientSocket.getOutputStream()) {

            // 为了认证，我们需要一个临时的 Catalog 实例。
            // 我们连接到默认数据库来获取用户信息。
            switchDatabase("default");

            byte[] salt = sendHandshake(out, 0);
            System.out.println("Sent handshake packet to client.");

            Packet authPacket = readPacket(in);
            if (authPacket == null) {
                System.err.println("Client disconnected after handshake.");
                return;
            }

            if (!authenticate(authPacket, salt, out)) {
                System.err.println("Authentication failed for connection ID: " + connectionId);
                return;
            }

            System.out.println("User '" + session.getUsername() + "' authenticated successfully for connection ID: " + connectionId);
            sendOkPacket(out, authPacket.sequenceId + 1, 0, 0);

            while (!clientSocket.isClosed()) {
                Packet commandPacket = readPacket(in);
                if (commandPacket == null) break;

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
                if (queryProcessor != null) queryProcessor.close(); // 关闭最后的 QP 实例
                if (!clientSocket.isClosed()) clientSocket.close();
                System.out.println("Client disconnected: " + session.getUsername() + "@" + clientSocket.getInetAddress());
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    private boolean authenticate(Packet authPacket, byte[] salt, OutputStream out) throws IOException, NoSuchAlgorithmException {
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
        byte[] storedPasswordHash = catalog.getPasswordHash(username);

        if (storedPasswordHash != null) {
            this.session = Session.createAuthenticatedSession(this.connectionId, username);
            System.out.println("Simplified authentication successful for user: '" + username + "'");
            return true;
        } else {
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

        System.out.println("[ConnID: " + connectionId + ", DB: " + currentDb + "] Received command type: 0x" + Integer.toHexString(commandType));


        switch (commandType) {
            case 0x01: // COM_QUIT
                clientSocket.close();
                break;

            case 0x02: // COM_INIT_DB (USE database)
                String dbName = new String(commandPacket.payload, 1, commandPacket.payload.length - 1, StandardCharsets.UTF_8);
                try {
                    switchDatabase(dbName);
                    sendOkPacket(out, serverSequenceId, 0, 0);
                } catch (Exception e) {
                    sendErrorPacket(out, serverSequenceId, 1049, "42000", "Unknown database '" + dbName + "'");
                }
                break;

            case 0x03: // COM_QUERY
                String sql = new String(commandPacket.payload, 1, commandPacket.payload.length - 1, StandardCharsets.UTF_8);
                System.out.println("[ConnID: " + connectionId + ", DB: " + currentDb + "] Received SQL: " + sql);


                if (queryProcessor == null && !(sql.toLowerCase().contains("create database") || sql.toLowerCase().contains("show databases"))) {
                    sendErrorPacket(out, serverSequenceId, 1046, "3D000", "No database selected");
                    return;
                }

                if (handleSpecialQuery(sql, out, serverSequenceId)) {
                    return;
                }

                Transaction txn = null;
                TransactionManager transactionManager = queryProcessor.getTransactionManager();

                try {
                    txn = transactionManager.begin();
                    TupleIterator iterator = queryProcessor.createExecutorForQuery(sql, txn, this.session);

                    if (iterator != null && iterator.getOutputSchema() != null) {
                        Schema schema = iterator.getOutputSchema();
                        List<Tuple> results = new ArrayList<>();
                        while (iterator.hasNext()) {
                            results.add(iterator.next());
                        }

                        serverSequenceId = sendResultSetHeader(out, serverSequenceId, schema.getColumns().size());
                        serverSequenceId = sendFieldPackets(out, serverSequenceId, schema);
                        serverSequenceId = sendEofPacket(out, serverSequenceId);

                        if (!results.isEmpty()) {
                            serverSequenceId = sendRowPackets(out, serverSequenceId, results);
                        }
                        sendEofPacket(out, serverSequenceId);
                    } else {
                        if (iterator != null && iterator.hasNext()) {
                            iterator.next();
                        }
                        sendOkPacket(out, serverSequenceId, 0, 0);
                    }

                    transactionManager.commit(txn);

                } catch (Exception e) {
                    System.err.println("Error executing query: " + e.getMessage());
                    if (e instanceof SemanticException && e.getMessage().toLowerCase().contains("access denied")) {
                        sendErrorPacket(out, serverSequenceId, 1142, "42000", e.getMessage());
                    } else {
                        sendErrorPacket(out, serverSequenceId, 1064, "42000", "Error: " + e.getMessage());
                    }

                    if (txn != null && txn.getState() == Transaction.State.ACTIVE) {
                        transactionManager.abort(txn);
                    }
                }
                break;
            default:
                sendOkPacket(out, serverSequenceId, 0, 0);
                break;
        }
    }

    private boolean handleSpecialQuery(String sql, OutputStream out, int sequenceId) throws IOException {
        String sqlLower = sql.toLowerCase().trim();

        // --- 核心修复：为 Navicat 的 schemata 查询提供真实数据 ---
        if (sqlLower.contains("from information_schema.schemata")) {
            System.out.println("Intercepted information_schema.schemata query. Returning real database list.");

            List<String> dbNames = queryProcessor.getDbManager().listDatabases();

            sequenceId = sendResultSetHeader(out, sequenceId, 1);
            sequenceId = sendSimpleFieldPacket(out, sequenceId, "SCHEMA_NAME");
            sequenceId = sendEofPacket(out, sequenceId);

            for (String dbName : dbNames) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                bos.write(writeLengthEncodedString(dbName));
                sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
            }
            sendEofPacket(out, sequenceId);

            return true;
        }

        if (sqlLower.startsWith("set ")) {
            sendOkPacket(out, sequenceId, 0, 0);
            return true;
        }

        if (sqlLower.startsWith("show ")) {
            if (sqlLower.contains("variables")) {
                return handleShowVariables(sql, out, sequenceId);
            } else if (sqlLower.contains("engines")) {
                return handleShowEngines(out, sequenceId);
            } else if (sqlLower.contains("status")) {
                return handleShowStatus(out, sequenceId);
            }
        }

        if (sqlLower.contains("@@") || sqlLower.contains("information_schema")) {
            return handleSystemVariables(sql, out, sequenceId);
        }

        if (sqlLower.equals("select database()")) {
            sendResultSetHeader(out, sequenceId, 1);
            sendSimpleFieldPacket(out, sequenceId + 1, "database()");
            sendEofPacket(out, sequenceId + 2);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString(this.currentDb != null ? this.currentDb : ""));
            writePacket(out, bos.toByteArray(), sequenceId + 3);
            sendEofPacket(out, sequenceId + 4);
            return true;
        }

        return false;
    }

    private boolean handleShowStatus(OutputStream out, int sequenceId) throws IOException {
        sendResultSetHeader(out, sequenceId, 2);
        sendSimpleFieldPacket(out, sequenceId + 1, "Variable_name");
        sendSimpleFieldPacket(out, sequenceId + 2, "Value");
        sendEofPacket(out, sequenceId + 3);

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
            bos.write(writeLengthEncodedString("def"));
            bos.write(writeLengthEncodedString("minidb"));
            bos.write(writeLengthEncodedString(""));
            bos.write(writeLengthEncodedString(""));
            bos.write(writeLengthEncodedString(col.getName()));
            bos.write(writeLengthEncodedString(col.getName()));
            bos.write(0x0c);
            bos.write(writeInt(33, 2));
            bos.write(writeInt(1024, 4));
            bos.write((byte) 0xfd);
            bos.write(writeInt(0, 2));
            bos.write((byte) 0x00);
            bos.write(new byte[2]);
            sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
        }
        return sequenceId;
    }

    private int sendRowPackets(OutputStream out, int sequenceId, List<Tuple> tuples) throws IOException {
        for (Tuple tuple : tuples) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (org.csu.sdolp.common.model.Value val : tuple.getValues()) {
                if (val.getValue() == null) {
                    bos.write(0xfb);
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
        packet.write(0x00);
        packet.write(writeLengthEncodedInt(affectedRows));
        packet.write(writeLengthEncodedInt(lastInsertId));
        packet.write(0x02);
        packet.write(0x00);
        packet.write(0x00);
        packet.write(0x00);
        return writePacket(out, packet.toByteArray(), sequenceId);
    }

    private int sendErrorPacket(OutputStream out, int sequenceId, int errorCode, String sqlState, String message) throws IOException {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.write(0xFF);
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
        sendResultSetHeader(out, sequenceId, 2);
        sendSimpleFieldPacket(out, sequenceId + 1, "Variable_name");
        sendSimpleFieldPacket(out, sequenceId + 2, "Value");
        sendEofPacket(out, sequenceId + 3);

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
        sendResultSetHeader(out, sequenceId, 1);
        sendSimpleFieldPacket(out, sequenceId + 1, "Value");
        sendEofPacket(out, sequenceId + 2);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(writeLengthEncodedString("dummy_value"));
        writePacket(out, bos.toByteArray(), sequenceId + 3);

        sendEofPacket(out, sequenceId + 4);
        return true;
    }

    private boolean handleInformationSchema(String sql, OutputStream out, int sequenceId) throws IOException {
        String sqlLower = sql.toLowerCase();

        if (sqlLower.contains("engines")) {
            sendResultSetHeader(out, sequenceId, 1);
            if (sqlLower.contains("count")) {
                sendSimpleFieldPacket(out, sequenceId + 1, "support_ndb");
            } else {
                sendSimpleFieldPacket(out, sequenceId + 1, "Result");
            }
            sendEofPacket(out, sequenceId + 2);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString("0"));
            writePacket(out, bos.toByteArray(), sequenceId + 3);

            sendEofPacket(out, sequenceId + 4);
            return true;
        }

        sendResultSetHeader(out, sequenceId, 1);
        sendSimpleFieldPacket(out, sequenceId + 1, "Result");
        sendEofPacket(out, sequenceId + 2);
        sendEofPacket(out, sequenceId + 3);
        return true;
    }
}