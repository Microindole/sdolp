package org.csu.sdolp.cli;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.StatementNode;
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
                System.out.println("[DEBUG] Full SQL Query: " + sql);
                if (!sql.trim().endsWith(";")) {
                    sql += ";";
                }
                String sqlLower = sql.toLowerCase().trim();
                if (sqlLower.startsWith("use ")) {
                    String dbNameFromQuery = sqlLower.substring(4).replace(";", "").trim(); // 变量名已修改
                    try {
                        switchDatabase(dbNameFromQuery);
                        sendOkPacket(out, serverSequenceId, 0, 0);
                    } catch (Exception e) {
                        sendErrorPacket(out, serverSequenceId, 1049, "42000", "Unknown database '" + dbNameFromQuery + "'");
                    }
                    return; // 立即返回，不继续处理
                }
                if (queryProcessor == null && !(sql.toLowerCase().contains("create database") || sql.toLowerCase().contains("show databases"))) {
                    sendErrorPacket(out, serverSequenceId, 1046, "3D000", "No database selected");
                    return;
                }
                if (handleSpecialQuery(sql, out, serverSequenceId)) {
                    return;
                }

                Transaction txn = null;
                try {
                    TransactionManager transactionManager = queryProcessor.getTransactionManager();
                    txn = transactionManager.begin();

                    // 1. 解析SQL为AST
                    Lexer lexer = new Lexer(sql);
                    Parser parser = new Parser(lexer.tokenize());
                    StatementNode ast = parser.parse();

                    // 2. 从AST中获取表名
                    String effectiveTableName = queryProcessor.getTableNameFromAst(ast);

                    // 3. 继续执行计划的创建和执行
                    TupleIterator iterator = queryProcessor.createExecutorForQuery(sql, txn, this.session);

                    if (iterator != null && iterator.getOutputSchema() != null) {
                        Schema schema = iterator.getOutputSchema();
                        List<Tuple> results = new ArrayList<>();
                        System.out.println("[DEBUG] Collecting results for query: " + sql);
                        int rowCount = 0;

                        while (iterator.hasNext()) {
                            Tuple tuple = iterator.next();
                            results.add(tuple);
                            rowCount++;
                        }

                        System.out.println("[DEBUG] Found " + rowCount + " rows");

                        int currentSeqId = serverSequenceId;
                        currentSeqId = sendResultSetHeader(out, currentSeqId, schema.getColumns().size());
                        currentSeqId = sendFieldPackets(out, currentSeqId, schema, effectiveTableName);
                        currentSeqId = sendEofPacket(out, currentSeqId);

                        if (!results.isEmpty()) {
                            currentSeqId = sendRowPackets(out, currentSeqId, results, schema);
                        }
                        sendEofPacket(out, currentSeqId);
                    } else {
                        if (iterator != null && iterator.hasNext()) {
                            iterator.next();
                        }
                        sendOkPacket(out, serverSequenceId, 0, 0);
                    }

                    transactionManager.commit(txn);

                } catch (ParseException e) {
                    System.err.println("Unsupported query syntax from client, returning empty result set. SQL: " + sql);
                    System.err.println("Parse Error: " + e.getMessage());

                    int currentSeqId = serverSequenceId;
                    currentSeqId = sendResultSetHeader(out, currentSeqId, 1);
                    currentSeqId = sendSimpleFieldPacket(out, currentSeqId, "Result");
                    currentSeqId = sendEofPacket(out, currentSeqId);
                    sendEofPacket(out, currentSeqId);

                } catch (Exception e) {
                    System.err.println("Error executing query: " + e.getMessage());
                    e.printStackTrace(); // 打印完整堆栈以供调试
                    if (e instanceof SemanticException && e.getMessage().toLowerCase().contains("access denied")) {
                        sendErrorPacket(out, serverSequenceId, 1142, "42000", e.getMessage());
                    } else {
                        sendErrorPacket(out, serverSequenceId, 1064, "42000", "Error: " + e.getMessage());
                    }

                    if (queryProcessor != null && txn != null && txn.getState() == Transaction.State.ACTIVE) {
                        queryProcessor.getTransactionManager().abort(txn);
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
        // 处理 SHOW CREATE TABLE
        if (sqlLower.contains("show create table")) {
            return handleShowCreateTable(sql, out, sequenceId);
        }

        // 处理 SHOW FULL COLUMNS
        if (sqlLower.contains("show full columns") || sqlLower.contains("show columns")) {
            return handleShowFullColumns(sql, out, sequenceId);
        }
        // Handle LIMIT queries with LIMIT 0 (structure check)
        if (sqlLower.contains("limit 0")) {
            // This is likely a structure check - return empty result with schema
            System.out.println("Intercepted LIMIT 0 query (structure check): " + sql);
            // Don't handle here, let it pass through to get proper schema
            return false;
        }

        // Handle information_schema.columns queries
        if (sqlLower.contains("information_schema.columns")) {
            return handleInformationSchemaColumns(sql, out, sequenceId);
        }

        // Handle information_schema.tables queries
        if (sqlLower.contains("information_schema.tables")) {
            return handleInformationSchemaTables(sql, out, sequenceId);
        }

        // Handle SELECT COUNT(*) queries
        if (sqlLower.startsWith("select count(*)")) {
            // Let this pass through to the actual query processor
            return false;
        }
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
            if (sqlLower.contains("full tables")) {
                return handleShowFullTables(out, sequenceId);
            } else if (sqlLower.contains("variables")) {
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

    private int sendFieldPackets(OutputStream out, int sequenceId, Schema schema, String tableName) throws IOException {
        for (Column col : schema.getColumns()) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString("def"));           // catalog
            bos.write(writeLengthEncodedString(this.currentDb != null ? this.currentDb : "")); // database
            bos.write(writeLengthEncodedString(tableName));                       // table name
            bos.write(writeLengthEncodedString(tableName));                       // org_table
            bos.write(writeLengthEncodedString(col.getName()));   // name
            bos.write(writeLengthEncodedString(col.getName()));   // org_name
            bos.write(0x0c);                                      // length of fixed fields

            int charset;
            byte fieldType;
            switch (col.getType()) {
                case INT:
                case BOOLEAN:
                    charset = 63; // mysql_charset_bin
                    fieldType = (byte) 0x03; // FIELD_TYPE_LONG
                    break;
                case DECIMAL:
                    charset = 63; // mysql_charset_bin
                    fieldType = (byte) 0xf6; // FIELD_TYPE_NEWDECIMAL
                    break;
                case DATE:
                    charset = 63; // mysql_charset_bin
                    fieldType = (byte) 0x0a; // FIELD_TYPE_DATE
                    break;
                case VARCHAR:
                default:
                    charset = 33; // mysql_charset_utf8
                    fieldType = (byte) 0xfd; // FIELD_TYPE_VARCHAR
                    break;
            }

            bos.write(writeInt(charset, 2));        // character set
            bos.write(writeInt(1024, 4));           // column length (can be a generic large value)
            bos.write(fieldType);                                // type
            bos.write(writeInt(0, 2));              // flags
            bos.write((byte) 0x00);                             // decimals
            bos.write(new byte[2]);                             // filler

            sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
        }
        return sequenceId;
    }

    private int sendRowPackets(OutputStream out, int sequenceId, List<Tuple> tuples, Schema schema) throws IOException {
        for (Tuple tuple : tuples) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < tuple.getValues().size(); i++) {
                org.csu.sdolp.common.model.Value val = tuple.getValues().get(i);
                if (val.getValue() == null) {
                    bos.write(0xfb);
                } else {
                    switch (schema.getColumns().get(i).getType()) {
                        case INT:
                        case BOOLEAN:
                            // 发送整数的字符串表示，因为MySQL协议的text protocol模式可以处理
                            bos.write(writeLengthEncodedString(val.getValue().toString()));
                            break;
                        case VARCHAR:
                        case DECIMAL:
                        case DATE:
                        default:
                            bos.write(writeLengthEncodedString(val.getValue().toString()));
                            break;
                    }
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

    private boolean handleShowFullTables(OutputStream out, int sequenceId) throws IOException {
        System.out.println("Intercepted SHOW FULL TABLES query. Returning table list.");

        // 1. 获取所有表名
        List<String> tableNames = queryProcessor.getCatalog().getTableNames();

        // 2. 发送结果集头和列描述
        sequenceId = sendResultSetHeader(out, sequenceId, 2);
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Tables_in_" + (this.currentDb != null ? this.currentDb : ""));
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Table_type");
        sequenceId = sendEofPacket(out, sequenceId);

        // 3. 发送数据行
        for (String tableName : tableNames) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString(tableName));
            bos.write(writeLengthEncodedString("BASE TABLE")); // 暂时硬编码为'BASE TABLE'
            sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
        }
        sendEofPacket(out, sequenceId);

        return true;
    }
    private boolean handleInformationSchemaColumns(String sql, OutputStream out, int sequenceId) throws IOException {
        System.out.println("Intercepted information_schema.columns query");

        // Parse the table name from the query if present
        String tableName = extractTableNameFromQuery(sql);

        sequenceId = sendResultSetHeader(out, sequenceId, 6);
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "COLUMN_NAME");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "DATA_TYPE");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "IS_NULLABLE");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "COLUMN_DEFAULT");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "COLUMN_KEY");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "EXTRA");
        sequenceId = sendEofPacket(out, sequenceId);

        if (tableName != null && catalog != null) {
            Schema schema = catalog.getTableSchema(tableName);
            if (schema != null) {
                for (Column col : schema.getColumns()) {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    bos.write(writeLengthEncodedString(col.getName()));
                    bos.write(writeLengthEncodedString(col.getType().toString()));
                    bos.write(writeLengthEncodedString("YES")); // Nullable
                    bos.write(writeLengthEncodedString("")); // Default
                    bos.write(writeLengthEncodedString("")); // Key
                    bos.write(writeLengthEncodedString("")); // Extra
                    sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
                }
            }
        }

        sendEofPacket(out, sequenceId);
        return true;
    }

    private boolean handleInformationSchemaTables(String sql, OutputStream out, int sequenceId) throws IOException {
        System.out.println("Intercepted information_schema.tables query");

        sequenceId = sendResultSetHeader(out, sequenceId, 2);
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "TABLE_NAME");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "TABLE_TYPE");
        sequenceId = sendEofPacket(out, sequenceId);

        List<String> tableNames = catalog.getTableNames();
        for (String tableName : tableNames) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString(tableName));
            bos.write(writeLengthEncodedString("BASE TABLE"));
            sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
        }

        sendEofPacket(out, sequenceId);
        return true;
    }

    private String extractTableNameFromQuery(String sql) {
        // Simple extraction - you may need to make this more robust
        String sqlLower = sql.toLowerCase();
        if (sqlLower.contains("where table_name")) {
            int start = sqlLower.indexOf("'", sqlLower.indexOf("table_name")) + 1;
            int end = sqlLower.indexOf("'", start);
            if (start > 0 && end > start) {
                return sql.substring(start, end);
            }
        }
        return null;
    }
    private boolean handleShowCreateTable(String sql, OutputStream out, int sequenceId) throws IOException {
        System.out.println("Intercepted SHOW CREATE TABLE query: " + sql);

        // 从 SQL 中提取表名
        String tableName = extractTableNameFromShowCreateTable(sql);
        if (tableName == null || catalog == null) {
            sendErrorPacket(out, sequenceId, 1146, "42S02", "Table doesn't exist");
            return true;
        }

        Schema schema = catalog.getTableSchema(tableName);
        if (schema == null) {
            sendErrorPacket(out, sequenceId, 1146, "42S02", "Table '" + tableName + "' doesn't exist");
            return true;
        }

        // 构建 CREATE TABLE 语句
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE `").append(tableName).append("` (\n");

        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            createTableSql.append("  `").append(col.getName()).append("` ");

            // 映射数据类型到 MySQL 类型
            switch (col.getType()) {
                case INT:
                    createTableSql.append("INT");
                    break;
                case VARCHAR:
                    createTableSql.append("VARCHAR(255)");
                    break;
                case DECIMAL:
                    createTableSql.append("DECIMAL(10,2)");
                    break;
                case DATE:
                    createTableSql.append("DATE");
                    break;
                case BOOLEAN:
                    createTableSql.append("BOOLEAN");
                    break;
                default:
                    createTableSql.append("VARCHAR(255)");
            }

            if (i < columns.size() - 1) {
                createTableSql.append(",");
            }
            createTableSql.append("\n");
        }
        createTableSql.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        // 发送结果
        sequenceId = sendResultSetHeader(out, sequenceId, 2);
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Table");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Create Table");
        sequenceId = sendEofPacket(out, sequenceId);

        // 发送数据行
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        bos.write(writeLengthEncodedString(tableName));
        bos.write(writeLengthEncodedString(createTableSql.toString()));
        sequenceId = writePacket(out, bos.toByteArray(), sequenceId);

        sendEofPacket(out, sequenceId);
        return true;
    }

    private String extractTableNameFromShowCreateTable(String sql) {
        // 移除反引号并提取表名
        String pattern = "show\\s+create\\s+table\\s+(`?)([^`\\s]+)(`?)";
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher m = p.matcher(sql);

        if (m.find()) {
            String tableName = m.group(2);
            // 如果包含数据库名，移除它
            if (tableName.contains(".")) {
                tableName = tableName.substring(tableName.lastIndexOf(".") + 1);
            }
            return tableName;
        }
        return null;
    }
    private boolean handleShowFullColumns(String sql, OutputStream out, int sequenceId) throws IOException {
        System.out.println("Intercepted SHOW FULL COLUMNS query: " + sql);

        // 从 SQL 中提取表名
        String tableName = extractTableNameFromShowColumns(sql);
        if (tableName == null || catalog == null) {
            sendErrorPacket(out, sequenceId, 1146, "42S02", "Table doesn't exist");
            return true;
        }

        Schema schema = catalog.getTableSchema(tableName);
        if (schema == null) {
            sendErrorPacket(out, sequenceId, 1146, "42S02", "Table '" + tableName + "' doesn't exist");
            return true;
        }

        // SHOW FULL COLUMNS 返回更多字段
        sequenceId = sendResultSetHeader(out, sequenceId, 9);
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Field");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Type");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Collation");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Null");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Key");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Default");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Extra");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Privileges");
        sequenceId = sendSimpleFieldPacket(out, sequenceId, "Comment");
        sequenceId = sendEofPacket(out, sequenceId);

        // 发送每列的信息
        for (Column col : schema.getColumns()) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString(col.getName()));

            // 类型
            String typeStr;
            switch (col.getType()) {
                case INT:
                    typeStr = "int(11)";
                    break;
                case VARCHAR:
                    typeStr = "varchar(255)";
                    break;
                case DECIMAL:
                    typeStr = "decimal(10,2)";
                    break;
                case DATE:
                    typeStr = "date";
                    break;
                case BOOLEAN:
                    typeStr = "tinyint(1)";
                    break;
                default:
                    typeStr = "varchar(255)";
            }
            bos.write(writeLengthEncodedString(typeStr));

            // Collation
            bos.write(writeLengthEncodedString("utf8mb4_general_ci"));

            // Null
            bos.write(writeLengthEncodedString("YES"));

            // Key
            bos.write(writeLengthEncodedString(""));

            // Default
            bos.write(0xfb); // NULL

            // Extra
            bos.write(writeLengthEncodedString(""));

            // Privileges
            bos.write(writeLengthEncodedString("select,insert,update,references"));

            // Comment
            bos.write(writeLengthEncodedString(""));

            sequenceId = writePacket(out, bos.toByteArray(), sequenceId);
        }

        sendEofPacket(out, sequenceId);
        return true;
    }

    private String extractTableNameFromShowColumns(String sql) {
        // 匹配 SHOW [FULL] COLUMNS FROM `table_name`
        String pattern = "show\\s+(?:full\\s+)?columns\\s+from\\s+(`?)([^`\\s]+)(`?)";
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher m = p.matcher(sql);

        if (m.find()) {
            return m.group(2);
        }
        return null;
    }
}