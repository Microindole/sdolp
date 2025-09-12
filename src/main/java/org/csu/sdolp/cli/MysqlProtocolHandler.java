package org.csu.sdolp.cli;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.executor.TupleIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MysqlProtocolHandler implements Runnable {

    private final Socket clientSocket;
    private final QueryProcessor queryProcessor;
    private final int connectionId;

    private static class Packet {
        final int sequenceId;
        final byte[] payload;
        Packet(int sequenceId, byte[] payload) {
            this.sequenceId = sequenceId;
            this.payload = payload;
        }
    }

    public MysqlProtocolHandler(Socket clientSocket, QueryProcessor queryProcessor, int connectionId) {
        this.clientSocket = clientSocket;
        this.queryProcessor = queryProcessor;
        this.connectionId = connectionId;
    }

    @Override
    public void run() {
        System.out.println("MySQL Protocol Handler started for connection ID: " + connectionId);
        try (InputStream in = clientSocket.getInputStream();
             OutputStream out = clientSocket.getOutputStream()) {

            int serverSequenceId = 0;

            serverSequenceId = sendHandshake(out, serverSequenceId);
            System.out.println("Sent handshake packet to client.");

            Packet authPacket = readPacket(in);

            if (authPacket == null) {
                System.err.println("Client disconnected after handshake. Handshake packet might be incompatible.");
                return;
            }
            System.out.println("Received authentication packet from client.");

            serverSequenceId = authPacket.sequenceId + 1;
            serverSequenceId = sendOkPacket(out, serverSequenceId, 0, 0);
            System.out.println("Sent OK packet to client, faking successful authentication.");

            while (!clientSocket.isClosed()) {
                Packet commandPacket = readPacket(in);
                if (commandPacket == null) {
                    break;
                }
                handleCommand(commandPacket, out);
            }

        } catch (IOException e) {
            if (!e.getMessage().contains("Connection reset by peer") && !e.getMessage().contains("Socket closed")) {
                System.err.println("Error handling client connection " + connectionId + ": " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Unexpected error in protocol handler " + connectionId + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (!clientSocket.isClosed()) clientSocket.close();
                System.out.println("Client disconnected: " + clientSocket.getInetAddress());
            } catch (IOException e) { /* ignore */ }
        }
    }

    private int sendHandshake(OutputStream out, int sequenceId) throws IOException {
        byte[] protocolVersion = {10};
        byte[] serverVersion = "8.0.28-minidb".getBytes(StandardCharsets.UTF_8);
        byte[] connectionIdBytes = writeInt(connectionId, 4);
        byte[] salt1 = new byte[8];
        new Random().nextBytes(salt1);
        byte[] salt2 = new byte[12];
        new Random().nextBytes(salt2);

        int capabilityFlagsInt = 0xf7ff;
        byte[] capabilitiesBytes = writeInt(capabilityFlagsInt, 4);
        byte characterSet = 33;
        byte[] statusFlags = {2, 0};
        byte[] authPluginName = "mysql_native_password".getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream packetStream = new ByteArrayOutputStream();
        packetStream.write(protocolVersion);
        packetStream.write(serverVersion);
        packetStream.write(0);
        packetStream.write(connectionIdBytes);
        packetStream.write(salt1);
        packetStream.write(0);
        packetStream.write(capabilitiesBytes, 0, 2);
        packetStream.write(characterSet);
        packetStream.write(statusFlags);
        packetStream.write(capabilitiesBytes, 2, 2);

        // *** 核心修复：正确写入盐值长度 ***
        packetStream.write((byte) (salt1.length + salt2.length)); // <<-- 这里不再是0，而是20！

        packetStream.write(new byte[10]);
        // 为了兼容某些客户端，盐值第二部分需要补一个NULL结尾符
        packetStream.write(salt2, 0, 11);
        packetStream.write(0);

        packetStream.write(authPluginName);
        packetStream.write(0);

        return writePacket(out, packetStream.toByteArray(), sequenceId);
    }

    private void handleCommand(Packet commandPacket, OutputStream out) throws Exception {
        int commandType = commandPacket.payload[0] & 0xFF;
        int serverSequenceId = 1;

        switch (commandType) {
            case 0x03: // COM_QUERY
                String sql = new String(commandPacket.payload, 1, commandPacket.payload.length - 1, StandardCharsets.UTF_8);
                if (sql.toLowerCase().startsWith("set names") || sql.toLowerCase().contains("@@")) {
                    sendOkPacket(out, 0, 0, 0);
                    return;
                }
                System.out.println("Received SQL query: " + sql);
                try {
                    TupleIterator iterator = queryProcessor.executeMysql(sql);

                    if (iterator == null || iterator.getOutputSchema() == null) {
                        sendOkPacket(out, serverSequenceId, 1, 0);
                        return;
                    }

                    Schema schema = iterator.getOutputSchema();
                    List<Tuple> results = new ArrayList<>();
                    while(iterator.hasNext()){
                        results.add(iterator.next());
                    }

                    serverSequenceId = sendResultSetHeader(out, serverSequenceId, schema.getColumns().size());
                    serverSequenceId = sendFieldPackets(out, serverSequenceId, schema);
                    if (!results.isEmpty()) {
                        serverSequenceId = sendRowPackets(out, serverSequenceId, results);
                    }
                    sendEofPacket(out, serverSequenceId);

                } catch (Exception e) {
                    e.printStackTrace();
                    sendErrorPacket(out, serverSequenceId, 1064, "42000", e.getMessage());
                }
                break;
            case 0x01: // COM_QUIT
                clientSocket.close();
                break;
            default:
                sendErrorPacket(out, serverSequenceId, 1045, "HY000", "Command not supported yet");
                break;
        }
    }

    private int sendResultSetHeader(OutputStream out, int sequenceId, int fieldCount) throws IOException {
        return writePacket(out, writeLengthEncodedInt(fieldCount), sequenceId);
    }
    private int sendFieldPackets(OutputStream out, int sequenceId, Schema schema) throws IOException {
        for (org.csu.sdolp.common.model.Column col : schema.getColumns()) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(writeLengthEncodedString("def"));
            bos.write(writeLengthEncodedString(""));
            bos.write(writeLengthEncodedString(col.getName()));
            bos.write(writeLengthEncodedString(col.getName()));
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
        return sendEofPacket(out, sequenceId);
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
        byte[] eofPacket = { (byte) 0xfe, 0x00, 0x00, 0x02, 0x00 };
        return writePacket(out, eofPacket, sequenceId);
    }
    private Packet readPacket(InputStream in) throws IOException {
        byte[] header = new byte[4];
        int n = in.read(header);
        if (n < 4) {
            return null;
        }
        int payloadLength = (header[0] & 0xFF) | ((header[1] & 0xFF) << 8) | ((header[2] & 0xFF) << 16);
        int sequenceId = header[3] & 0xFF;
        if (payloadLength == 0) {
            return new Packet(sequenceId, new byte[0]);
        }
        byte[] payload = new byte[payloadLength];
        int bytesRead = 0;
        while(bytesRead < payloadLength) {
            int read = in.read(payload, bytesRead, payloadLength - bytesRead);
            if (read == -1) throw new IOException("Incomplete packet read");
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
        byte[] okHeader = {0x00};
        byte[] statusFlags = {0x02, 0x00};
        byte[] warnings = {0x00, 0x00};
        ByteArrayOutputStream packetStream = new ByteArrayOutputStream();
        packetStream.write(okHeader);
        packetStream.write(writeLengthEncodedInt(affectedRows));
        packetStream.write(writeLengthEncodedInt(lastInsertId));
        packetStream.write(statusFlags);
        packetStream.write(warnings);
        return writePacket(out, packetStream.toByteArray(), sequenceId);
    }
    private int sendErrorPacket(OutputStream out, int sequenceId, int errorCode, String sqlState, String message) throws IOException {
        byte[] errorHeader = {(byte) 0xFF};
        byte[] errorCodeBytes = writeInt(errorCode, 2);
        byte[] sqlStateMarker = {'#'};
        byte[] sqlStateBytes = sqlState.getBytes(StandardCharsets.UTF_8);
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream packetStream = new ByteArrayOutputStream();
        packetStream.write(errorHeader);
        packetStream.write(errorCodeBytes);
        packetStream.write(sqlStateMarker);
        packetStream.write(sqlStateBytes);
        packetStream.write(messageBytes);
        return writePacket(out, packetStream.toByteArray(), sequenceId);
    }
    private byte[] writeInt(int value, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) ((value >> (i * 8)) & 0xFF);
        }
        return bytes;
    }
    private byte[] writeLengthEncodedInt(long n) {
        if (n < 251) return new byte[]{(byte) n};
        if (n < 65536) return new byte[]{(byte) 0xfc, (byte) (n), (byte) (n >> 8)};
        if (n < 16777216) return new byte[]{(byte) 0xfd, (byte) (n), (byte) (n >> 8), (byte) (n >> 16)};
        return new byte[]{(byte) 0xfe, (byte) (n), (byte) (n >> 8), (byte) (n >> 16), (byte) (n >> 24),
                (byte) (n >> 32), (byte) (n >> 40), (byte) (n >> 48), (byte) (n >> 56)};
    }
    private byte[] writeLengthEncodedString(String s) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] data = s.getBytes(StandardCharsets.UTF_8);
        bos.write(writeLengthEncodedInt(data.length));
        bos.write(data);
        return bos.toByteArray();
    }
}