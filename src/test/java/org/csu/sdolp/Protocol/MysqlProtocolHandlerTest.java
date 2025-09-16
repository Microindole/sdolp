package org.csu.sdolp.Protocol;

import org.csu.sdolp.cli.server.MysqlProtocolHandler;
import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.catalog.Catalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * 专门用于测试 MySQL 通信协议处理器 (MysqlProtocolHandler) 的单元测试.
 * 这个测试使用模拟 (Mock) 对象来隔离网络和数据库引擎的依赖.
 */
public class MysqlProtocolHandlerTest {

    private final String TEST_DB_NAME = "protocol_test_db";

    // 使用 Mock 对象来模拟依赖
    private Socket mockSocket;
    private QueryProcessor mockQueryProcessor;
    private ByteArrayInputStream clientInputStream; // 模拟客户端发送的数据
    private ByteArrayOutputStream serverOutputStream; // 捕获服务器发送的数据

    @BeforeEach
    void setUp() throws IOException {
        deleteDirectory(new File("data/" + TEST_DB_NAME));

        mockSocket = Mockito.mock(Socket.class);
        mockQueryProcessor = Mockito.mock(QueryProcessor.class);
        serverOutputStream = new ByteArrayOutputStream();

        when(mockSocket.getOutputStream()).thenReturn(serverOutputStream);
        // 增加一个默认行为，防止 isClosed() 抛出异常
        when(mockSocket.isClosed()).thenReturn(false);
    }

    @AfterEach
    void tearDown() throws IOException {
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    private void setClientInput(byte[] data) throws IOException {
        clientInputStream = new ByteArrayInputStream(data);
        when(mockSocket.getInputStream()).thenReturn(clientInputStream);
    }

    @Test
    void testHandshakeAndAuthentication() throws Exception {
        System.out.println("--- Test: Handshake and Authentication Flow ---");

        // --- 修复点 1: 创建一个结构更完整的认证包 ---
        ByteArrayOutputStream authPayloadStream = new ByteArrayOutputStream();
        // Client Flags (4 bytes)
        authPayloadStream.write(new byte[]{ (byte)0x85, (byte)0xa2, (byte)0x03, 0x00 });
        // Max Packet Size (4 bytes)
        authPayloadStream.write(new byte[]{ 0x00, 0x00, 0x00, 0x01 });
        // Charset (1 byte)
        authPayloadStream.write((byte) 0xff);
        // Filler (23 bytes)
        authPayloadStream.write(new byte[23]);
        // Username
        authPayloadStream.write("root".getBytes(StandardCharsets.UTF_8));
        authPayloadStream.write((byte) 0x00);
        // Password (length prefixed)
        authPayloadStream.write((byte) 0x00);

        byte[] authPacketPayload = authPayloadStream.toByteArray();
        byte[] authPacket = createPacket(authPacketPayload, 1);
        setClientInput(authPacket);

        Catalog mockCatalog = Mockito.mock(Catalog.class);
        when(mockQueryProcessor.getCatalog()).thenReturn(mockCatalog);
        when(mockCatalog.getPasswordHash("root")).thenReturn("dummy_hash".getBytes());

        MysqlProtocolHandler handler = new MysqlProtocolHandler(mockSocket, mockQueryProcessor, mockCatalog, 12345);

        // --- 修复点 2: 在单独的线程中运行handler，并设置超时 ---
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(handler);
        executor.shutdown();
        // 给handler足够的时间处理握手和认证，然后结束
        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        when(mockSocket.isClosed()).thenReturn(true); // 模拟客户端断开连接，让run()循环退出

        byte[] serverResponseBytes = serverOutputStream.toByteArray();
        String serverResponseString = new String(serverResponseBytes, StandardCharsets.UTF_8);

        assertTrue(serverResponseBytes.length > 4, "Server should send some data.");
        assertTrue(serverResponseBytes[4] == 10, "Server should send a handshake packet starting with protocol version 10.");
        assertTrue(serverResponseString.contains("minidb"), "Handshake packet should contain the server version string 'minidb'.");

        // --- 修复点 3: 更精确地计算握手包长度来定位OK包 ---
        int handshakePayloadLength = (serverResponseBytes[0] & 0xFF) | ((serverResponseBytes[1] & 0xFF) << 8) | ((serverResponseBytes[2] & 0xFF) << 16);
        int handshakePacketLength = 4 + handshakePayloadLength;

        assertTrue(serverResponseBytes.length > handshakePacketLength, "Server should send more than just the handshake.");
        assertTrue(serverResponseBytes[handshakePacketLength + 4] == 0x00, "Server should send an OK packet (starts with 0x00) after successful authentication.");
        System.out.println("[SUCCESS] Handshake and authentication flow verified.");
    }

    private byte[] createPacket(byte[] payload, int sequenceId) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int len = payload.length;
        bos.write((byte) (len & 0xFF));
        bos.write((byte) ((len >> 8) & 0xFF));
        bos.write((byte) ((len >> 16) & 0xFF));
        bos.write((byte) sequenceId);
        try {
            bos.write(payload);
        } catch (IOException e) {
            // Should not happen with ByteArrayOutputStream
        }
        return bos.toByteArray();
    }

    private void deleteDirectory(File directory) {
        if (!directory.exists()) return;
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}