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

        // --- 步骤 1: 创建一个模拟客户端发送的、结构完整的认证包 ---
        ByteArrayOutputStream authPayloadStream = new ByteArrayOutputStream();
        // Client Flags (4 bytes)
        authPayloadStream.write(new byte[]{ (byte)0x85, (byte)0xa2, (byte)0x03, 0x00 });
        // Max Packet Size (4 bytes)
        authPayloadStream.write(new byte[]{ 0x00, 0x00, 0x00, 0x01 });
        // Charset (1 byte)
        authPayloadStream.write((byte) 0xff);
        // Filler (23 bytes)
        authPayloadStream.write(new byte[23]);
        // Username 'root'
        authPayloadStream.write("root".getBytes(StandardCharsets.UTF_8));
        authPayloadStream.write((byte) 0x00);
        // Password (empty)
        authPayloadStream.write((byte) 0x00);

        byte[] authPacketPayload = authPayloadStream.toByteArray();
        byte[] authPacket = createPacket(authPacketPayload, 1);
        setClientInput(authPacket);

        // 注意: MysqlProtocolHandler 的 run() 方法会忽略这里传入的 mock 对象,
        // 并创建一个真实的 QueryProcessor。我们仍然需要传入它们以满足构造函数的要求。
        Catalog mockCatalog = Mockito.mock(Catalog.class);
        when(mockQueryProcessor.getCatalog()).thenReturn(mockCatalog);
        when(mockCatalog.getPasswordHash("root")).thenReturn("dummy_hash".getBytes());

        MysqlProtocolHandler handler = new MysqlProtocolHandler(mockSocket, mockQueryProcessor, mockCatalog, 12345);

        // --- 步骤 2: 在单独的线程中运行 handler ---
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(handler);
        executor.shutdown();

        // 原始的 500ms 超时对于真实的数据库初始化（包括日志恢复）来说可能太短。
        // 我们将其增加到 5 秒，以确保 handler 线程有足够的时间完成所有启动和认证流程。
        boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(terminated, "Handler thread should terminate within the extended timeout.");

        when(mockSocket.isClosed()).thenReturn(true); // 确保在检查结果前，模拟的socket已关闭

        // --- 步骤 3: 验证服务器的响应 ---
        byte[] serverResponseBytes = serverOutputStream.toByteArray();
        String serverResponseString = new String(serverResponseBytes, StandardCharsets.UTF_8);

        assertTrue(serverResponseBytes.length > 4, "Server should send some data.");
        assertTrue(serverResponseBytes[4] == 10, "Server should send a handshake packet starting with protocol version 10.");
        assertTrue(serverResponseString.contains("minidb"), "Handshake packet should contain the server version string 'minidb'.");

        // 更精确地计算握手包长度来定位后续的 OK 包
        int handshakePayloadLength = (serverResponseBytes[0] & 0xFF) | ((serverResponseBytes[1] & 0xFF) << 8) | ((serverResponseBytes[2] & 0xFF) << 16);
        int handshakePacketLength = 4 + handshakePayloadLength;

        assertTrue(serverResponseBytes.length > handshakePacketLength, "Server should send more than just the handshake (i.e., an OK packet).");
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