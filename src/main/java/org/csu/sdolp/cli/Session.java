package org.csu.sdolp.cli;

/**
 * 代表一个客户端会话，存储连接状态信息。
 */
public class Session {
    private final String username;
    private final int connectionId;
    private boolean isAuthenticated = false;

    public Session(int connectionId) {
        this.connectionId = connectionId;
        this.username = null; // Initially not logged in
    }

    private Session(int connectionId, String username) {
        this.connectionId = connectionId;
        this.username = username;
        this.isAuthenticated = true;
    }

    /**
     * 创建一个认证成功的会话。
     */
    public static Session createAuthenticatedSession(int connectionId, String username) {
        return new Session(connectionId, username);
    }

    public String getUsername() {
        return username;
    }

    public boolean isAuthenticated() {
        return isAuthenticated;
    }

    public int getConnectionId() {
        return connectionId;
    }
}