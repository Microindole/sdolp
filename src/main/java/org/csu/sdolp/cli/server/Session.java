package org.csu.sdolp.cli.server;

import lombok.Getter;
import lombok.Setter;
import org.csu.sdolp.transaction.Transaction;

/**
 * 代表一个客户端会话，存储连接状态信息。
 */
public class Session {
    @Getter
    private final String username;
    @Getter
    private final int connectionId;
    private boolean isAuthenticated = false;
    @Setter
    @Getter
    private String currentDatabase;

    @Setter
    @Getter
    private Transaction activeTransaction;

    public Session(int connectionId) {
        this.connectionId = connectionId;
        this.username = null;
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

    public boolean isAuthenticated() {
        return isAuthenticated;
    }

    public boolean isInTransaction() {
        return this.activeTransaction != null;
    }

}