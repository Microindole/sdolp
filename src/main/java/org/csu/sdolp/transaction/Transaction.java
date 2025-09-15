package org.csu.sdolp.transaction;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class Transaction {
    public enum State {
        ACTIVE,
        PREPARED,
        COMMITTED,
        ABORTED
    }

    private static final AtomicInteger nextTxnId = new AtomicInteger(0);
    private final int transactionId;
    @Setter
    private State state;
    private final Set<Integer> lockedPageIds;

    // --- 记录该事务的上一条日志的LSN ---
    @Setter
    private long prevLSN = -1; // 初始值为-1，表示没有前序日志

    public Transaction() {
        this.transactionId = nextTxnId.getAndIncrement();
        this.state = State.ACTIVE;
        this.lockedPageIds = new HashSet<>();
    }

    public Transaction(int transactionId) {
        this.transactionId = transactionId;
        this.state = State.ACTIVE;
        this.lockedPageIds = new HashSet<>();
    }

}