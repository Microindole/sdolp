package org.csu.sdolp.transaction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Transaction {
    public enum State {
        ACTIVE,
        COMMITTED,
        ABORTED
    }

    private static final AtomicInteger nextTxnId = new AtomicInteger(0);
    private final int transactionId;
    private State state;
    // 用于记录该事务持有的所有页的锁
    private final Set<Integer> lockedPageIds;

    public Transaction() {
        this.transactionId = nextTxnId.getAndIncrement();
        this.state = State.ACTIVE;
        this.lockedPageIds = new HashSet<>();
    }

    public int getTransactionId() {
        return transactionId;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Set<Integer> getLockedPageIds() {
        return lockedPageIds;
    }
}