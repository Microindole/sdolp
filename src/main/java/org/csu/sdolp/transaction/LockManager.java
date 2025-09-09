package org.csu.sdolp.transaction;

import org.csu.sdolp.storage.page.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {
    public enum LockMode {
        SHARED,
        EXCLUSIVE
    }

    private static class LockRequest {
        public final int txnId;
        public final LockMode lockMode;
        public boolean granted;

        public LockRequest(int txnId, LockMode lockMode) {
            this.txnId = txnId;
            this.lockMode = lockMode;
            this.granted = false;
        }
    }

    private static class LockRequestQueue {
        public final List<LockRequest> requestList = new ArrayList<>();
        private final ReentrantLock latch = new ReentrantLock();
        private final Condition condition = latch.newCondition();
        private int sharingCount = 0; // 记录共享锁的数量
    }

    private final Map<Integer, LockRequestQueue> lockTable = new ConcurrentHashMap<>();

    /**
     * 检查一个新的锁请求是否可以被授予。
     * @param newRequest 新的锁请求
     * @param queue 当前页的锁请求队列
     * @return 如果可以授予，返回 true
     */
    private boolean isLockCompatible(LockRequest newRequest, LockRequestQueue queue) {
        for (LockRequest req : queue.requestList) {
            if (req.granted && req.txnId != newRequest.txnId) {
                // 如果新请求是排他锁，任何已存在的锁都会冲突
                if (newRequest.lockMode == LockMode.EXCLUSIVE) {
                    return false;
                }
                // 如果新请求是共享锁，而已存在的是排他锁，则冲突
                if (req.lockMode == LockMode.EXCLUSIVE) {
                    return false;
                }
            }
        }
        return true;
    }

    private void lock(Transaction txn, PageId pageId, LockMode lockMode) throws InterruptedException {
        int pageNum = pageId.getPageNum();
        LockRequestQueue queue = lockTable.computeIfAbsent(pageNum, k -> new LockRequestQueue());
        LockRequest request = new LockRequest(txn.getTransactionId(), lockMode);

        queue.latch.lock();
        try {
            queue.requestList.add(request);
            // 当无法立即获得锁时，循环等待
            while (!isLockCompatible(request, queue)) {
                queue.condition.await();
            }
            // 授予锁
            request.granted = true;
            if (request.lockMode == LockMode.SHARED) {
                queue.sharingCount++;
            }
            txn.getLockedPageIds().add(pageNum);
            System.out.println("Transaction " + txn.getTransactionId() + " acquired " + lockMode + " lock on page " + pageNum);

        } finally {
            queue.latch.unlock();
        }
    }

    public void lockShared(Transaction txn, PageId pageId) throws InterruptedException {
        lock(txn, pageId, LockMode.SHARED);
    }

    public void lockExclusive(Transaction txn, PageId pageId) throws InterruptedException {
        lock(txn, pageId, LockMode.EXCLUSIVE);
    }

    public void unlock(Transaction txn, PageId pageId) {
        int pageNum = pageId.getPageNum();
        LockRequestQueue queue = lockTable.get(pageNum);
        if (queue == null) return;

        queue.latch.lock();
        try {
            // 移除该事务的请求
            queue.requestList.removeIf(req -> {
                if (req.txnId == txn.getTransactionId()) {
                    if (req.granted && req.lockMode == LockMode.SHARED) {
                        queue.sharingCount--;
                    }
                    return true;
                }
                return false;
            });
            txn.getLockedPageIds().remove(pageNum);
            // 唤醒所有等待的线程，让他们重新竞争锁
            queue.condition.signalAll();
            System.out.println("Transaction " + txn.getTransactionId() + " released lock on page " + pageNum);
        } finally {
            queue.latch.unlock();
        }
    }
}