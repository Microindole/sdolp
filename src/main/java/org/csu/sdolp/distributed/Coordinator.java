package org.csu.sdolp.distributed;

import org.csu.sdolp.cli.server.Session;
import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.transaction.Transaction;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分布式事务协调者 (Transaction Coordinator)。
 * 负责实现两阶段提交 (2PC) 协议。
 */
public class Coordinator {

    private final List<QueryProcessor> participants;
    private final Map<String, Transaction> localTransactionMap;
    private final Session mockSession = Session.createAuthenticatedSession(-1, "root");

    public Coordinator(List<QueryProcessor> participants) {
        this.participants = participants;
        this.localTransactionMap = new ConcurrentHashMap<>();
    }

    /**
     * 执行一个分布式事务。
     * @param operations 一个Map，Key是参与者，Value是该参与者需要执行的SQL。
     * @return 如果事务成功提交，返回 true。
     */
    public boolean executeDistributedTransaction(Map<QueryProcessor, String> operations) {
        // 全局唯一事务ID
        final String globalTxnId = UUID.randomUUID().toString();
        System.out.printf("%n[Coordinator] === 开始分布式事务 [GlobalID: %s] ===%n", globalTxnId);

        boolean allPrepared = true;

        // --- 阶段 1: 准备 (Prepare Phase) ---
        System.out.println("[Coordinator] --- 阶段 1: 发送 PREPARE 请求 ---");
        for (QueryProcessor participant : operations.keySet()) {
            try {
                // 每个参与者开启一个本地事务
                Transaction localTxn = participant.getTransactionManager().begin();
                localTransactionMap.put(globalTxnId + participant.getDbName(), localTxn);

                String sql = operations.get(participant);
                boolean prepared = participant.prepareTransaction(localTxn, sql, mockSession);
                if (!prepared) {
                    allPrepared = false;
                    break;
                }
            } catch (Exception e) {
                System.err.printf("[Coordinator] 参与者 %s 在准备阶段失败: %s%n", participant.getDbName(), e.getMessage());
                allPrepared = false;
                break;
            }
        }

        // --- 阶段 2: 提交/回滚 (Commit/Abort Phase) ---
        if (allPrepared) {
            System.out.println("[Coordinator] --- 阶段 2: 所有参与者准备就绪。发送 GLOBAL COMMIT ---");
            for (QueryProcessor participant : operations.keySet()) {
                try {
                    Transaction localTxn = localTransactionMap.get(globalTxnId + participant.getDbName());
                    participant.commitPrepared(localTxn);
                } catch (Exception e) {
                    // 这是一个严重问题，理论上不应发生。需要人工干预。
                    System.err.printf("[Coordinator] 严重错误！在发送 COMMIT 后，参与者 %s 提交失败: %s%n", participant.getDbName(), e.getMessage());
                    return false;
                }
            }
            System.out.printf("[Coordinator] === 分布式事务 [GlobalID: %s] 成功提交 ===%n", globalTxnId);
            return true;
        } else {
            System.out.println("[Coordinator] --- 阶段 2: 至少一个参与者准备失败。发送 GLOBAL ABORT ---");
            for (QueryProcessor participant : operations.keySet()) {
                try {
                    Transaction localTxn = localTransactionMap.get(globalTxnId + participant.getDbName());
                    if (localTxn != null) {
                        participant.abortPrepared(localTxn);
                    }
                } catch (Exception e) {
                    System.err.printf("[Coordinator] 严重错误！在发送 ABORT 后，参与者 %s 回滚失败: %s%n", participant.getDbName(), e.getMessage());
                }
            }
            System.out.printf("[Coordinator] === 分布式事务 [GlobalID: %s] 已回滚 ===%n", globalTxnId);
            return false;
        }
    }
}