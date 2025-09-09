package org.csu.sdolp.executor.expressions;

import org.csu.sdolp.common.model.Tuple;

/**
 * 谓词的抽象基类，用于在 WHERE 子句中对元组进行求值。
 */
public abstract class AbstractPredicate {
    public abstract boolean evaluate(Tuple tuple);
}