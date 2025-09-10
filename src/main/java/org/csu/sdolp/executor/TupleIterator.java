package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import java.io.IOException;

/**
 * @author hidyouth
 * @description: 元组迭代器接口，是所有执行算子的基类
 * 定义了火山模型的基本操作
 */
public interface TupleIterator {

    /**
     * 获取下一条元组
     * @return 如果存在，则返回下一条元组；否则返回 null
     */
    Tuple next() throws IOException;

    /**
     * 检查是否还有更多元组
     * @return 如果有则返回 true
     */
    boolean hasNext() throws IOException;

    Schema getOutputSchema();
}