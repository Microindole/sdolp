// sdolp/executor/JoinExecutor.java

package org.csu.sdolp.executor.dml;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.PlanNode;
import org.csu.sdolp.compiler.planner.plan.query.JoinPlanNode;
import org.csu.sdolp.compiler.planner.plan.query.SeqScanPlanNode;
import org.csu.sdolp.engine.ExpressionEvaluator; // 我们将需要它
import org.csu.sdolp.executor.TupleIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用嵌套循环连接算法执行 JOIN 操作。
 */
public class JoinExecutor implements TupleIterator {

    private final JoinPlanNode plan;
    private final TupleIterator leftChild;
    private final TupleIterator rightChild;
    private final Schema outputSchema;
    // 此处修改：新增成员变量来存储左右表的元数据信息
    private final TableInfo leftTableInfo;
    private final TableInfo rightTableInfo;
    private Tuple leftTuple; // 当前外层循环的元组
    private List<Tuple> rightTuples; // 内存中缓存的右表所有元组
    private int rightTupleIndex;
    private Tuple nextTuple;

    public JoinExecutor(JoinPlanNode plan, TupleIterator leftChild, TupleIterator rightChild) {
        this.plan = plan;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.outputSchema = plan.getOutputSchema();
        this.leftTuple = null;
        this.rightTuples = null;
        this.rightTupleIndex = 0;
        this.nextTuple = null;
        // 此处修改：通过递归查找来获取底层的 TableInfo，使其更通用
        this.leftTableInfo = findTableInfo(plan.getLeft());
        this.rightTableInfo = findTableInfo(plan.getRight());

        if (this.leftTableInfo == null || this.rightTableInfo == null) {
            throw new IllegalStateException("Could not find table information for one of the join sides.");
        }
//        // 此处修改：从子计划中提取 TableInfo
//        // 这一步是关键，为后续的表达式求值提供必要的上下文
//        PlanNode leftPlan = plan.getLeft();
//        PlanNode rightPlan = plan.getRight();
//
//        // 假设 JOIN 的直接子节点是 SeqScanPlanNode，这在简单查询中是常见情况
//        if (leftPlan instanceof SeqScanPlanNode && rightPlan instanceof SeqScanPlanNode) {
//            this.leftTableInfo = ((SeqScanPlanNode) leftPlan).getTableInfo();
//            this.rightTableInfo = ((SeqScanPlanNode) rightPlan).getTableInfo();
//        } else {
//            // 在更复杂的系统中，这里可能需要递归地向下查找扫描节点
//            // 但对于当前项目，这个简化是可行的
//            throw new IllegalStateException("JoinExecutor currently expects its direct children to be SeqScanPlanNodes.");
//        }
    }
    /**
     * 此处修改：新增一个递归辅助方法来向下遍历计划树，直到找到SeqScanPlanNode以获取TableInfo。
     * @param node 当前的计划节点
     * @return 找到的 TableInfo，如果找不到则返回 null
     */
    private TableInfo findTableInfo(PlanNode node) {
        if (node instanceof SeqScanPlanNode seqScanPlan) {
            return seqScanPlan.getTableInfo();
        }

        // 这是一个简化的反射式递归，适用于单子节点的计划节点（如Filter, Project, Limit等）
        // 在真实的数据库中，计划节点的结构会更标准化
        try {
            java.lang.reflect.Method getChildMethod = node.getClass().getMethod("getChild");
            PlanNode childNode = (PlanNode) getChildMethod.invoke(node);
            return findTableInfo(childNode);
        } catch (Exception e) {
            // 如果节点没有 getChild 方法，或者发生其他反射错误，则返回 null
            return null;
        }
    }

    // 初始化，将右表全部加载到内存
    private void init() throws IOException {
        if (rightTuples == null) {
            rightTuples = new ArrayList<>();
            while (rightChild.hasNext()) {
                rightTuples.add(rightChild.next());
            }
        }
    }

    @Override
    public Tuple next() throws IOException {
        // 如果 hasNext() 没有被调用过，先调用它来预取
        if (nextTuple == null && !hasNext()) {
            return null;
        }
        // 返回预取好的元组，并清空缓存
        Tuple result = this.nextTuple;
        this.nextTuple = null;
        return result;
    }


    @Override
    public boolean hasNext() throws IOException {
        // 如果已经预取了一个，直接返回 true
        if (nextTuple != null) {
            return true;
        }

        init(); // 确保右表已缓存

        while (true) {
            // 如果当前左表元组为空，就从左边的子执行器获取下一个
            if (leftTuple == null) {
                if (leftChild.hasNext()) {
                    leftTuple = leftChild.next();
                    rightTupleIndex = 0; // 每换一个新的左元组，右表指针重置
                } else {
                    return false; // 左表已经遍历完毕，连接结束
                }
            }
            // 遍历右表，寻找匹配项
            while (rightTupleIndex < rightTuples.size()) {
                Tuple rightTuple = rightTuples.get(rightTupleIndex);
                rightTupleIndex++;
                // 组合左右元组用于条件判断
                List<Value> combinedValues = new ArrayList<>(leftTuple.getValues());
                combinedValues.addAll(rightTuple.getValues());
                Tuple combinedTuple = new Tuple(combinedValues);
                // 检查连接条件是否满足
                if (ExpressionEvaluator.evaluate(plan.getJoinCondition(), outputSchema, combinedTuple,leftTableInfo, rightTableInfo)) {
                    this.nextTuple = combinedTuple; // 找到匹配项，缓存并返回 true
                    return true;
                }
            }
            // 当前左元组已经和所有右元组匹配完毕，清空 leftTuple 以便外层循环获取下一个
            leftTuple = null;
        }
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
}