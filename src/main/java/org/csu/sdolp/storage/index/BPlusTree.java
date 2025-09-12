package org.csu.sdolp.storage.index;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public class BPlusTree {

    private int rootPageId;
    private final BufferPoolManager bufferPoolManager;
    private final Comparator<Value> keyComparator;

    public BPlusTree(BufferPoolManager bufferPoolManager, int rootPageId) {
        this.bufferPoolManager = bufferPoolManager;
        this.rootPageId = rootPageId;
        this.keyComparator = Comparator.comparingInt(v -> (Integer) v.getValue());
    }

    public boolean isEmpty() throws IOException {
        if (rootPageId == -1) return true;
        BPlusTreeNodePage rootNode = getNode(rootPageId);
        return rootNode.getKeyCount() == 0;
    }

    public RID search(Value key) throws IOException {
        if (isEmpty()) return null;
        BPlusTreeLeafPage leafNode = findLeafPage(key);
        if (leafNode == null) return null;
        int index = leafNode.keyIndexLookup(key);
        if (index < leafNode.getKeyCount() && keyComparator.compare(leafNode.getKey(index), key) == 0) {
            return leafNode.getRid(index);
        }
        return null;
    }

    private BPlusTreeLeafPage findLeafPage(Value key) throws IOException {
        int currentPageId = rootPageId;
        if (currentPageId == -1) return null;
        BPlusTreeNodePage currentNode = getNode(currentPageId);
        if (currentNode == null) return null;

        while (currentNode.getNodeType() != BPlusTreeNodePage.NodeType.LEAF) {
            BPlusTreeInternalPage internalNode = (BPlusTreeInternalPage) currentNode;
            int childIndex = internalNode.childIndexLookup(key);
            currentPageId = internalNode.getChildPageId(childIndex);
            currentNode = getNode(currentPageId);
            if (currentNode == null) return null;
        }
        return (BPlusTreeLeafPage) currentNode;
    }

    public void insert(Value key, RID rid) throws IOException {
        if (isEmpty()) {
            startNewTree(key, rid);
            return;
        }

        BPlusTreeLeafPage leafNode = findLeafPage(key);

        int index = leafNode.keyIndexLookup(key);
        if (index < leafNode.getKeyCount() && keyComparator.compare(leafNode.getKey(index), key) == 0) {
            return; // Key already exists
        }

        if (leafNode.getKeyCount() < leafNode.getMaxSize()) {
            leafNode.insert(key, rid);
            bufferPoolManager.flushPage(leafNode.page.getPageId());
        } else {
            List<Value> tempKeys = new ArrayList<>();
            List<RID> tempRids = new ArrayList<>();
            for (int i = 0; i < leafNode.getKeyCount(); i++) {
                tempKeys.add(leafNode.getKey(i));
                tempRids.add(leafNode.getRid(i));
            }
            tempKeys.add(index, key);
            tempRids.add(index, rid);

            Page newPage = bufferPoolManager.newPage();
            BPlusTreeLeafPage newLeafNode = new BPlusTreeLeafPage(newPage);
            newLeafNode.init(newPage.getPageId().getPageNum(), leafNode.getParentPageId());

            splitLeafNode(leafNode, newLeafNode, tempKeys, tempRids);

            Value middleKey = newLeafNode.getKey(0);
            insertIntoParent(leafNode, middleKey, newLeafNode);

            bufferPoolManager.flushPage(leafNode.page.getPageId());
            bufferPoolManager.flushPage(newLeafNode.page.getPageId());
        }
    }

    private void startNewTree(Value key, RID rid) throws IOException {
        BPlusTreeLeafPage rootNode = (BPlusTreeLeafPage) getNode(rootPageId);
        rootNode.init(rootPageId, -1);
        rootNode.insert(key, rid);
        bufferPoolManager.flushPage(rootNode.page.getPageId());
    }

    private void splitLeafNode(BPlusTreeLeafPage oldNode, BPlusTreeLeafPage newNode, List<Value> keys, List<RID> rids) {
        int totalSize = keys.size();
        int splitPoint = (totalSize + 1) / 2;
        int originalNextPageId = oldNode.getNextLeafPageId();

        oldNode.init(oldNode.page.getPageId().getPageNum(), oldNode.getParentPageId());

        for (int i = 0; i < splitPoint; i++) {
            oldNode.insert(keys.get(i), rids.get(i));
        }
        for (int i = splitPoint; i < totalSize; i++) {
            newNode.insert(keys.get(i), rids.get(i));
        }

        newNode.setNextLeafPageId(originalNextPageId);
        oldNode.setNextLeafPageId(newNode.page.getPageId().getPageNum());
    }

    private void insertIntoParent(BPlusTreeNodePage leftChild, Value key, BPlusTreeNodePage rightChild) throws IOException {
        if (leftChild.getParentPageId() == -1) {
            Page newRootPage = bufferPoolManager.newPage();
            BPlusTreeInternalPage newRoot = new BPlusTreeInternalPage(newRootPage);
            newRoot.init(newRootPage.getPageId().getPageNum(), -1);
            newRoot.populate(leftChild.page.getPageId().getPageNum(), key, rightChild.page.getPageId().getPageNum());

            leftChild.setParentPageId(newRootPage.getPageId().getPageNum());
            rightChild.setParentPageId(newRootPage.getPageId().getPageNum());

            this.rootPageId = newRootPage.getPageId().getPageNum();
            bufferPoolManager.flushPage(newRootPage.getPageId());
            bufferPoolManager.flushPage(leftChild.page.getPageId());
            bufferPoolManager.flushPage(rightChild.page.getPageId());
            return;
        }

        BPlusTreeInternalPage parentNode = (BPlusTreeInternalPage) getNode(leftChild.getParentPageId());

        if (parentNode.getKeyCount() < parentNode.getMaxSize()) {
            parentNode.insert(key, rightChild.page.getPageId().getPageNum());
            bufferPoolManager.flushPage(parentNode.page.getPageId());
        } else {
            Page newInternalPage = bufferPoolManager.newPage();
            BPlusTreeInternalPage newInternalNode = new BPlusTreeInternalPage(newInternalPage);
            newInternalNode.init(newInternalPage.getPageId().getPageNum(), parentNode.getParentPageId());

            Value promotedKey = splitInternalNode(parentNode, key, rightChild.page.getPageId().getPageNum(), newInternalNode);

            insertIntoParent(parentNode, promotedKey, newInternalNode);
            bufferPoolManager.flushPage(parentNode.page.getPageId());
            bufferPoolManager.flushPage(newInternalNode.page.getPageId());
        }
    }

    private Value splitInternalNode(BPlusTreeInternalPage oldNode, Value newKey, int newChildId, BPlusTreeInternalPage newNode) throws IOException {
        int totalKeys = oldNode.getKeyCount();
        List<Value> tempKeys = new ArrayList<>();
        List<Integer> tempPointers = new ArrayList<>();

        // 1. 将旧节点的所有键和指针加载到临时列表中
        tempPointers.add(oldNode.getChildPageId(0));
        for (int i = 1; i <= totalKeys; i++) {
            tempKeys.add(oldNode.getKey(i));
            tempPointers.add(oldNode.getChildPageId(i));
        }

        // 2. 插入新的键和指针
        int insertPos = oldNode.childIndexLookup(newKey) + 1;
        tempKeys.add(insertPos - 1, newKey);
        tempPointers.add(insertPos, newChildId);

        // 3. 找到分裂点并移除要提升的键
        int splitPoint = (totalKeys + 1) / 2;
        Value promotedKey = tempKeys.remove(splitPoint);

        // 4. 重新填充旧节点 (这部分逻辑之前是正确的)
        oldNode.init(oldNode.page.getPageId().getPageNum(), oldNode.getParentPageId());
        oldNode.setChildPageId(0, tempPointers.get(0));
        for (int i = 0; i < splitPoint; i++) {
            oldNode.insert(tempKeys.get(i), tempPointers.get(i + 1));
        }

        newNode.setChildPageId(0, tempPointers.get(splitPoint + 1));
        int keyIndex = splitPoint;
        int pointerIndex = splitPoint + 2;
        while(keyIndex < tempKeys.size()){
            newNode.insertAtEnd(tempKeys.get(keyIndex), tempPointers.get(pointerIndex));
            keyIndex++;
            pointerIndex++;
        }
        // 6. 更新被移动到新节点的子节点的父节点指针
        for (int i = 0; i <= newNode.getKeyCount(); i++) {
            BPlusTreeNodePage childNode = getNode(newNode.getChildPageId(i));
            if (childNode != null) {
                childNode.setParentPageId(newNode.page.getPageId().getPageNum());
                bufferPoolManager.flushPage(childNode.page.getPageId());
            }
        }
        return promotedKey;
    }

    public boolean delete(Value key) throws IOException {
        if (isEmpty()) return false;

        BPlusTreeLeafPage leafNode = findLeafPage(key);
        if (leafNode == null) return false;
        int index = leafNode.keyIndexLookup(key);

        if (index >= leafNode.getKeyCount() || keyComparator.compare(leafNode.getKey(index), key) != 0) {
            return false; // Key not found
        }

        leafNode.delete(key);
        handleUnderflow(leafNode);

        bufferPoolManager.flushPage(leafNode.page.getPageId());
        return true;
    }

    private void handleUnderflow(BPlusTreeNodePage node) throws IOException {
        if (node.getParentPageId() == -1) {
            if (node.getNodeType() == BPlusTreeNodePage.NodeType.INTERNAL && node.getKeyCount() == 0) {
                BPlusTreeInternalPage internalRoot = (BPlusTreeInternalPage) node;
                this.rootPageId = internalRoot.getChildPageId(0);
                BPlusTreeNodePage newRootNode = getNode(this.rootPageId);
                if (newRootNode != null) {
                    newRootNode.setParentPageId(-1);
                    bufferPoolManager.flushPage(newRootNode.page.getPageId());
                }
                bufferPoolManager.deletePage(node.page.getPageId());
            }
            return;
        }

        if (node.getKeyCount() >= node.getMinSize()) {
            return;
        }

        BPlusTreeInternalPage parentNode = (BPlusTreeInternalPage) getNode(node.getParentPageId());
        int nodeIndex = parentNode.getChildIndexByPageId(node.page.getPageId().getPageNum());

        if (nodeIndex > 0) {
            BPlusTreeNodePage leftSibling = getNode(parentNode.getChildPageId(nodeIndex - 1));
            if (leftSibling.getKeyCount() > leftSibling.getMinSize()) {
                redistribute(leftSibling, node, parentNode);
                return;
            }
        }

        if (nodeIndex < parentNode.getKeyCount()) {
            BPlusTreeNodePage rightSibling = getNode(parentNode.getChildPageId(nodeIndex + 1));
            if (rightSibling.getKeyCount() > rightSibling.getMinSize()) {
                redistribute(rightSibling, node, parentNode);
                return;
            }
        }

        if (nodeIndex > 0) {
            BPlusTreeNodePage leftSibling = getNode(parentNode.getChildPageId(nodeIndex - 1));
            merge(leftSibling, node, parentNode);
        } else {
            BPlusTreeNodePage rightSibling = getNode(parentNode.getChildPageId(nodeIndex + 1));
            merge(node, rightSibling, parentNode);
        }
    }

    private void redistribute(BPlusTreeNodePage fromNode, BPlusTreeNodePage toNode, BPlusTreeInternalPage parent) throws IOException {

        int fromNodeIndex = parent.getChildIndexByPageId(fromNode.page.getPageId().getPageNum());
        int toNodeIndex = parent.getChildIndexByPageId(toNode.page.getPageId().getPageNum());

        if (fromNodeIndex < toNodeIndex) { // Borrow from left
            int parentKeyIndex = fromNodeIndex + 1;
            if (fromNode.getNodeType() == BPlusTreeNodePage.NodeType.LEAF) {
                BPlusTreeLeafPage fromLeaf = (BPlusTreeLeafPage) fromNode;
                BPlusTreeLeafPage toLeaf = (BPlusTreeLeafPage) toNode;
                KeyValuePair pair = fromLeaf.removeAndGetLast();
                toLeaf.insertAtFront(pair.key(), pair.rid());
                parent.setKey(parentKeyIndex, toLeaf.getKey(0));
            } else {
                BPlusTreeInternalPage fromInternal = (BPlusTreeInternalPage) fromNode;
                BPlusTreeInternalPage toInternal = (BPlusTreeInternalPage) toNode;
                Value keyToMoveUp = fromInternal.getKey(fromInternal.getKeyCount());
                int pointerToMove = fromInternal.getChildPageId(fromInternal.getKeyCount());
                fromInternal.setKeyCount(fromInternal.getKeyCount() - 1);
                Value keyToMoveDown = parent.getKey(parentKeyIndex);
                parent.setKey(parentKeyIndex, keyToMoveUp);
                toInternal.insertAtFront(keyToMoveDown, pointerToMove);
            }
        } else { // Borrow from right
            int parentKeyIndex = toNodeIndex + 1;
            if (fromNode.getNodeType() == BPlusTreeNodePage.NodeType.LEAF) {
                BPlusTreeLeafPage fromLeaf = (BPlusTreeLeafPage) fromNode;
                BPlusTreeLeafPage toLeaf = (BPlusTreeLeafPage) toNode;
                KeyValuePair pair = fromLeaf.removeAndGetFirst();
                toLeaf.insertAtEnd(pair.key(), pair.rid());
                parent.setKey(parentKeyIndex, fromLeaf.getKey(0));
            } else {
                BPlusTreeInternalPage fromInternal = (BPlusTreeInternalPage) fromNode;
                BPlusTreeInternalPage toInternal = (BPlusTreeInternalPage) toNode;

                Value keyToMoveUp = fromInternal.getKey(1);
                int pointerToMove = fromInternal.removeAndGetFirstPointer();
                Value keyToMoveDown = parent.getKey(parentKeyIndex);

                parent.setKey(parentKeyIndex, keyToMoveUp);
                toInternal.insertAtEnd(keyToMoveDown, pointerToMove);
            }
        }
        bufferPoolManager.flushPage(fromNode.page.getPageId());
        bufferPoolManager.flushPage(toNode.page.getPageId());
        bufferPoolManager.flushPage(parent.page.getPageId());
    }

    private void merge(BPlusTreeNodePage leftNode, BPlusTreeNodePage rightNode, BPlusTreeInternalPage parent) throws IOException {

        int rightNodeIndexInParent = parent.getChildIndexByPageId(rightNode.page.getPageId().getPageNum());
        Value keyToMoveDown = parent.getKey(rightNodeIndexInParent);

        if (leftNode.getNodeType() == BPlusTreeNodePage.NodeType.LEAF) {
            BPlusTreeLeafPage leftLeaf = (BPlusTreeLeafPage) leftNode;
            BPlusTreeLeafPage rightLeaf = (BPlusTreeLeafPage) rightNode;
            for (int i = 0; i < rightLeaf.getKeyCount(); i++) {
                leftLeaf.insertAtEnd(rightLeaf.getKey(i), rightLeaf.getRid(i));
            }
            leftLeaf.setNextLeafPageId(rightLeaf.getNextLeafPageId());
        } else {
            BPlusTreeInternalPage leftInternal = (BPlusTreeInternalPage) leftNode;
            BPlusTreeInternalPage rightInternal = (BPlusTreeInternalPage) rightNode;

            leftInternal.insertAtEnd(keyToMoveDown, rightInternal.getChildPageId(0));
            for (int i = 1; i <= rightInternal.getKeyCount(); i++) {
                leftInternal.insertAtEnd(rightInternal.getKey(i), rightInternal.getChildPageId(i));
            }
        }

        parent.remove(rightNodeIndexInParent);
        bufferPoolManager.deletePage(rightNode.page.getPageId());
        bufferPoolManager.flushPage(leftNode.page.getPageId());
        bufferPoolManager.flushPage(parent.page.getPageId());

        handleUnderflow(parent);
    }

    private BPlusTreeNodePage getNode(int pageId) throws IOException {
        if (pageId == -1) return null;
        Page page = bufferPoolManager.getPage(new PageId(pageId));
        if (page == null) return null;
        BPlusTreeNodePage tempNode = new BPlusTreeLeafPage(page);
        if (tempNode.getNodeType() == BPlusTreeNodePage.NodeType.LEAF) {
            return new BPlusTreeLeafPage(page);
        } else {
            return new BPlusTreeInternalPage(page);
        }
    }

    public void printTree() throws IOException {
        if (isEmpty() || rootPageId == -1) {
            System.out.println("Tree is empty.");
            return;
        }
        System.out.println("--- B+Tree Structure ---");
        printNode(rootPageId, 0);
        System.out.println("------------------------");
    }

    private void printNode(int pageId, int level) throws IOException {
        if (pageId == -1) return;
        BPlusTreeNodePage node = getNode(pageId);
        if (node == null) return;
        String indent = "  ".repeat(level);
        System.out.println(indent + node.toString());

        if (node.getNodeType() == BPlusTreeNodePage.NodeType.INTERNAL) {
            BPlusTreeInternalPage internal = (BPlusTreeInternalPage) node;
            for (int i = 0; i <= internal.getKeyCount(); i++) {
                printNode(internal.getChildPageId(i), level + 1);
            }
        }
    }
}