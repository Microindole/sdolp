package org.csu.sdolp.storage.page;

/**
 * 页的唯一标识符。
 */
public class PageId {
    private final int pageNum;
    // 未来可扩展为支持多个文件
    // private final int fileId;

    public PageId(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageNum() {
        return pageNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageId pageId = (PageId) o;
        return pageNum == pageId.pageNum;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(pageNum);
    }
}