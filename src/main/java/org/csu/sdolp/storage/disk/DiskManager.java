package org.csu.sdolp.storage.disk;

import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.csu.sdolp.storage.page.Page.PAGE_SIZE;

/**
 * 磁盘管理器，封装底层的文件读写操作。
 */
public class DiskManager {
    private final String dbFilePath;
    private RandomAccessFile dbFile;

    // --- FIX: 隔离文件头和数据区 ---
    // 预留 4KB 作为文件头，专门用于存储元数据
    private static final int DB_FILE_HEADER_SIZE = 4096;
    private static final long FREE_LIST_HEADER_POINTER_OFFSET = 0;

    private int freeListHeadPageId = -1;
    private int nextFreePageId = 0;

    public DiskManager(String dbFilePath) {
        this.dbFilePath = dbFilePath;
    }

    public void open() throws IOException {
        File file = new File(dbFilePath);
        boolean isNewFile = !file.exists() || file.length() == 0;

        if (!file.exists()) {
            file.createNewFile();
        }
        this.dbFile = new RandomAccessFile(file, "rw");

        if (isNewFile) {
            // 如果是新文件，初始化头指针区域并写入-1
            dbFile.seek(FREE_LIST_HEADER_POINTER_OFFSET);
            dbFile.writeInt(freeListHeadPageId);
            // --- FIX: 新文件的第一个可用页应该是 Page 0 ---
            // 因为 Catalog 会使用 Page 0，所以我们从 0 开始分配
            this.nextFreePageId = 0;
        } else {
            // 如果是现有文件，读取头指针
            dbFile.seek(FREE_LIST_HEADER_POINTER_OFFSET);
            this.freeListHeadPageId = dbFile.readInt();
            // --- FIX: 计算下一个页号时，要考虑文件头占用的空间 ---
            // 简单起见，我们假设文件头之外都是整页
            this.nextFreePageId = (int) (dbFile.length() / PAGE_SIZE);
        }
    }

    public void close() throws IOException {
        if (dbFile != null) {
            // --- FIX: 关闭前，将最新的空闲链表头指针持久化 ---
            dbFile.seek(FREE_LIST_HEADER_POINTER_OFFSET);
            dbFile.writeInt(freeListHeadPageId);
            dbFile.close();
            dbFile = null; // 避免重复关闭
        }
    }

    public void writePage(Page page) throws IOException {
        // --- FIX: 写入页时，要为4字节的文件头预留空间 ---
        // 所有页的偏移量都应该加上文件头的大小
        long offset = (long) page.getPageId().getPageNum() * PAGE_SIZE + 4;
        dbFile.seek(offset);
        dbFile.write(page.getData().array());
    }

    public Page readPage(PageId pageId) throws IOException {
        // --- FIX: 读取页时，同样要加上文件头的偏移量 ---
        long offset = (long) pageId.getPageNum() * PAGE_SIZE + 4;
        byte[] pageData = new byte[PAGE_SIZE];

        if (offset >= dbFile.length()) {
            return new Page(pageId, pageData);
        }
        dbFile.seek(offset);
        dbFile.readFully(pageData);
        return new Page(pageId, pageData);
    }

    public synchronized PageId allocatePage() throws IOException {
        if (freeListHeadPageId != -1) {
            int reusedPageNum = freeListHeadPageId;
            PageId reusedPageId = new PageId(reusedPageNum);
            Page reusedPage = readPage(reusedPageId);
            int nextFreeId = reusedPage.getData().getInt(0);
            freeListHeadPageId = nextFreeId;
            return reusedPageId;
        } else {
            return new PageId(nextFreePageId++);
        }
    }

    public synchronized void deallocatePage(PageId pageId) throws IOException {
        int deallocatedPageNum = pageId.getPageNum();
        Page page = readPage(pageId);
        page.getData().putInt(0, freeListHeadPageId);
        writePage(page);
        freeListHeadPageId = deallocatedPageNum;
    }

    public long getFileLength() throws IOException {
        return dbFile.length();
    }
}