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

        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        boolean isNewFile = !file.exists() || file.length() == 0;

        if (!file.exists()) {
            file.createNewFile();
        }
        this.dbFile = new RandomAccessFile(file, "rw");

        if (isNewFile) {
            // 如果是新文件，写入一个完整的、内容为0的文件头
            byte[] header = new byte[DB_FILE_HEADER_SIZE];
            dbFile.write(header);
            // 在文件头内写入空闲链表指针
            dbFile.seek(FREE_LIST_HEADER_POINTER_OFFSET);
            dbFile.writeInt(freeListHeadPageId);
            // 数据页从0开始分配，因为不再与文件头冲突
            this.nextFreePageId = 0;
        } else {
            // 如果是现有文件，从文件头读取指针
            dbFile.seek(FREE_LIST_HEADER_POINTER_OFFSET);
            this.freeListHeadPageId = dbFile.readInt();
            // 计算下一个可用页ID时，要减去文件头的大小
            this.nextFreePageId = (int) ((dbFile.length() - DB_FILE_HEADER_SIZE) / PAGE_SIZE);
        }
    }

    public void close() throws IOException {
        if (dbFile != null) {
            // 关闭前，将空闲链表的头指针持久化到文件头
            dbFile.seek(FREE_LIST_HEADER_POINTER_OFFSET);
            dbFile.writeInt(freeListHeadPageId);
            dbFile.getFD().sync();
            dbFile.close();
            dbFile = null;
        }
    }

    public void writePage(Page page) throws IOException {
        // 写入页时，要加上文件头的偏移量
        long offset = (long) page.getPageId().getPageNum() * PAGE_SIZE + DB_FILE_HEADER_SIZE;
        dbFile.seek(offset);
        dbFile.write(page.getData().array());
        dbFile.getFD().sync();
    }

    public Page readPage(PageId pageId) throws IOException {
        // 读取页时，也要加上文件头的偏移量
        long offset = (long) pageId.getPageNum() * PAGE_SIZE + DB_FILE_HEADER_SIZE;
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