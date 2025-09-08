package org.csu.sdolp.storage.disk;


import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static org.csu.sdolp.storage.page.Page.PAGE_SIZE;

/**
 * 磁盘管理器，封装底层的文件读写操作。
 */
public class DiskManager {
    private final String dbFilePath;
    private RandomAccessFile dbFile;

    private int nextFreePageId = 0;


    public DiskManager(String dbFilePath) {
        this.dbFilePath = dbFilePath;
    }

    public void open() throws IOException {
        File file = new File(dbFilePath);
        // 如果文件不存在，则创建
        if (!file.exists()) {
            file.createNewFile();
        }
        this.dbFile = new RandomAccessFile(file, "rw");
        this.nextFreePageId = (int) (dbFile.length() / PAGE_SIZE);
    }

    public void close() throws IOException {
        if (dbFile != null) {
            dbFile.close();
        }
    }

    /**
     * 将一个页写入磁盘。
     * @param page 要写入的Page对象
     */
    public void writePage(Page page) throws IOException {
        long offset = (long) page.getPageId().getPageNum() * PAGE_SIZE;
        dbFile.seek(offset);
        dbFile.write(page.getData().array());
    }

    /**
     * 从磁盘读取一个页。
     * @param pageId 要读取的页ID
     * @return 读取到的Page对象
     */
    public Page readPage(PageId pageId) throws IOException {
        long offset = (long) pageId.getPageNum() * PAGE_SIZE;
        byte[] pageData = new byte[PAGE_SIZE];
        dbFile.seek(offset);
        dbFile.readFully(pageData);
        return new Page(pageId, pageData);
    }
    
    /**
     * 分配一个新页的页号。
     * @return 新分配的页ID
     */
    public synchronized PageId allocatePage() throws IOException {
        return new PageId(nextFreePageId++);
    }

    // TODO: 实现其他方法，如释放页等。
}