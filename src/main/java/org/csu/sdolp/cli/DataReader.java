package org.csu.sdolp.cli;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 一个用于读取和分析 minidb.data 文件的工具。
 */
public class DataReader {

    public static void main(String[] args) throws IOException {
        // 要分析的文件路径，确保它与你的项目路径一致
        final String DB_FILE_PATH = "minidb.data";

        System.out.println("--- MiniDB 数据文件读取器 ---");
        System.out.println("正在分析文件: " + DB_FILE_PATH);

        DiskManager diskManager = new DiskManager(DB_FILE_PATH);
        diskManager.open();
        
        // 我们也需要一个 BufferPoolManager 和 Catalog 来正确解析元数据
        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        Catalog catalog = new Catalog(bufferPoolManager);
        
        System.out.println("\n--- 步骤 1: 解析系统目录 ---");
        
        // 从 Catalog 获取元数据表的信息
        TableInfo tablesTableInfo = catalog.getTable(Catalog.CATALOG_TABLES_TABLE_NAME);
        TableInfo columnsTableInfo = catalog.getTable(Catalog.CATALOG_COLUMNS_TABLE_NAME);
        
        System.out.println("成功加载目录。");
        System.out.println("表目录 (_catalog_tables) 存储在 Page: " + tablesTableInfo.getFirstPageId().getPageNum());
        System.out.println("列目录 (_catalog_columns) 存储在 Page: " + columnsTableInfo.getFirstPageId().getPageNum());

        System.out.println("\n--- _catalog_tables 内容 ---");
        Page tablesPage = bufferPoolManager.getPage(tablesTableInfo.getFirstPageId());
        List<Tuple> tableTuples = tablesPage.getAllTuples(tablesTableInfo.getSchema());
        for (Tuple t : tableTuples) {
            System.out.println(t);
        }

        System.out.println("\n--- _catalog_columns 内容 ---");
        Page columnsPage = bufferPoolManager.getPage(columnsTableInfo.getFirstPageId());
        List<Tuple> columnTuples = columnsPage.getAllTuples(columnsTableInfo.getSchema());
        for (Tuple t : columnTuples) {
            System.out.println(t);
        }

        System.out.println("\n--- 步骤 2: 原始页面数据扫描 ---");
        long fileLength = diskManager.getFileLength();
        int numPages = (int) (fileLength / Page.PAGE_SIZE);
        System.out.println("文件总大小: " + fileLength + " 字节, 共包含 " + numPages + " 个页。");

        for (int i = 0; i < numPages; i++) {
            System.out.println("\n--- 读取 Page " + i + " ---");
            PageId pageId = new PageId(i);
            Page page = diskManager.readPage(pageId);
            byte[] data = page.getData().array();

            // 尝试将整个页面的数据作为字符串打印，看看有没有可读内容
            // 注意：这只对纯文本内容有效
            String rawText = new String(data, StandardCharsets.UTF_8).trim();
            if (!rawText.isEmpty() && isAsciiPrintable(rawText)) {
                 System.out.println("页面包含可读文本: \"" + rawText.split("\0")[0] + "\""); // 只显示第一个 null 字符前的内容
            } else {
                 System.out.println("页面不包含可读文本，可能是结构化数据或空页。");
            }
        }
        
        diskManager.close();
    }
    
    // 辅助函数，检查字符串是否主要由可打印的ASCII字符组成
    private static boolean isAsciiPrintable(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        int printableChars = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c >= 32 && c < 127) {
                printableChars++;
            }
        }
        // 如果可打印字符占80%以上，我们认为它是可读的
        return (double) printableChars / str.length() > 0.8;
    }
}