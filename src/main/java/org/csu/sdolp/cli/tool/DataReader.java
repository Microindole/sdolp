package org.csu.sdolp.cli.tool;

import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;

import javax.swing.JFileChooser; // 导入GUI组件
import java.awt.HeadlessException; // 导入用于处理无GUI环境的异常
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * 一个用于读取和分析指定数据库数据文件 (.data) 的工具。
 * 这个版本更智能，支持在控制台查看数据或将整个数据库导出为 SQL 文件，
 * 并提供了图形化的目录选择功能和专用的导出文件夹。
 */
public class DataReader {

    private static final String EXPORT_DEFAULT_DIR = "exports"; // 专用导出目录

    public static void main(String[] args) throws IOException {
        DatabaseManager dbManager = new DatabaseManager();
        List<String> databases = dbManager.listDatabases();

        if (databases.isEmpty()) {
            System.out.println("在 'data' 目录下没有找到任何数据库。");
            return;
        }

        System.out.println("--- MiniDB 数据工具 ---");
        System.out.println("可用的数据库: " + databases);

        try (Scanner scanner = new Scanner(System.in)) {
            String dbName = selectDatabase(scanner, databases);

            System.out.println("\n请选择操作:");
            System.out.println("  1. 在控制台查看数据");
            System.out.println("  2. 导出为 SQL 文件");
            int choice = selectChoice(scanner);

            String dbFilePath = DatabaseManager.getDbFilePath(dbName);
            DiskManager diskManager = new DiskManager(dbFilePath);
            diskManager.open();
            BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
            Catalog catalog = new Catalog(bufferPoolManager);

            if (choice == 1) {
                displayDataInConsole(dbName, catalog, bufferPoolManager);
            } else {
                exportDatabaseToSql(scanner, dbName, catalog, bufferPoolManager);
            }

            diskManager.close();
        }
    }

    /**
     * 模式二：导出数据库为SQL文件，包含目录选择逻辑
     */
    private static void exportDatabaseToSql(Scanner scanner, String dbName, Catalog catalog, BufferPoolManager bufferPoolManager) throws IOException {
        // 1. 创建默认导出目录
        File defaultDir = new File(EXPORT_DEFAULT_DIR);
        if (!defaultDir.exists()) {
            defaultDir.mkdirs();
        }

        // 2. 让用户选择导出目录
        System.out.println("\n请选择导出目录:");
        System.out.println("  1. 使用默认目录 (" + defaultDir.getAbsolutePath() + ")");
        System.out.println("  2. 选择自定义目录 (将弹出文件选择窗口)");
        int dirChoice = 0;
        while (dirChoice != 1 && dirChoice != 2) {
            System.out.print("请输入你的选择 (1 或 2): ");
            try {
                dirChoice = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) { /* 忽略错误输入 */ }
        }

        String exportPathStr;
        if (dirChoice == 2) {
            exportPathStr = selectCustomDirectory(scanner);
        } else {
            exportPathStr = defaultDir.getAbsolutePath();
        }

        if (exportPathStr == null || exportPathStr.trim().isEmpty()) {
            System.out.println("未选择目录，导出已取消。");
            return;
        }

        // 3. 确定最终文件名 (包含智能后缀处理)
        String defaultFileName = dbName + "_dump_" + System.currentTimeMillis() + ".sql";
        System.out.print("请输入导出的 SQL 文件名 (默认: " + defaultFileName + "): ");
        String userInputFileName = scanner.nextLine();

        String finalFileName = processFileName(userInputFileName, defaultFileName);

        File outputFile = new File(exportPathStr, finalFileName);

        // 4. 执行导出
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            System.out.println("\n--- 开始导出数据库 '" + dbName + "' 到 " + outputFile.getAbsolutePath() + " ---");

            writer.write("-- MiniDB SQL Dump\n");
            writer.write("-- Database: " + dbName + "\n");
            writer.write("-- ------------------------------------------------------\n\n");

            List<String> userTables = catalog.getTableNames();
            for (String tableName : userTables) {
                System.out.println("正在导出表: " + tableName + "...");
                TableInfo tableInfo = catalog.getTable(tableName);

                writer.write("-- Table structure for table `" + tableName + "`\n");
                writer.write(generateCreateTableSql(tableInfo) + "\n\n");

                List<Tuple> tuples = getAllTuplesForTable(tableInfo, bufferPoolManager);
                if (!tuples.isEmpty()) {
                    writer.write("-- Dumping data for table `" + tableName + "`\n");
                    for (Tuple tuple : tuples) {
                        writer.write(generateInsertSql(tableInfo, tuple) + "\n");
                    }
                    writer.write("\n");
                }
            }
            System.out.println("--- 导出成功！ ---");
            System.out.println("文件已保存到: " + outputFile.getAbsolutePath());

        } catch (IOException e) {
            System.err.println("导出失败: " + e.getMessage());
        }
    }

    private static String processFileName(String userInput, String defaultValue) {
        if (userInput.trim().isEmpty()) {
            return defaultValue; // 用户直接回车，使用默认文件名
        }
        // 检查用户输入是否以 .sql 结尾 (忽略大小写)
        if (!userInput.toLowerCase().endsWith(".sql")) {
            return userInput + ".sql"; // 自动添加后缀
        }
        return userInput; // 用户已经输入了后缀，直接使用
    }

    /**
     * 尝试打开GUI目录选择器，如果失败则回退到命令行输入。
     */
    private static String selectCustomDirectory(Scanner scanner) {
        try {
            // 尝试使用 Swing GUI
            JFileChooser chooser = new JFileChooser();
            chooser.setDialogTitle("请选择导出SQL文件的保存目录");
            chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            chooser.setAcceptAllFileFilterUsed(false);

            if (chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
                return chooser.getSelectedFile().getAbsolutePath();
            } else {
                System.out.println("您取消了选择。");
                return null;
            }
        } catch (HeadlessException e) {
            // 如果在没有GUI的环境中运行，则捕获异常并降级为命令行输入
            System.out.println("\n检测到非图形界面环境，已切换到手动输入模式。");
            while (true) {
                System.out.print("请输入自定义导出目录的完整路径: ");
                String path = scanner.nextLine();
                File dir = new File(path);
                if (dir.exists() && dir.isDirectory()) {
                    return path;
                } else {
                    System.err.println("错误: 路径不存在或不是一个有效的目录，请重试。");
                }
            }
        }
    }

    // selectDatabase, selectChoice, displayDataInConsole, generateCreateTableSql,
    // generateInsertSql, formatValueForSql, getAllTuplesForTable 这些方法保持不变，
    // 请从上一个回答中保留它们，或者直接使用这份完整文件。
    // ... (此处省略其他未变化的方法，以节省篇幅，请确保它们存在)

    private static String selectDatabase(Scanner scanner, List<String> databases) {
        String dbName = "";
        boolean isValidDb = false;
        while (!isValidDb) {
            System.out.print("请输入要操作的数据库名称: ");
            dbName = scanner.nextLine();
            if (databases.contains(dbName)) {
                isValidDb = true;
            } else {
                System.err.println("错误: 数据库 '" + dbName + "' 不存在，请重试。");
            }
        }
        return dbName;
    }

    private static int selectChoice(Scanner scanner) {
        int choice = 0;
        while (choice != 1 && choice != 2) {
            System.out.print("请输入你的选择 (1 或 2): ");
            try {
                choice = Integer.parseInt(scanner.nextLine());
                if (choice != 1 && choice != 2) {
                    System.err.println("无效输入，请输入 1 或 2。");
                }
            } catch (NumberFormatException e) {
                System.err.println("无效输入，请输入一个数字。");
            }
        }
        return choice;
    }

    private static void displayDataInConsole(String dbName, Catalog catalog, BufferPoolManager bufferPoolManager) throws IOException {
        System.out.println("\n--- 正在读取数据库 '" + dbName + "' 的数据 ---");
        List<String> userTables = catalog.getTableNames();
        System.out.println("在数据库 '" + dbName + "' 中找到 " + userTables.size() + " 个用户表: " + userTables);

        for (String tableName : userTables) {
            System.out.println("\n--- 表 '" + tableName + "' 的数据 ---");
            TableInfo tableInfo = catalog.getTable(tableName);
            List<Tuple> allTuplesInTable = getAllTuplesForTable(tableInfo, bufferPoolManager);
            System.out.println(QueryResultFormatter.format(tableInfo.getSchema(), allTuplesInTable));
        }
    }

    private static String generateCreateTableSql(TableInfo tableInfo) {
        StringBuilder sb = new StringBuilder();
        Schema schema = tableInfo.getSchema();
        sb.append("CREATE TABLE ").append(tableInfo.getTableName()).append(" (\n");

        List<String> columnDefs = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            String colDef = "  " + column.getName() + " " + column.getType().name();
            if (column.getName().equalsIgnoreCase(schema.getPrimaryKeyColumnName())) {
                colDef += " PRIMARY KEY";
            }
            columnDefs.add(colDef);
        }
        sb.append(String.join(",\n", columnDefs));
        sb.append("\n);");
        return sb.toString();
    }

    private static String generateInsertSql(TableInfo tableInfo, Tuple tuple) {
        Schema schema = tableInfo.getSchema();
        String columns = schema.getColumnNames().stream().collect(Collectors.joining(", "));
        String values = tuple.getValues().stream()
                .map(DataReader::formatValueForSql)
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s);",
                tableInfo.getTableName(), columns, values);
    }

    private static String formatValueForSql(Value value) {
        if (value.getValue() == null) {
            return "NULL";
        }
        switch (value.getType()) {
            case VARCHAR:
            case DATE:
                return "'" + value.getValue().toString().replace("'", "''") + "'";
            case INT:
            case DECIMAL:
            case BOOLEAN:
                return value.getValue().toString();
            default:
                return "'" + value.getValue().toString() + "'";
        }
    }

    private static List<Tuple> getAllTuplesForTable(TableInfo tableInfo, BufferPoolManager bufferPoolManager) throws IOException {
        List<Tuple> allTuples = new ArrayList<>();
        if (tableInfo == null) return allTuples;

        PageId currentPageId = tableInfo.getFirstPageId();
        while (currentPageId != null && currentPageId.getPageNum() != -1) {
            Page page = bufferPoolManager.getPage(currentPageId);
            allTuples.addAll(page.getAllTuples(tableInfo.getSchema()));
            int nextPageNum = page.getNextPageId();
            currentPageId = nextPageNum != -1 ? new PageId(nextPageNum) : null;
        }
        return allTuples;
    }
}