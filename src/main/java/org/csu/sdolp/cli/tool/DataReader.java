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

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class DataReader {
    /**
     * 新增：可被外部调用的静态方法，用于导出数据库
     */
    public static void exportDatabaseToFile(String dbName, File outputFile) throws IOException {
        DiskManager diskManager = new DiskManager(DatabaseManager.getDbFilePath(dbName));
        diskManager.open();
        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        Catalog catalog = new Catalog(bufferPoolManager);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("-- MiniDB SQL Dump\n");
            writer.write("-- Database: " + dbName + "\n");
            writer.write("-- ------------------------------------------------------\n\n");

            List<String> userTables = catalog.getTableNames();
            for (String tableName : userTables) {
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
        } finally {
            diskManager.close();
        }
    }

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

            if (choice == 1) {
                displayDataInConsole(dbName);
            } else {
                exportDatabaseToSqlInteractive(scanner, dbName);
            }
        }
    }

    private static void displayDataInConsole(String dbName) throws IOException {
        DiskManager diskManager = new DiskManager(DatabaseManager.getDbFilePath(dbName));
        diskManager.open();
        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        Catalog catalog = new Catalog(bufferPoolManager);

        System.out.println("\n--- 正在读取数据库 '" + dbName + "' 的数据 ---");
        List<String> userTables = catalog.getTableNames();
        System.out.println("在数据库 '" + dbName + "' 中找到 " + userTables.size() + " 个用户表: " + userTables);

        for (String tableName : userTables) {
            System.out.println("\n--- 表 '" + tableName + "' 的数据 ---");
            TableInfo tableInfo = catalog.getTable(tableName);
            List<Tuple> allTuplesInTable = getAllTuplesForTable(tableInfo, bufferPoolManager);
            System.out.println(QueryResultFormatter.format(tableInfo.getSchema(), allTuplesInTable));
        }
        diskManager.close();
    }

    private static void exportDatabaseToSqlInteractive(Scanner scanner, String dbName) throws IOException {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("请选择SQL文件的保存位置");
        chooser.setSelectedFile(new File(dbName + "_dump.sql"));
        chooser.setFileFilter(new FileNameExtensionFilter("SQL File", "sql"));

        if (chooser.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            exportDatabaseToFile(dbName, file);
            System.out.println("--- 导出成功！ ---");
            System.out.println("文件已保存到: " + file.getAbsolutePath());
        } else {
            System.out.println("导出已取消。");
        }
    }
    private static String selectDatabase(Scanner scanner, List<String> databases) { String dbName = ""; boolean isValidDb = false; while (!isValidDb) { System.out.print("请输入要操作的数据库名称: "); dbName = scanner.nextLine(); if (databases.contains(dbName)) { isValidDb = true; } else { System.err.println("错误: 数据库 '" + dbName + "' 不存在，请重试。"); } } return dbName; }
    private static int selectChoice(Scanner scanner) { int choice = 0; while (choice != 1 && choice != 2) { System.out.print("请输入你的选择 (1 或 2): "); try { choice = Integer.parseInt(scanner.nextLine()); if (choice != 1 && choice != 2) { System.err.println("无效输入，请输入 1 或 2。"); } } catch (NumberFormatException e) { System.err.println("无效输入，请输入一个数字。"); } } return choice; }
    private static String generateCreateTableSql(TableInfo tableInfo) { StringBuilder sb = new StringBuilder(); Schema schema = tableInfo.getSchema(); sb.append("CREATE TABLE ").append(tableInfo.getTableName()).append(" (\n"); List<String> columnDefs = new ArrayList<>(); for (Column column : schema.getColumns()) { String colDef = "  " + column.getName() + " " + column.getType().name(); if (column.getName().equalsIgnoreCase(schema.getPrimaryKeyColumnName())) { colDef += " PRIMARY KEY"; } columnDefs.add(colDef); } sb.append(String.join(",\n", columnDefs)); sb.append("\n);"); return sb.toString(); }
    private static String generateInsertSql(TableInfo tableInfo, Tuple tuple) { Schema schema = tableInfo.getSchema(); String columns = schema.getColumnNames().stream().collect(Collectors.joining(", ")); String values = tuple.getValues().stream().map(DataReader::formatValueForSql).collect(Collectors.joining(", ")); return String.format("INSERT INTO %s (%s) VALUES (%s);", tableInfo.getTableName(), columns, values); }
    private static String formatValueForSql(Value value) { if (value.getValue() == null) { return "NULL"; } switch (value.getType()) { case VARCHAR: case DATE: return "'" + value.getValue().toString().replace("'", "''") + "'"; case INT: case DECIMAL: case BOOLEAN: return value.getValue().toString(); default: return "'" + value.getValue().toString() + "'"; } }
    private static List<Tuple> getAllTuplesForTable(TableInfo tableInfo, BufferPoolManager bufferPoolManager) throws IOException { List<Tuple> allTuples = new ArrayList<>(); if (tableInfo == null) return allTuples; PageId currentPageId = tableInfo.getFirstPageId(); while (currentPageId != null && currentPageId.getPageNum() != -1) { Page page = bufferPoolManager.getPage(currentPageId); allTuples.addAll(page.getAllTuples(tableInfo.getSchema())); int nextPageNum = page.getNextPageId(); currentPageId = nextPageNum != -1 ? new PageId(nextPageNum) : null; } return allTuples; }
}