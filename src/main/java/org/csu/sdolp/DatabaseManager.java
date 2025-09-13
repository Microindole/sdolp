package org.csu.sdolp;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseManager {

    private static final String DB_ROOT_DIR = "data"; // 所有数据库都存放在这个目录下

    public DatabaseManager() {
        File rootDir = new File(DB_ROOT_DIR);
        if (!rootDir.exists()) {
            rootDir.mkdirs();
        }
    }

    public void createDatabase(String dbName) {
        File dbDir = new File(DB_ROOT_DIR, dbName);
        if (dbDir.exists()) {
            throw new RuntimeException("Database '" + dbName + "' already exists.");
        }
        dbDir.mkdirs();
    }

    public List<String> listDatabases() {
        File rootDir = new File(DB_ROOT_DIR);
        File[] directories = rootDir.listFiles(File::isDirectory);
        if (directories == null) {
            return List.of();
        }
        return Arrays.stream(directories)
                .map(File::getName)
                .collect(Collectors.toList());
    }

    public static String getDbFilePath(String dbName) {
        return DB_ROOT_DIR + File.separator + dbName + File.separator + "minidb.data";
    }

    public void dropDatabase(String dbName) {
        File dbDir = new File(DB_ROOT_DIR, dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            throw new RuntimeException("Database '" + dbName + "' does not exist.");
        }
        // 递归删除整个目录
        deleteDirectory(dbDir);
    }

    private void deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}