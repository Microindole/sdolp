package org.csu.sdolp.cli.client;

import com.formdev.flatlaf.themes.FlatMacDarkLaf;
import org.csu.sdolp.cli.tool.DataReader;
import org.csu.sdolp.cli.tool.LogReader;
import org.csu.sdolp.compiler.lexer.TokenType;
import org.fife.ui.autocomplete.AutoCompletion;
import org.fife.ui.autocomplete.BasicCompletion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.autocomplete.DefaultCompletionProvider;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.Theme;
import org.fife.ui.rtextarea.RTextScrollPane;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseWheelEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Vector;

/**
 * 一个功能强大的、基于GUI的数据库交互式Shell客户端
 */
public class AdvancedShell extends JFrame {

    // --- UI Components ---
    private JComboBox<String> serverComboBox;
    private JTextField portField;
    private JTextField usernameField;
    private JButton connectButton;
    private RSyntaxTextArea sqlEditor;
    private JTabbedPane resultTabbedPane;
    private JTable resultTable;
    private JTextArea consoleTextArea;
    private JLabel statusBar;

    // --- Networking & State ---
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final List<String> commandHistory = new ArrayList<>();
    private int historyIndex = 0;

    public AdvancedShell() {
        super("MiniDB 高级客户端");
        initComponents();
        setupAutoCompletion();
        layoutComponents();
        addListeners();

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLocationRelativeTo(null);
    }

    private void initComponents() {
        serverComboBox = new JComboBox<>(new String[]{"localhost", "127.0.0.1"});
        serverComboBox.setEditable(true);
        portField = new JTextField("8848", 5);
        usernameField = new JTextField("root", 10);
        connectButton = new JButton("连接");

        sqlEditor = new RSyntaxTextArea(20, 60);
        sqlEditor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        try {
            Theme theme = Theme.load(getClass().getResourceAsStream("/org/fife/ui/rsyntaxtextarea/themes/monokai.xml"));
            theme.apply(sqlEditor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        sqlEditor.setCodeFoldingEnabled(true);
        sqlEditor.setFont(new Font("Consolas", Font.PLAIN, 16));

        resultTable = new JTable();
        resultTable.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        resultTable.setRowHeight(30);
        resultTable.setShowGrid(true);
        resultTable.setDefaultRenderer(Object.class, new ThemedTableCellRenderer());
        JTableHeader header = resultTable.getTableHeader();
        header.setFont(new Font("Segoe UI", Font.BOLD, 16));
        header.setDefaultRenderer(new ThemedHeaderRenderer());

        consoleTextArea = new JTextArea();
        consoleTextArea.setEditable(false);
        consoleTextArea.setFont(new Font("Monospaced", Font.PLAIN, 14));
        consoleTextArea.setMargin(new Insets(5, 5, 5, 5));

        resultTabbedPane = new JTabbedPane();
        resultTabbedPane.addTab("表格视图", new JScrollPane(resultTable));
        resultTabbedPane.addTab("控制台视图", new JScrollPane(consoleTextArea));

        statusBar = new JLabel("未连接");
    }

    private void setupAutoCompletion() {
        CompletionProvider provider = createCompletionProviderFromTokenType();
        AutoCompletion ac = new AutoCompletion(provider);
        ac.setAutoActivationEnabled(true);
        ac.setAutoActivationDelay(300);
        ac.setChoicesWindowSize(350, 240);
        ac.install(sqlEditor);

        InputMap im = sqlEditor.getInputMap();
        ActionMap am = sqlEditor.getActionMap();
        im.put(KeyStroke.getKeyStroke("TAB"), "smart-tab");
        am.put("smart-tab", new AbstractAction() {
            public void actionPerformed(java.awt.event.ActionEvent e) {
                if (ac.isPopupVisible()) {
                    ac.doCompletion();
                } else {
                    ac.doCompletion();
                }
            }
        });
    }

    private CompletionProvider createCompletionProviderFromTokenType() {
        DefaultCompletionProvider provider = new DefaultCompletionProvider();
        EnumSet<TokenType> keywords = EnumSet.range(TokenType.SELECT, TokenType.FULL);
        EnumSet<TokenType> functions = EnumSet.range(TokenType.COUNT, TokenType.MAX);
        EnumSet<TokenType> dataTypes = EnumSet.of(TokenType.INT, TokenType.VARCHAR, TokenType.DECIMAL, TokenType.DATE, TokenType.BOOLEAN);

        for (TokenType type : TokenType.values()) {
            if (keywords.contains(type) || functions.contains(type) || dataTypes.contains(type)) {
                String text = type.name();
                if (functions.contains(type)) {
                    provider.addCompletion(new BasicCompletion(provider, text + "()"));
                } else {
                    provider.addCompletion(new BasicCompletion(provider, text));
                }
            }
        }
        return provider;
    }

    private void layoutComponents() {
        JToolBar toolBar = new JToolBar();
        toolBar.setFloatable(false);
        toolBar.setMargin(new Insets(5, 5, 5, 5));
        toolBar.add(new JLabel(" 服务器: "));
        toolBar.add(serverComboBox);
        toolBar.add(new JLabel(" 端口: "));
        toolBar.add(portField);
        toolBar.add(new JLabel(" 用户名: "));
        toolBar.add(usernameField);
        toolBar.add(connectButton);
        toolBar.add(new JSeparator(SwingConstants.VERTICAL));

        JButton executeButton = new JButton("执行 (F5)");
        executeButton.addActionListener(e -> executeSql());
        toolBar.add(executeButton);

        JButton importButton = new JButton("导入SQL");
        importButton.addActionListener(e -> importSqlFile());
        toolBar.add(importButton);

        JButton exportButton = new JButton("💾 导出SQL");
        exportButton.addActionListener(e -> exportDatabase());
        toolBar.add(exportButton);

        JButton logButton = new JButton("📜 查看日志");
        logButton.addActionListener(e -> showLogReader());
        toolBar.add(logButton);


        JButton clearButton = new JButton("清空");
        clearButton.addActionListener(e -> sqlEditor.setText(""));
        toolBar.add(clearButton);

        toolBar.add(Box.createHorizontalGlue());

        JButton helpButton = new JButton("使用说明");
        helpButton.addActionListener(e -> showHelpDialog());
        toolBar.add(helpButton);

        RTextScrollPane sp = new RTextScrollPane(sqlEditor);
        sp.setBorder(BorderFactory.createEmptyBorder());
        JSplitPane mainSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, sp, resultTabbedPane);
        mainSplitPane.setResizeWeight(0.45);
        mainSplitPane.setBorder(null);

        JPanel statusPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        statusPanel.add(statusBar);

        setLayout(new BorderLayout());
        add(toolBar, BorderLayout.NORTH);
        add(mainSplitPane, BorderLayout.CENTER);
        add(statusPanel, BorderLayout.SOUTH);
    }

    private void addListeners() {
        connectButton.addActionListener(e -> toggleConnection());

        sqlEditor.getInputMap().put(KeyStroke.getKeyStroke(KeyEvent.VK_F5, 0), "execute");
        sqlEditor.getActionMap().put("execute", new AbstractAction() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent e) {
                executeSql();
            }
        });

        sqlEditor.addMouseWheelListener((MouseWheelEvent e) -> {
            if (e.isControlDown()) {
                Font font = sqlEditor.getFont();
                int newSize = font.getSize() - e.getWheelRotation();
                if (newSize > 8 && newSize < 48) {
                    sqlEditor.setFont(new Font(font.getName(), font.getStyle(), newSize));
                }
            } else {
                JScrollPane scrollPane = (JScrollPane) SwingUtilities.getAncestorOfClass(JScrollPane.class, sqlEditor);
                if (scrollPane != null) {
                    scrollPane.dispatchEvent(SwingUtilities.convertMouseEvent(sqlEditor, e, scrollPane));
                }
            }
        });

        sqlEditor.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode() == KeyEvent.VK_UP) {
                    if (!commandHistory.isEmpty()) {
                        if (historyIndex > 0) historyIndex--;
                        sqlEditor.setText(commandHistory.get(historyIndex));
                        e.consume();
                    }
                } else if (e.getKeyCode() == KeyEvent.VK_DOWN) {
                    if (!commandHistory.isEmpty() && historyIndex < commandHistory.size() - 1) {
                        historyIndex++;
                        sqlEditor.setText(commandHistory.get(historyIndex));
                        e.consume();
                    }
                }
            }
        });
    }

    private void showHelpDialog() {
        String helpText = """
            <html><body style='width: 350px; font-family: sans-serif; font-size: 12px;'>
            <h2 style='color:#569CD6;'>MiniDB 高级客户端 - 使用说明</h2><hr>
            <h3>快捷键 & 功能:</h3>
            <ul>
                <li><b>智能补全:</b><ul><li>输入时会自动弹出关键字建议。</li><li>在任何时候按 <b>Tab</b> 键可触发建议或直接补全。</li></ul></li><br>
                <li><b>执行查询:</b><ul><li>点击工具栏上的 <b>▶️ 执行</b> 按钮。</li><li>在编辑器中按 <b>F5</b> 键。</li><li>如果选中了一段SQL，将只执行选中的部分。</li></ul></li><br>
                <li><b>字体缩放:</b><ul><li>在SQL编辑器区域，按住 <b>Ctrl</b> 并滚动<b>鼠标滚轮</b>。</li></ul></li><br>
                <li><b>历史命令:</b><ul><li>在SQL编辑器区域，使用<b>上下箭头键</b>翻阅历史记录。</li></ul></li>
            </ul></body></html>""";
        JOptionPane.showMessageDialog(this, new JLabel(helpText), "使用说明", JOptionPane.INFORMATION_MESSAGE);
    }

    private void updateResultTable(String serverResponse) {
        DefaultTableModel model = new DefaultTableModel() {
            @Override public boolean isCellEditable(int row, int column) { return false; }
        };

        String[] lines = serverResponse.replace("<br>", "\n").split("\n");
        if (lines.length < 4 || !lines[0].startsWith("+--")) {
            resultTable.setModel(model);
            return;
        }

        try {
            Vector<String> columnNames = new Vector<>(Arrays.asList(
                    Arrays.stream(lines[1].split("\\|")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new)
            ));
            model.setColumnIdentifiers(columnNames);

            for (int i = 3; i < lines.length - 2; i++) {
                Vector<Object> row = new Vector<>(Arrays.asList(
                        Arrays.stream(lines[i].split("\\|")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new)
                ));
                if(row.size() == model.getColumnCount()){
                    model.addRow(row);
                }
            }

            resultTable.setModel(model);
            resultTable.setRowSorter(new TableRowSorter<>(model));
            resultTabbedPane.setSelectedIndex(0);
        } catch (Exception e) {
            e.printStackTrace();
            appendToConsole("客户端解析表格数据时出错: " + e.getMessage());
        }
    }

    private void importSqlFile() {
        if (socket == null || socket.isClosed()) {
            JOptionPane.showMessageDialog(this, "请先连接到数据库。", "错误", JOptionPane.ERROR_MESSAGE);
            return;
        }

        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("请选择要导入的 SQL 文件");
        FileNameExtensionFilter filter = new FileNameExtensionFilter("SQL Scripts (*.sql)", "sql");
        chooser.setFileFilter(filter);

        int returnValue = chooser.showOpenDialog(this);
        if (returnValue == JFileChooser.APPROVE_OPTION) {
            File sqlFile = chooser.getSelectedFile();
            executeSqlScript(sqlFile);
        }
    }

    private void executeSqlScript(File sqlFile) {
        statusBar.setText("正在导入SQL脚本: " + sqlFile.getName() + "...");
        resultTabbedPane.setSelectedIndex(1);
        appendToConsole("--- 开始执行脚本: " + sqlFile.getName() + " ---");

        SwingWorker<Integer, String> worker = new SwingWorker<>() {
            @Override
            protected Integer doInBackground() throws Exception {
                String content = Files.readString(sqlFile.toPath());
                content = content.replaceAll("(?m)^--.*$", "");
                String[] statements = content.split(";");
                int executedCount = 0;

                for (String statement : statements) {
                    String singleLineStatement = statement.trim().replaceAll("\\s+", " ");
                    if (!singleLineStatement.isEmpty()) {
                        out.println(singleLineStatement + ";");
                        String response = in.readLine();
                        publish(">> " + singleLineStatement + "\n" + response.replace("<br>", "\n"));
                        executedCount++;
                    }
                }
                return executedCount;
            }

            @Override
            protected void process(List<String> chunks) {
                for(String msg : chunks) {
                    appendToConsole(msg);
                }
            }

            @Override
            protected void done() {
                try {
                    int count = get();
                    appendToConsole("--- 脚本执行完毕，共执行 " + count + " 条语句 ---");
                    statusBar.setText("脚本导入成功！");
                } catch (Exception e) {
                    appendToConsole("--- 脚本执行出错: " + e.getMessage() + " ---");
                    statusBar.setText("脚本导入失败！");
                }
            }
        };
        worker.execute();
    }

    // --- 显示日志读取器窗口 ---
    private void showLogReader() {
        // LogReader本身就是一个JFrame，可以直接创建并显示
        // 为了避免阻塞主GUI，也在一个新的线程中显示它
        SwingUtilities.invokeLater(() -> {
            LogReader logReader = new LogReader();
            logReader.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE); // 关闭时不退出整个应用
            logReader.setVisible(true);
        });
    }

    // --- 导出数据库为SQL文件 ---
    private void exportDatabase() {
        String currentDb = "default";

        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("请选择SQL文件的保存位置");
        chooser.setSelectedFile(new File(currentDb + "_dump.sql"));
        chooser.setFileFilter(new FileNameExtensionFilter("SQL File", "sql"));

        if (chooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File file = chooser.getSelectedFile();
            statusBar.setText("正在导出数据库到 " + file.getName() + "...");

            // 在后台线程中执行导出
            SwingWorker<Void, Void> worker = new SwingWorker<>() {
                @Override
                protected Void doInBackground() throws Exception {
                    DataReader.exportDatabaseToFile(currentDb, file);
                    return null;
                }

                @Override
                protected void done() {
                    try {
                        get();
                        statusBar.setText("数据库导出成功！");
                        JOptionPane.showMessageDialog(AdvancedShell.this,
                                "数据库已成功导出到:\n" + file.getAbsolutePath(),
                                "导出成功",
                                JOptionPane.INFORMATION_MESSAGE);
                    } catch (Exception e) {
                        statusBar.setText("数据库导出失败！");
                        JOptionPane.showMessageDialog(AdvancedShell.this,
                                "导出失败: " + e.getMessage(),
                                "导出错误",
                                JOptionPane.ERROR_MESSAGE);
                    }
                }
            };
            worker.execute();
        }
    }

    private void toggleConnection() { if (socket == null || socket.isClosed()) connect(); else disconnect(); }
    private void connect() { String host = (String) serverComboBox.getSelectedItem(); int port; try { port = Integer.parseInt(portField.getText()); } catch (NumberFormatException e) { JOptionPane.showMessageDialog(this, "端口号必须是数字。", "连接错误", JOptionPane.ERROR_MESSAGE); return; } String username = usernameField.getText(); statusBar.setText("正在连接到 " + host + ":" + port + "..."); SwingWorker<Void, String> worker = new SwingWorker<>() { @Override protected Void doInBackground() throws Exception { socket = new Socket(host, port); out = new PrintWriter(socket.getOutputStream(), true); in = new BufferedReader(new InputStreamReader(socket.getInputStream())); publish("Server: " + in.readLine()); out.println(username); publish("Server: " + in.readLine()); return null; } @Override protected void process(List<String> chunks) { for (String msg : chunks) appendToConsole(msg); } @Override protected void done() { try { get(); connectButton.setText("断开连接"); statusBar.setText("已连接到 " + host + ":" + port + " | 用户: " + username); } catch (Exception e) { JOptionPane.showMessageDialog(AdvancedShell.this, "连接失败: " + e.getMessage(), "连接错误", JOptionPane.ERROR_MESSAGE); statusBar.setText("连接失败"); disconnect(); } } }; worker.execute(); }
    private void disconnect() { try { if (socket != null) socket.close(); } catch (Exception e) { /* ignore */ } socket = null; connectButton.setText("连接"); statusBar.setText("未连接"); appendToConsole("连接已断开。"); }
    private void executeSql() {
        // 优先执行选中的文本，否则执行全部文本
        String sqlText = sqlEditor.getSelectedText() != null && !sqlEditor.getSelectedText().isEmpty() ? sqlEditor.getSelectedText() : sqlEditor.getText();
        if (sqlText.trim().isEmpty()) {
            return;
        }
        if (socket == null || socket.isClosed()) {
            JOptionPane.showMessageDialog(this, "请先连接到数据库。", "错误", JOptionPane.ERROR_MESSAGE);
            return;
        }

        // 将SQL文本添加到历史记录
        if (!commandHistory.contains(sqlText)) {
            commandHistory.add(sqlText);
        }
        historyIndex = commandHistory.size();

        long startTime = System.currentTimeMillis();
        statusBar.setText("正在执行查询...");

        SwingWorker<Void, String> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                // 1. 按分号分割所有SQL命令
                String[] sqlStatements = sqlText.split(";");

                for (String sql : sqlStatements) {
                    String singleSql = sql.trim();
                    if (singleSql.isEmpty()) {
                        continue;
                    }

                    // 2.【核心修复】将换行符替换为空格，确保命令在逻辑上是一行
                    String singleLineSql = singleSql.replaceAll("\\s+", " ");

                    // 3. 逐条发送SQL语句到服务器
                    out.println(singleLineSql + ";");
                    String response = in.readLine(); // 每发送一条，就等待一次响应

                    // 4. 将每次的请求和响应都发布，以便在控制台更新
                    publish(">> " + singleLineSql + "\n" + (response != null ? response.replace("<br>", "\n") : "与服务器断开连接。"));

                    // 将最后一条命令的响应也保存下来，用于可能更新表格视图
                    if (response != null) {
                        final String lastResponse = response;
                        SwingUtilities.invokeLater(() -> updateResultTable(lastResponse));
                    }


                    if (response == null) {
                        // 如果中途连接断开，则停止执行
                        publish("与服务器的连接已断开。");
                        break;
                    }
                }
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                // 实时更新控制台视图
                for (String msg : chunks) {
                    appendToConsole(msg);
                }
            }

            @Override
            protected void done() {
                try {
                    get(); // 调用 get() 来捕获 doInBackground 中可能抛出的异常
                    long duration = System.currentTimeMillis() - startTime;
                    statusBar.setText("查询完成 | 耗时: " + duration + "ms");
                    resultTabbedPane.setSelectedIndex(1); // 执行后，默认切换到信息更全的控制台视图

                } catch (Exception e) {
                    statusBar.setText("执行错误: " + e.getMessage());
                    appendToConsole("错误: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        };
        worker.execute();
    }

    private void appendToConsole(String message) { consoleTextArea.append(message + "\n"); consoleTextArea.setCaretPosition(consoleTextArea.getDocument().getLength()); }

    private static class ThemedTableCellRenderer extends DefaultTableCellRenderer {
        public ThemedTableCellRenderer() { setBorder(new EmptyBorder(0, 10, 0, 10)); }
        @Override public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (!isSelected) {
                setBackground(row % 2 == 0 ? UIManager.getColor("Table.background") : UIManager.getColor("Table.alternateRowColor"));
            }
            return this;
        }
    }
    private static class ThemedHeaderRenderer extends DefaultTableCellRenderer {
        public ThemedHeaderRenderer() { setOpaque(true); setBorder(UIManager.getBorder("TableHeader.cellBorder")); setHorizontalAlignment(JLabel.CENTER); }
        @Override public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            setFont(table.getTableHeader().getFont());
            return this;
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            FlatMacDarkLaf.setup();
            new AdvancedShell().setVisible(true);
        });
    }
}