package org.csu.sdolp.cli.client;

import org.csu.sdolp.compiler.lexer.TokenType;
import org.fife.ui.autocomplete.AutoCompletion;
import org.fife.ui.autocomplete.BasicCompletion;
import org.fife.ui.autocomplete.CompletionProvider;
import org.fife.ui.autocomplete.DefaultCompletionProvider;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rtextarea.RTextScrollPane;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Vector;

/**
 * 一个功能强大的、基于GUI的数据库交互式Shell客户端 (终极专业版)。
 * 特性：
 * 1.  **智能代码补全**: 自动/手动(Ctrl+Space)触发SQL关键字提示，支持Tab键补全。
 * 2.  **专业表格美化**: 自定义表头、交替行高亮、单元格内边距，观感大幅提升。
 * 3.  **现代化的UI设计**：使用Swing和Nimbus L&F。
 * 4.  **专业的SQL编辑器**：由RSyntaxTextArea提供，支持语法高亮。
 * 5.  **双视图结果展示**：结果可在表格和控制台之间切换。
 * 6.  **历史命令**：支持使用上下箭头翻阅历史SQL。
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
        setupLookAndFeel();
        initComponents();
        setupAutoCompletion(); // 设置自动补全
        layoutComponents();
        addListeners();

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1200, 800);
        setLocationRelativeTo(null);
    }

    private void setupLookAndFeel() {
        try {
            UIManager.setLookAndFeel("javax.swing.plaf.nimbus.NimbusLookAndFeel");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initComponents() {
        serverComboBox = new JComboBox<>(new String[]{"localhost", "127.0.0.1"});
        serverComboBox.setEditable(true);
        portField = new JTextField("8848", 5);
        usernameField = new JTextField("root", 10);
        connectButton = new JButton("🔌 连接");

        sqlEditor = new RSyntaxTextArea(20, 60);
        sqlEditor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        sqlEditor.setCodeFoldingEnabled(true);
        sqlEditor.setAntiAliasingEnabled(true);
        sqlEditor.setFont(new Font("Consolas", Font.PLAIN, 16));

        // --- 表格美化 ---
        resultTable = new JTable();
        resultTable.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        resultTable.setRowHeight(28);
        resultTable.setGridColor(new Color(224, 224, 224));
        resultTable.setShowGrid(true);
        resultTable.setDefaultRenderer(Object.class, new ModernTableCellRenderer());
        JTableHeader header = resultTable.getTableHeader();
        header.setFont(new Font("Segoe UI", Font.BOLD, 15));
        header.setForeground(Color.WHITE);
        header.setBackground(new Color(60, 63, 65));
        header.setOpaque(true);
        header.setBorder(BorderFactory.createLineBorder(new Color(80, 80, 80)));

        consoleTextArea = new JTextArea();
        consoleTextArea.setEditable(false);
        consoleTextArea.setFont(new Font("Monospaced", Font.PLAIN, 14));
        consoleTextArea.setMargin(new Insets(5, 5, 5, 5));
        consoleTextArea.setBackground(new Color(43, 43, 43));
        consoleTextArea.setForeground(new Color(187, 187, 187));
        consoleTextArea.setCaretColor(Color.WHITE);

        resultTabbedPane = new JTabbedPane();
        resultTabbedPane.addTab("📊 表格视图", new JScrollPane(resultTable));
        resultTabbedPane.addTab("🖥️ 控制台视图", new JScrollPane(consoleTextArea));

        statusBar = new JLabel("未连接");
    }

    /**
     * 为SQL编辑器设置自动补全功能。
     */
    private void setupAutoCompletion() {
        CompletionProvider provider = createCompletionProviderFromTokenType();
        AutoCompletion ac = new AutoCompletion(provider);
        ac.setTriggerKey(KeyStroke.getKeyStroke("control SPACE"));
        ac.setAutoActivationEnabled(true);
        ac.setAutoActivationDelay(300);
        ac.setChoicesWindowSize(350, 240);
        ac.install(sqlEditor);

        InputMap im = sqlEditor.getInputMap();
        ActionMap am = sqlEditor.getActionMap();
        im.put(KeyStroke.getKeyStroke("TAB"), "tab-pressed");
        am.put("tab-pressed", new AbstractAction() {
            public void actionPerformed(java.awt.event.ActionEvent e) {
                if (ac.isPopupVisible()) {
                    ac.doCompletion();
                } else {
                    sqlEditor.replaceSelection("\t");
                }
            }
        });
    }

    /**
     * 动态地从 TokenType enum 创建代码提示的提供者。
     */
    private CompletionProvider createCompletionProviderFromTokenType() {
        DefaultCompletionProvider provider = new DefaultCompletionProvider();

        // 定义哪些TokenType属于关键字、函数或数据类型
        EnumSet<TokenType> keywords = EnumSet.range(TokenType.SELECT, TokenType.FULL);
        EnumSet<TokenType> functions = EnumSet.range(TokenType.COUNT, TokenType.MAX);
        EnumSet<TokenType> dataTypes = EnumSet.of(TokenType.INT, TokenType.VARCHAR, TokenType.DECIMAL, TokenType.DATE, TokenType.BOOLEAN);

        for (TokenType type : TokenType.values()) {
            if (keywords.contains(type) || functions.contains(type) || dataTypes.contains(type)) {
                String text = type.name();
                // 对于函数，我们可以添加括号
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

        JButton executeButton = new JButton("▶️ 执行 (F5)");
        executeButton.addActionListener(e -> executeSql());
        toolBar.add(executeButton);

        JButton clearButton = new JButton("🗑️ 清空");
        clearButton.addActionListener(e -> sqlEditor.setText(""));
        toolBar.add(clearButton);

        RTextScrollPane sp = new RTextScrollPane(sqlEditor);
        JSplitPane mainSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, sp, resultTabbedPane);
        mainSplitPane.setResizeWeight(0.4);

        JPanel statusPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        statusPanel.setBorder(BorderFactory.createEtchedBorder());
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

        sqlEditor.addKeyListener(new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
                if (e.getKeyCode() == KeyEvent.VK_UP) {
                    if (!commandHistory.isEmpty()) {
                        if (historyIndex > 0) {
                            historyIndex--;
                        }
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

    private void toggleConnection() {
        if (socket == null || socket.isClosed()) {
            connect();
        } else {
            disconnect();
        }
    }

    private void connect() {
        String host = (String) serverComboBox.getSelectedItem();
        int port;
        try {
            port = Integer.parseInt(portField.getText());
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this, "端口号必须是数字。", "连接错误", JOptionPane.ERROR_MESSAGE);
            return;
        }
        String username = usernameField.getText();
        statusBar.setText("正在连接到 " + host + ":" + port + "...");

        SwingWorker<Void, String> worker = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                socket = new Socket(host, port);
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                publish("Server: " + in.readLine());
                out.println(username);
                publish("Server: " + in.readLine());
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                for (String msg : chunks) appendToConsole(msg);
            }

            @Override
            protected void done() {
                try {
                    get();
                    connectButton.setText("🔌 断开连接");
                    statusBar.setText("已连接到 " + host + ":" + port + " | 用户: " + username);
                } catch (Exception e) {
                    JOptionPane.showMessageDialog(AdvancedShell.this, "连接失败: " + e.getMessage(), "连接错误", JOptionPane.ERROR_MESSAGE);
                    statusBar.setText("连接失败");
                    disconnect();
                }
            }
        };
        worker.execute();
    }

    private void disconnect() {
        try {
            if (socket != null) socket.close();
        } catch (Exception e) { /* ignore */ }
        socket = null;
        connectButton.setText("🔌 连接");
        statusBar.setText("未连接");
        appendToConsole("连接已断开。");
    }

    private void executeSql() {
        String sql = sqlEditor.getSelectedText() != null && !sqlEditor.getSelectedText().isEmpty()
                ? sqlEditor.getSelectedText()
                : sqlEditor.getText();
        if (sql.trim().isEmpty()) return;
        if (socket == null || socket.isClosed()) {
            JOptionPane.showMessageDialog(this, "请先连接到数据库。", "错误", JOptionPane.ERROR_MESSAGE);
            return;
        }

        if (!commandHistory.contains(sql)) commandHistory.add(sql);
        historyIndex = commandHistory.size();

        long startTime = System.currentTimeMillis();
        statusBar.setText("正在执行查询...");

        SwingWorker<String, Void> worker = new SwingWorker<>() {
            @Override
            protected String doInBackground() throws Exception {
                out.println(sql);
                return in.readLine();
            }

            @Override
            protected void done() {
                try {
                    String response = get();
                    long duration = System.currentTimeMillis() - startTime;
                    if (response != null) {
                        appendToConsole(">> " + sql.replace("\n", " ") + "\n" + response.replace("<br>", "\n"));
                        updateResultTable(response);
                        statusBar.setText("查询完成 | 耗时: " + duration + "ms");
                    } else {
                        statusBar.setText("与服务器断开连接。");
                        disconnect();
                    }
                } catch (Exception e) {
                    statusBar.setText("执行错误: " + e.getMessage());
                    appendToConsole("错误: " + e.getMessage());
                }
            }
        };
        worker.execute();
    }

    private void appendToConsole(String message) {
        consoleTextArea.append(message + "\n");
        consoleTextArea.setCaretPosition(consoleTextArea.getDocument().getLength());
    }

    private void updateResultTable(String serverResponse) {
        String[] lines = serverResponse.replace("<br>", "\n").split("\n");
        DefaultTableModel model = (DefaultTableModel) resultTable.getModel();
        model.setRowCount(0);
        model.setColumnCount(0);

        if (lines.length < 4 || !lines[0].startsWith("+--")) {
            return;
        }

        Vector<String> columnNames = new Vector<>(Arrays.asList(
                Arrays.stream(lines[1].split("\\|")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new)
        ));
        model.setColumnIdentifiers(columnNames);

        for (int i = 3; i < lines.length - 2; i++) {
            Vector<Object> row = new Vector<>(Arrays.asList(
                    Arrays.stream(lines[i].split("\\|")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new)
            ));
            model.addRow(row);
        }

        resultTable.setRowSorter(new TableRowSorter<>(model));
        resultTabbedPane.setSelectedIndex(0);
    }

    /**
     * 新增内部类：用于美化表格渲染
     */
    private static class ModernTableCellRenderer extends DefaultTableCellRenderer {
        private static final Color EVEN_ROW_COLOR = new Color(248, 248, 248);
        private static final Color ODD_ROW_COLOR = Color.WHITE;
        private static final Color SELECTION_COLOR = new Color(60, 141, 188);

        public ModernTableCellRenderer() {
            setBorder(new EmptyBorder(0, 10, 0, 10));
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
            super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            if (isSelected) {
                setForeground(Color.WHITE);
                setBackground(SELECTION_COLOR);
            } else {
                setForeground(table.getForeground());
                setBackground(row % 2 == 0 ? ODD_ROW_COLOR : EVEN_ROW_COLOR);
            }
            return this;
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new AdvancedShell().setVisible(true));
    }
}