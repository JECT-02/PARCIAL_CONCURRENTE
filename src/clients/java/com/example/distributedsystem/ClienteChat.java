package com.example.distributedsystem;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClienteChat extends JFrame {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 8080;

    private String clientId;

    private final JTextArea messageArea;
    private final JTextField textField;
    private final JButton sendButton;
    private final DefaultTableModel tableModel;
    private final JTable table;
    private final JScrollPane tableScrollPane;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public ClienteChat() {
        super("Chat Banco");

        // --- Prompt for Client ID ---
        this.clientId = JOptionPane.showInputDialog(this, "Ingrese su ID de Cliente:", "Inicio de Sesión", JOptionPane.PLAIN_MESSAGE);
        if (this.clientId == null || this.clientId.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "El ID de cliente no puede estar vacío.", "Error", JOptionPane.ERROR_MESSAGE);
            System.exit(0);
        }

        // --- UI Setup ---
        setSize(600, 500);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        JLabel titleLabel = new JLabel("Usuario Validado: " + clientId, SwingConstants.CENTER);
        add(titleLabel, BorderLayout.NORTH);

        // Panel central que contendrá el chat y la tabla
        JPanel centerPanel = new JPanel(new GridLayout(2, 1));

        // Área de chat
        messageArea = new JTextArea();
        messageArea.setEditable(false);
        JScrollPane messageScrollPane = new JScrollPane(messageArea);
        centerPanel.add(messageScrollPane);

        // Tabla para datos estructurados
        tableModel = new DefaultTableModel();
        table = new JTable(tableModel);
        tableScrollPane = new JScrollPane(table);
        tableScrollPane.setVisible(false); // Oculta hasta que se necesite
        centerPanel.add(tableScrollPane);

        add(centerPanel, BorderLayout.CENTER);

        // Panel inferior para entrada de texto
        JPanel bottomPanel = new JPanel(new BorderLayout());
        textField = new JTextField();
        sendButton = new JButton("Enviar");
        bottomPanel.add(textField, BorderLayout.CENTER);
        bottomPanel.add(sendButton, BorderLayout.EAST);
        add(bottomPanel, BorderLayout.SOUTH);

        // --- Panel de Ayuda ---
        JTextArea helpArea = new JTextArea();
        helpArea.setEditable(false);
        helpArea.setBackground(new Color(240, 240, 240));
        helpArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        helpArea.setMargin(new Insets(5, 5, 5, 5));
        helpArea.setText(
            "--- Comandos Disponibles ---\n\n" +
            "CONSULTAR_CUENTA\n" +
            "    Muestra el saldo y datos de tu cuenta.\n\n" +
            "ESTADO_PAGO_PRESTAMO\n" +
            "    Muestra tus préstamos activos.\n\n" +
            "CONSULTAR_HISTORIAL\n" +
            "    Muestra tu historial de operaciones.\n\n" +
            "TRANSFERIR_CUENTA <destino> <monto>\n" +
            "    Transfiere un monto a otra cuenta.\n\n" +
            "PAGAR_DEUDA <id_prestamo> <monto>\n" +
            "    Paga un monto a un préstamo específico.\n"
        );
        JScrollPane helpScrollPane = new JScrollPane(helpArea);
        helpScrollPane.setPreferredSize(new Dimension(250, 0));
        add(helpScrollPane, BorderLayout.EAST);

        // --- Action Listeners ---
        sendButton.addActionListener(new SendButtonListener());
        textField.addActionListener(new SendButtonListener());

        // --- Network and Logic ---
        try {
            socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Iniciar el hilo para escuchar al servidor
            new Thread(new ServerListener()).start();

        } catch (IOException e) {
            e.printStackTrace();
            JOptionPane.showMessageDialog(this, "No se pudo conectar al servidor.", "Error de Conexión", JOptionPane.ERROR_MESSAGE);
            System.exit(1);
        }
    }

    private class SendButtonListener implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            String text = textField.getText().trim();
            if (text.isEmpty()) return;

            messageArea.append("Yo: " + text + "\n");
            tableScrollPane.setVisible(false);

            String[] parts = text.split("\\s+");
            String command = parts[0];
            String finalCommandPayload;

            // Lógica inteligente para comandos específicos del ClienteChat
            if (command.equalsIgnoreCase("TRANSFERIR_CUENTA") && parts.length == 3) {
                // Usuario escribe: TRANSFERIR_CUENTA <destino> <monto>
                finalCommandPayload = String.format("TRANSFERIR_CUENTA|%s|%s|%s", clientId, parts[1], parts[2]);
            } else if (command.equalsIgnoreCase("CONSULTAR_CUENTA") && parts.length == 1) {
                // Usuario escribe: CONSULTAR_CUENTA
                finalCommandPayload = String.format("CONSULTAR_CUENTA|%s", clientId);
            } else if (command.equalsIgnoreCase("ESTADO_PAGO_PRESTAMO") && parts.length == 1) {
                // Usuario escribe: ESTADO_PAGO_PRESTAMO
                finalCommandPayload = String.format("ESTADO_PAGO_PRESTAMO|%s", clientId);
            } else if (command.equalsIgnoreCase("CONSULTAR_HISTORIAL") && parts.length == 1) {
                // Usuario escribe: CONSULTAR_HISTORIAL
                finalCommandPayload = String.format("CONSULTAR_HISTORIAL|%s", clientId);
            } else if (command.equalsIgnoreCase("PAGAR_DEUDA") && parts.length == 3) {
                // Usuario escribe: PAGAR_DEUDA <id_prestamo> <monto>
                finalCommandPayload = String.format("PAGAR_DEUDA|%s|%s", parts[1], parts[2]);
            } else {
                // Para todos los demás comandos, usar la lógica general
                finalCommandPayload = text.replace(" ", "|");
            }

            String request = String.format("QUERY|%s|%s", clientId, finalCommandPayload);
            out.println(request);
            textField.setText("");
        }
    }

    // Listener para manejar los mensajes que llegan del servidor
    private class ServerListener implements Runnable {
        @Override
        public void run() {
            try {
                String serverMessage;
                while ((serverMessage = in.readLine()) != null) {
                    if (serverMessage.startsWith("TABLE_DATA|")) {
                        String[] parts = serverMessage.split("\\|");
                        String[] columns = parts[1].split(",");
                        
                        tableModel.setRowCount(0);
                        tableModel.setColumnIdentifiers(columns);

                        for (int i = 2; i < parts.length; i++) {
                            // El servidor ahora nos garantiza que cada parte es una fila
                            // y que las columnas vienen separadas por comas
                            tableModel.addRow(parts[i].split(","));
                        }
                        tableScrollPane.setVisible(true);
                    } else {
                        // Mensaje de texto normal
                        messageArea.append(serverMessage + "\n");
                    }
                }
            } catch (IOException e) {
                messageArea.append("--- Conexión perdida con el servidor ---\n");
            } finally {
                try {
                    if (socket != null) socket.close();
                } catch (IOException e) { e.printStackTrace(); }
            }
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new ClienteChat().setVisible(true);
        });
    }
}
