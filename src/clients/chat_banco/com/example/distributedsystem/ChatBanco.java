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

public class ChatBanco extends JFrame {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 8080;

    private final JTextArea messageArea;
    private final JTextField textField;
    private final JButton sendButton;
    private final DefaultTableModel tableModel;
    private final JTable table;
    private final JScrollPane tableScrollPane;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public ChatBanco() {
        super("Chat Banco (Privilegiado)");

        // --- UI Setup ---
        setSize(800, 600); // Tamaño un poco más grande para el panel de ayuda
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        JLabel titleLabel = new JLabel("Cliente Banco Privilegiado", SwingConstants.CENTER);
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
                    "--- Comandos Disponibles (Privilegiado) ---\n\n" +
                    "CONSULTAR_CUENTA <id_cuenta>\n" +
                    "    Consulta el saldo y datos de CUALQUIER cuenta.\n\n" +
                    "ESTADO_PAGO_PRESTAMO <id_cuenta>\n" +
                    "    Muestra los préstamos asociados a CUALQUIER cuenta.\n\n" +
                    "CONSULTAR_HISTORIAL <id_cuenta>\n" +
                    "    Muestra el historial de operaciones de CUALQUIER cuenta.\n\n" +
                    "TRANSFERIR_CUENTA <id_origen> <id_destino> <monto>\n" +
                    "    Realiza una transferencia entre CUALQUIER par de cuentas.\n"
                );
                JScrollPane helpScrollPane = new JScrollPane(helpArea);
                helpScrollPane.setPreferredSize(new Dimension(350, 0)); // Más ancho
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

            // Lógica para comandos privilegiados
            if (command.equalsIgnoreCase("CONSULTAR_CUENTA") && parts.length == 2) {
                finalCommandPayload = String.format("CONSULTAR_CUENTA|%s", parts[1]);
            } else if (command.equalsIgnoreCase("ESTADO_PAGO_PRESTAMO") && parts.length == 2) {
                finalCommandPayload = String.format("ESTADO_PAGO_PRESTAMO|%s", parts[1]);
            } else if (command.equalsIgnoreCase("CONSULTAR_HISTORIAL") && parts.length == 2) {
                finalCommandPayload = String.format("CONSULTAR_HISTORIAL|%s", parts[1]);
            } else if (command.equalsIgnoreCase("TRANSFERIR_CUENTA") && parts.length == 4) {
                finalCommandPayload = String.format("TRANSFERIR_CUENTA|%s|%s|%s", parts[1], parts[2], parts[3]);
            } else {
                messageArea.append("ERROR: Comando o formato incorrecto. Consulta el panel de ayuda.\n");
                textField.setText("");
                return;
            }

            String request = String.format("QUERY|privilegiado|%s", finalCommandPayload); // Usar un ID especial para el servidor
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
                            tableModel.addRow(parts[i].split(","));
                        }
                        tableScrollPane.setVisible(true);
                    } else {
                        // Mensaje de texto normal
                        messageArea.append(serverMessage + "\n");
                        tableScrollPane.setVisible(false);
                    }
                }
            } catch (IOException e) {
                messageArea.append("--- Conexión perdida con el servidor ---\\n");
            } finally {
                try {
                    if (socket != null) socket.close();
                } catch (IOException e) { e.printStackTrace(); }
            }
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new ChatBanco().setVisible(true);
        });
    }
}
