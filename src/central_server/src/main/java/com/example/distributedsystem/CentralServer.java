package com.example.distributedsystem;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class CentralServer {

    private static final int SERVER_PORT = 8080;
    private static final int NUM_PARTITIONS = 3;
    private static final ExecutorService clientExecutor = Executors.newCachedThreadPool();

    // Mapa de Topología de Datos: PartitionID -> Lista de Nodos (el primero es el primario)
    private static final Map<Integer, List<String>> partitionTopology = new ConcurrentHashMap<>();

    static {
        // Nodo 1: Primario de P1, Réplicas de P2, P3
        // Nodo 2: Primario de P2, Réplicas de P1, P3
        // Nodo 3: Primario de P3, Réplicas de P1, P2
        partitionTopology.put(1, Arrays.asList("localhost:9091", "localhost:9092", "localhost:9093"));
        partitionTopology.put(2, Arrays.asList("localhost:9092", "localhost:9093", "localhost:9091"));
        partitionTopology.put(3, Arrays.asList("localhost:9093", "localhost:9091", "localhost:9092"));
    }

    public static void main(String[] args) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
            System.out.println("[SERVIDOR] Servidor Central escuchando en el puerto " + SERVER_PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientExecutor.submit(new ClientHandler(clientSocket));
            }
        } finally {
            clientExecutor.shutdown();
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        ClientHandler(Socket socket) { this.clientSocket = socket; }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                String request;
                while ((request = in.readLine()) != null) {
                    System.out.println("[SERVIDOR] Cliente -> Servidor: " + request);
                    String response = processRequest(request);
                    System.out.println("[SERVIDOR] Servidor -> Cliente: " + response);
                    out.println(response);
                }
            } catch (IOException e) {
                // System.err.println("[SERVIDOR] Error en handler: " + e.getMessage());
            } finally {
                try { clientSocket.close(); } catch (IOException e) { /* ign */ }
            }
        }
    }

    private static String processRequest(String request) {
        String[] parts = request.split("\\|");
        if (parts.length < 3 || !parts[0].equals("QUERY")) {
            return "RESPONSE|ERROR|Formato de request inválido";
        }

        String queryType = parts[2];

        // Delegar a los handlers específicos
        if (queryType.equals("ARQUEO_CUENTAS")) {
            return handleArqueo(parts);
        }
        if (queryType.equals("TRANSFERIR_CUENTA")) {
            return handleTransfer(parts);
        }
        if (queryType.equals("PAGAR_DEUDA")) {
            return handlePagarDeuda(parts);
        }

        // Lógica genérica para consultas simples basadas en un solo ID de cuenta
        return handleSimpleQuery(parts);
    }

    private static String handlePagarDeuda(String[] queryParts) {
        String idCuentaPago;
        String idPrestamo;
        String monto;

        if (queryParts.length == 6) { // Admin client: QUERY|privilegiado|PAGAR_DEUDA|id_cuenta|id_prestamo|monto
            idCuentaPago = queryParts[3];
            idPrestamo = queryParts[4];
            monto = queryParts[5];
        } else if (queryParts.length == 5) { // Chat client: QUERY|clientId|PAGAR_DEUDA|id_prestamo|monto
            idCuentaPago = queryParts[1]; // Use the logged-in client's ID
            idPrestamo = queryParts[3];
            monto = queryParts[4];
        } else {
            return "RESPONSE|ERROR|Parámetros incorrectos para PAGAR_DEUDA";
        }

        try {
            // El worker necesita el idCuentaPago para validar la pertenencia del préstamo y debitar
            String workerCommand = "PAGAR_DEUDA|" + idCuentaPago + "|" + idPrestamo + "|" + monto;
            String workerRequest = "EXECUTE|" + UUID.randomUUID().toString().substring(0, 8) + "|" + workerCommand;

            // La lógica de pago se ejecuta en el nodo primario de la cuenta del cliente que paga
            int accountId = Integer.parseInt(idCuentaPago);
            int partitionId = (accountId % NUM_PARTITIONS) + 1;
            String primaryNode = partitionTopology.get(partitionId).get(0);

            String result = sendToWorker(primaryNode, workerRequest);
            String[] resultParts = result.split("\\|"); // RESULT|tx_id|STATUS|DATA

            if (resultParts.length > 2 && resultParts[2].equals("SUCCESS")) {
                return "RESPONSE|SUCCESS|" + (resultParts.length > 3 ? resultParts[3] : "");
            } else {
                return "RESPONSE|ERROR|" + (resultParts.length > 3 ? resultParts[3] : "Error desconocido en el worker.");
            }

        } catch (Exception e) {
            System.err.println("[SERVIDOR] Error crítico durante el pago de deuda: " + e.getMessage());
            return "RESPONSE|ERROR|Error interno del servidor durante el pago de deuda.";
        }
    }

    private static String handleSimpleQuery(String[] queryParts) {
        String accountIdStr;
        int accountId;
        try {
            accountIdStr = queryParts[3];
            accountId = Integer.parseInt(accountIdStr);
        } catch (Exception e) {
            return "RESPONSE|ERROR|Parámetro de ID de cuenta inválido o faltante.";
        }

        int partitionId = (accountId % NUM_PARTITIONS) + 1;
        List<String> targetNodes = partitionTopology.get(partitionId);
        if (targetNodes == null) {
            return "RESPONSE|ERROR|No se encontró topología para la partición " + partitionId;
        }

        String txId = UUID.randomUUID().toString().substring(0, 8);
        String workerRequest = "EXECUTE|" + txId + "|" + String.join("|", Arrays.copyOfRange(queryParts, 2, queryParts.length));

        for (String nodeAddress : targetNodes) {
            try {
                String result = sendToWorker(nodeAddress, workerRequest);
                String[] resultParts = result.split("\\|");

                if (resultParts.length > 3 && resultParts[2].equals("SUCCESS") && resultParts[3].startsWith("TABLE_DATA")) {
                    return String.join("|", Arrays.copyOfRange(resultParts, 3, resultParts.length));
                }
                if (resultParts.length > 2 && !resultParts[2].equals("ERROR")) {
                    return "RESPONSE|" + resultParts[2] + "|" + (resultParts.length > 3 ? String.join("|", Arrays.copyOfRange(resultParts, 3, resultParts.length)) : "");
                }
                if (resultParts.length > 3 && !resultParts[3].contains("inaccesible")) {
                    return "RESPONSE|ERROR|" + resultParts[3];
                }
            } catch (IOException e) {
                System.err.println("[SERVIDOR] Fallo al contactar nodo " + nodeAddress + ". Intentando con el siguiente...");
            }
        }
        return "RESPONSE|ERROR|La operación no pudo ser completada por ningún nodo.";
    }

    private static String handleTransfer(String[] queryParts) {
        // Formato esperado: QUERY|clientId|TRANSFERIR_CUENTA|origen|destino|monto
        if (queryParts.length != 6) {
            return "RESPONSE|ERROR|Parámetros incorrectos para TRANSFERIR_CUENTA";
        }
        try {
            String idOrigenStr = queryParts[3];
            String idDestinoStr = queryParts[4];
            String montoStr = queryParts[5];

            int idOrigen = Integer.parseInt(idOrigenStr);
            int idDestino = Integer.parseInt(idDestinoStr);

            int origenPartitionId = (idOrigen % NUM_PARTITIONS) + 1;
            int destinoPartitionId = (idDestino % NUM_PARTITIONS) + 1;

            // Si es intra-partición, delegar al worker con el comando original
            if (origenPartitionId == destinoPartitionId) {
                System.out.println("[SERVIDOR] Transferencia intra-partición detectada.");
                String workerRequest = "EXECUTE|" + UUID.randomUUID().toString().substring(0, 8) + "|" + String.join("|", Arrays.copyOfRange(queryParts, 2, queryParts.length));
                String primaryNode = partitionTopology.get(origenPartitionId).get(0);
                String result = sendToWorker(primaryNode, workerRequest);
                String[] resultParts = result.split("\\|"); // RESULT|tx_id|STATUS|DATA
                if (resultParts.length > 2 && resultParts[2].equals("SUCCESS")) {
                    return "RESPONSE|SUCCESS|Transferencia completada";
                } else {
                    return "RESPONSE|ERROR|" + (resultParts.length > 3 ? resultParts[3] : "Error desconocido en el worker.");
                }
            }

            // Si es inter-partición, orquestar la transacción
            System.out.println("[SERVIDOR] Transferencia inter-partición detectada. Orquestando...");
            String primaryNodeOrigen = partitionTopology.get(origenPartitionId).get(0);
            String primaryNodeDestino = partitionTopology.get(destinoPartitionId).get(0);

            // 1. Debitar de la cuenta de origen
            String debitDesc = "TRANSFERENCIA_ENVIADA_A:" + idDestinoStr;
            String debitRequest = "EXECUTE|" + UUID.randomUUID().toString().substring(0, 8) + "|DEBIT|" + idOrigenStr + "|" + montoStr + "|" + debitDesc;
            String debitResult = sendToWorker(primaryNodeOrigen, debitRequest);
            String[] debitResultParts = debitResult.split("\\|");

            if (debitResultParts.length < 3 || !debitResultParts[2].equals("SUCCESS")) {
                return "RESPONSE|ERROR|No se pudo debitar de la cuenta de origen: " + (debitResultParts.length > 3 ? debitResultParts[3] : "Error desconocido");
            }

            System.out.println("[SERVIDOR] Fase 1 completada: Débito exitoso.");

            // 2. Acreditar a la cuenta de destino
            String creditDesc = "TRANSFERENCIA_RECIBIDA_DE:" + idOrigenStr;
            String creditRequest = "EXECUTE|" + UUID.randomUUID().toString().substring(0, 8) + "|CREDIT|" + idDestinoStr + "|" + montoStr + "|" + creditDesc;
            String creditResult;
            try {
                creditResult = sendToWorker(primaryNodeDestino, creditRequest);
            } catch (IOException e) {
                // Si el crédito falla, hay que revertir el débito
                System.err.println("[SERVIDOR] Fase 2 fallida: No se pudo contactar al nodo de destino. Reversando débito...");
                String refundDesc = "REEMBOLSO_TRANSFERENCIA_FALLIDA_A:" + idDestinoStr;
                String refundRequest = "EXECUTE|" + UUID.randomUUID().toString().substring(0, 8) + "|CREDIT|" + idOrigenStr + "|" + montoStr + "|" + refundDesc;
                sendToWorker(primaryNodeOrigen, refundRequest); // Intentar la reversión
                return "RESPONSE|ERROR|No se pudo acreditar a la cuenta de destino. La transacción ha sido reversada.";
            }
            
            String[] creditResultParts = creditResult.split("\\|");
            if (creditResultParts.length < 3 || !creditResultParts[2].equals("SUCCESS")) {
                // Si el crédito falla, hay que revertir el débito
                System.err.println("[SERVIDOR] Fase 2 fallida: El nodo de destino reportó un error. Reversando débito...");
                String refundDesc = "REEMBOLSO_TRANSFERENCIA_FALLIDA_A:" + idDestinoStr;
                String refundRequest = "EXECUTE|" + UUID.randomUUID().toString().substring(0, 8) + "|CREDIT|" + idOrigenStr + "|" + montoStr + "|" + refundDesc;
                sendToWorker(primaryNodeOrigen, refundRequest); // Intentar la reversión
                return "RESPONSE|ERROR|No se pudo acreditar a la cuenta de destino. La transacción ha sido reversada.";
            }

            System.out.println("[SERVIDOR] Fase 2 completada: Crédito exitoso.");
            return "RESPONSE|SUCCESS|Transferencia completada";

        } catch (Exception e) {
            System.err.println("[SERVIDOR] Error crítico durante la transferencia: " + e.getMessage());
            return "RESPONSE|ERROR|Error interno del servidor durante la transferencia.";
        }
    }

    private static final String[] ALL_WORKER_NODES = {"localhost:9091", "localhost:9092", "localhost:9093"};

    private static String handleArqueo(String[] queryParts) {
        String txId = UUID.randomUUID().toString().substring(0, 8);
        String workerRequest = "EXECUTE|" + txId + "|ARQUEO_CUENTAS";
        
        List<Future<Double>> futures = new ArrayList<>();
        ExecutorService arqueoExecutor = Executors.newFixedThreadPool(ALL_WORKER_NODES.length);

        for (String nodeAddress : ALL_WORKER_NODES) {
            Future<Double> future = arqueoExecutor.submit(() -> {
                try {
                    String result = sendToWorker(nodeAddress, workerRequest);
                    String[] resultParts = result.split("\\|"); // RESULT|tx_id|SUCCESS|partial_sum
                    if (resultParts.length == 4 && resultParts[2].equals("SUCCESS")) {
                        return Double.parseDouble(resultParts[3]);
                    }
                } catch (IOException | NumberFormatException e) {
                    System.err.println("[ARQUEO] Error con el nodo " + nodeAddress + ": " + e.getMessage());
                }
                return 0.0; // Retornar 0 si el nodo falla
            });
            futures.add(future);
        }

        double grandTotal = 0.0;
        for (Future<Double> future : futures) {
            try {
                grandTotal += future.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("[ARQUEO] Error obteniendo futuro: " + e.getMessage());
            }
        }
        arqueoExecutor.shutdown();

        return "RESPONSE|SUCCESS|Arqueo total de cuentas: " + String.format("%.2f", grandTotal);
    }

    private static String sendToWorker(String nodeAddress, String request) throws IOException {
        String[] addr = nodeAddress.split(":");
        try (Socket workerSocket = new Socket(addr[0], Integer.parseInt(addr[1]));
             PrintWriter workerOut = new PrintWriter(workerSocket.getOutputStream(), true);
             BufferedReader workerIn = new BufferedReader(new InputStreamReader(workerSocket.getInputStream()))) {
            
            System.out.println("[SERVIDOR] Servidor -> Nodo " + nodeAddress + ": " + request);
            workerOut.println(request);
            String response = workerIn.readLine();
            System.out.println("[SERVIDOR] Nodo " + nodeAddress + " -> Servidor: " + response);
            if (response == null) throw new IOException("El nodo no devolvió respuesta.");
            return response;
        }
    }
}