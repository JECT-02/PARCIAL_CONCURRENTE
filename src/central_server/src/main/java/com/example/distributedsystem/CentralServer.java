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

        if (queryType.equals("ARQUEO_CUENTAS")) {
            return handleArqueo(parts);
        }

        String accountIdStr = parts[3]; // Asumimos que el ID de cuenta es siempre el primer parámetro
        
        if (accountIdStr == null || accountIdStr.isEmpty()) {
            return "RESPONSE|ERROR|ID de cuenta no puede ser nulo";
        }

        int accountId = Integer.parseInt(accountIdStr);
        int partitionId = (accountId % NUM_PARTITIONS) + 1;
        
        List<String> targetNodes = partitionTopology.get(partitionId);
        if (targetNodes == null) {
            return "RESPONSE|ERROR|No se encontró topología para la partición " + partitionId;
        }

        String txId = UUID.randomUUID().toString().substring(0, 8);
        String workerRequest = "EXECUTE|" + txId + "|" + String.join("|", Arrays.copyOfRange(parts, 2, parts.length));

        // Lógica de Failover: intentar con el primario, luego con las réplicas
        for (String nodeAddress : targetNodes) {
            try {
                String result = sendToWorker(nodeAddress, workerRequest);
                String[] resultParts = result.split("\\|"); // RESULT|tx_id|STATUS|DATA

                if (resultParts.length > 2 && !resultParts[2].equals("ERROR")) {
                    return "RESPONSE|" + resultParts[2] + "|" + (resultParts.length > 3 ? String.join("|", Arrays.copyOfRange(resultParts, 3, resultParts.length)) : "");
                }
                // Si el error es de negocio (ej. fondos insuficientes), no reintentar.
                if (resultParts.length > 3 && !resultParts[3].contains("inaccesible")){
                    return "RESPONSE|ERROR|" + resultParts[3];
                }

            } catch (IOException e) {
                System.err.println("[SERVIDOR] Fallo al contactar nodo " + nodeAddress + ". Intentando con el siguiente...");
            }
        }

        return "RESPONSE|ERROR|La operación no pudo ser completada por ningún nodo.";
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