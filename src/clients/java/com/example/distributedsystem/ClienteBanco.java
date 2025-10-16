package com.example.distributedsystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClienteBanco {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 8080;
    private static final AtomicInteger txCounter = new AtomicInteger(0);

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            return;
        }

        String command = args[0];

        try {
            switch (command) {
                case "CONSULTAR_CUENTA":
                    if (args.length != 2) {
                        printUsage();
                        return;
                    }
                    consultarCuenta(args[1]);
                    break;
                case "TRANSFERIR_CUENTA":
                    if (args.length != 4) {
                        printUsage();
                        return;
                    }
                    transferirCuenta(args[1], args[2], args[3]);
                    break;
                case "ESTADO_PAGO_PRESTAMO":
                    if (args.length != 2) {
                        printUsage();
                        return;
                    }
                    consultarPrestamo(args[1]);
                    break;
                case "CONSULTAR_HISTORIAL":
                    if (args.length != 2) {
                        printUsage();
                        return;
                    }
                    consultarHistorial(args[1]);
                    break;
                default:
                    System.out.println("Comando no reconocido.");
                    printUsage();
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: Los parámetros numéricos no son válidos.");
            printUsage();
        }
    }

    private static void consultarCuenta(String idCuenta) {
        String query = String.format("CONSULTAR_CUENTA|%s", idCuenta);
        executeRequest(query);
    }

    private static void consultarPrestamo(String idCuenta) {
        String query = String.format("ESTADO_PAGO_PRESTAMO|%s", idCuenta);
        executeRequest(query);
    }

    private static void consultarHistorial(String idCuenta) {
        String query = String.format("CONSULTAR_HISTORIAL|%s", idCuenta);
        executeRequest(query);
    }

    private static void transferirCuenta(String idOrigen, String idDestino, String monto) {
        String query = String.format("TRANSFERIR_CUENTA|%s|%s|%s", idOrigen, idDestino, monto);
        executeRequest(query);
    }
    
    private static void runStressTest(int numThreads, int numRequests) {
        System.out.printf("Iniciando prueba de estrés con %d hilos y %d solicitudes por hilo...%n", numThreads, numRequests);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                for (int j = 0; j < numRequests; j++) {
                    // Simular una mezcla de operaciones
                    if (Math.random() > 0.5) { // 50% de probabilidad de ser consulta
                        String cuenta = String.valueOf(new Random().nextInt(10000) + 1);
                        consultarCuenta(cuenta);
                    } else { // 50% de ser transferencia
                        String origen = String.valueOf(new Random().nextInt(10000) + 1);
                        String destino = String.valueOf(new Random().nextInt(10000) + 1);
                        if (origen.equals(destino)) destino = String.valueOf(Integer.parseInt(destino) + 1);
                        String monto = String.format("%.2f", new Random().nextDouble() * 100);
                        transferirCuenta(origen, destino, monto);
                    }
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("La prueba de estrés excedió el tiempo límite.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("Prueba de estrés completada en %d ms.%n", (endTime - startTime));
    }


    private static void executeRequest(String query) {
        String clientId = "privilegiado"; // ClienteBanco es un cliente privilegiado
        String request = String.format("QUERY|%s|%s", clientId, query);

        try (
            Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println(request);
            // System.out.println("[CLIENTE BANCO] Enviado: " + request); // Suprimir este log

            String response = in.readLine();
            // System.out.println("[CLIENTE BANCO] Recibido: " + response); // Suprimir este log

            // Procesar y mostrar solo la información relevante
            if (response != null) {
                if (response.startsWith("TABLE_DATA|")) {
                    // Imprimir la tabla directamente
                    String[] parts = response.split("\\|");
                    String headers = parts[1];
                    System.out.println(headers.replace(',', '\t')); // Imprimir cabeceras
                    for (int i = 2; i < parts.length; i++) {
                        System.out.println(parts[i].replace(',', '\t')); // Imprimir filas
                    }
                } else if (response.startsWith("RESPONSE|SUCCESS|")) {
                    System.out.println(response.substring("RESPONSE|SUCCESS|".length()));
                } else if (response.startsWith("RESPONSE|ERROR|")) {
                    System.err.println(response.substring("RESPONSE|ERROR|".length()));
                } else {
                    System.out.println(response); // En caso de un formato inesperado
                }
            } else {
                System.err.println("El servidor no devolvió respuesta.");
            }

        } catch (IOException e) {
            System.err.println("[CLIENTE BANCO] Error de comunicación con el servidor: " + e.getMessage());
        }
    }

    private static void printUsage() {
        System.out.println("Uso: java ClienteBanco <comando> [opciones]");
        System.out.println("Comandos:");
        System.out.println("  CONSULTAR_CUENTA <id_cuenta>");
        System.out.println("  ESTADO_PAGO_PRESTAMO <id_cuenta>");
        System.out.println("  CONSULTAR_HISTORIAL <id_cuenta>");
        System.out.println("  TRANSFERIR_CUENTA <id_origen> <id_destino> <monto>");
    }
}
