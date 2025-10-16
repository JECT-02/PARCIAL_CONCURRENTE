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
                case "consulta":
                    if (args.length != 2) {
                        printUsage();
                        return;
                    }
                    consultarCuenta(args[1]);
                    break;
                case "transferencia":
                    if (args.length != 4) {
                        printUsage();
                        return;
                    }
                    transferirCuenta(args[1], args[2], args[3]);
                    break;
                case "stress":
                     if (args.length != 3) {
                        printUsage();
                        return;
                    }
                    runStressTest(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
                    break;
                case "estado_pago_prestamo":
                    if (args.length != 2) {
                        printUsage();
                        return;
                    }
                    consultarPrestamo(args[1]);
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
        int txId = txCounter.incrementAndGet();
        String request = String.format("EXECUTE|%d|%s", txId, query);

        try (
            Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            out.println(request);
            System.out.println("Enviado: " + request);

            String response = in.readLine();
            System.out.println("Recibido: " + response);

        } catch (IOException e) {
            System.err.println("Error de comunicación con el servidor: " + e.getMessage());
        }
    }

    private static void printUsage() {
        System.out.println("Uso: java ClienteBanco <comando> [opciones]");
        System.out.println("Comandos:");
        System.out.println("  consulta <id_cuenta>");
        System.out.println("  estado_pago_prestamo <id_cuenta>");
        System.out.println("  transferencia <id_origen> <id_destino> <monto>");
        System.out.println("  stress <num_hilos> <num_solicitudes_por_hilo>");
    }
}
