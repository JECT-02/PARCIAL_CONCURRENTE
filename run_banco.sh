#!/bin/bash

# Este script actúa como un envoltorio para ejecutar comandos de ClienteBanco desde la CLI.
# Asume que ClienteBanco.class ya ha sido compilado.

# Recompilar ClienteBanco.java para asegurar que siempre esté actualizado
javac -d src/clients/java/ src/clients/java/com/example/distributedsystem/ClienteBanco.java
if [ $? -ne 0 ]; then
    echo "Error al compilar ClienteBanco.java. Abortando."
    exit 1
fi

CP="src/clients/java/"
MAIN_CLASS="com.example.distributedsystem.ClienteBanco"

# Mostrar uso si no se proporcionan argumentos
if [ "$#" -eq 0 ]; then
    echo "Uso: bash run_banco.sh <comando> [opciones]"
    echo "Comandos disponibles (privilegiados):"
    echo "  CONSULTAR_CUENTA <id_cuenta>"
    echo "  ESTADO_PAGO_PRESTAMO <id_cuenta>"
    echo "  CONSULTAR_HISTORIAL <id_cuenta>"
    echo "  TRANSFERIR_CUENTA <id_origen> <id_destino> <monto>"
    exit 1
fi

# Ejecutar ClienteBanco con los argumentos proporcionados
java -cp "$CP" "$MAIN_CLASS" "$@"
