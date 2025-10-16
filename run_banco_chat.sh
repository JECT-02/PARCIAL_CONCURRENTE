#!/bin/bash

# Este script compila los clientes Java y muestra las instrucciones de uso.
# Asume que el Servidor Central y los Nodos Trabajadores ya están en ejecución.

echo "Compilando Clientes Java (ClienteBanco y ClienteChat)..."
javac -d src/clients/java/ src/clients/java/com/example/distributedsystem/ClienteBanco.java
if [ $? -ne 0 ]; then
    echo "Error al compilar ClienteBanco. Abortando."
    exit 1
fi
javac -d src/clients/java/ src/clients/java/com/example/distributedsystem/ClienteChat.java
if [ $? -ne 0 ]; then
    echo "Error al compilar ClienteChat. Abortando."
    exit 1
fi
javac -d src/clients/chat_banco/ src/clients/chat_banco/com/example/distributedsystem/ChatBanco.java
if [ $? -ne 0 ]; then
    echo "Error al compilar ChatBanco. Abortando."
    exit 1
fi
echo "Clientes Java compilados." 

echo -e "\n==================================================="
echo "Clientes Java compilados y listos para usar." 
echo "==================================================="
echo "Iniciando Cliente ChatBanco (Privilegiado)..."
java -cp src/clients/chat_banco/ com.example.distributedsystem.ChatBanco &
CHATBANCO_PID=$!
echo "Cliente ChatBanco iniciado con PID: $CHATBANCO_PID"
echo ""
echo "Puedes usar ClienteChat (el cliente normal) ejecutando:"
echo "java -cp src/clients/java/ com.example.distributedsystem.ClienteChat"
echo ""
echo "O ClienteBanco (el cliente de consola) desde otra terminal con comandos como:"
echo "java -cp src/clients/java/ com.example.distributedsystem.ClienteBanco consulta 123"
echo "java -cp src/clients/java/ com.example.distributedsystem.ClienteBanco estado_pago_prestamo 123"
echo "java -cp src/clients/java/ com.example.distributedsystem.ClienteBanco transferencia 123 456 500.00"
echo ""
echo "Para detener todos los componentes (incluido ChatBanco), ejecuta:"
echo "pkill -f 'java -cp src/central_server/' && pkill -f 'python3 src/worker_nodes/worker.py' && pkill -f 'java -cp src/clients/java/ com.example.distributedsystem.ClienteChat' && pkill -f 'java -cp src/clients/chat_banco/ com.example.distributedsystem.ChatBanco'"
echo "==================================================="