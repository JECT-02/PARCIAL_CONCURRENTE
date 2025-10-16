
#!/bin/bash

# Puertos a verificar y limpiar
PORTS=(8080 9091 9092 9093)

echo "INFO: Verificando y limpiando puertos antes de iniciar..."

for PORT in ${PORTS[@]}; do
  # lsof -t -i:PORT busca el PID del proceso que usa el puerto
  PID=$(lsof -t -i:$PORT)
  if [ -n "$PID" ]; then
    echo "WARN: El puerto $PORT ya está en uso por el proceso con PID $PID. Terminándolo..."
    kill -9 $PID
    # Esperar un momento para que el puerto se libere
    sleep 1
  fi
done

echo "INFO: Puertos limpiados."

# --- Compilar y Ejecutar --- 

mkdir -p logs

echo "INFO: Compilando el Servidor Central y los Clientes Java..."
javac -d out src/central_server/src/main/java/com/example/distributedsystem/CentralServer.java src/clients/java/com/example/distributedsystem/ClienteBanco.java src/clients/java/com/example/distributedsystem/ClienteChat.java

if [ $? -ne 0 ]; then
    echo "ERROR: La compilación de Java falló. Abortando."
    exit 1
fi

echo "INFO: Iniciando el Servidor Central en el puerto 8080..."
java -cp out com.example.distributedsystem.CentralServer > logs/server.log 2>&1 &

# Darle un momento al servidor para que inicie
sleep 2

echo "INFO: Iniciando los Nodos Trabajadores..."
python3 src/worker_nodes/worker.py --port 9091 --node-id 1 > logs/worker1.log 2>&1 &
python3 src/worker_nodes/worker.py --port 9092 --node-id 2 > logs/worker2.log 2>&1 &
python3 src/worker_nodes/worker.py --port 9093 --node-id 3 > logs/worker3.log 2>&1 &

sleep 1

echo "
SUCCESS: El sistema distribuido ha sido iniciado."
echo "- Servidor Central en el puerto 8080 (logs en logs/server.log)."
echo "- Nodos Trabajadores en los puertos 9091, 9092, 9093 (logs en logs/workerN.log)."
echo "Puedes interactuar con el sistema usando los scripts: run_banco.sh, run_chat.sh y run_banco_chat.sh"
