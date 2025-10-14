
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

echo "INFO: Compilando el Servidor Central..."
javac -d src/central_server/ src/central_server/src/main/java/com/example/distributedsystem/CentralServer.java

if [ $? -ne 0 ]; then
    echo "ERROR: La compilación de Java falló. Abortando."
    exit 1
fi

echo "INFO: Iniciando el Servidor Central en el puerto 8080..."
java -cp src/central_server/ com.example.distributedsystem.CentralServer &

# Darle un momento al servidor para que inicie
sleep 2

echo "INFO: Iniciando los Nodos Trabajadores..."
python3 src/worker_nodes/worker.py --port 9091 --node-id 1 &
python3 src/worker_nodes/worker.py --port 9092 --node-id 2 &
python3 src/worker_nodes/worker.py --port 9093 --node-id 3 &

sleep 1

echo "
SUCCESS: El sistema distribuido ha sido iniciado."
echo "- Servidor Central en el puerto 8080."
echo "- Nodos Trabajadores en los puertos 9091, 9092, 9093."
echo "Puedes interactuar con el sistema usando los clientes."
