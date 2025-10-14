
# Guía de Uso del Sistema Distribuido Bancario

Esta guía explica cómo compilar, ejecutar y utilizar todos los componentes del sistema desde la consola.

## Prerrequisitos

- **Java:** JDK 8 o superior (`java` y `javac` deben estar en el PATH).
- **Python:** Python 3.6 o superior (`python3` debe estar en el PATH).
- **Tkinter (para la GUI):** En sistemas Linux, puede que necesites instalarlo por separado. En Debian/Ubuntu:
  ```bash
  sudo apt-get install -y python3-tk
  ```

## Paso 1: Generar los Datos de Prueba

Este comando creará el directorio `data/` con las particiones y réplicas de las cuentas, préstamos y transacciones distribuidas en subdirectorios para cada nodo.

```bash
python3 src/clients/generador_datos.py
```

## Paso 2: Compilar y Ejecutar el Servidor Central

Primero, compila el código fuente de Java:

```bash
javac -d src/central_server/ src/central_server/src/main/java/com/example/distributedsystem/CentralServer.java
```

Luego, ejecuta el servidor en segundo plano. Escuchará en el puerto `8080`.

```bash
java -cp src/central_server/ com.example.distributedsystem.CentralServer &
```

## Paso 3: Ejecutar los Nodos Trabajadores

Abre tres terminales o ejecuta los siguientes comandos en segundo plano. Cada uno representa un nodo que escucha en un puerto diferente y gestiona un conjunto de datos.

```bash
# Iniciar Nodo 1 (Puerto 9091)
python3 src/worker_nodes/worker.py --port 9091 --node-id 1 &

# Iniciar Nodo 2 (Puerto 9092)
python3 src/worker_nodes/worker.py --port 9092 --node-id 2 &

# Iniciar Nodo 3 (Puerto 9093)
python3 src/worker_nodes/worker.py --port 9093 --node-id 3 &
```

En este punto, todo el backend del sistema está en funcionamiento.

## Paso 4: Usar los Clientes

Puedes interactuar con el sistema usando el cliente de consola o el cliente gráfico.

### A. Cliente de Banco (Línea de Comandos)

Es útil para pruebas rápidas y para la prueba de estrés.

**Ejemplos de Consultas Individuales:**

```bash
# Consultar saldo de la cuenta 25
python3 src/clients/client_banco.py 'CONSULTAR_CUENTA|25'

# Realizar una transferencia
python3 src/clients/client_banco.py 'TRANSFERIR_CUENTA|25|30|75.50'

# Consultar préstamos de la cuenta 107
python3 src/clients/client_banco.py 'ESTADO_PAGO_PRESTAMO|107'

# Realizar un arqueo total de todas las cuentas
python3 src/clients/client_banco.py 'ARQUEO_CUENTAS'
```

**Ejecutar la Prueba de Estrés:**

Este comando simulará 200 transferencias aleatorias concurrentes.

```bash
python3 src/clients/client_banco.py --stress-test 200
```

### B. Cliente de Chat (GUI)

Para una experiencia más interactiva.

**Lanzar la aplicación:**

```bash
python3 src/clients/client_chat.py
```

1.  Al abrirse, te pedirá un **ID de Cliente** (puedes usar `10`, `107`, etc.).
2.  En el campo de texto inferior, escribe las consultas en formato `COMANDO param1 param2 ...` y presiona Enter.
    - `CONSULTAR_CUENTA 10`
    - `ESTADO_PAGO_PRESTAMO 107`
    - `TRANSFERIR_CUENTA 10 25 50.50`

## Paso 5: Detener Todo el Sistema

Para detener todos los procesos (servidor y nodos) que se ejecutan en segundo plano, puedes usar los siguientes comandos:

```bash
# Detener el servidor Java
pkill java

# Detener todos los workers de Python
pkill python3
```
