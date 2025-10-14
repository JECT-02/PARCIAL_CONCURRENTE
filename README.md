# Proyecto de Sistema Distribuido de Transacciones Bancarias

Este proyecto implementa un sistema distribuido de transacciones bancarias basado en los requisitos del documento `parcialCC4P1_252_v35.pdf`. El sistema simula un entorno bancario con clientes que realizan consultas y transacciones a través de una red de nodos trabajadores coordinados por un servidor central.

## Arquitectura

El sistema sigue una arquitectura de microservicios distribuida con los siguientes componentes principales:

1.  **Servidor Central (`central_server` - Java):**
    *   Actúa como el cerebro del sistema. Es un servidor concurrente que maneja múltiples conexiones de clientes y nodos.
    *   No tiene estado de negocio; su función principal es de **coordinador y enrutador**.
    *   Recibe las solicitudes de los clientes, determina qué nodo(s) trabajador(es) deben procesar la solicitud basándose en la ubicación de las particiones de datos, y delega la tarea.
    *   Gestiona un mapa de la topología de la red, conociendo qué particiones de datos y réplicas reside en cada nodo.
    *   Maneja la **conmutación por error (failover)**. Si un nodo no responde, reenvía la solicitud a otro nodo que contenga una réplica de los datos necesarios.

2.  **Nodos Trabajadores (`worker_nodes` - Python):**
    *   Son los encargados de realizar el trabajo pesado. Cada nodo es un servidor concurrente que gestiona un subconjunto de los datos del sistema.
    *   Almacenan las **particiones y réplicas** de las tablas de la base de datos en archivos de texto (`.txt`).
    *   Ejecutan las operaciones CRUD (Crear, Leer, Actualizar, Borrar) sobre los datos, como `CONSULTAR_CUENTA`, `TRANSFERIR_CUENTA`, etc.
    *   Utilizan todos los núcleos de CPU disponibles para procesar transacciones de manera concurrente.

3.  **Clientes (`clients` - Python):**
    *   **Cliente Banco (`client_banco.py`):** Una aplicación de línea de comandos (CLI) para realizar operaciones masivas y simular una alta concurrencia. Se utiliza para la creación inicial de cuentas y para pruebas de estrés.
    *   **Cliente Chat (`client_chat.py`):** Una aplicación de escritorio con una interfaz gráfica de usuario (GUI) simple (usando Tkinter) que permite a un usuario final realizar consultas de forma interactiva.

4.  **Base de Datos Distribuida (`data`):**
    *   Los datos no residen en un motor de base de datos tradicional, sino en **archivos de texto plano (`.txt`)**.
    *   Las tablas (ej. `cuentas.txt`) se dividen en múltiples **particiones**.
    *   Cada partición se **replica 3 veces** a través de diferentes nodos trabajadores para garantizar la alta disponibilidad y la tolerancia a fallos.

## Diagrama de Protocolo de Comunicación

La comunicación entre los componentes se realiza a través de sockets TCP utilizando un protocolo de texto simple basado en el formato `COMANDO|PARAMETRO1|PARAMETRO2|...`.

*   **Cliente -> Servidor Central:**
    *   `QUERY|ID_CLIENTE|TIPO_QUERY|PARAMETROS...`
    *   Ej: `QUERY|user123|CONSULTAR_CUENTA|101`

*   **Servidor Central -> Nodo Trabajador:**
    *   `EXECUTE|ID_TRANSACCION|TIPO_QUERY|PARAMETROS...`
    *   Ej: `EXECUTE|tx456|CONSULTAR_CUENTA|101`

*   **Nodo Trabajador -> Servidor Central:**
    *   `RESULT|ID_TRANSACCION|ESTADO|DATOS...`
    *   Ej: `RESULT|tx456|SUCCESS|1500.00`
    *   Ej: `RESULT|tx457|ERROR|Fondos insuficientes`

*   **Servidor Central -> Cliente:**
    *   `RESPONSE|ESTADO|MENSAJE`
    *   Ej: `RESPONSE|SUCCESS|Su saldo es 1500.00`

## Cómo Construir y Ejecutar el Sistema

(Esta sección se completará a medida que se implementen los componentes).

### Prerrequisitos
- Java (JDK 8 o superior)
- Python 3

### Pasos
1.  **Generar Datos:** Ejecutar el script para crear las 10,000 cuentas y distribuirlas en particiones.
2.  **Iniciar el Servidor Central:** Compilar y ejecutar el servidor Java.
3.  **Iniciar los Nodos Trabajadores:** Ejecutar los scripts de los nodos Python.
4.  **Ejecutar los Clientes:** Utilizar los clientes de Python para interactuar con el sistema.
