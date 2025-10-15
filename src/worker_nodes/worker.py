import socket
import threading
import os
import argparse
import time
import logging

# --- Configuración de Logging ---

def setup_logging(node_id):
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [NODO %(node_id)s] - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, f'worker_{node_id}.log')),
            logging.StreamHandler()
        ]
    )
    # Añadir el ID del nodo al logger para formato
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.node_id = node_id
        return record
    logging.setLogRecordFactory(record_factory)

# --- Lógica de Sincronización y Archivos ---

FILE_LOCK = threading.RLock()

def read_all_lines(file_path):
    with FILE_LOCK:
        if not os.path.exists(file_path):
            return None, f"Archivo no encontrado: {file_path}"
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.readlines(), None

def write_all_lines(file_path, lines):
    with FILE_LOCK:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)

def find_line_and_index(lines, item_id):
    for i, line in enumerate(lines):
        try:
            if line.strip().split(',')[0] == str(item_id):
                return i, line.strip(), None
        except IndexError:
            continue
    return -1, None, "ID no encontrado"

# --- Lógica de Queries ---

def handle_query(query_parts, node_data_dir):
    query_type = query_parts[0]
    params = query_parts[1:]
    logging.info(f"Procesando query: {query_type} con params: {params}")

    if query_type == "CONSULTAR_CUENTA":
        if len(params) != 1: return "ERROR|Parámetros incorrectos"
        id_cuenta = params[0]
        part_index = (int(id_cuenta) - 1) % 3 + 1
        file_path = os.path.join(node_data_dir, f"cuentas_part{part_index}.txt")
        
        logging.info(f"Consultando cuenta {id_cuenta} en {file_path}")
        lines, err = read_all_lines(file_path)
        if err: logging.error(err); return f"ERROR|{err}"
        
        _, linea, err = find_line_and_index(lines, id_cuenta)
        if err: logging.error(err); return f"ERROR|{err}"

        campos = linea.split(',')
        # Devolver id_cliente, saldo, fecha_apertura
        datos_cuenta = ",".join([campos[1], campos[2], campos[3]])
        logging.info(f"Cuenta {id_cuenta} encontrada. Datos: {datos_cuenta}")
        return f"SUCCESS|{datos_cuenta}"

    elif query_type == "TRANSFERIR_CUENTA":
        if len(params) != 3: return "ERROR|Parámetros incorrectos"
        id_origen, id_destino, monto_str = params
        monto = float(monto_str)

        part_origen_idx = (int(id_origen) - 1) % 3 + 1
        file_origen = os.path.join(node_data_dir, f"cuentas_part{part_origen_idx}.txt")
        logging.info(f"Accediendo a archivo origen: {file_origen}")
        lines_origen, err = read_all_lines(file_origen)
        if err: logging.error(err); return f"ERROR|{err}"
        idx_origen, linea_origen, err = find_line_and_index(lines_origen, id_origen)
        if err: logging.error(err); return f"ERROR|Cuenta de origen {id_origen} no encontrada en {file_origen}"
        
        part_destino_idx = (int(id_destino) - 1) % 3 + 1
        file_destino = os.path.join(node_data_dir, f"cuentas_part{part_destino_idx}.txt")
        logging.info(f"Accediendo a archivo destino: {file_destino}")
        lines_destino, err = read_all_lines(file_destino)
        if err: logging.error(err); return f"ERROR|{err}"
        idx_destino, linea_destino, err = find_line_and_index(lines_destino, id_destino)
        if err: logging.error(err); return f"ERROR|Cuenta de destino {id_destino} no encontrada en {file_destino}"

        campos_origen = linea_origen.split(',')
        saldo_origen = float(campos_origen[2])
        logging.info(f"Saldo origen (cuenta {id_origen}): {saldo_origen}")
        if saldo_origen < monto:
            logging.warning(f"Fondos insuficientes para transferir {monto} de la cuenta {id_origen}")
            return "ERROR|Fondos insuficientes"
        
        campos_origen[2] = str(saldo_origen - monto)
        lines_origen[idx_origen] = ",".join(campos_origen) + '\n'

        campos_destino = linea_destino.split(',')
        saldo_destino = float(campos_destino[2])
        logging.info(f"Saldo destino (cuenta {id_destino}): {saldo_destino}")
        campos_destino[2] = str(saldo_destino + monto)
        lines_destino[idx_destino] = ",".join(campos_destino) + '\n'

        logging.info(f"Escribiendo cambios en {file_origen}")
        write_all_lines(file_origen, lines_origen)
        logging.info(f"Escribiendo cambios en {file_destino}")
        write_all_lines(file_destino, lines_destino)

        logging.info("Transferencia completada exitosamente.")
        return "SUCCESS|Transferencia completada"

    elif query_type == "ARQUEO_CUENTAS":
        primary_partition_index = args.node_id
        file_path = os.path.join(node_data_dir, f"cuentas_part{primary_partition_index}.txt")
        logging.info(f"Iniciando arqueo para la partición primaria {primary_partition_index} en {file_path}")

        lines, err = read_all_lines(file_path)
        if err: logging.error(err); return f"ERROR|{err}"

        total_sum = 0.0
        for line in lines:
            try:
                saldo = float(line.strip().split(',')[2])
                total_sum += saldo
            except (IndexError, ValueError):
                continue
        
        logging.info(f"Suma parcial para la partición {primary_partition_index}: {total_sum}")
        return f"SUCCESS|{total_sum}"

    elif query_type == "ESTADO_PAGO_PRESTAMO":
        if len(params) != 1: return "ERROR|Parámetros incorrectos"
        id_cuenta = params[0]
        id_cliente_str = f"cliente_{id_cuenta}"
        logging.info(f"Buscando préstamos para el cliente {id_cliente_str}")
        resultados = []
        for i in range(1, 4):
            file_path = os.path.join(node_data_dir, f"prestamos_part{i}.txt")
            if not os.path.exists(file_path): continue
            
            lines, err = read_all_lines(file_path)
            if err or not lines: continue

            for line in lines:
                try:
                    campos = line.strip().split(',')
                    if campos[1] == id_cliente_str:
                        resultados.append(line.strip())
                except IndexError:
                    continue
        
        if not resultados:
            logging.warning(f"No se encontraron préstamos para {id_cliente_str}")
            return "ERROR|No se encontraron préstamos para la cuenta"
        
        logging.info(f"Se encontraron {len(resultados)} préstamos para {id_cliente_str}")
        return f"SUCCESS|{'#'.join(resultados)}"
    else:
        return f"ERROR|Query '{query_type}' no soportada"

# --- Servidor TCP Concurrente ---

class ThreadedTCPRequestHandler(threading.Thread):
    # ... (sin cambios)
    def __init__(self, client_socket, addr, node_data_dir):
        super().__init__()
        self.client_socket = client_socket
        self.addr = addr
        self.node_data_dir = node_data_dir

    def run(self):
        try:
            data = self.client_socket.recv(1024)
            request = data.decode('utf-8').strip()
            
            parts = request.split('|')
            if len(parts) < 3 or parts[0] != 'EXECUTE':
                response = "ERROR|Formato inválido"
            else:
                tx_id = parts[1]
                query_result = handle_query(parts[2:], self.node_data_dir)
                response = f"RESULT|{tx_id}|{query_result}"

            self.client_socket.sendall(response.encode('utf-8'))
        except Exception as e:
            logging.error(f"Error en conexión con {self.addr}: {e}")
        finally:
            self.client_socket.close()

class WorkerServer:
    # ... (sin cambios)
    def __init__(self, host, port, node_id):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.node_data_dir = os.path.join('data', f"nodo{node_id}")
        if not os.path.exists(self.node_data_dir):
            raise FileNotFoundError(f"El directorio de datos {self.node_data_dir} no existe.")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        logging.info(f"escuchando en {self.host}:{self.port}")

        try:
            while True:
                client_sock, address = self.server_socket.accept()
                handler_thread = ThreadedTCPRequestHandler(client_sock, address, self.node_data_dir)
                handler_thread.start()
        except KeyboardInterrupt:
            logging.info("detenido.")
        finally:
            self.server_socket.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nodo Trabajador del Sistema Distribuido.")
    parser.add_argument("--host", type=str, default="localhost", help="Host del nodo.")
    parser.add_argument("--port", type=int, required=True, help="Puerto del nodo.")
    parser.add_argument("--node-id", type=int, required=True, help="ID del nodo (ej: 1)")
    args = parser.parse_args()

    setup_logging(args.node_id)
    worker = WorkerServer(args.host, args.port, args.node_id)
    worker.start()
