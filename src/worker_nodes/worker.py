import socket
import threading
import os
import argparse
import time
import logging
import datetime
from decimal import Decimal, getcontext

# --- Configuración de Precisión Decimal ---
getcontext().prec = 12 # Precisión suficiente para cálculos financieros

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

def log_history(id_cuenta, command, details, node_data_dir, new_balance=None):
    part_index = (int(id_cuenta) - 1) % 3 + 1
    hist_file = os.path.join(node_data_dir, f"historial_part{part_index}.txt")
    fecha = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    balance_str = f'{new_balance:.2f}' if new_balance is not None else 'N/A'
    log_line = f"{id_cuenta},{command},{details},{fecha},{balance_str}\n"
    # Usamos el mismo lock global para asegurar consistencia
    with FILE_LOCK:
        with open(hist_file, 'a', encoding='utf-8') as f:
            f.write(log_line)

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

def handle_atomic_transfer(params, node_data_dir, file_path):
    id_origen, id_destino, monto_str = params
    monto = Decimal(monto_str)

    with FILE_LOCK:
        lines, err = read_all_lines(file_path)
        if err: return f"ERROR|{err}"

        idx_origen, linea_origen, err = find_line_and_index(lines, id_origen)
        if err: return f"ERROR|Cuenta de origen {id_origen} no encontrada"
        
        idx_destino, linea_destino, err = find_line_and_index(lines, id_destino)
        if err: return f"ERROR|Cuenta de destino {id_destino} no encontrada"

        campos_origen = linea_origen.split(',')
        saldo_origen = Decimal(campos_origen[2])
        if saldo_origen < monto:
            return "ERROR|Fondos insuficientes"
        
        campos_destino = linea_destino.split(',')
        saldo_destino = Decimal(campos_destino[2])

        nuevo_saldo_origen = saldo_origen - monto
        nuevo_saldo_destino = saldo_destino + monto

        campos_origen[2] = str(nuevo_saldo_origen)
        lines[idx_origen] = ",".join(campos_origen) + '\n'

        campos_destino[2] = str(nuevo_saldo_destino)
        lines[idx_destino] = ",".join(campos_destino) + '\n'
        
        write_all_lines(file_path, lines)
        log_history(id_origen, "TRANSFERENCIA_ENVIADA", f"A:{id_destino} M:{monto}", node_data_dir, new_balance=nuevo_saldo_origen)
        log_history(id_destino, "TRANSFERENCIA_RECIBIDA", f"DE:{id_origen} M:{monto}", node_data_dir, new_balance=nuevo_saldo_destino)

    logging.info(f"Transferencia atómica completada en {file_path}")
    return "SUCCESS|Transferencia completada"

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
        log_history(id_cuenta, "CONSULTAR_CUENTA", "", node_data_dir, new_balance=Decimal(campos[2]))

        # Devolver id_cuenta, id_cliente, saldo, fecha_apertura
        datos_cuenta = ",".join([campos[0], campos[1], campos[2], campos[3]])
        logging.info(f"Cuenta {id_cuenta} encontrada. Datos: {datos_cuenta}")
        return f"SUCCESS|TABLE_DATA|ID Cuenta,ID Cliente,Saldo,Fecha Apertura|{datos_cuenta}"

    elif query_type == "TRANSFERIR_CUENTA": # Solo para transferencias intra-partición
        if len(params) != 3: return "ERROR|Parámetros incorrectos"
        id_origen, id_destino, _ = params

        part_origen_idx = (int(id_origen) - 1) % 3 + 1
        file_origen = os.path.join(node_data_dir, f"cuentas_part{part_origen_idx}.txt")
        part_destino_idx = (int(id_destino) - 1) % 3 + 1
        file_destino = os.path.join(node_data_dir, f"cuentas_part{part_destino_idx}.txt")

        if file_origen != file_destino:
            return "ERROR|TRANSFERIR_CUENTA solo soporta transferencias en la misma partición"
        
        return handle_atomic_transfer(params, node_data_dir, file_origen)

    elif query_type == "DEBIT":
        if len(params) < 2: return "ERROR|Parámetros incorrectos para DEBIT"
        id_cuenta, monto_str = params[0], params[1]
        description = params[2] if len(params) > 2 else "DEBIT"
        monto = Decimal(monto_str)
        part_index = (int(id_cuenta) - 1) % 3 + 1
        file_path = os.path.join(node_data_dir, f"cuentas_part{part_index}.txt")

        with FILE_LOCK:
            lines, err = read_all_lines(file_path)
            if err: return f"ERROR|{err}"
            idx, linea, err = find_line_and_index(lines, id_cuenta)
            if err: return f"ERROR|Cuenta {id_cuenta} no encontrada"

            campos = linea.split(',')
            saldo = Decimal(campos[2])
            if saldo < monto:
                return "ERROR|Fondos insuficientes"
            
            nuevo_saldo = saldo - monto
            campos[2] = str(nuevo_saldo)
            lines[idx] = ",".join(campos) + '\n'
            write_all_lines(file_path, lines)
            log_history(id_cuenta, description, f"M:{monto}", node_data_dir, new_balance=nuevo_saldo)
        
        return f"SUCCESS|Débito de {monto} completado"

    elif query_type == "CREDIT":
        if len(params) < 2: return "ERROR|Parámetros incorrectos para CREDIT"
        id_cuenta, monto_str = params[0], params[1]
        description = params[2] if len(params) > 2 else "CREDIT"
        monto = Decimal(monto_str)
        part_index = (int(id_cuenta) - 1) % 3 + 1
        file_path = os.path.join(node_data_dir, f"cuentas_part{part_index}.txt")

        with FILE_LOCK:
            lines, err = read_all_lines(file_path)
            if err: return f"ERROR|{err}"
            idx, linea, err = find_line_and_index(lines, id_cuenta)
            if err: return f"ERROR|Cuenta {id_cuenta} no encontrada"

            campos = linea.split(',')
            saldo = Decimal(campos[2])
            nuevo_saldo = saldo + monto
            campos[2] = str(nuevo_saldo)
            lines[idx] = ",".join(campos) + '\n'
            write_all_lines(file_path, lines)
            log_history(id_cuenta, description, f"M:{monto}", node_data_dir, new_balance=nuevo_saldo)

        return f"SUCCESS|Crédito de {monto} completado"

    elif query_type == "PAGAR_DEUDA":
        if len(params) != 3: return "ERROR|Parámetros incorrectos para PAGAR_DEUDA (se espera ID_CUENTA, ID_PRESTAMO y MONTO)"
        id_cuenta, id_prestamo, monto_pago_str = params
        monto_pago = Decimal(monto_pago_str)
        id_cliente_str = f"cliente_{id_cuenta}"

        with FILE_LOCK:
            # 1. Encontrar el préstamo específico del cliente
            prestamo_encontrado = False
            for i in range(1, 4):
                prestamos_file_path = os.path.join(node_data_dir, f"prestamos_part{i}.txt")
                if not os.path.exists(prestamos_file_path): continue

                lines_prestamos, err = read_all_lines(prestamos_file_path)
                if err or not lines_prestamos: continue

                for j, linea in enumerate(lines_prestamos):
                    campos_prestamo = linea.strip().split(',')
                    # Validar que el préstamo exista, pertenezca al cliente y esté activo
                    if campos_prestamo[0] == id_prestamo and campos_prestamo[1] == id_cliente_str and campos_prestamo[4] == 'Activo':
                        prestamo_encontrado = True
                        idx_prestamo, linea_prestamo = j, linea.strip()
                        break
                if prestamo_encontrado: break
            
            if not prestamo_encontrado:
                return "ERROR|El préstamo no existe, no le pertenece o ya fue cancelado."

            # 2. Validar monto del pago
            campos_prestamo = linea_prestamo.split(',')
            monto_total_deuda = Decimal(campos_prestamo[2])
            monto_ya_pagado = Decimal(campos_prestamo[3])
            deuda_restante = monto_total_deuda - monto_ya_pagado

            if monto_pago > deuda_restante:
                return f"ERROR|El monto {monto_pago} es mayor a su deuda de {deuda_restante}"

            # 3. Validar y obtener saldo de la cuenta
            part_cuenta_idx = (int(id_cuenta) - 1) % 3 + 1
            cuentas_file_path = os.path.join(node_data_dir, f"cuentas_part{part_cuenta_idx}.txt")
            lines_cuentas, err = read_all_lines(cuentas_file_path)
            if err: return f"ERROR|{err}"
            idx_cuenta, linea_cuenta, err = find_line_and_index(lines_cuentas, id_cuenta)
            if err: return f"ERROR|No se encontró la cuenta {id_cuenta}"

            campos_cuenta = linea_cuenta.split(',')
            saldo_cuenta = Decimal(campos_cuenta[2])

            if saldo_cuenta < monto_pago:
                return "ERROR|Fondos insuficientes para realizar el pago."

            # 4. Actualizar ambos registros
            nuevo_saldo_cuenta = saldo_cuenta - monto_pago
            campos_cuenta[2] = str(nuevo_saldo_cuenta)
            lines_cuentas[idx_cuenta] = ",".join(campos_cuenta) + '\n'

            nuevo_monto_pagado = monto_ya_pagado + monto_pago
            campos_prestamo[3] = str(nuevo_monto_pagado)
            
            if nuevo_monto_pagado >= monto_total_deuda:
                campos_prestamo[4] = 'Cancelado'
                mensaje_final = f"Deuda del préstamo {id_prestamo} saldada. Usted ya no tiene esta deuda."
            else:
                nueva_deuda = monto_total_deuda - nuevo_monto_pagado
                mensaje_final = f"Pago recibido. Su nueva deuda para el préstamo {id_prestamo} es {nueva_deuda}"

            lines_prestamos[idx_prestamo] = ",".join(campos_prestamo) + '\n'

            # 5. Escribir cambios y log
            write_all_lines(cuentas_file_path, lines_cuentas)
            write_all_lines(prestamos_file_path, lines_prestamos)
            log_history(id_cuenta, "PAGAR_DEUDA", f"P:{id_prestamo} M:{monto_pago}", node_data_dir, new_balance=nuevo_saldo_cuenta)

        return f"SUCCESS|{mensaje_final}"

    elif query_type == "CONSULTAR_HISTORIAL":
        if len(params) != 1: return "ERROR|Parámetros incorrectos"
        id_cuenta = params[0]
        logging.info(f"Consultando historial para la cuenta {id_cuenta}")

        historial = []
        for i in range(1, 4):
            file_path = os.path.join(node_data_dir, f"historial_part{i}.txt")
            if not os.path.exists(file_path): continue
            
            lines, err = read_all_lines(file_path)
            if err or not lines: continue

            for line in lines:
                try:
                    campos = line.strip().split(',')
                    if campos[0] == id_cuenta:
                        historial.append(line.strip())
                except IndexError:
                    continue
        
        if not historial:
            return "SUCCESS|No hay historial para esta cuenta."
        
        table_rows = []
        for line in sorted(historial, key=lambda x: x.split(',')[3], reverse=True):
            campos = line.strip().split(',')
            id_cliente_hist, command, details, fecha, saldo_despues = campos
            mensaje = f"{command} ({details})"
            table_rows.append(f"{id_cliente_hist},{mensaje},{fecha},{saldo_despues}")
        
        return f"SUCCESS|TABLE_DATA|ID Cliente,Operación,Fecha,Saldo Resultante|{'|'.join(table_rows)}"

    elif query_type == "ARQUEO_CUENTAS":
        primary_partition_index = args.node_id
        file_path = os.path.join(node_data_dir, f"cuentas_part{primary_partition_index}.txt")
        logging.info(f"Iniciando arqueo para la partición primaria {primary_partition_index} en {file_path}")

        lines, err = read_all_lines(file_path)
        if err: logging.error(err); return f"ERROR|{err}"

        total_sum = Decimal('0.0')
        for line in lines:
            try:
                saldo = Decimal(line.strip().split(',')[2])
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
        
        log_history(id_cuenta, "ESTADO_PAGO_PRESTAMO", "", node_data_dir)

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
                        id_prestamo = campos[0]
                        estado = campos[4]
                        monto_total = Decimal(campos[2])
                        monto_pagado = Decimal(campos[3])
                        deuda_restante = monto_total - monto_pagado
                        # Formatear la línea para la nueva tabla simplificada
                        linea_simplificada = f"{id_prestamo},{estado},{deuda_restante}"
                        resultados.append(linea_simplificada)
                except IndexError:
                    continue
        
        if not resultados:
            return "SUCCESS|Usted no tiene préstamos activos."
        
        logging.info(f"Se encontraron {len(resultados)} préstamos para {id_cliente_str}")
        headers = "ID Préstamo,Estado,Deuda Restante"
        return f"SUCCESS|TABLE_DATA|{headers}|{'|'.join(resultados)}"
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
