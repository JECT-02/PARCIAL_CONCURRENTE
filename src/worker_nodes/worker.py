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
TWO_PLACES = Decimal('0.01')

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

def get_current_balance(id_cuenta, node_data_dir):
    part_index = (int(id_cuenta) - 1) % 3 + 1
    file_path = os.path.join(node_data_dir, f"cuentas_part{part_index}.txt")
    lines, err = read_all_lines(file_path)
    if err: return None
    _, linea, err = find_line_and_index(lines, id_cuenta)
    if err: return None
    try:
        return Decimal(linea.split(',')[2]).quantize(TWO_PLACES)
    except (IndexError, ValueError):
        return None

def log_history(id_cuenta, command, details, balance, node_data_dir):
    try:
        part_index = (int(id_cuenta) - 1) % 3 + 1
        hist_file = os.path.join(node_data_dir, f"historial_part{part_index}.txt")
        fecha = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        balance_str = f'{balance:.2f}' if balance is not None else 'N/A'
        details_cleaned = str(details).replace('\n', ' ').replace('|', ' ')
        log_line = f"{fecha}|{id_cuenta}|{command}|{details_cleaned}|{balance_str}\n"
        
        with FILE_LOCK:
            with open(hist_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
    except Exception as e:
        logging.error(f"Fallo al escribir en el historial: {e}")

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
    monto = Decimal(monto_str).quantize(TWO_PLACES)

    with FILE_LOCK:
        lines, err = read_all_lines(file_path)
        if err: return f"ERROR|{err}"

        idx_origen, linea_origen, err = find_line_and_index(lines, id_origen)
        if err: return f"ERROR|Cuenta de origen {id_origen} no encontrada"
        
        idx_destino, linea_destino, err = find_line_and_index(lines, id_destino)
        if err: return f"ERROR|Cuenta de destino {id_destino} no encontrada"

        campos_origen = linea_origen.split(',')
        saldo_origen = Decimal(campos_origen[2]).quantize(TWO_PLACES)
        if saldo_origen < monto:
            log_history(id_origen, "TRANSFERIR_CUENTA", f"A:{id_destino} M:{monto}", saldo_origen, node_data_dir)
            return "ERROR|Fondos insuficientes"
        
        campos_destino = linea_destino.split(',')
        saldo_destino = Decimal(campos_destino[2]).quantize(TWO_PLACES)

        nuevo_saldo_origen = (saldo_origen - monto).quantize(TWO_PLACES)
        nuevo_saldo_destino = (saldo_destino + monto).quantize(TWO_PLACES)

        campos_origen[2] = f"{nuevo_saldo_origen:.2f}"
        lines[idx_origen] = ",".join(campos_origen) + '\n'

        campos_destino[2] = f"{nuevo_saldo_destino:.2f}"
        lines[idx_destino] = ",".join(campos_destino) + '\n'
        
        write_all_lines(file_path, lines)
        log_history(id_origen, "TRANSFERENCIA_ENVIADA", f"A:{id_destino} M:{monto}", nuevo_saldo_origen, node_data_dir)
        log_history(id_destino, "TRANSFERENCIA_RECIBIDA", f"DE:{id_origen} M:{monto}", nuevo_saldo_destino, node_data_dir)

    return "SUCCESS|Transferencia completada"

# --- Lógica de Queries ---
def handle_query(query_parts, node_data_dir):
    query_type = query_parts[0]
    params = query_parts[1:]
    logging.info(f"Procesando query: {query_type} con params: {params}")

    try:
        if query_type == "CONSULTAR_CUENTA":
            if len(params) != 1: return "ERROR|Parámetros incorrectos"
            id_cuenta = params[0]
            part_index = (int(id_cuenta) - 1) % 3 + 1
            file_path = os.path.join(node_data_dir, f"cuentas_part{part_index}.txt")
            lines, err = read_all_lines(file_path)
            if err: return f"ERROR|{err}"
            _, linea, err = find_line_and_index(lines, id_cuenta)
            if err: return f"ERROR|{err}"
            campos = linea.split(',')
            saldo_actual = Decimal(campos[2]).quantize(TWO_PLACES)
            log_history(id_cuenta, query_type, "", saldo_actual, node_data_dir)
            datos_cuenta = ",".join([campos[0], campos[1], f"{saldo_actual:.2f}", campos[3]])
            return f"SUCCESS|TABLE_DATA|ID Cuenta,ID Cliente,Saldo,Fecha Apertura|{datos_cuenta}"

        elif query_type == "TRANSFERIR_CUENTA":
            if len(params) != 3: return "ERROR|Parámetros incorrectos"
            id_origen, id_destino, monto_str = params
            if id_origen == id_destino: return "ERROR|Cuentas de origen y destino no pueden ser la misma."
            part_origen_idx = (int(id_origen) - 1) % 3 + 1
            file_origen = os.path.join(node_data_dir, f"cuentas_part{part_origen_idx}.txt")
            part_destino_idx = (int(id_destino) - 1) % 3 + 1
            file_destino = os.path.join(node_data_dir, f"cuentas_part{part_destino_idx}.txt")
            if file_origen != file_destino: return "ERROR|TRANSFERIR_CUENTA solo soporta transferencias en la misma partición"
            return handle_atomic_transfer(params, node_data_dir, file_origen)

        elif query_type == "DEBIT":
            if len(params) < 2: return "ERROR|Parámetros incorrectos para DEBIT"
            id_cuenta, monto_str = params[0], params[1]
            description = params[2] if len(params) > 2 else "DEBIT"
            monto = Decimal(monto_str).quantize(TWO_PLACES)
            with FILE_LOCK:
                part_index = (int(id_cuenta) - 1) % 3 + 1
                file_path = os.path.join(node_data_dir, f"cuentas_part{part_index}.txt")
                lines, err = read_all_lines(file_path)
                if err: return f"ERROR|{err}"
                idx, linea, err = find_line_and_index(lines, id_cuenta)
                if err: return f"ERROR|Cuenta {id_cuenta} no encontrada"
                campos = linea.split(',')
                saldo = Decimal(campos[2]).quantize(TWO_PLACES)
                if saldo < monto: 
                    log_history(id_cuenta, description, f"M:{monto}", saldo, node_data_dir)
                    return "ERROR|Fondos insuficientes"
                nuevo_saldo = (saldo - monto).quantize(TWO_PLACES)
                campos[2] = f"{nuevo_saldo:.2f}"
                lines[idx] = ",".join(campos) + '\n'
                write_all_lines(file_path, lines)
                log_history(id_cuenta, description, f"M:{monto}", nuevo_saldo, node_data_dir)
                return f"SUCCESS|Débito de {monto:.2f} completado"

        elif query_type == "CREDIT":
            if len(params) < 2: return "ERROR|Parámetros incorrectos para CREDIT"
            id_cuenta, monto_str = params[0], params[1]
            description = params[2] if len(params) > 2 else "CREDIT"
            monto = Decimal(monto_str).quantize(TWO_PLACES)
            with FILE_LOCK:
                part_index = (int(id_cuenta) - 1) % 3 + 1
                file_path = os.path.join(node_data_dir, f"cuentas_part{part_index}.txt")
                lines, err = read_all_lines(file_path)
                if err: return f"ERROR|{err}"
                idx, linea, err = find_line_and_index(lines, id_cuenta)
                if err: return f"ERROR|Cuenta {id_cuenta} no encontrada"
                campos = linea.split(',')
                saldo_actual = Decimal(campos[2]).quantize(TWO_PLACES)
                nuevo_saldo = (saldo_actual + monto).quantize(TWO_PLACES)
                campos[2] = f"{nuevo_saldo:.2f}"
                lines[idx] = ",".join(campos) + '\n'
                write_all_lines(file_path, lines)
                log_history(id_cuenta, description, f"M:{monto}", nuevo_saldo, node_data_dir)
                return f"SUCCESS|Crédito de {monto:.2f} completado"

        elif query_type == "PAGAR_DEUDA":
            if len(params) != 3: return "ERROR|Parámetros incorrectos para PAGAR_DEUDA"
            id_cuenta, id_prestamo, monto_pago_str = params
            monto_pago = Decimal(monto_pago_str).quantize(TWO_PLACES)
            if monto_pago <= 0:
                return "ERROR|El monto a pagar debe ser positivo."
            id_cliente_str = f"cliente_{id_cuenta}"
            with FILE_LOCK:
                prestamo_encontrado = False
                prestamos_file_path, lines_prestamos, idx_prestamo, linea_prestamo = None, None, -1, None
                for i in range(1, 4):
                    temp_path = os.path.join(node_data_dir, f"prestamos_part{i}.txt")
                    if not os.path.exists(temp_path): continue
                    temp_lines, err = read_all_lines(temp_path)
                    if err or not temp_lines: continue
                    for j, linea in enumerate(temp_lines):
                        campos_prestamo = linea.strip().split(',')
                        if campos_prestamo[0] == id_prestamo and campos_prestamo[1] == id_cliente_str:
                            prestamos_file_path, lines_prestamos, idx_prestamo, linea_prestamo = temp_path, temp_lines, j, linea.strip()
                            prestamo_encontrado = True
                            break
                    if prestamo_encontrado: break
                if not prestamo_encontrado: return "ERROR|El préstamo no existe o no le pertenece."
                
                campos_prestamo = linea_prestamo.split(',')
                monto_total_prestamo = Decimal(campos_prestamo[2]).quantize(TWO_PLACES)
                monto_ya_pagado = Decimal(campos_prestamo[3]).quantize(TWO_PLACES)
                deuda_restante = (monto_total_prestamo - monto_ya_pagado).quantize(TWO_PLACES)
                fecha_limite = datetime.datetime.strptime(campos_prestamo[5], '%Y-%m-%d').date()
                
                if deuda_restante <= 0:
                    return "SUCCESS|Esta deuda ya ha sido cancelada."
                if fecha_limite < datetime.date.today():
                    log_history(id_cuenta, query_type, f"P:{id_prestamo} M:{monto_pago}", get_current_balance(id_cuenta, node_data_dir), node_data_dir)
                    return "ERROR|Su deuda está vencida. Por favor, contacte al banco para recibir ayuda."
                
                saldo_cuenta = get_current_balance(id_cuenta, node_data_dir)
                if saldo_cuenta is None:
                    return "ERROR|No se pudo obtener el saldo de la cuenta."
                saldo_cuenta = saldo_cuenta.quantize(TWO_PLACES)
                if saldo_cuenta < monto_pago:
                    log_history(id_cuenta, query_type, f"P:{id_prestamo} M:{monto_pago}", saldo_cuenta, node_data_dir)
                    return f"ERROR|Fondos insuficientes. Necesita {monto_pago:.2f} pero solo tiene {saldo_cuenta:.2f}"
                
                part_cuenta_idx = (int(id_cuenta) - 1) % 3 + 1
                cuentas_file_path = os.path.join(node_data_dir, f"cuentas_part{part_cuenta_idx}.txt")
                lines_cuentas, _ = read_all_lines(cuentas_file_path)
                idx_cuenta, _, _ = find_line_and_index(lines_cuentas, id_cuenta)
                campos_cuenta = lines_cuentas[idx_cuenta].strip().split(',')
                
                nuevo_saldo_cuenta = (saldo_cuenta - monto_pago).quantize(TWO_PLACES)
                
                response = ""
                if monto_pago >= deuda_restante:
                    vuelto = (monto_pago - deuda_restante).quantize(TWO_PLACES)
                    nuevo_monto_pagado = monto_total_prestamo
                    nuevo_saldo_cuenta = (nuevo_saldo_cuenta + vuelto).quantize(TWO_PLACES)
                    campos_prestamo[4] = 'Cancelado'
                    response = f"SUCCESS|Deuda del préstamo {id_prestamo} saldada. Se devolvió {vuelto:.2f} a su cuenta."
                else:
                    nuevo_monto_pagado = (monto_ya_pagado + monto_pago).quantize(TWO_PLACES)
                    deuda_actualizada = (deuda_restante - monto_pago).quantize(TWO_PLACES)
                    response = f"SUCCESS|Pago de {monto_pago:.2f} recibido. Su nueva deuda para el préstamo {id_prestamo} es {deuda_actualizada:.2f}"
                
                campos_cuenta[2] = f"{nuevo_saldo_cuenta:.2f}"
                lines_cuentas[idx_cuenta] = ",".join(campos_cuenta) + '\n'
                
                campos_prestamo[3] = f"{nuevo_monto_pagado:.2f}"
                lines_prestamos[idx_prestamo] = ",".join(campos_prestamo) + '\n'
                
                write_all_lines(cuentas_file_path, lines_cuentas)
                write_all_lines(prestamos_file_path, lines_prestamos)
                log_history(id_cuenta, query_type, f"P:{id_prestamo} M:{monto_pago}", nuevo_saldo_cuenta, node_data_dir)
                return response

        elif query_type == "CONSULTAR_HISTORIAL":
            if len(params) != 1: return "ERROR|Parámetros incorrectos"
            id_cuenta = params[0]
            historial = []
            for i in range(1, 4):
                file_path = os.path.join(node_data_dir, f"historial_part{i}.txt")
                if not os.path.exists(file_path): continue
                lines, err = read_all_lines(file_path)
                if err or not lines: continue
                for line in lines:
                    try:
                        parts = line.strip().split('|')
                        if len(parts) > 2 and parts[1] == id_cuenta:
                            operacion = parts[2]
                            if operacion != "DEVOLUCION":
                                historial.append(line.strip().replace('|', ','))
                    except IndexError:
                        continue # Ignorar líneas malformadas en el historial
            if not historial: return "SUCCESS|No hay historial para esta cuenta."
            headers = "Fecha,ID Cuenta,Operación,Detalles,Saldo en ese Instante"
            table_rows = sorted(historial, key=lambda x: x.split(',')[0], reverse=True)
            return f"SUCCESS|TABLE_DATA|{headers}|{'|'.join(table_rows)}"

        elif query_type == "ESTADO_PAGO_PRESTAMO":
            if len(params) != 1: return "ERROR|Parámetros incorrectos"
            id_cuenta = params[0]
            id_cliente_str = f"cliente_{id_cuenta}"
            resultados = []
            today = datetime.date.today()
            for i in range(1, 4):
                file_path = os.path.join(node_data_dir, f"prestamos_part{i}.txt")
                if not os.path.exists(file_path): continue
                lines, err = read_all_lines(file_path)
                if err or not lines: continue
                for line in lines:
                    try:
                        campos = line.strip().split(',')
                        if campos[1] == id_cliente_str:
                            monto_total = Decimal(campos[2]).quantize(TWO_PLACES)
                            monto_pagado = Decimal(campos[3]).quantize(TWO_PLACES)
                            fecha_limite_str = campos[5]
                            
                            monto_pendiente = (monto_total - monto_pagado).quantize(TWO_PLACES)

                            if monto_pendiente <= 0: estado_actual = "Cancelado"
                            elif datetime.datetime.strptime(fecha_limite_str, '%Y-%m-%d').date() < today: estado_actual = "Vencido"
                            else: estado_actual = "Activo"
                            
                            linea_formateada = f"{campos[0]},{monto_total:.2f},{monto_pagado:.2f},{monto_pendiente:.2f},{estado_actual},{fecha_limite_str}"
                            resultados.append(linea_formateada)
                    except (IndexError, ValueError): continue
            if not resultados: 
                response = "SUCCESS|Usted no tiene préstamos."
            else:
                headers = "ID Préstamo,Monto Total,Monto Pagado,Monto Pendiente,Estado Actual,Fecha Límite"
                response = f"SUCCESS|TABLE_DATA|{headers}|{'|'.join(resultados)}"
            log_history(id_cuenta, query_type, "", get_current_balance(id_cuenta, node_data_dir), node_data_dir)
            return response

        elif query_type == "ARQUEO_CUENTAS":
            total_sum = Decimal('0.00').quantize(TWO_PLACES)
            for i in range(1, 4):
                file_path = os.path.join(node_data_dir, f"cuentas_part{i}.txt")
                if not os.path.exists(file_path): continue
                lines, err = read_all_lines(file_path)
                if err or not lines: continue
                for line in lines:
                    try: total_sum = (total_sum + Decimal(line.strip().split(',')[2])).quantize(TWO_PLACES)
                    except (IndexError, ValueError): continue
            return f"SUCCESS|{total_sum:.2f}"

        else:
            return f"ERROR|Query '{query_type}' no soportada"

    except Exception as e:
        logging.error(f"Error inesperado procesando query '{query_type}': {e}")
        return f"ERROR|Error interno del worker: {e}"


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
