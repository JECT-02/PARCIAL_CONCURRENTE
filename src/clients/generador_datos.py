import os
import random
import datetime
import shutil

NUM_CUENTAS = 10000
NUM_PRESTAMOS = 5000
NUM_TRANSACCIONES = 20000
NUM_PARTICIONES = 3
NUM_NODOS = 3
DATA_DIR = '../../data'

def generar_datos():
    # Limpiar y crear directorio de datos principal
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)

    print("Generando datos base...")
    # --- Generar Datos en Memoria ---
    cuentas = [f"{i},{f'cliente_{i}'},{round(random.uniform(0.0, 10000.0), 2)},{(datetime.date(2020, 1, 1) + datetime.timedelta(days=random.randint(0, 1825))).strftime('%Y-%m-%d')}" for i in range(1, NUM_CUENTAS + 1)]
    prestamos = [f"{i},{f'cliente_{random.randint(1, NUM_CUENTAS)}'},{round(random.uniform(500.0, 20000.0), 2)},{round(random.uniform(0, 5000.0), 2)},{'Activo' if random.random() > 0.5 else 'Cancelado'},{(datetime.date(2021, 1, 1) + datetime.timedelta(days=random.randint(0, 1095))).strftime('%Y-%m-%d')}" for i in range(1, NUM_PRESTAMOS + 1)]
    transacciones = [f"{i},{random.randint(1, NUM_CUENTAS)},{random.choice(['Deposito', 'Retiro', 'Transferencia'])},{round(random.uniform(10.0, 1000.0), 2)},{(datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S')}" for i in range(1, NUM_TRANSACCIONES + 1)]

    tablas = {
        'cuentas': cuentas,
        'prestamos': prestamos,
        'transacciones': transacciones
    }

    # --- Crear Particiones Temporales ---
    temp_partition_dir = os.path.join(DATA_DIR, 'temp_partitions')
    os.makedirs(temp_partition_dir)

    for nombre_tabla, datos in tablas.items():
        for i, linea in enumerate(datos):
            particion_index = (i % NUM_PARTICIONES) + 1
            file_path = os.path.join(temp_partition_dir, f"{nombre_tabla}_part{particion_index}.txt")
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write(linea + '\n')
    
    print("Distribuyendo particiones y réplicas en los nodos...")
    # --- Distribuir Particiones y Réplicas en Nodos ---
    for i in range(1, NUM_NODOS + 1):
        nodo_dir = os.path.join(DATA_DIR, f"nodo{i}")
        os.makedirs(nodo_dir)
        
        for j in range(1, NUM_PARTICIONES + 1):
            for nombre_tabla in tablas.keys():
                part_file_name = f"{nombre_tabla}_part{j}.txt"
                src_path = os.path.join(temp_partition_dir, part_file_name)
                
                # El nodo `i` es el primario para la partición `i`
                # La distribución de réplicas sigue un patrón circular
                if (i == j) or ((i % NUM_NODOS) + 1 == j) or ((j % NUM_NODOS) + 1 == i):
                    shutil.copy(src_path, os.path.join(nodo_dir, part_file_name))

    # Limpiar particiones temporales
    shutil.rmtree(temp_partition_dir)

    print(f"Datos generados, particionados y replicados en los directorios de nodos dentro de '{DATA_DIR}'.")

if __name__ == "__main__":
    generar_datos()