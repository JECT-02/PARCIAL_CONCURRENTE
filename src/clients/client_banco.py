import socket
import argparse
import random
import time
import threading

def run_query(host, port, client_id, query):
    request = f"QUERY|{client_id}|{query}"
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(request.encode('utf-8') + b'\n')
            response = s.recv(1024).decode('utf-8')
            # print(f"[CLIENTE] Enviado: {request} -> Recibido: {response}")
            return response
    except Exception as e:
        # print(f"[CLIENTE] Error en query: {e}")
        return None

def stress_test(host, port, num_transactions):
    print(f"--- Iniciando Prueba de Estrés con {num_transactions} transacciones ---")
    start_time = time.time()
    threads = []
    for i in range(num_transactions):
        # Simular transferencias aleatorias
        id_origen = random.randint(1, 10000)
        id_destino = random.randint(1, 10000)
        while id_origen == id_destino:
            id_destino = random.randint(1, 10000)
        monto = round(random.uniform(1.0, 50.0), 2)
        
        query = f"TRANSFERIR_CUENTA|{id_origen}|{id_destino}|{monto}"
        client_id = f"stress_client_{i}"

        # Ejecutar cada request en un hilo para simular concurrencia
        thread = threading.Thread(target=run_query, args=(host, port, client_id, query))
        threads.append(thread)
        thread.start()

        print(f"Lanzada transacción {i + 1}/{num_transactions}", end='\r')
        time.sleep(random.uniform(0.01, 0.05)) # Delay aleatorio entre lanzamientos

    for t in threads:
        t.join() # Esperar a que todos los hilos terminen

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n--- Prueba de Estrés Finalizada ---")
    print(f"Tiempo total: {duration:.2f} segundos")
    print(f"Transacciones por segundo (aprox): {num_transactions / duration:.2f}")

def main():
    parser = argparse.ArgumentParser(description="Cliente Banco para el Sistema Distribuido.")
    parser.add_argument("query", nargs='?', default=None, help="La query a enviar. Formato: TIPO_QUERY|PARAM1|...")
    parser.add_argument("--stress-test", type=int, metavar="N", help="Ejecuta una prueba de estrés con N transacciones.")
    
    args = parser.parse_args()

    host = "localhost"
    port = 8080

    if args.stress_test:
        stress_test(host, port, args.stress_test)
    elif args.query:
        client_id = "client_banco_01"
        print(f"[CLIENTE] Conectado a {host}:{port}")
        response = run_query(host, port, client_id, args.query)
        print(f"[CLIENTE] Enviado: QUERY|{client_id}|{args.query}")
        print(f"[CLIENTE] Recibido: {response}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()