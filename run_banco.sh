#!/bin/bash
# Script para ejecutar el Cliente Banco
# Permite pasar argumentos directamente al programa Java.
# Ejemplo:
# ./run_banco.sh consulta 101
# ./run_banco.sh transferencia 101 102 50.0
# ./run_banco.sh stress 10 100

java -cp out com.example.distributedsystem.ClienteBanco "$@"
