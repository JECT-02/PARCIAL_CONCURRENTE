#!/bin/bash
# Script para compilar todos los fuentes de Java del proyecto

# Limpiar el directorio de salida
rm -rf out/*

# Crear el directorio de salida si no existe
mkdir -p out

# Compilar todos los archivos .java y ponerlos en el directorio out
find src -name "*.java" -print | xargs javac -d out

# Verificar si la compilación fue exitosa
if [ $? -eq 0 ]; then
    echo "Compilación exitosa."
else
    echo "Error en la compilación."
fi
