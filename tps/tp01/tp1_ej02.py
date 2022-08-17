import csv
from sqlite3 import DatabaseError

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = '../../recursos generales/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej02.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = []


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    db = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    for fila in df_filas:
        procesar_fila(db, fila)
    generar_reporte(db)
    finalizar(db)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(linea+'\n')


# Funcion para poner el codigo que cree las estructuras a usarse en el este ejercicio
def inicializar(conn):
    # Para este ejercicio, la BBDD constará de un lista en la que iremos agregando tuplas
    return []


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
def procesar_fila(database, fila):
    # Definimos la posible tupla a insertar
    # Utilizamos los ids por si hay especialidades/tipos con el mismo nombre pero distintos ids
    tupla_a_insertar = (
        fila["id_especialidad"],
        fila["nombre_especialidad"],
        fila["id_tipo_especialidad"],
        fila["nombre_tipo_especialidad"],
    )

    # si la tupla no está en la BBDD, la insertamos
    if tupla_a_insertar not in database:
        database.append(tupla_a_insertar)

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
def generar_reporte(database):
    # Creamos el archivo
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')

    # Ordenamos la base de datos primero por el elemento 3 (nombre_tipo_especialidad)
    # y luego por el elemento 1 (nombre_especialidad)
    database_ordenada = sorted(database, key = lambda x : (x[3], x[1]))

    # Procesamos elemento por elemento de la BBDD
    for elemento in database_ordenada:
        # el elemento a insertar constará de cada valor de los elementos 1 y 3 de las tuplas
        # (nombre_especialidad y nombre_tipo_especialidad)
        elemento_a_insertar = ", ".join(
            [str(elemento[i]) if elemento[i] is not None else "" for i in (1,3)])
        
        grabar_linea(archivo, elemento_a_insertar)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    # Borrar la estructura de la base de datos
    database.clear()


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
