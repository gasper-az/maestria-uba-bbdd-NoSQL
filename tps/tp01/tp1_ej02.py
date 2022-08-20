import csv

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
    # Para este ejercicio, la BBDD constará de un lista
    return []


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
def procesar_fila(database, fila):
    database.append(fila)

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
def generar_reporte(database):
    db = [(elem["nombre_especialidad"], elem["nombre_tipo_especialidad"]) for elem in database]    
    registros_unicos = set(db)
    # Ordenamos la base de datos primero por el elemento nombre_tipo_especialidad
    # y luego por nombre_especialidad
    database_ordenada = sorted(registros_unicos, key = lambda x : (x[1], x[0]))
    
    # Creamos el archivo
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')

    # Procesamos elemento por elemento de la BBDD
    for elemento in database_ordenada:
        # el elemento a insertar constará de cada valor de los elementos 1 y 3 de las tuplas
        # (nombre_especialidad y nombre_tipo_especialidad)
        elemento_a_insertar = ", ".join(
            [str(elemento[i]) if elemento[i] is not None else "" for i in (0, 1)])
        
        grabar_linea(archivo, elemento_a_insertar)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    # Borrar la estructura de la base de datos
    database.clear()


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
