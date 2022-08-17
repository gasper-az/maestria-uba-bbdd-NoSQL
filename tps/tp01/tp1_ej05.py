import csv

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = '../../recursos generales/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej05.txt'

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
    ## Para este ejercicio, la BBDD constará de una lista
    return []


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
def procesar_fila(database, fila):
    
    # Si no es el deportista o la ciudad indicada, no procesamos la fila
    if fila["nombre_especialidad"] != "carrera 100 m" or fila["nombre_torneo"] != "Torneo de Argentina":
        return

    tupla_a_insertar = (
        fila["id_torneo"],
        fila["nombre_torneo"],
        fila["id_especialidad"],
        fila["nombre_especialidad"],
        fila["id_deportista"],
        fila["nombre_deportista"],
        int(fila["marca"]) # importante hacer el cast a int
    )
    
    # si la tupla no está en la database, la agregamos (append)
    if tupla_a_insertar not in database:
        database.append(tupla_a_insertar)


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
def generar_reporte(database):
    # ordenamos por nombre_tipo_especialidad, y luego por marca
    database_ordenada = sorted(database, key = lambda x : (x[3], x[6]))

    archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    
    # Procesamos el top 3
    for elemento in database_ordenada[: 3]:
        # (nombre_torneo, nombre_especialidad, nombre_deportista, marca)
        elemento_a_insertar = ", ".join(
            [str(elemento[i]) if elemento[i] is not None else "" for i in (1,3,5,6)])
        
        grabar_linea(archivo, elemento_a_insertar)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    database.clear()


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
