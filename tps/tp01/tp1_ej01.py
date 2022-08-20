import csv

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = '../../recursos generales/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej01.txt'

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
    # Si el id_deportista no es 10, 20, o 30, no procesamos la fila
    if fila["id_deportista"] not in ("10", "20", "30"):
        return

    database.append(fila)

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
def generar_reporte(database):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w')

    columnas = ["id_deportista", "nombre_deportista", "id_pais_deportista", "nombre_pais_deportista",
    "id_especialidad", "nombre_especialidad", "id_tipo_especialidad", "nombre_tipo_especialidad",
    "id_torneo", "nombre_torneo", "id_ciudad_torneo", "nombre_ciudad_torneo",
    "id_pais_torneo", "nombre_pais_torneo"]
    
    for elemento in database:
        # el elemento a insertar constará de cada valor de la tupla, separados por ","
        elemento_a_insertar = ", ".join(
            [str(elemento[i]) if elemento[i] is not None else "" for i in columnas])
        
        grabar_linea(archivo, elemento_a_insertar)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    # Borrar la estructura de la base de datos
    database.clear()


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
