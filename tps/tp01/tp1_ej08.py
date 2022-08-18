import csv
import codecs
from re import S

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = '../../recursos generales/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej08.txt'

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
    # print(linea)
    archivo.write(linea+'\n')

# Funcion para poner el codigo que cree las estructuras a usarse en el este ejercicio
def inicializar(conn):
    ## Para este ejercicio, la BBDD constará de una lista
    return []


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
def procesar_fila(database, fila):

    tupla_a_insertar = (
        fila["id_especialidad"],
        fila["nombre_especialidad"],
        fila["id_torneo"],
        fila["nombre_torneo"],
        fila["id_deportista"],
        fila["nombre_deportista"],
        int(fila["marca"]), # importante hacer el cast a int
        fila["nombre_tipo_especialidad"]
    )

    # si la tupla no está en la database, la agregamos (append)
    if tupla_a_insertar not in database:
        database.append(tupla_a_insertar)


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
def generar_reporte(database):
    archivo = codecs.open(nombre_archivo_resultado_ejercicio, encoding='utf-8', mode='w')

    nombre_torneos = set(map(lambda x : x[3], database))
    nombre_torneos_ordenados = sorted(nombre_torneos, key = lambda x : x)
    
    agrupacion_por_nombre_torneo = [[registro for registro in database if registro[3] == nombre_torneo] for nombre_torneo in nombre_torneos_ordenados]

    for grupo_torneo in agrupacion_por_nombre_torneo:
        nombres_especialidades = set(map(lambda x: x[1], grupo_torneo))
        nombres_especialidades_ordenadas = sorted(nombres_especialidades, key = lambda x : x)

        agrupacion_torneo_por_especialidad = [[registro for registro in grupo_torneo if registro[1] == nombre_especialidad] for nombre_especialidad in nombres_especialidades_ordenadas]
        for grupo_torneo_especialidad in agrupacion_torneo_por_especialidad:
            nombre_tipo_especialidad = grupo_torneo_especialidad[0][7] ## el valor será el mismo en todos los registros de cada grupo, por eso con el primero basta
            grupo_ordenado_por_marcas = sorted(grupo_torneo_especialidad, key = lambda x : x[6], reverse = ordenDescendiente(nombre_tipo_especialidad))
            # Procesamos el TOP 3
            for elemento in grupo_ordenado_por_marcas[: 3]:
                elemento_a_insertar = ", ".join(
                [str(elemento[i]) if elemento[i] is not None else "" for i in (3,1,5,6)])
            
                grabar_linea(archivo, elemento_a_insertar)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    database.clear()

def ordenDescendiente(nombre_tipo_especialidad):
    return nombre_tipo_especialidad != "tiempo"

# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
