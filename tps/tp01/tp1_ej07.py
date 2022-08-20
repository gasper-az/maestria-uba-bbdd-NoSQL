import csv
import codecs

from tp1_ej08 import ordenDescendiente

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = '../../recursos generales/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej07.txt'

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
    database.append(fila)

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
def generar_reporte(database):
    archivo = codecs.open(nombre_archivo_resultado_ejercicio, encoding='utf-8', mode='w')

    distintas_especialidades = set(map(lambda x : x["nombre_especialidad"], database))
    agrupacion_especialidad = [[registro for registro in database if registro["nombre_especialidad"] == especialidad] for especialidad in distintas_especialidades]
    agrupacion_especialidad_ordenadas = sorted(agrupacion_especialidad, key= lambda x : x[0]["nombre_especialidad"])
    
    for agrupacion_especialidad in agrupacion_especialidad_ordenadas:
        ## tomamos el primero, ya que son todas los mismos tipos para una misma especialidad
        tipo_especializacion = agrupacion_especialidad[0]["nombre_tipo_especialidad"]
        ordenados = sorted(agrupacion_especialidad, key=(lambda x : int(x["marca"])), reverse=ordenDescendiente(tipo_especializacion))
        for elemento in ordenados[:3]:
            elemento_a_insertar = "{torneo}, {especialidad}, {deportista}, {marca}".format(
                    torneo = str(elemento["nombre_torneo"]),
                    especialidad = str(elemento["nombre_especialidad"]),
                    deportista = str(elemento["nombre_deportista"]),
                    marca = str(elemento["marca"])
                )

            grabar_linea(archivo, elemento_a_insertar)
                

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    database.clear()

def ordenDescendiente(nombre_tipo_especialidad):
    return nombre_tipo_especialidad != "tiempo"

# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
