import csv
import codecs

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
    ## Para este ejercicio, la BBDD constará de una tupla
    return {}


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
def procesar_fila(database, fila):
    # Nuestra key será el "nombre_especialidad"
    database_key = fila["nombre_especialidad"]

    # Si la key no está en nuestra BBDD, agregamos un elemento
    if database_key not in database.keys():
        # Tupla: contiene lista de elementos (torneo, deportista, marca)
        # y el nombre del tipo de especialidad (se utilizará para determinar ordenamiento en el podio)
        database[database_key] = ([], fila["nombre_tipo_especialidad"])
    
    tupla_a_insertar = (
        fila["id_torneo"],
        fila["nombre_torneo"],
        fila["id_deportista"],
        fila["nombre_deportista"],
        int(fila["marca"]) # importante hacer el cast a int
    )

    torneos_por_especialidad = database[database_key][0] ## Lista dentro de nuestra tupla
    torneos_por_especialidad.append(tupla_a_insertar) if tupla_a_insertar not in torneos_por_especialidad else torneos_por_especialidad

# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
def generar_reporte(database):
    archivo = codecs.open(nombre_archivo_resultado_ejercicio, encoding='utf-8', mode='w')

    ## Ordenamos por nombre_especialidad
    especialidades_ordenadas = sorted(database.keys(), key = lambda x : x)

    for especialidad in especialidades_ordenadas:
        datos = database[especialidad][0] ## todos los datos por especialidad (id torneo, tornero, id deportista, deportista, marca)
        nombre_tipo_especialidad = database[especialidad][1]
        distintos_torneos = set(map(lambda x : x[1], datos)) # obtenemos los nombres torneos
        distintos_torneos_ordenados = sorted(distintos_torneos, key = lambda x : x) # ordenamos alfabéticamente

        agrupacion_por_torneo = [[y for y in datos if y[1] == x] for x in distintos_torneos_ordenados] # agrupamos por nombre de torneo
        for grupo in agrupacion_por_torneo:
            ## Ordenamos por marca  (acá utilizamos "nombre_tipo_especialidad" para saber si el orden deber ser Ascendente o no)
            grupo_ordenado = sorted(grupo, key = (lambda x : x[4]), reverse=ordenDescendiente(nombre_tipo_especialidad))
            # Procesamos el TOP 3
            for elemento in grupo_ordenado[: 3]:
                elemento_a_insertar = "{torneo}, {especialidad}, {deportista}, {marca}".format(
                    torneo = str(elemento[1]),
                    especialidad = especialidad,
                    deportista = str(elemento[3]),
                    marca = str(elemento[4])
                )

                grabar_linea(archivo, elemento_a_insertar)

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    database.clear()

def ordenDescendiente(nombre_tipo_especialidad):
    return nombre_tipo_especialidad != "tiempo"

# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
