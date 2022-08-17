import csv

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = '../../recursos generales/full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp1_ej03.txt'

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
    # Para este ejercicio, utilizaremos un dictionary para facilitar 
    return {}


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
def procesar_fila(database, fila):
    # Nuestra key será el "id_tipo_especialidad"
    database_key = fila["id_tipo_especialidad"]

    # Si la key no está en nuestra BBDD, agregamos un elemento
    if database_key not in database.keys():
        # El valor será una tupla formada por "nombre_tipo_especialidad" mas una lista de especialidades
        # Al crearlo, la lista de especialidades estará vacía
        database[database_key] = (fila["nombre_tipo_especialidad"], [])

    especialidad_a_agregar = fila["id_especialidad"]
    especialidades = database[database_key][1] ## Lista dentro de nuestra tupla
    especialidades.append(especialidad_a_agregar) if especialidad_a_agregar not in especialidades else especialidades


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(database):

    archivo = open(nombre_archivo_resultado_ejercicio, 'w')
    for registro in database.values():
        linea = "{tipo_especialidad}, {cant_especialidades}".format(
            tipo_especialidad = registro[0],
            cant_especialidades = str(len(registro[1]))
        )

        grabar_linea(archivo, linea)


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(database):
    database.clear()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
