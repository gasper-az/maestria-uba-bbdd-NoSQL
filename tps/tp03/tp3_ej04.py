import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp3_ej04.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'cassandraurl': 'localhost',
    'cassandrapuerto': 9042
}


# Funcion que dada la configuracion y ubicacion del archivo, carga la base de datos, genera el reporte, y borra la
# base de datos
def ejecutar(file, conn):
    import time

    start = time.time()
    db = inicializar(conn)
    df_filas = csv.DictReader(open(file, "r", encoding="utf-8"))
    count = 0
    startbloque = time.time()
    for fila in df_filas:
        procesar_fila(db, fila)
        count += 1
        if 0 == count%100:
            endbloque = time.time()
            tiempo = endbloque-startbloque
            print(str(count) + " en " + str(tiempo) + " segundos")
            startbloque = time.time()
    generar_reporte(db)
    finalizar(db)
    end = time.time()
    print("tiempo total en segundos")
    print(end - start)


# Funcion que dado un archivo abierto y una linea, imprime por consola y guarda al final de archivo esa linea
def grabar_linea(archivo, linea):
    print(linea)
    archivo.write(str(linea) + '\n')


def inicializar(conn):
    cassandra_session = Cluster(contact_points=[conn["cassandraurl"]], port=conn["cassandrapuerto"]).connect()
    
    cassandra_session.execute("CREATE KEYSPACE IF NOT EXISTS tp3 WITH replication = {'class' : 'SimpleStrategy', 'replication_factor':1};")
    cassandra_session.execute('USE tp3')
   
    cassandra_session.execute("DROP TABLE IF EXISTS deportista_marca;")
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS deportista_marca (nombre_deportista TEXT, nombre_especialidad TEXT, nombre_torneo TEXT, intento INT, marca INT, PRIMARY KEY ((nombre_deportista, nombre_especialidad), marca, nombre_torneo, intento)) WITH CLUSTERING ORDER BY (marca DESC);")

    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    db.execute(f"INSERT INTO deportista_marca (nombre_deportista, nombre_especialidad, nombre_torneo, intento, marca) VALUES ('{fila['nombre_deportista']}', '{fila['nombre_especialidad']}', '{fila['nombre_torneo']}', {fila['intento']}, {fila['marca']});")
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")

    filas_desc = db.execute(f"SELECT nombre_deportista, nombre_especialidad, nombre_torneo, intento, marca FROM deportista_marca PER PARTITION LIMIT 1;")

    for fila_desc in filas_desc:
        filas_asc =  db.execute(f"SELECT nombre_deportista, nombre_especialidad, nombre_torneo, intento, marca FROM deportista_marca WHERE nombre_deportista = '{fila_desc.nombre_deportista}' AND nombre_especialidad = '{fila_desc.nombre_especialidad}' ORDER BY marca ASC PER PARTITION LIMIT 1;")

        fila_asc = filas_asc[0]

        if ("carrera" in fila_asc.nombre_especialidad.lower()):
            ## Para las carreras, el valor mínimo (mejor) es el ordenado ascendentemente
            grabar_linea(archivo, f"{fila_asc.nombre_deportista}, {fila_asc.nombre_especialidad}, {fila_asc.nombre_torneo}, {fila_asc.intento}, {fila_asc.marca};")
            grabar_linea(archivo, f"{fila_desc.nombre_deportista}, {fila_desc.nombre_especialidad}, {fila_desc.nombre_torneo}, {fila_desc.intento}, {fila_desc.marca};")
        else:
            ## Para las NO carreras, el mejor valor es el ordernado descendentemente
            grabar_linea(archivo, f"{fila_desc.nombre_deportista}, {fila_desc.nombre_especialidad}, {fila_desc.nombre_torneo}, {fila_desc.intento}, {fila_desc.marca};")
            grabar_linea(archivo, f"{fila_asc.nombre_deportista}, {fila_asc.nombre_especialidad}, {fila_asc.nombre_torneo}, {fila_asc.intento}, {fila_asc.marca};")

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.execute("DROP KEYSPACE tp3;")
    # pass


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
