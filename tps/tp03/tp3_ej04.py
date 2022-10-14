import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
# archivo_entrada = 'full_export.csv'
archivo_entrada = 'full_export_version_corta_brasil.csv'
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
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS deportista_marca (nombre_deportista TEXT, nombre_especialidad TEXT, nombre_torneo TEXT, intento INT, marca INT, PRIMARY KEY (nombre_deportista, marca, nombre_especialidad, nombre_torneo, intento)) WITH CLUSTERING ORDER BY (marca DESC);")

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

    filas_group_by = db.execute(f"SELECT nombre_deportista, MAX(marca) as max_marca, MIN(marca) as min_marca from deportista_marca group by nombre_deportista;")

    for fila_group_by in filas_group_by:
        filas = db.execute(f"SELECT nombre_deportista, nombre_especialidad, nombre_torneo, intento, marca from deportista_marca WHERE nombre_deportista = '{fila_group_by.nombre_deportista}' AND marca in ({fila_group_by.min_marca}, {fila_group_by.max_marca}) ORDER BY marca DESC ALLOW FILTERING;")

        for fila in filas:
            grabar_linea(archivo, f"{fila.nombre_deportista}, {fila.nombre_especialidad}, {fila.nombre_torneo}, {fila.intento}, {fila.marca};")


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.execute("DROP KEYSPACE tp3;")
    # pass


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
