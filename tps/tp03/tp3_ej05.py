import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp3_ej05.txt'

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
   
    cassandra_session.execute("DROP TABLE IF EXISTS especialidad;")
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS especialidad (nombre_especialidad TEXT PRIMARY KEY);")

    cassandra_session.execute("DROP TABLE IF EXISTS especialidad_ranking;")
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS especialidad_ranking (nombre_especialidad TEXT, nombre_deportista TEXT, intento INT, marca INT, PRIMARY KEY ((nombre_especialidad), marca, nombre_deportista, intento)) WITH CLUSTERING ORDER BY (marca DESC);")

    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    db.execute(f"INSERT INTO especialidad (nombre_especialidad) VALUES ('{fila['nombre_especialidad']}');")
    db.execute(f"INSERT INTO especialidad_ranking (nombre_especialidad, nombre_deportista, intento, marca) VALUES ('{fila['nombre_especialidad']}', '{fila['nombre_deportista']}', {fila['intento']}, {fila['marca']});")
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")

    especialidades = db.execute(f"SELECT nombre_especialidad from especialidad;")

    for especialidad in especialidades:
        order = "ASC" if "carrera" in especialidad.nombre_especialidad.lower() else "DESC"
        ranking = db.execute(f"SELECT nombre_especialidad, nombre_deportista, intento, marca from especialidad_ranking where nombre_especialidad = '{especialidad.nombre_especialidad}' ORDER BY marca {order} PER PARTITION LIMIT 3;")

        for rank in ranking:
            grabar_linea(archivo, f"{rank.nombre_especialidad}, {rank.nombre_deportista}, {rank.intento}, {rank.marca};")

# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.execute("DROP KEYSPACE tp3;")
    # pass


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
