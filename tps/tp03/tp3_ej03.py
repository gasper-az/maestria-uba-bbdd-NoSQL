import csv
from cassandra.cluster import Cluster

# Ubicacion del archivo CSV con el contenido provisto por la catedra
# archivo_entrada = 'full_export.csv'
archivo_entrada = 'full_export_version_corta_brasil.csv'
nombre_archivo_resultado_ejercicio = 'tp3_ej03.txt'

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
   
    cassandra_session.execute("DROP TABLE IF EXISTS tipo_especialidades;")
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS tipo_especialidades ( id_tipo INT, tipo_especialidad TEXT, especialidad TEXT, PRIMARY KEY (id_tipo, especialidad)) WITH CLUSTERING ORDER BY (especialidad ASC);")
   
    cassandra_session.execute("DROP TABLE IF EXISTS tipo_especialidades_counter;")
    cassandra_session.execute("CREATE TABLE IF NOT EXISTS tipo_especialidades_counter ( tipo_especialidad TEXT PRIMARY KEY, cantidad COUNTER );")
    return cassandra_session

# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    db.execute(f"INSERT INTO tipo_especialidades (id_tipo, tipo_especialidad, especialidad) VALUES ({fila['id_tipo_especialidad']}, '{fila['nombre_tipo_especialidad']}', '{fila['nombre_especialidad']}');")
    db.execute(f"UPDATE tipo_especialidades_counter set cantidad = cantidad + 1 WHERE tipo_especialidad = '{fila['nombre_tipo_especialidad']}';")
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding="utf-8")
    filas=db.execute(f"SELECT id_tipo, tipo_especialidad, especialidad FROM tipo_especialidades;")
    for fila in filas:
        grabar_linea(archivo, f"{fila.id_tipo}, {fila.tipo_especialidad}, {fila.especialidad}")

    filas_counter = db.execute(f"SELECT tipo_especialidad, cantidad FROM tipo_especialidades_counter;")
    for fila in filas_counter:
        grabar_linea(archivo, f"{fila.tipo_especialidad}, {fila.cantidad}")


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.execute("DROP KEYSPACE tp3;")
    # pass


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
