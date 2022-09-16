import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2_ej06.txt'

# Objeto de configuracion para conectarse a la base de datos usada en este ejercicio
conexion = {
    'redisurl': 'localhost',
    'redispuerto': 6379
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
        if 0 == count % 10000000000000:
            endbloque = time.time()
            tiempo = endbloque - startbloque
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
    r = redis.Redis(conn["redisurl"], conn["redispuerto"], db=0, decode_responses=True)
    return r
    # crear db


# Funcion que dada una linea del archivo CSV (en forma de objeto) va a encargarse de insertar el (o los) objetos
# necesarios
# Debe ser implementada por el alumno
def procesar_fila(db, fila):
    deportista = fila["nombre_deportista"]
    especialidad = fila["nombre_especialidad"]
    score = fila["marca"]
    torneo = fila["nombre_torneo"]
    intento = fila["intento"]

    db.zadd(f"especialidad:{especialidad}", {f"{torneo}:{intento}:{deportista}": score})
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')

    keys = db.keys("*")

    for key in sorted(keys):
    
        especialidad = str.replace(key, "especialidad:", "")
        # total = db.zcard(key)

        # Las carreras deben ordenarse de menor a mayor (mide tiempo)
        orden_desc = "carrera".upper() not in especialidad.upper()

        # todos los members del sorted set, con scores
        all_members_with_scores = db.zrange(key, 0, 2, withscores=True, desc=orden_desc)

        for member in all_members_with_scores:
            member_split = member[0].split(":")
            # torneo = member_split[0]
            intento = member_split[1]
            deportista = member_split[2]
            score = member[1]
            linea = f"{especialidad}, {deportista}, {intento}, {score}"
            grabar_linea(archivo, linea)
    
    archivo.close()


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
