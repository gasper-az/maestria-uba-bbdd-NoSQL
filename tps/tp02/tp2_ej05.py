import csv
import redis

# Ubicacion del archivo CSV con el contenido provisto por la catedra
archivo_entrada = 'full_export.csv'
nombre_archivo_resultado_ejercicio = 'tp2_ej05.txt'

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
        if 0 == count % 100:
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

    db.zadd(f"deportista:{deportista}:especialidad:{especialidad}", {f"torneo:{torneo}:intento:{intento}": score})
    # insertar elemento en entidad para el ejercicio actual


# Funcion que realiza el o los queries que resuelven el ejercicio, utilizando la base de datos.
# Debe ser implementada por el alumno
def generar_reporte(db):
    archivo = open(nombre_archivo_resultado_ejercicio, 'w', encoding='utf-8')

    keys = db.keys("*")

    for key in sorted(keys):
        dep_esp = key.split(":especialidad:")
        especialidad = dep_esp[1]
        deportista = str.replace(dep_esp[0], "deportista:", "")

        # Cantidad de members en el sorted set
        total = db.zcard(key)

        # Las carreras deben ordenarse de mayor a menor (peor resultado a mejor resultado, mide tiempo)
        orden_desc = "carrera".upper() in especialidad.upper()

        # todos los members del sorted set, con scores
        all_members_with_scores = db.zrange(key, 0, total, withscores=True, desc=orden_desc)
        n = len(all_members_with_scores) - 1

        # Obtengo el peor torneo, intento, y marca
        worst_torneo_intento = all_members_with_scores[0][0]
        worst_t_i_split = worst_torneo_intento.split(":intento:")
        worst_intento = worst_t_i_split[1]
        worst_torneo = str.replace(worst_t_i_split[0], "torneo:", "")

        worst_score = all_members_with_scores[0][1]

        # Obtengo el mejor torneo, intento, y marca
        best_torneo_intento = all_members_with_scores[n][0]
        best_t_i_split = best_torneo_intento.split(":intento:")
        best_intento = best_t_i_split[1]
        best_torneo = str.replace(best_t_i_split[0], "torneo:", "")

        best_score = all_members_with_scores[n][1]

        linea_best = f"{deportista}, {especialidad}, {best_torneo}, {best_intento}, {best_score}"
        linea_worst = f"{deportista}, {especialidad}, {worst_torneo}, {worst_intento}, {worst_score}"
        grabar_linea(archivo, linea_best)
        grabar_linea(archivo, linea_worst)
    
    archivo.close()


# Funcion para el borrado de estructuras generadas para este ejercicio
def finalizar(db):
    db.flushdb()
    # Borrar la estructura de la base de datos


# Llamado a la ejecucion del programa
ejecutar(archivo_entrada, conexion)
