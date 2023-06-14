
import pandas as pd
import psycopg2
from psycopg2 import sql
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Datos de conexión
hostname = os.getenv('AWS_REDSHIFT_HOST')
port = int(os.getenv('AWS_REDSHIFT_PORT'))
database = os.getenv('AWS_REDSHIFT_DBNAME')
username = os.getenv('AWS_REDSHIFT_USER')
password = os.getenv('AWS_REDSHIFT_PASSWORD')

# Nombre de la tabla
table_name = 'entregable1'
schema_name = os.getenv('AWS_REDSHIFT_SCHEMA')


# Obtener datos de la API
def obtener_datos(usuario):
    print("\nObteniendo datos de la api...")
    url = f"https://api.github.com/users/{usuario}/repos"
    headers = {
        "Accept": "application/vnd.github.inertia-preview+json"}  # Encabezado necesario para acceder a los proyectos
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception('Error al obtener los datos de la API')


def crear_tabla():
    print("\nCreando tabla...")
    conn = psycopg2.connect(
        host=hostname,
        port=port,
        dbname=database,
        user=username,
        password=password
    )
    cur = conn.cursor()

    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (id INT, project_name VARCHAR(255), url VARCHAR(500), created TIMESTAMP, updated TIMESTAMP) sortkey(project_name);")

    conn.commit()
    cur.close()
    conn.close()


def insertar_valores(datos):
    conn = psycopg2.connect(
        host=hostname,
        port=port,
        dbname=database,
        user=username,
        password=password
    )
    cur = conn.cursor()

    cur.execute(
        f"INSERT INTO {schema_name}.{table_name} (id, project_name, url, created, updated) VALUES (%s, %s, %s, %s, %s)",
        (datos['id'], datos['name'], datos['html_url'], datos['created_at'], datos['updated_at']))

    conn.commit()
    cur.close()
    conn.close()


def consultar_datos():
    print("\nConsultando datos de la tabla...")
    conn = psycopg2.connect(
        host=hostname,
        port=port,
        dbname=database,
        user=username,
        password=password
    )
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {schema_name}.{table_name}")
    rows = cur.fetchall()

    for row in rows:
        print(row)

    cur.close()
    conn.close()


def main():
    crear_tabla()
    datos_api = obtener_datos("CoderContenidos")
    df = pd.DataFrame(datos_api)
    df = df.drop_duplicates()

    conn = psycopg2.connect(
        host=hostname,
        port=port,
        dbname=database,
        user=username,
        password=password
    )
    print("\nInsertando datos de la tabla...")

    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                insert_query = sql.SQL('''
                    INSERT INTO datos (id, name, html_url, created_at, updated_at)
                    VALUES ({}, {}, {})
                ''').format(
                    sql.Literal(row['id']),
                    sql.Literal(row['name']),
                    sql.Literal(row['html_url']),
                    sql.Literal(row['created_at']),
                    sql.Literal(row['updated_at'])
                )
                cursor.execute(insert_query)
            conn.commit()
            print("Los datos se han insertado correctamente en la tabla.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error al insertar los datos en la tabla:", error)


    consultar_datos()


if __name__ == '__main__':
    main()
