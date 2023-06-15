
import pandas as pd
import psycopg2
from psycopg2 import sql
import requests
import os
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

# Datos de conexión
hostname = os.getenv('AWS_REDSHIFT_HOST')
port = int(os.getenv('AWS_REDSHIFT_PORT'))
database = os.getenv('AWS_REDSHIFT_DBNAME')
username = os.getenv('AWS_REDSHIFT_USER')
password = os.getenv('AWS_REDSHIFT_PASSWORD')

# Nombre de la tabla
table_name = 'entregable2'
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

    #cur.execute(f"drop TABLE {schema_name}.{table_name} ;")
    #conn.commit()

    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (id INT NOT NULL, project_name VARCHAR(255), url VARCHAR(500), created TIMESTAMP, updated TIMESTAMP, dias_update VARCHAR(30), CONSTRAINT PK__id2 primary key (id))  sortkey(project_name);")

    conn.commit()
    cur.close()
    conn.close()



def consultar_datos():
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

def existe_dato(ids):
    conn = psycopg2.connect(
        host=hostname,
        port=port,
        dbname=database,
        user=username,
        password=password
    )
    cur = conn.cursor()

    cur.execute(f"SELECT count(1) FROM {schema_name}.{table_name} WHERE id={ids}")
    existe = cur.fetchone()[0]

    return existe
    cur.close()
    conn.close()


def main():
    crear_tabla()
    datos_api = obtener_datos("CoderContenidos")
    df = pd.DataFrame(datos_api)

    conn = psycopg2.connect(
        host=hostname,
        port=port,
        dbname=database,
        user=username,
        password=password
    )

    print(df.dtypes)

    df['created_at'] = pd.to_datetime(df['created_at'])
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    df['dias_update'] = df ['updated_at'] - df['created_at']
    print("\nInsertando datos de la tabla...")

    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
              if existe_dato(row['id']) < 1:
                 insert_query = sql.SQL('''
                    INSERT INTO {}.{} (id, project_name, url, created, updated, dias_update)
                    VALUES ({}, {}, {}, {}, {}, {})
                 ''').format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.Literal(row['id']),
                    sql.Literal(row['name']),
                    sql.Literal(row['html_url']),
                    sql.Literal(row['created_at']),
                    sql.Literal(row['updated_at']),
                    sql.Literal(row['dias_update'])
                 )
                 cursor.execute(insert_query)
                 conn.commit()
            print("\nLos datos se han insertado correctamente en la tabla.\n")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error al insertar los datos en la tabla:", error)


    consultar_datos()


if __name__ == '__main__':
    main()
