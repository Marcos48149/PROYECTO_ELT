import requests
import pandas as pd
from configparser import ConfigParser
import psycopg2
from delta import *
from sqlalchemy import MetaData, create_engine, text,column
from sqlalchemy.orm import sessionmaker 
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from datetime import datetime, timezone,timedelta




def guardar_nueva_tabla(df, path, mode="overwrite", partition_cols=None):

    write_deltalake(
        path, df, mode=mode, partition_by=partition_cols
    )
    return 'se creo un delta'


def guardar_en_tabla_delta(data, path, partition_cols=None):
    """
    Guarda solo nuevos datos en formato Delta Lake usando la operación MERGE,
    comparando los datos ya cargados con los datos que se desean almacenar
    asegurando que no se guarden registros duplicados.

    Args:
      data (pd.DataFrame): Los datos que se desean guardar.
      path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      partition_cols (list): Columnas para particionar los datos (opcional).
    """

    try:
        # Leer la tabla Delta para obtener la fecha máxima
        table = DeltaTable(path)
        df = table.to_pandas()

        # Convertir las fechas especificando el formato correcto
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
        data['date'] = pd.to_datetime(data['date'], format="%Y-%m-%d %H:%M:%S", errors='coerce')

        # Localizar las fechas en UTC si no tienen zona horaria
        if data['date'].dt.tz is None:
            data['date'] = data['date'].dt.tz_localize('UTC')

        # Obtener la fecha máxima de la tabla Delta
        fecha_max_target = df['date'].max()

        # Localizar la fecha en UTC si no tiene información de zona horaria
        if fecha_max_target.tzinfo is None:
            fecha_max_target = fecha_max_target.tz_localize('UTC')

        fechas_max_utc = fecha_max_target.astimezone(timezone.utc)

        # Filtrar los datos nuevos que tienen fecha/hora posterior
        datos_nuevos = data[data['date'] > fechas_max_utc]
        datos_nuevos2 = pa.Table.from_pandas(datos_nuevos)

        # aca borro una columna adicinal que me esta creando automaticamente pyarrow 
        datos_nuevos2 = datos_nuevos2.drop(['__index_level_0__'])

        # Verificar si hay datos nuevos para guardar
        if datos_nuevos2.num_rows > 0:
            # realizar un merge para agregar todos los datos que no coinciden con los datos de destino
            # tambien se crea updates con todas las columnas a insertar de forma dinamica
            columnas = [str(col) for col in datos_nuevos2.schema.names]
            updates = {col: f"source.{col}" for col in columnas}
            ( 
            table.merge(
                source=datos_nuevos2,
                predicate="target.date = source.date",
                source_alias="source",
                target_alias="target"
                ).when_not_matched_insert(
                updates=updates
                ).execute()
            )
        else:
            print('No hay datos nuevos para ingresar, la tabla esta actualizada')

    except TableNotFoundError:
        # Si no existe la tabla Delta Lake, se guarda como nueva
        guardar_nueva_tabla(data, path, partition_cols=partition_cols)
    

 

def conexion_consultas(config_file, section,driverdb):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parámetros:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.

    Retorna:
    Un objeto de conexión a la base de datos.
    """
    try:
        # Lectura del archivo de configuración
        parser = ConfigParser()
        parser.read(config_file)
        # Creación de un diccionario donde cargamos los parámetros de la base de datos
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            # Creación de la conexión a la base de datos con psycopg2
            engine = create_engine(
            f"{driverdb}://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?sslmode=disable"
            )
            
            # Realizar consultas
            Session = sessionmaker(bind=engine)
            session = Session()

            # Realizar la consulta para obtener la última fecha de extracción
            result = session.execute(text("SELECT MAX(fecha_extraccion) AS ultima_fecha FROM ultima_extraccion"))
            ultima_fecha = result.scalar()  # Utilizamos scalar() para obtener un único valor

            # Verificar si ultima_fecha es None (cuando no hay registros en la tabla)
            if ultima_fecha is None:
                ultima_fecha = datetime.now(timezone.utc)

                session.close()  # Cerrar la sesión
                return ultima_fecha
            
            return ultima_fecha

        else:
            print(f"Sección {section} no encontrada en el archivo de configuración.")
            return None
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None

def devolver_session(config_file, section, driverdb):
    # Este código crea una conexión a la base de datos utilizando la configuración proporcionada
    try:
        parser = ConfigParser()
        parser.read(config_file)
        conn = None
        db = {}

        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            engine = create_engine(
                f"{driverdb}://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?sslmode=disable"
            )

            Session = sessionmaker(bind=engine)
            session = Session()

            return session  # Devuelve la sesión para realizar consultas

        else:
            print(f"Sección {section} no encontrada en el archivo de configuración.")
            return None
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None


def actualizar_fecha_y_proceso(session, fecha_extraccion, proceso):
    """
    Funcion para actualizar la fecha de extracción y el nombre del proceso en la base de datos.
    
    Parámetros:
    session: Sesión de SQLAlchemy.
    fecha_extraccion: Fecha de la última extracción.
    proceso: Nombre del proceso (tipo de extracción).
    """
    try:
        # Consulta UPDATE 
        query = """
        INSERT INTO ultima_extraccion (fecha_extraccion, proceso)
        VALUES (:fecha_extraccion, :proceso)
        """
        
        # Ejecutar la consulta con los parámetros proporcionados
        session.execute(text(query), {'fecha_extraccion': fecha_extraccion, 'proceso': proceso})

        # Confirmar la transacción
        session.commit()
        print("Fecha y proceso actualizados correctamente.")
    except Exception as e:
        print(f"Error al actualizar la base de datos: {e}")
        session.rollback()  # Revertir la transacción si ocurre un error

def conectar_a_db(config_file, section, driverdb):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parámetros:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.
    driverdb (str): El driver de la base de datos a la que se conectará.

    Retorna:
    Un objeto de conexión a la base de datos.
    """
    try:
        # Lectura del archivo de configuración
        parser = ConfigParser()
        parser.read(config_file)

        # Creación de un diccionario
        # donde cargaremos los parámetros de la base de datos
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            # Creación de la conexión a la base de datos
            engine = create_engine(
                f"{driverdb}://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}"
            )
            
            return engine

        else:
            print(
                f"Sección {section} no encontrada en el archivo de configuración.")
            return None
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None

def obtener_data(base_url, endpoint, params=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    params (dict): Parámetros de consulta para enviar con la solicitud.
    headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
    dict: Los datos obtenidos de la API en formato JSON.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
           
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None
    

def construir_tabla(data,sort_column=None, ascending=False):
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    Parámetros:
    data (dict): Los datos en formato JSON obtenidos de una API.

    Retorna:
    DataFrame: Un DataFrame de pandas que contiene los datos.
    """
    try:
        df = pd.DataFrame(data)
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

            # Verificar si hubo valores no convertidos
            if df['date'].isnull().any():
                print("Advertencia: Algunas fechas no pudieron ser convertidas.")


        #df['date'] = df['date'].dt.tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')

        if sort_column and sort_column in df.columns:
            df = df.sort_values(by=sort_column, ascending=ascending)

        

        return df
    except:
        print("Los datos no están en el formato esperado")
        return None
