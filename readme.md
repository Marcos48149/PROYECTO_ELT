# Proyecto ELT(Extract, Load, Transform)
## Descripción
Este proyecto tiene como objetivo realizar la extracción, carga y transformación (ELT) de datos de criptomonedas en tiempo real utilizando una API pública. El flujo de trabajo incluye la ingesta de datos crudos desde la API, almacenamiento de la ultima fecha de extraccion en una base de datos en la nube como AWS(instancia: PostgreSQL), para luego extraer unicamente los datos desde(ultima fecha de extraccion) hasta la (fecha actual) , almacenamiento en archivos Delta Lake locales para procesamiento posterior, y finalmente, la transformación de estos datos.

El proyecto está compuesto por:
- Un archivo `.ipynb` donde se realizan las operaciones de extracción, transformación y carga.
- Un archivo `.py` que contiene la lógica de negocio para las transformaciones y otras operaciones.
- Un archivo `requirements.txt` con las dependencias necesarias para ejecutar el proyecto.

Las tecnologías principales utilizadas en este proyecto son:
- **Python**: Para la lógica de procesamiento y análisis de datos.
- **PostgreSQL**: Para el almacenamiento de datos transformados y la gestión de consultas.
- **Pandas**: Para manipulación de datos.
- **PyArrow**: Para trabajar con datos en formato Apache Arrow y Delta Lake.
- **SQLAlchemy**: Para la interacción con bases de datos SQL (como PostgreSQL).


## Arquitectura
El proyecto sigue una arquitectura de tres capas (bronce, plata y oro) para el procesamiento de datos:

1. **Capa Bronce (Raw Layer)**:
   - En esta capa, los datos crudos se extraen de la API de criptomonedas en tiempo real. Los datos se almacenan en un formato Delta Lake utilizando **PyArrow**, lo que permite manejar grandes volúmenes de datos de forma eficiente y realizar procesamiento en el futuro.
   
2. **Capa Plata (Staging Layer)**:
   - Una vez que los datos se almacenan en la capa bronce, se leen desde Delta Lake para realizar transformaciones. 
   
3. **Capa Oro (Analytics Layer)**:
   - "QUEDA POR IMPLEMENTAR PROXIMAMENTE"


## Fuentes de Datos
API pública(https://site.financialmodelingprep.com/)

## Instalación
1. Como primer paso abrir la terminal y crear un entorno virtual
   python -m venv .venv

2. Activar entorno 
    En windows
    .venv\Scripts\activate

3. Instalar dependencias
    pip install -r requirements.txt

## Uso
Ejemplo de uso:

# Parámetros de la API
endpoint1 = 'historical-chart'  # Endpoint para obtener los datos históricos
timeframe = "1hour"  # Periodo de tiempo (por ejemplo: "1hour", "1day")
symbol = "ALIENUSD"  # Símbolo de la criptomoneda (por ejemplo: "BTCUSD", "ETHUSD")

# Parámetros para la consulta
params = {
    "from": "fecha desde la cual obtener los datos",  # Fecha de inicio en formato 'YYYY-MM-DD HH:MM:SS'
    "to": "fecha hasta la cual obtener los datos",  # Fecha de fin en formato 'YYYY-MM-DD HH:MM:SS'
    "apikey": "tu API key de la API"  # Tu clave de acceso a la API
}


