# Prueba Tecnica - Virgin Mobile - Ing. Datos 

El objetivo principal de esta prueba es evaluar la versatilidad, curiosidad y capacidad analítica, con el fin de trabajar con datos reales y transformarlos en información util para ayudar a la toma de decisiones. Como extra, para esta prueba se realiza un proceso de carga hacia una base de datos, la cual es a su vez creada a traves de docker-compose, lo cual simularia la carga hacia un ambiente de SQL real de una empresa. 

Este proyecto trata de un sistema modular de pipelines de datos Nodular basado en DAG (Directed Acyclic Graph) diseñado para procesar y transformar datos de campañas segun los datos proporcionados; para este caso se cuenta con datos de mensajes, campañas, clientes y holidays descargados desde Kagle a traves del siguiente link: (https://www.kaggle.com/datasets/mkechinov/direct-messaging/data). 

El Pipeline diseñado permite ejecutar flujos de trabajo complejos de manera paralela y eficiente, a su vez permite que se integren más Nodos, los cuales pueden ser desarrollados y alojados en la carpeta Modulos y el pipeline lo detectara automaticamente si se siguen los patrones de diseño de los modulos ya creados. 

## Características

- **Arquitectura Modular / Nodular**: Nodos reutilizables para diferentes operaciones (lectura/escritura de datos, transformaciones generales y hechas a la medida, integración con LLMs, integracion con Bases de datos)
- **Ejecución Paralela**: Motor de pipelines que ejecuta nodos concurrentemente usando ThreadPoolExecutor, el cual es un modulo estandar para la ejecución de tareas en modo paralelo. Se debe tener en cuenta cuantos hilos se maneja ya que si se esta trabajando en un ambiente local, el número de hilos dependera de los nucleos del CPU.
- **Configuración Declarativa**: Pipelines definidos en archivos YAML, los cuales representan los pasos a seguir en el pipeline, estos trabajos pueden ser sencillos desde la lectura de una API o un archivo CSV, hasta complejos como la integracion con modelos LLM.
- **Soporte Multi-Fuente**: El proyecto cuenta con Lectura/escritura de fuentes como CSV, Parquet, bases de datos PostgreSQL y APIs; si se requiere de una fuente extra, se puede desarrollar teniendo en cuenta el diseño de los demas Nodos.
- **Transformaciones Avanzadas**: El proyecto a su vez realiza procesos de limpieza de datos, enriquecimiento, análisis de rendimiento de campañas. Estos procesos se pueden observar en la carpeta de datos. 
- **Integración LLM**: Procesamiento de lenguaje natural para análisis de archivos segun el promt que se le indique.
- **Logging Estructurado**: El proyecto cuenta con un sistema de logging configurable con salida a archivos o consola, por su defecto este se encuentra apagado con el fin de no llenar el CLI con todo el flujo de trabajo, sin embargo se puede habilitar ejecutando el pipeline con --ver-cli, lo cual encendera el proceso de lectura en el CLI.
- **Validación de Tipos**: El proyecto realiza una verificación de compatibilidad de tipos entre nodos conectados, lo cual hace que se integre más facilmente a futuros nodos que se le puedan agregar. 

## Tecnologías

- **Python 3.12+**
- **Polars** - Procesamiento de datos de alto rendimiento
- **Pandas** - Manipulación de datos
- **SQLAlchemy** - ORM para bases de datos
- **PostgreSQL** - Base de datos principal
- **Docker** - Contenedorización de servicios
- **Cerberus** - Validación de esquemas YAML
- **LangChain** - Integración con modelos de lenguaje
- **Pytest** - Testing
- **Ruff** - Linting y formateo

## Instalación

### Prerrequisitos

- Python 3.12 o superior
- uv (gestor de paquetes)
- Docker (opcional, para servicios de base de datos)

**Nota:** uv es un gestor de paquetes (similar a pip), sin embargo, uv es un gestor de paquetes mucho más robusto y su velocidad supera por mucho los getores de paquetes normalmente utilizados como pip, poetry y pip-sync como se puede ver en el siguiente link (https://docs.astral.sh/uv/), uv se puede descargar siguiendo los pasos en el siguiente link: https://docs.astral.sh/uv/getting-started/installation/ 

### Instalación Local

1. Clonar el repositorio:
```bash
git clone https://github.com/luiska1803/reto_virgin_mobile.git
# Entras a la carpeta que se haya creado, ejemplo:
cd reto_virgin_mobile
```

2. Instalar las dependencias:
```bash
uv venv
uv sync
```

3. Configura las variables de entorno:
Copia el archivo `.env.example` a `.env` y configura las variables necesarias.

## Uso

### Ejecución de Pipelines

El sistema se ejecuta a través de la línea de comandos usando `main.py`:

```bash
# Ejecutar un pipeline específico
uv run main.py --yaml ./pipelines/limpieza/raw_api_data.yaml

# Validar un pipeline sin ejecutarlo
uv run main.py --yaml ./pipelines/limpieza/raw_api_data.yaml --validate-only

# Especificar nodo de entrada personalizado
uv run main.py --yaml ./pipelines/limpieza/raw_api_data.yaml --entry custom_node

# Salida de logs en consola
uv run main.py --yaml ./pipelines/limpieza/raw_api_data.yaml --ver-cli
```

### Proceso automatizado con comandos del Makefile

**Antes de ejecutar el pipeline se requiere que se descargue la data desde la fuente de kagle: https://www.kaggle.com/datasets/mkechinov/direct-messaging/data y se coloque esta data en la ruta de `./data/bronze_data/`, se puede usar el archivo de `descarga.py` si lo prefieren.**


```bash
# Levantar servicios Docker
make init_docker

# Limpiar servicios Docker
make clear_docker

# Ejecutar pipelines de limpieza
make clean

# Ejecutar transformaciones
make transformations

# Cargar datos a PostgreSQL
make carga_sql

# Cargar datos a CSV
make carga_csv

# Realizar validaciones de los dataframes generados
make validations

# Ejecutar tests
make tests

# Ejecutar linting
make lint
```

### Servicios Docker

Para usar la base de datos PostgreSQL y pgAdmin:

**Nota: Se requiere que tengas docker instalado para este paso.**

```bash
make init_docker
```

Esto iniciará:
- PostgreSQL en `localhost:5432`
- pgAdmin en `localhost:8080`



## Arquitectura

### Estructura del Proyecto

```
reto_virgin_mobile/
├── .github/                        # Carpeta interaccion con github actions
│   └── workflows/                  # Procesos CI/CD de github actions
├── config/                         # Configuraciones
│   ├── load_config.py              # Carga de variables de entorno
│   ├── logging_utils.py            # Utilidades de logging
│   ├── schema_pipeline/            # Esquemas de validación
│   ├── sql/                        # Esquemas de base de datos
|   └── envpaths.yaml               # paths de ejecución del proyecto
├── src/
│   ├── modulos/                    # Nodos de pipeline
│   │   ├── API_Module.py
│   │   ├── CSV_Module.py
│   │   ├── DB_Module.py
│   │   ├── LLM_Module.py
│   │   ├── Parquet_Module.py
│   │   ├── Transform_Module.py
│   │   └── Utility_Module.py
│   ├── submodulos/                 # Modulos creados para interaccion con los Nodos
│   │   ├── databases/          
│   │   └── llm/
│   └── pipeline_engine/            # Motor de ejecución
│       ├── PipelineEngine.py
│       ├── PipelineLoader.py
│       ├── NodesEngine.py
│       └── NodesRegistry.py
├── pipelines/                      # Definiciones de pipelines YAML
│   ├── carga/                      # Pipelines de carga
│   ├── limpieza/                   # Pipelines de limpieza
│   ├── transformacion/             # Pipelines de transformación
│   └── validaciones/               # Pipelines de validación sobre archivos generados
├── test/                           # Tests unitarios
├── scripts/                        # Scripts auxiliares
└── main.py                         # Punto de entrada CLI
```

### Motor de Pipelines

El `PipelineEngine` es el corazón del sistema:

- **Ejecución DAG**: Los pipelines se definen como grafos acíclicos dirigidos
- **Paralelismo**: Ejecución concurrente de nodos usando ThreadPoolExecutor
- **Buffering**: Manejo de múltiples inputs por nodo
- **Defer Output**: Soporte para operaciones asíncronas con finalización diferida

### Nodos Disponibles

#### Lectores
- `APIReaderNode`: Lectura desde APIs REST
- `CSVReaderNode`: Lectura de archivos CSV
- `ParquetReaderNode`: Lectura de archivos Parquet
- `DatabaseNode`: Lectura desde PostgreSQL

#### Escritores
- `CSVWriterNode`: Escritura a CSV
- `ParquetWriterNode`: Escritura a Parquet
- `DatabaseNode`: Escritura a PostgreSQL

#### Transformaciones
- `MergeDataNode`: Unión de datasets
- `HolidaysEnrichedNode`: Enriquecimiento de datos de holidays
- `clearMessagesNode`: Limpieza de mensajes
- `GetCampaignPerformanceNode`: Análisis de rendimiento de campañas

#### Utilidades
- `FilterNode`: Filtrado de datos
- `DropDuplicateNode`: Eliminación de duplicados
- `DropColumnsNode`: Eliminación de columnas
- `RenameColumnsNode`: Renombrado de columnas
- `CastColumnsNode`: Conversión de tipos

#### Validaciones:
- `DataQualityNode`: Validación de datos en las columnas de los dataframes

#### IA
- `LLMNode`: Integración con modelos de lenguaje (OpenAI, Bedrock)

### Definición de Pipeline

Los pipelines se definen en archivos YAML con la siguiente estructura:

**Ejemplo de yaml:**

```yaml
pipeline:
  name: pipeline_ejemplo
  entrypoint: nodo_inicial

  nodes:
    - name: nodo_inicial
      type: CSVReaderNode
      outputs: [nodo_transformacion]
      params:
        config:
          file_paths: ./data/input.csv

    - name: nodo_transformacion
      type: DropColumnsNode
      outputs: [nodo_final]
      params:
        config:
          columnas: [columna_a_eliminar]

    - name: nodo_final
      type: CSVWriterNode
      params:
        config:
          file_path: ./data/output.csv
```

## Desarrollo

### Ejecutar Tests

```bash
make tests
```

### Linting

```bash
make lint
```

### CI/CD

El proyecto incluye GitHub Actions para:
- Ejecución automática de tests en push/PR
- Validación de linting
- Verificación de sintaxis


### Proceso para agregar Nuevos Nodos

1. Crear una clase que herede de `BaseNode` de `src.pipeline_engine.NodesEngine` y alojarla en `src/modulos/`
2. Implementa el método `run(self, data)`
3. Define `input_type` y `output_type` si es necesario
4. El registro es automático a través de `NodesRegistry`
