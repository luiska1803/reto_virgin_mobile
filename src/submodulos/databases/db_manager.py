import os
from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

class DatabaseManager:
    """
    Esta clase es para la gestión de la conectividad y operación con las 
    bases de datos. Soportando SQLite, PostgreSQL y MySQL con ayuda
    de una connection pool, parametrizada con queries.

    Attributes:
        db_type (str): The type of database ('sqlite', 'postgresql', 'mysql').
        engine: SQLAlchemy engine for connection management.
        logger: Logger instance for debugging and monitoring.
    """

    def __init__(
        self,
        config
    ) -> None:
        """
        Inicializa el DatabaseManager.

        Args:
            db_type: Tipo de Base de datos ('sqlite', 'postgresql', 'mysql').
            host: Database host (No se requiere para SQLite).
            port: Database port (No se requiere para SQLite).
            database: Nombre de la base de datos o file path para SQLite.
            user: Database user (No se requiere para SQLite).
            password: Database password (No se requiere para SQLite).

        Raises:
            ValueError: Si db_type no es soportada.
        """
        
        user = os.getenv("DB_USER", "")
        password = os.getenv("DB_PASSWORD", "")
        port = int(os.getenv("DB_PORT", ""))
        database = os.getenv("DB_NAME", "")
        host = os.getenv("DB_HOST", "")
        self.config = config
        self.db_type = self.config.get("db_type", "").lower()
        url = self._build_url_conn(host, port, database, user, password)
        self.engine = create_engine(url, echo=False)
        self.logger = None

    def _build_url_conn(
        self,
        host: Optional[str],
        port: Optional[int],
        database: str,
        user: Optional[str],
        password: Optional[str]
    ) -> str:
        """ 
            Construcción de la URL para el tipo de base de datos que se requiera.
        """
        
        if self.db_type == 'sqlite':
            return f'sqlite:///{database}'
        elif self.db_type == 'postgresql':
            if not all([host, port, user, password]):
                raise ValueError("Host, port, user, and password required for PostgreSQL")
            return f'postgresql://{user}:{password}@{host}:{port}/{database}'
        elif self.db_type == 'mysql':
            if not all([host, port, user, password]):
                raise ValueError("Host, port, user, and password required for MySQL")
            return f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
        else:
            raise ValueError(f"Tipo de Base de datos {self.db_type} no esta soportado para trabajarla. ")

    def bulk_insert(self, table: str, data: List[Dict[str, Any]]):
        """
        Realiza el bulk de un datafrema de pandas hacia una tabla en especifica

        Args:
            table: nombre de la tabla donde se realizara el bulk.
            data: dataframe de pandas. 
        """
        data.to_sql(table, self.engine, if_exists="append", index=False)

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Ejecuta un SELECT query y retorna el resultado. 

        Args:
            query: SQL query string.
            params: Diccionario de parametros para la parametrización del query.

        Returns:
            Lista de dictionarios representados por rows, or None si no se tiene resultados.

        Raises:
            SQLAlchemyError: On query execution failure.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                if result.returns_rows:
                    return [dict(row._mapping) for row in result]
                return None
        except SQLAlchemyError as e:
            self.logger.error(f"Error de ejecución del Query: {e}")
            raise

    def select(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Ejecuta un SELECT query.

        Args:
            query: SQL SELECT query.
            params: Parameters for the query.

        Returns:
            Lista de resultados rows como diccionarios.
        """
        result = self.execute_query(query, params)
        return result if result else []

    def execute_dml(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Ejecuta INSERT, UPDATE, DELETE queries con el soporte transaccional.

        Args:
            query: SQL query string.
            params: Diccionario de parametros para la parametrización del query.

        Returns:
            Numero de filas afectadas.

        Raises:
            SQLAlchemyError: On execution failure (transaction rolled back).
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(text(query), params or {})
                    return result.rowcount
        except SQLAlchemyError as e:
            self.logger.error(f"DML execution failed: {e}")
            raise

    def insert(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Ejecuta un INSERT query.

        Args:
            query: SQL INSERT query.
            params: Parametros de la query.

        Returns:
            Numero de filas afectadas.
        """
        return self.execute_dml(query, params)

    def update(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Ejecuta un UPDATE query.

        Args:
            query: SQL UPDATE query.
            params: Parametros de la query.

        Returns:
            Numero de filas afectadas.
        """
        return self.execute_dml(query, params)

    def delete(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Ejecuta un DELETE query.

        Args:
            query: SQL DELETE query.
            params: Parametros de la query.

        Returns:
            Numero de filas afectadas.
        """
        return self.execute_dml(query, params)