import polars as pl
from typing import Dict, Any

from src.pipeline_engine.NodesEngine import BaseNode
from src.submodulos.databases.db_manager import DatabaseManager


class DatabaseNode(BaseNode):
    """
    DatabaseNode (compatible con Polars)

    Este nodo permite insertar o consultar datos desde una base de datos
    usando DatabaseManager. Admite tanto `pl.DataFrame` como listas de diccionarios.

    Parámetros YAML esperados:
    --------------------------
    - db_config: dict con los parámetros de conexión (db_type, host, etc.)
    - table: str, nombre de la tabla destino
    - operation: str, 'insert' o 'select' (default 'insert')
    - query: str (solo para select)
    - params: dict (parámetros opcionales para el query)
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None
        self.db_manager = None
        self.table = None

    def _insert_row(self, row: Dict[str, Any]) -> None:
        """Inserta una fila individual (dict)."""
        columns = list(row.keys())
        placeholders = ', '.join(f':{k}' for k in columns)
        query = f"INSERT INTO {self.table} ({', '.join(columns)}) VALUES ({placeholders})"
        self.db_manager.insert(query, row)

    def _insert_polars_df(self, df: pl.DataFrame):
        """Inserta un DataFrame completo de Polars."""
        if df.is_empty():
            if self.logger:
                self.logger.warning(f"[{self.name}] DataFrame vacío. No se insertan filas.")
            return

        records = df.to_dicts()
        for i, row in enumerate(records):
            try:
                self._insert_row(row)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"[{self.name}] Error insertando fila {i}: {e}")
                raise

        if self.logger:
            self.logger.info(f"[{self.name}] {len(records)} filas insertadas en {self.table}.")

    def run(self, data: Any = None):
        """Ejecuta el nodo (insert o select)."""
        self.db_manager = DatabaseManager(config=self.config)
        self.db_manager.logger = self.logger
        self.table = self.config.get("table")
        operation = self.config.get("operation", "insert").lower()

        data = data["data"]
        
        if self.logger:
            self.logger.info(f"[{self.name}] Operación: {operation}")
    
        # Operación de inserción
        if operation == "insert":
            if isinstance(data, pl.DataFrame):
                self._insert_polars_df(data)
            elif isinstance(data, list) and all(isinstance(row, dict) for row in data):
                for i, row in enumerate(data):
                    self._insert_row(row)
                    if self.logger:
                        self.logger.debug(f"[{self.name}] Fila {i} insertada.")
            else:
                if self.logger:
                    self.logger.warning(f"[{self.name}] Tipo de dato no soportado: {type(data)}")
                raise TypeError(f"Tipo de dato no soportado: {type(data)}")

        # Operación de lectura
        elif operation == "select":
            query = self.config.get("query")
            params = self.config.get("params", {})
            if not query:
                raise ValueError(f"[{self.name}] Se requiere un query para SELECT.")
            
            result = self.db_manager.select(query, params)
            
            # Si db_manager devuelve lista de dicts → convertir a Polars
            if isinstance(result, list) and result and isinstance(result[0], dict):
                result_df = pl.DataFrame(result)
            elif isinstance(result, pl.DataFrame):
                result_df = result
            else:
                result_df = pl.DataFrame()

            if self.logger:
                self.logger.info(f"[{self.name}] SELECT retornó {result_df.shape[0]} filas.")
            

            print(result_df)

            return result_df
