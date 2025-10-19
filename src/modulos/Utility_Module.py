import polars as pl
from typing import Dict, Any

from src.pipeline_engine.NodesEngine import BaseNode

class FilterNode(BaseNode):
    """
    FilterNode permite aplicar una condición lógica sobre un DataFrame de Polars.
    La condición debe ser una expresión en formato Polars, usando su sintaxis
    (por ejemplo, `pl.col("edad") > 30 & (pl.col("pais") == "Colombia")`).

    Parámetros YAML esperados:
    --------------------------
    - condition : str
        Expresión lógica en formato Polars.
        Ejemplo: 'pl.col("edad") > 30 & (pl.col("pais") == "Colombia")'

    Ejemplo de uso en YAML:
    -----------------------
    - name: FiltrarAdultosColombianos
      type: FilterNode
      parameters:
        config:
            condition: 'pl.col("edad") > 30 & (pl.col("pais") == "Colombia")'
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Any):
        """
        Aplica un filtro dinámico sobre un DataFrame de Polars.
        Devuelve un nuevo DataFrame filtrado.
        """
        data = data["data"]
        condition_str = self.config.get("condition")
        salida = self.config.get("salida", "data")

        if not condition_str:
            raise ValueError(f"[{self.name}] Falta 'condition' en config.")

        if not isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError(f"[{self.name}] Se esperaba un DataFrame o LazyFrame de Polars, no {type(data)}.")

        try:
            # Convertimos el string a una expresión evaluable de Polars
            condition_expr = eval(condition_str, {"pl": pl})
            if not isinstance(condition_expr, pl.Expr):
                raise ValueError(f"[{self.name}] La condición no generó una expresión válida de Polars.")

            # Aplicamos el filtro
            self.logger and self.logger.debug(f"[{self.name}] Aplicando filtro: {condition_str}")
            filtered_df = data.filter(condition_expr)

            return {salida: filtered_df} 

        except Exception as e:
            msg = f"[{self.name}] Error evaluando la condición '{condition_str}': {e}"
            if self.logger:
                self.logger.error(msg)
            raise RuntimeError(msg)

class DropDuplicateNode(BaseNode):
    """
    DropDuplicateNode elimina filas duplicadas de un DataFrame de Polars.

    Parámetros YAML esperados:
    --------------------------
    - columnas : List (Opcional)
        Lista de columnas sobre las cuales se eliminarán duplicados.
        Si no se especifica, se eliminan duplicados considerando todas las columnas.

    Ejemplo de uso en YAML:
    -----------------------
    - name: EliminarDuplicados
      type: DropDuplicateNode
      parameters:
        config:
            columnas:
                - columna_1
                - columna_2
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Any):
        """
        Elimina filas duplicadas de un DataFrame de Polars.

        Args:
            data (pl.DataFrame): DataFrame de entrada.
        Returns:
            pl.DataFrame: DataFrame sin duplicados.
        """
        data = data["data"]
        columnas = self.config.get("columnas", None)
        salida = self.config.get("salida", "data")

        if not isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError(f"[{self.name}] Se esperaba un DataFrame o LazyFrame de Polars, no {type(data)}.")

        try:
            # Si no se especifican columnas, se usan todas
            subset = columnas if columnas else None

            if self.logger:
                if subset:
                    self.logger.info(f"[{self.name}] Eliminando duplicados sobre columnas: {subset}")
                else:
                    self.logger.info(f"[{self.name}] Eliminando duplicados considerando todas las columnas")

            # Elimina duplicados
            df_sin_duplicados = data.unique(subset=subset, keep="first")

            return {salida: df_sin_duplicados} 

        except Exception as e:
            msg = f"[{self.name}] Error eliminando duplicados: {e}"
            if self.logger:
                self.logger.error(msg)
            raise RuntimeError(msg)

class DropNullteNode(BaseNode):
    """
    DropNullteNode (Polars) elimina o reemplaza valores nulos en un DataFrame.

    Parámetros YAML esperados:
    --------------------------
    - columnas : List (Opcional)
        Columnas sobre las cuales se eliminarán filas con valores nulos.
        Si no se especifica, se evaluarán todas las columnas.

    - fillna : Dict (Opcional)
        Diccionario de columnas y valores por defecto para reemplazar nulos.
        Ejemplo:
            fillna:
                columna_1: "valor_default"
                columna_2: 0

    Ejemplo de uso en YAML:
    -----------------------
    - name: LimpiarNulos
      type: DropNullteNode
      parameters:
        config:
            columnas:
                - columna_1
                - columna_2
            fillna:
                columna_1: "desconocido"
                columna_2: 0
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Any):
        data = data["data"]
        columnas = self.config.get("columnas", None)
        drop_all = self.config.get("drop_all", False) 
        fillna = self.config.get("fillna", {})
        salida = self.config.get("salida", "data")

        if not isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError(f"[{self.name}] Se esperaba un DataFrame o LazyFrame de Polars, no {type(data)}.")

        try:

            # Eliminar filas con nulos en las columnas especificadas
            if columnas:
                if self.logger:
                    self.logger.info(f"[{self.name}] Eliminando filas con nulos en columnas: {columnas}")
                data = data.drop_nulls(subset=columnas)
            
            if drop_all:
                if self.logger:
                    self.logger.info(f"[{self.name}] Eliminando filas con nulos en cualquier columna")
                data = data.drop_nulls()
            
            # Reemplazar valores nulos según fillna
            if fillna:
                if self.logger:
                    self.logger.info(f"[{self.name}] Reemplazando valores nulos con fillna: {fillna}")
                for columna, valor in fillna.items():
                    data = data.with_columns(pl.col(columna).fill_null(valor))
            
            return {salida: data} 

        except Exception as e:
            msg = f"[{self.name}] Error procesando nulos: {e}"
            if self.logger:
                self.logger.error(msg)
            raise RuntimeError(msg)

class RenameColumnsNode(BaseNode):
    """
    RenameColumnsNode (Polars) permite cambiar el nombre de una o varias columnas
    de un DataFrame de Polars.

    Parámetros YAML esperados:
    --------------------------
    - rename_map : Dict[str, str] (Obligatorio)
        Diccionario con los mapeos de columnas: {columna_actual: columna_nueva}

    Ejemplo de uso en YAML:
    -----------------------
    - name: RenombrarColumnas
      type: RenameColumnsNode
      parameters:
        config:
            rename_map:
                old_col1: new_col1
                old_col2: new_col2
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Any) -> pl.DataFrame:
        data = data["data"]
        rename_map = self.config.get("rename_map", None)
        salida = self.config.get("salida", "data")

        if not rename_map:
            raise ValueError(f"[{self.name}] Falta 'rename_map' en configuración.")

        if not isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError(f"[{self.name}] Se esperaba un DataFrame o LazyFrame de Polars, no {type(data)}.")

        try:
            if self.logger:
                self.logger.info(f"[{self.name}] Renombrando columnas: {rename_map}")

            df = data.rename(rename_map)

            if self.logger:
                self.logger.debug(f"[{self.name}] Columnas finales: {df.columns}")

            return {salida: df}

        except Exception as e:
            msg = f"[{self.name}] Error renombrando columnas: {e}"
            if self.logger:
                self.logger.error(msg)
            raise RuntimeError(msg)


class CastColumnsNode(BaseNode):
    """
        Castea columnas según el tipo especificado en YAML.
        Optimizado para reducir consumo de memoria.
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def _normalize_boolean(self, expr: pl.Expr) -> pl.Expr:
        """
        Normaliza valores booleanos representados como texto o números.
        Ej: "t", "true", "v", "1", "si" → True
            "f", "false", "0", "no" → False
        """
        verdaderos = ["t", "true", "1", "y", "yes", "v", "verdadero", "si"]
        falsos = ["f", "false", "0", "n", "no", "falso"]

        return (
            expr.cast(pl.Utf8)
            .str.to_lowercase()
            .map_elements(lambda x: True if x in verdaderos else False if x in falsos else None, return_dtype=pl.Boolean)
        )

    def run(self, data: Any) -> pl.DataFrame:

        salida = self.config.get("salida", "data")
        data = data["data"]

        if not isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError(f"[{self.name}] Se esperaba un DataFrame o LazyFrame de Polars, no {type(data)}.")

        cast_map = self.config.get("cast_map", {})
        df = data

        for col, dtype in cast_map.items():
            if col not in df.columns:
                if self.logger:
                    self.logger.warning(f"[{self.name}] Columna '{col}' no encontrada, se omite.")
                continue

            try:
                if dtype == "timestamp":
                    df = df.with_columns(pl.col(col).str.to_datetime(strict=False).alias(col))
                elif dtype == "int":
                    df = df.with_columns(pl.col(col).cast(pl.Int64).alias(col))
                elif dtype == "float":
                    df = df.with_columns(pl.col(col).cast(pl.Float64).alias(col))
                elif dtype == "str":
                    df = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                elif dtype == "category": 
                    df = df.with_columns(pl.col(col).cast(pl.Categorical).alias(col))
                elif dtype == "bool":
                    df = df.with_columns(self._normalize_boolean(pl.col(col)).alias(col))
                else:
                    if self.logger:
                        self.logger.warning(f"[{self.name}] Tipo '{dtype}' no soportado en columna '{col}'.")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"[{self.name}] Error al castear '{col}' a {dtype}: {e}")

        return {salida: df} 


class DropColumnsNode(BaseNode):
    """
    DropColumnsNode (Polars) elimina columnas específicas de un DataFrame.

    Parámetros YAML esperados:
    --------------------------
    - columnas : List
        Columnas que se desean eliminar del DataFrame o LazyFrame.

    Ejemplo de uso en YAML:
    -----------------------
    - name: EliminarColumnas
      type: DropColumnsNode
      parameters:
        config:
            columnas:
                - columna_1
                - columna_2
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Any):
        data = data["data"]
        columnas = self.config.get("columnas", [])
        salida = self.config.get("salida", "data")

        if not isinstance(data, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError(f"[{self.name}] Se esperaba un DataFrame o LazyFrame de Polars, no {type(data)}.")

        try:
            if columnas:
                if self.logger:
                    self.logger.info(f"[{self.name}] Eliminando columnas: {columnas}")
                data = data.drop(columnas)
            else:
                if self.logger:
                    self.logger.warning(f"[{self.name}] No se especificaron columnas para eliminar, se retorna el DataFrame sin cambios.")

            return {salida: data}

        except Exception as e:
            msg = f"[{self.name}] Error eliminando columnas: {e}"
            if self.logger:
                self.logger.error(msg)
            raise RuntimeError(msg)
