import polars as pl
import os
from pathlib import Path
from typing import Any, Dict, Generator, Union
from src.pipeline_engine.NodesEngine import BaseNode

class CSVReaderNode(BaseNode):
    """
    CSVReaderNode (Polars) permite leer archivos CSV de forma eficiente,
    con soporte para múltiples archivos, lectura en chunks y exclusión de ficheros.

    Configuración esperada:
    -----------------------
    - file_paths: lista de rutas a archivos CSV
    - folder_path: ruta a un directorio que contenga CSVs
    - excluir_files: lista de archivos a excluir
    - sep: separador del CSV (por defecto ',')
    - usecols: lista de columnas a leer
    - usar_chunk: bool, indica si se usa lectura por partes
    - chunksize: número de filas por chunk
    """

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(
        self, 
        data: Any = None,
    ) -> Union[pl.DataFrame, Generator[pl.DataFrame, None, None]]:
        file_paths = self.config.get("file_paths", [])
        usecols = self.config.get("usecols", None)
        sep = self.config.get("separadores", ",")
        lazy_mode = self.config.get("lazy_mode", False)
        chunk_mode = self.config.get("chunk_mode", False)
        part_chunk = self.config.get("part_chunk", 0)
        chunksize = self.config.get("chunksize", 500_000)
        folder_path = self.config.get("folder_path", None)
        excluir_files = self.config.get("excluir_files", [])
        salida = self.config.get("salida", "data")

        if not file_paths and not folder_path:
            raise ValueError(f"[{self.name}] Debes definir 'file_paths' o 'folder_path' en config.")

        # Leer desde folder_path si aplica
        if folder_path:
            folder_path = Path(folder_path).resolve()
            file_paths = sorted(
                [f for f in folder_path.iterdir() if f.is_file() and f.suffix == ".csv"]
            )

            # Excluir archivos no deseados
            if excluir_files:
                file_paths = [f for f in file_paths if f.name not in excluir_files]

        # Asegurar lista de files
        if isinstance(file_paths, str):
            file_paths = [file_paths]

        try:
            for file_path in file_paths:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"[{self.name}] No se encontró {file_path}")

                if self.logger:
                    self.logger.info(f"[{self.name}] Procesando archivo: {file_path}")

                # Lectura eficiente con Polars
                if lazy_mode:
                    # usa `scan_csv` (lazy) con streaming
                    df_lazy = pl.scan_csv(file_path, has_header=True, separator=sep)
                    if self.logger:
                        self.logger.debug(f"[{self.name}] Lectura completada de streaming iniciada.")
                    #return df_lazy
                    return {salida: df_lazy} 

                if chunk_mode: 
                    skip_rows = part_chunk * chunksize
                    columns = pl.read_csv(file_path, has_header=True, n_rows=0).columns
                    df = pl.read_csv(
                        file_path,
                        separator=sep,
                        columns=usecols,
                        has_header=True if part_chunk == 0 else False,
                        skip_rows= skip_rows + 1 if part_chunk > 0 else 0,  # saltar header si no es el primer chunk
                        n_rows=chunksize,
                        new_columns=columns
                    )
                    if self.logger:
                        self.logger.debug(f"[{self.name}] Lectura completada: {df.shape}")
                    #return df
                    return {salida: df} 

                else:
                    df = pl.read_csv(file_path, separator=sep, columns=usecols, low_memory=True)
                    if self.logger:
                        self.logger.debug(f"[{self.name}] Lectura completada: {df.shape}")
                    #return df
                    return {salida: df} 

        except Exception as e:
            if self.logger:
                self.logger.exception(f"[{self.name}] Error leyendo archivo CSV: {e}")
            raise RuntimeError(f"[{self.name}] [Error] leyendo archivo CSV: {e}")


class CSVWriterNode(BaseNode):
    """
    CSVWriterNode (Polars) escribe DataFrames o listas de diccionarios a CSV.
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Any):
        file_path = self.config.get("file_path")
        sep = self.config.get("sep", ",")
        mode = self.config.get("mode", "w")
        header = self.config.get("header", True)
        
        data = data["data"]
        
        if not file_path:
            raise ValueError(f"[{self.name}] Falta 'file_path' en configuración")

        if not file_path.endswith(".csv"):
            file_path += ".csv"

        try:
            if isinstance(data, list):
                # Lista de diccionarios
                df = pl.DataFrame(data)
                df.write_csv(file_path, separator=sep, include_header=header)
            
            elif isinstance(data, dict):
                df = pl.DataFrame([data])
                df.write_csv(file_path, separator=sep, include_header=not os.path.exists(file_path))

            elif isinstance(data, pl.DataFrame):
                data.write_csv(file_path, separator=sep, include_header=header)
            
            elif hasattr(data, "__iter__") and not isinstance(data, (str, bytes, pl.DataFrame)):
                # Generador de DataFrames o diccionarios
                for i, batch in enumerate(data):
                    if isinstance(batch, dict):
                        df = pl.DataFrame([batch])
                    elif isinstance(batch, pl.DataFrame):
                        df = batch
                    else:
                        raise TypeError(f"[{self.name}] Tipo no soportado: {type(batch)}")

                    write_mode = "wb" if (mode == "w" and i == 0) else "ab"
                    with open(file_path, write_mode) as f:
                        df.write_csv(f, separator=sep, include_header=(header and i == 0))
            else:
                raise TypeError(f"[{self.name}] Tipo de entrada no soportado: {type(data)}")

            if self.logger:
                self.logger.info(f"[{self.name}] Archivo CSV escrito exitosamente en {file_path}")

            return {"output_path": file_path}

        except Exception as e:
            if self.logger:
                self.logger.exception(f"[{self.name}] Error al escribir CSV: {e}")
            raise RuntimeError(f"[{self.name}] [Error] escribiendo archivo CSV: {e}")
