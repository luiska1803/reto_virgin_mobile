import os
from pathlib import Path
from typing import Any, Dict, Generator, Union
import polars as pl
from src.pipeline_engine.NodesEngine import BaseNode  # Ajusta el import según tu proyecto


class ParquetReaderNode(BaseNode):
    """
    ParquetReaderNode (Polars) permite leer archivos Parquet de forma eficiente,
    con soporte para múltiples archivos, lectura en streaming (lazy) y exclusión de ficheros.

    Configuración esperada:
    -----------------------
    - file_paths: lista de rutas a archivos Parquet
    - folder_path: ruta a un directorio que contenga Parquets
    - excluir_files: lista de archivos a excluir
    - usecols: lista de columnas a leer
    - usar_streaming: bool, indica si se usa lectura lazy (streaming)
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
        usar_streaming = self.config.get("usar_streaming", False)
        folder_path = self.config.get("folder_path", None)
        excluir_files = self.config.get("excluir_files", [])
        chunk_mode = self.config.get("chunk_mode", False)
        part_chunk = self.config.get("part_chunk", 0)
        chunksize = self.config.get("chunksize", 500_000)
        salida = self.config.get("salida", "data")

        if not file_paths and not folder_path:
            raise ValueError(f"[{self.name}] Debes definir 'file_paths' o 'folder_path' en config.")

        # Si se define folder_path, listar los archivos parquet
        if folder_path:
            folder_path = Path(folder_path).resolve()
            file_paths = sorted(
                [f for f in folder_path.iterdir() if f.is_file() and f.suffix == ".parquet"]
            )

            # Excluir archivos especificados
            if excluir_files:
                file_paths = [f for f in file_paths if f.name not in excluir_files]

        # Asegurar que file_paths sea una lista
        if isinstance(file_paths, str):
            file_paths = [file_paths]

        try:
            for file_path in file_paths:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"[{self.name}] No se encontró {file_path}")

                if self.logger:
                    self.logger.info(f"[{self.name}] Procesando archivo: {file_path}")

                # Lectura con Polars
                if usar_streaming:
                    # Lazy (streaming): no carga todo el archivo a memoria
                    df_lazy = pl.scan_parquet(file_path)
                    df = df_lazy.collect(streaming=True)
                    if self.logger:
                        self.logger.debug(f"[{self.name}] Lectura en streaming iniciada.")

                elif chunk_mode:
                    df_lazy = pl.scan_parquet(file_path)
                    df = (
                        df_lazy.slice(part_chunk * chunksize, chunksize).collect()
                    )
                    if self.logger:
                        self.logger.debug(f"[{self.name}] Lectura completada del chunk {part_chunk} completada")
                        
                else:
                    # Lectura directa a memoria
                    df = pl.read_parquet(file_path, columns=usecols)

                if self.logger:
                    self.logger.debug(f"[{self.name}] Lectura completada: {df.shape}")

                return {salida: df} 

        except Exception as e:
            if self.logger:
                self.logger.exception(f"[{self.name}] Error leyendo archivo Parquet: {e}")
            raise RuntimeError(f"[{self.name}] [Error] leyendo archivo Parquet: {e}")



class ParquetWriterNode(BaseNode):
    """
    ParquetWriterNode (Polars) escribe DataFrames o listas de diccionarios a archivos Parquet.
    Soporta modos de escritura, compresión y escritura incremental por lotes (chunks).
    
    Configuración esperada:
    -----------------------
    - file_path: ruta de salida del archivo Parquet
    - mode: "w" (sobrescribir) o "a" (adjuntar, si aplica por lotes)
    - compression: tipo de compresión ("snappy", "zstd", "gzip", etc.)
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Any):
        file_path = self.config.get("file_path")
        mode = self.config.get("mode", "w")
        compression = self.config.get("compression", "snappy")
        
        data = data["data"]

        if not file_path:
            raise ValueError(f"[{self.name}] Falta 'file_path' en configuración")

        if not file_path.endswith(".parquet"):
            file_path += ".parquet"

        try:
            # --- Si el input es una lista de diccionarios ---
            if isinstance(data, list):
                df = pl.DataFrame(data)
                df.write_parquet(file_path, compression=compression)

            # --- Si el input es un solo diccionario ---
            elif isinstance(data, dict):
                df = pl.DataFrame([data])
                df.write_parquet(file_path, compression=compression)

            # --- Si el input es un DataFrame de Polars ---
            elif isinstance(data, pl.DataFrame):
                if mode == "w" or not os.path.exists(file_path):
                    data.write_parquet(file_path, compression=compression)
                else:
                    # Append manual: leer existente y concatenar
                    df_existing = pl.read_parquet(file_path)
                    df_concat = pl.concat([df_existing, data])
                    df_concat.write_parquet(file_path, compression=compression)
            
            elif isinstance(data, pl.LazyFrame):
                # Ejecuta en modo streaming y eficiente
                data.sink_parquet(file_path)

            # --- Si es un generador o iterable de DataFrames / dicts ---
            elif hasattr(data, "__iter__") and not isinstance(data, (str, bytes, pl.DataFrame)):
                first = True
                for batch in data:
                    if isinstance(batch, dict):
                        df = pl.DataFrame([batch])
                    elif isinstance(batch, pl.DataFrame):
                        df = batch
                    else:
                        raise TypeError(f"[{self.name}] Tipo no soportado: {type(batch)}")

                    if mode == "w" and first:
                        df.write_parquet(file_path, compression=compression)
                        first = False
                    else:
                        df_existing = pl.read_parquet(file_path)
                        df_concat = pl.concat([df_existing, df])
                        df_concat.write_parquet(file_path, compression=compression)
            else:
                raise TypeError(f"[{self.name}] Tipo de entrada no soportado: {type(data)}")

            if self.logger:
                self.logger.info(f"[{self.name}] Archivo Parquet escrito exitosamente en {file_path}")

            return {"output_path": file_path}

        except Exception as e:
            if self.logger:
                self.logger.exception(f"[{self.name}] Error al escribir Parquet: {e}")
            raise RuntimeError(f"[{self.name}] [Error] escribiendo archivo Parquet: {e}")