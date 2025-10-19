import time
import requests
import polars as pl
from typing import Any, Dict, Union, Generator
from src.pipeline_engine.NodesEngine import BaseNode

class APIReaderNode(BaseNode):
    """
    APIReader es un nodo que permite leer API REST, a partir de parametros otorgados
    devolviendo un pd.DataFrame con los datos leídos.

    Atributos:
    ----------
    input_type : None
        No requiere datos de entrada.

    output_type : pd.DataFrame
        Devuelve un DataFrame de pandas con los datos de la API leído.

    Configuración esperada en YAML:
    -------------------------------
    - api_url: url de la APi que se leera (obligatorio)
    - timeout: timeout de la api (por defecto 5 segundos)
    - params: Parametros de lectura de la API (Opcional: por defecto en blanco)
    - selected: Lista de campos para seleccion de la API (Opcional, por defecto en blanco)

    Ejemplo de uso en YAML:
    -----------------------
    - name: LectorAPI
      type: APIReader
      parameters:
        api_url: https://api.com
        timeout: 5
        params: 
            parametro_1: parametro_1
            parametro_2: parametro_2
        selected: 
            - param1
            - param2
            - paramN
    """
    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None
   
    def run(
        self, 
        data: Any = None,
    ) -> Union[pl.DataFrame, Generator[pl.DataFrame, None, None]]:
        """
            Funcion para recibir informacion a partir de la API otorgada por
            la URL presente en el YAML.
        """
        api_url = self.config.get("api_url", None)
        timeout = self.config.get("timeout", 5)
        params = self.config.get("params", {})
        selected = self.config.get("selected", [])
        salida = self.config.get("salida", "data")
        

        try:
            self.logger.info(f"Extrayendo datos desde la API: {api_url}")

            response = requests.get(
                api_url, 
                params=params, 
                timeout=timeout
            )

            if response.status_code == 429:  # Estado para "too Many Request"
                retry_after = int(response.headers.get("Retry-After", 1))
                time.sleep(retry_after)
                self.logger.info(f"[{self.name}] Reintentando conexión a la API {api_url}")
                return APIReaderNode.run()

            api_data = response.json()

            if api_data:
                self.logger.info(f"[{self.name}] Conexión a la API {api_url} exitosa.")
                
                if selected: 
                    dicc = {key: api_data[0][key] for key in selected} 
                    self.logger.info(f"[{self.name}] Recolectando datos {selected}")
                    return {salida: pl.DataFrame([dicc])}
                else:
                    self.logger.info(f"[{self.name}] Recolectando todos los datos de la API.")
                    return {salida: pl.DataFrame(api_data)}
        
        except Exception as e:
            self.logger.error(f"Error al extraer datos: {e}")
            raise e
        