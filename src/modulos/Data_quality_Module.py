import polars as pl
import datetime
from typing import Any, Generator, Union, Dict
from src.pipeline_engine.NodesEngine import BaseNode

class DataQualityNode(BaseNode):
    """
    DataQualityNode valida la calidad de los datos según reglas configurables.

    Parámetros YAML esperados:
    --------------------------
    - reglas : List[Dict[str, str]]
        Lista de reglas de validación. Cada regla debe tener:
          - columna: nombre de la columna sobre la que se aplica
          - regla: tipo de validación a aplicar
            Reglas soportadas:
              * no_nulos
              * valores_positivos
              * formato_email
              * formato_fecha
              * no_duplicados
              * edad_mayor_18
              * nulos_mayor_95

    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None
        

    def run(
        self, 
        data: Any
    ) -> Union[pl.DataFrame, Generator[pl.DataFrame, None, None]]:
        resultados = []
        data = data["data"]
        reglas = self.config.get("reglas", [])
        salida = self.config.get("salida", "data")
        total = data.height

        for regla_conf in reglas:
            columna = regla_conf.get("columna")
            regla = regla_conf.get("regla")

            if columna not in data.columns:
                resultados.append({
                    "columna": columna,
                    "regla": regla,
                    "estado": "FALLA",
                    "detalle": "Columna no encontrada"
                })
                continue

            if regla == "no_nulos":
                faltantes = data[columna].null_count()
                estado = "OK" if faltantes == 0 else "FALLA"
                detalle = f"{faltantes} valores nulos"

            elif regla == "valores_positivos":
                negativos = data.filter(pl.col(columna) < 0).height
                estado = "OK" if negativos == 0 else "FALLA"
                detalle = f"{negativos} valores negativos"

            elif regla == "formato_email":
                regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
                invalidos = data.filter(~pl.col(columna).cast(pl.Utf8).str.contains(regex)).height
                estado = "OK" if invalidos == 0 else "FALLA"
                detalle = f"{invalidos} correos inválidos"
            
            elif regla == "no_duplicados":
                unicos = data.select(pl.col(columna).n_unique()).item()
                duplicados = total - unicos
                estado = "OK" if duplicados == 0 else "FALLA"
                detalle = f"{duplicados} duplicados encontrados"
            
            elif regla == "validacion_boolean":
                try:
                    serie = data[columna]
                    dtype = serie.dtype

                    if dtype == pl.Boolean:
                        estado = "OK"
                        detalle = f"Columna '{columna}' es tipo booleano."
                    
                    else:
                        estado = "FALLA"
                        detalle = "Valores no corresponden a booleanos"
                
                except Exception as e:
                    estado = "FALLA"
                    detalle = f"Error al validar columna booleana: {e}"

            elif regla == "formato_fecha":
                try:
                    serie = data[columna]

                    if serie.dtype in [pl.Datetime, pl.Date]:
                        estado = "OK"
                        detalle = f"Columna '{columna}' ya es tipo {serie.dtype}"
                    
                    # para fechas con formato timestamp - UNIX
                    elif serie.dtype in [pl.Int64, pl.UInt64, pl.Float64]:
                        data = data.with_columns(pl.from_epoch(pl.col(columna)).alias(columna))
                        estado = "OK"
                        detalle = f"Columna '{columna}' convertida desde timestamp numérico"
                    
                    elif serie.dtype == pl.Utf8:
                        data = data.with_columns(pl.col(columna).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False))
                        estado = "OK"
                        detalle = f"Columna '{columna}' convertida desde string"
                    
                    else:
                        estado = "FALLA"
                        detalle = f"Tipo no compatible para fecha: {serie.dtype}"
                
                except Exception as e:
                    estado = "FALLA"
                    detalle = f"Error al validar formato fecha: {e}"

            elif regla == "fecha_no_futura":
                hoy = datetime.now().date()
                futuras = data.filter(pl.col(columna).cast(pl.Datetime) > hoy).height
                estado = "OK" if futuras == 0 else "FALLA"
                detalle = f"{futuras} registros con fecha futura"

            elif regla == "edad_mayor_18":
                menores = data.filter(pl.col(columna) < 18).height
                estado = "OK" if menores == 0 else "FALLA"
                detalle = f"{menores} registros con edad menor a 18"
            
            elif regla == "nulos_mayor_95":
                nulos = data[columna].null_count()
                porcentaje_nulos = (nulos / total) * 100 if total > 0 else 0
                estado = "FALLA" if porcentaje_nulos > 95 else "OK"
                detalle = f"{porcentaje_nulos:.2f}% de nulos en la columna"
            

            else:
                estado = "FALLA"
                detalle = f"Regla '{regla}' no reconocida"

            resultados.append({
                "columna": columna,
                "regla": regla,
                "estado": estado,
                "detalle": detalle
            })

        resultados.append({
                "columna": "Total Registros",
                "regla": "",
                "estado": "",
                "detalle": f"{total} de registros"
            })

        return {salida: pl.DataFrame(resultados)} 