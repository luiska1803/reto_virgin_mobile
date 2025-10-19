import polars as pl
from typing import Any, Dict
from src.pipeline_engine.NodesEngine import BaseNode

class DummyStartNode(BaseNode):
    """
        Nodo inicial que no hace procesamiento, solo dispara los siguientes.
        Este nodo funciona para cuando se requieren multiples entradas
    """
    def run(self, data=None):
        self.logger.info(f"[{self.name}] Nodo de inicio ejecutado. Disparando ramas de lectura.")
        # Devuelve algo para que el engine se pueda propagar a cada nodo
        return {"trigger": True}
    
class MergeDataNode(BaseNode):
    """
    
    Este nodo combina dos DataFrames de Polars utilizando la función `join`, 
    permitiendo realizar uniones tipo inner, left, outer, etc. según la configuración YAML.
    
    Configuración esperada:
    -----------------------
    - on_merge: columna común (opcional)
    - left_on: columna del primer DataFrame
    - right_on: columna del segundo DataFrame
    - how: tipo de join ("inner", "left", "outer", etc.)

    """
    required_inputs = ["data_1", "data_2"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        data_1 = data.get("data_1")
        data_2 = data.get("data_2")

        if not isinstance(data_1, pl.DataFrame) or not isinstance(data_2, pl.DataFrame):
            raise TypeError(f"[{self.name}] Ambos inputs deben ser DataFrames de Polars.")

        on_merge = self.config.get("on_merge")
        left_on = self.config.get("left_on")
        right_on = self.config.get("right_on")
        how = self.config.get("how", "inner")

        try:
            if on_merge:
                joined = data_1.join(data_2, on=on_merge, how=how)
            elif left_on and right_on:
                joined = data_1.join(data_2, left_on=left_on, right_on=right_on, how=how)
            else:
                raise ValueError(f"[{self.name}] Debes especificar 'on_merge' o ('left_on', 'right_on').")
        except Exception as e:
            raise RuntimeError(f"[{self.name}] Error al hacer join: {e}")

        return {"data": joined}

class HolidaysEnrichedNode(BaseNode):
    """
        Este nodo enriquece un DataFrame de fechas añadiendo columnas derivadas del 
        campo `date`, incluyendo día, mes, año y nombre del día de la semana.

        Parámetros YAML esperados:
        --------------------------
        (No requiere parámetros de configuración)

    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        data = data.get("data")

        if not isinstance(data, pl.DataFrame):
            raise TypeError(f"[{self.name}] Ambos inputs deben ser DataFrames de Polars.")

        data = data.with_columns([
            pl.col("date").dt.day().alias("day"),
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.year().alias("year")
        ])

        dias = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]

        data = data.with_columns([
            pl.col("date").dt.weekday().map_elements(
                lambda x: dias[int(x)] if x is not None and 0 <= int(x) <= 6 else None,
                return_dtype=pl.String
            ).alias("weekday")
        ])

        return {"data": data} 

class getHolidaysNode(BaseNode):
    """
        Este nodo compara una o más columnas de fecha con un listado de días festivos,
        y añade columnas booleanas indicando si la fecha corresponde a un día festivo.

        Parámetros YAML esperados:
        --------------------------
        - list_col_dates: list[str] (obligatorio)
            Lista de columnas de fecha que se desean comprobar.
        - col_holidays: str (obligatorio)
            Nombre de la columna que contiene los días festivos en el segundo DataFrame.

    """
    required_inputs = ["data_1", "data_2"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        data_1 = data.get("data_1")
        data_2 = data.get("data_2")

        if not isinstance(data_1, pl.DataFrame) or not isinstance(data_2, pl.DataFrame):
            raise TypeError(f"[{self.name}] Ambos inputs deben ser DataFrames de Polars.")

        list_col_dates = self.config.get("list_col_dates", [])
        col_holidays = self.config.get("col_holidays", "")

        data_2 = data_2.with_columns(
            pl.col(col_holidays).cast(pl.Date)
        )

        festivos_set = set(data_2[col_holidays].to_list())

        try:
            for columna in list_col_dates:
                data_1 = data_1.with_columns(pl.col(columna).cast(pl.Date).alias(f"{columna}_"))

                data_1 = data_1.with_columns(
                    pl.col(f"{columna}_").is_in(festivos_set).alias(f"{columna}_holiday")
                ).drop(f"{columna}_")
        
        except Exception as e:
            raise RuntimeError(f"[{self.name}] Error: {e}")

        return {"data": data_1}    

class clearMessagesNode(BaseNode):
    """
        clearMessagesNode (Polars)

        Este nodo filtra los mensajes de acuerdo con campañas y clientes válidos.
        Solo mantiene los registros cuyo `campaign_id` y `client_id` se encuentren en los 
        DataFrames de referencia.

        Parámetros YAML esperados:
        --------------------------
        (No requiere parámetros de configuración)
    """
    required_inputs = ["data_1", "data_2", "data_3"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        data_1 = data.get("data_1")
        data_2 = data.get("data_2")
        data_3 = data.get("data_3")

        if not isinstance(data_1, pl.DataFrame) or not isinstance(data_2, pl.DataFrame):
            raise TypeError(f"[{self.name}] Ambos inputs deben ser DataFrames de Polars.")
        
        lista_filtro_campaigns = set(data_2.select("id")["id"].to_list())
        lista_filtro_clients = set(data_3.select("client_id")["client_id"].to_list())
        df_filtrado = data_1.filter(
            (pl.col("campaign_id").is_in(lista_filtro_campaigns)) & 
            (pl.col("client_id").is_in(lista_filtro_clients))
        )

        return {"data": df_filtrado}   


class GetCampaignPerformanceNode(BaseNode):
    """
        Este nodo calcula métricas de rendimiento de campañas a partir de datos de mensajes enviados.
        Aplica agregaciones por `campaign_id` y genera métricas clave como tasas de apertura,
        clics, conversiones, desuscripciones y rebotes.

        Parámetros YAML esperados:
        --------------------------
        (No requiere parámetros de configuración)
    """
    required_inputs = ["data"]

    def __init__(self, name: str, config: Dict[str, Any] = None):
        super().__init__(name, config)
        self.logger = None

    def run(self, data: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        data = data.get("data")

        if not isinstance(data, pl.DataFrame):
            raise TypeError(f"[{self.name}] El inputs debe ser DataFrames de Polars.")

        # Junto las columnas de bounced para solo tener una y poderla calcular.
        data = data.with_columns(
            (pl.col("is_hard_bounced") | pl.col("is_soft_bounced")).alias("is_bounced")
        )

        # Agrupamos por campaña y calculamos las metricas totales
        data_agg = data.group_by("campaign_id").agg([
            pl.count("message_id").alias("total_sent"),
            pl.sum("is_opened").alias("total_opened"),
            pl.sum("is_clicked").alias("total_clicked"),
            pl.sum("is_purchased").alias("total_purchased"),
            pl.sum("is_unsubscribed").alias("total_unsubscribed"),
            pl.sum("is_bounced").alias("total_bounced")
        ])

        # Calculamos las tasas
        data_agg = data_agg.with_columns([
            (pl.col("total_opened") / pl.col("total_sent")).alias("open_rate"), # Ratio de mensajes abiertos
            (pl.when(pl.col("total_opened")>0)
                .then(pl.col("total_clicked") / pl.col("total_opened"))
                .otherwise(0)
            ).alias("click_rate"), # Cuando se abre el mensaje -> ratio de clics
            (pl.when(pl.col("total_clicked")>0)
                .then(pl.col("total_purchased") / pl.col("total_clicked"))
                .otherwise(0)
            ).alias("conversion_rate"), # tasa de conversion 
            (pl.col("total_unsubscribed") / pl.col("total_sent")).alias("unsubscribe_rate"), # Tasa de desuscripciones
            (pl.col("total_bounced") / pl.col("total_sent")).alias("bounce_rate")
        ])

        # Redondear a 2 decimales los ratios
        data_agg = data_agg.with_columns([
            pl.col(["open_rate","click_rate","conversion_rate","unsubscribe_rate","bounce_rate"]).round(2)
        ])
        
        return {"data": data_agg}    

