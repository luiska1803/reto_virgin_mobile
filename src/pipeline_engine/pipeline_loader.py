import os
import re
import yaml
from cerberus import Validator
from typing import Any, get_origin, get_args

from config.schema_pipeline.pipeline_schema import pipeline_schema
from src.pipeline_engine.PipelineEngine import PipelineEngine
from src.pipeline_engine.NodesRegistry import get_node_class


class PipelineLoader:

    def __init__(self):
        self.logger = None

    def are_types_compatible(self, output_type, input_type) -> bool:
        """
            Determina si el tipo de salida de un nodo es compatible con el tipo de 
            entrada de otro.

            Soporta tipos genéricos como `List[str]`, `Dict[str, Any]`, así como 
            tipos simples y `Any`.

            Args:
                output_type: Tipo declarado de salida del nodo origen.
                input_type: Tipo declarado de entrada del nodo destino.

            Returns:
                bool: True si los tipos son compatibles o genéricos, False en caso 
                contrario.
        """
        if input_type is Any or output_type is Any:
            return True
        if output_type is None or input_type is None:
            return True
        if output_type == input_type:
            return True

        origin_out = get_origin(output_type)
        origin_in = get_origin(input_type)

        if origin_out and origin_out == origin_in:
            args_out = get_args(output_type)
            args_in = get_args(input_type)
            return all(a == b or b is Any or a is Any for a, b in zip(args_out, args_in))

        return False
    
    def resolve_env_vars(self, obj: Any) -> Any:
        """
            Reemplaza variables de entorno en cadenas de texto del objeto dado.

            Busca expresiones de la forma `${VAR_NAME}` y las sustituye por el valor correspondiente
            definido en el entorno del sistema. Funciona recursivamente sobre estructuras anidadas.

            Args:
                obj (Any): Objeto que puede ser un string, lista o diccionario con posibles placeholders.

            Returns:
                Any: Objeto con las variables de entorno resueltas.

            Raises:
                ValueError: Si alguna variable de entorno referenciada no está definida.
        """
        if isinstance(obj, dict):
            return {k: self.resolve_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.resolve_env_vars(i) for i in obj]
        elif isinstance(obj, str):
            matches = re.findall(r"\$\{(\w+)\}", obj)
            for var in matches:
                env_value = os.environ.get(var)
                if env_value is None:
                    self.logger.error(f"[ENV] Variable de entorno no definida: {var}")
                    raise ValueError(f"Variable de entorno no definida: {var}")
                obj = obj.replace(f"${{{var}}}", env_value)
            return obj
        return obj


    def validate_pipeline_schema(self, config: dict) -> None:
        """
            Valida que el diccionario de configuración del pipeline cumpla con el esquema esperado.

            Utiliza Cerberus y el esquema definido en `pipeline_schema` para validar que el archivo
            YAML contiene todos los campos requeridos y en el formato correcto.

            Args:
                config (dict): Diccionario cargado desde YAML que define el pipeline.

            Raises:
                ValueError: Si la validación del esquema falla, incluyendo detalles de error.
        """
        validator = Validator(pipeline_schema)
        if not validator.validate(config):
            self.logger.error(f"[SCHEMA] Configuración inválida del pipeline {validator.errors}")
            raise ValueError(f"Configuración inválida del pipeline: {validator.errors}")


    def instantiate_nodes(self, pipeline_config: dict) -> tuple[PipelineEngine, dict]:
        """
            Crea todas las instancias de nodos y construye las conexiones entre ellos.

            Este método:
            - Crea objetos de nodos usando su tipo y parámetros desde el YAML.
            - Construye el grafo de ejecución conectando las salidas de cada nodo a los siguientes.
            - Valida compatibilidad de tipos entre nodos conectados si están definidos.

            Args:
                pipeline_config (dict): Configuración de pipeline (sección 'pipeline' del YAML).

            Returns:
                tuple[PipelineEngine, dict]: Una instancia de `PipelineEngine` con el grafo 
                completo y un diccionario que mapea nombres de nodos a sus instancias.

            Raises:
                ValueError: Si se encuentra un tipo de nodo desconocido.
                TypeError: Si los tipos de entrada y salida de nodos conectados no son compatibles.
        """

        engine = PipelineEngine()
        node_map = {}

        # Crear nodos
        for node_conf in pipeline_config["nodes"]:
            node_type = node_conf["type"]

            try:
                cls = get_node_class(node_type)
                self.logger.debug(f"[NODE] Registrado nodo dinámicamente: {node_type}")
            except ValueError as e:
                self.logger.error(f"[NODE] {e}")
                raise
            
            node = cls(node_conf["name"], **node_conf.get("params", {}))
            node_map[node.name] = node
            engine.add_node(node)
            self.logger.debug(f"[NODE] Instanciado nodo: {node.name} ({node_type})")

        # Conectar nodos
        for node_conf in pipeline_config["nodes"]:
            node = node_map[node_conf["name"]]
            for output_name in node_conf.get("outputs", []):
                output_node = node_map[output_name]
                node.add_output(output_node)

                # Validación de compatibilidad de tipos
                output_type = getattr(node, "output_type", None)
                input_type = getattr(output_node, "input_type", None)

                if not self.are_types_compatible(output_type, input_type):
                    self.logger.error(f"[TYPE ERROR] {node.name} → {output_node.name} | {output_type} ≠ {input_type}")
                    raise TypeError(
                        f"Incompatibilidad de tipos entre '{node.name}' (output: {output_type}) "
                        f"y '{output_node.name}' (input: {input_type})"
                    )
                self.logger.debug(f"[LINK] {node.name} → {output_node.name}")

        return engine, node_map

    def build_pipeline_from_yaml(self, yaml_path: str) -> tuple[PipelineEngine, str]:
        """
            Construye un pipeline completo a partir de un archivo YAML.

            Este método:
            - Carga el YAML desde disco.
            - Resuelve variables de entorno (`${VAR_NAME}`).
            - Valida la estructura del pipeline usando Cerberus.
            - Instancia todos los nodos y conexiones.
            - Retorna el grafo de ejecución y el nodo de entrada principal.

            Args:
                yaml_path (str): Ruta al archivo YAML con la definición del pipeline.

            Returns:
                tuple[PipelineEngine, str]: El motor de ejecución del pipeline y el nombre 
                del nodo inicial.

            Raises:
                ValueError: Si hay errores de esquema, variables de entorno faltantes, o nodo 
                de entrada inválido.
        """
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f)

        config = self.resolve_env_vars(config)
        self.validate_pipeline_schema(config)

        engine, node_map = self.instantiate_nodes(config["pipeline"])
        entrypoint = config["pipeline"].get("entrypoint")
        name = config["pipeline"].get("name")


        if not entrypoint or entrypoint not in node_map:
            self.logger.error(f"[ENTRYPOINT] Nodo de entrada inválido o no definido: '{entrypoint}'")
            raise ValueError(f"El nodo de entrada '{entrypoint}' no está definido en el pipeline")
        
        if not name:
            self.logger.error("[NAME] Campo 'name' no definido en el YAML")
            raise ValueError("El campo name para identificar el nombre del pipeline, no está definido en el pipeline")

        self.logger.info(f"[BUILD] Pipeline '{name}' cargado correctamente con {len(node_map)} nodos: {list(node_map.keys())}")

        return engine, entrypoint, name

