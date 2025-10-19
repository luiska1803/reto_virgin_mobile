import os
import time
import argparse

from config.logging_utils import Logger
from config.load_config import cargar_envars, validate_file_path
from src.pipeline_engine.pipeline_loader import PipelineLoader

from dotenv import load_dotenv
load_dotenv()
    
def parse_args() -> argparse.Namespace:
    """
    Parsea los argumentos de línea de comandos necesarios para ejecutar el pipeline.

    Returns:
        argparse.Namespace: Objeto con los argumentos parseados:
            - yaml (str): Ruta al archivo YAML.
            - entry (str|None): Nodo de entrada opcional.
            - validate_only (bool): Flag para validar sin ejecutar.
    """
    parser = argparse.ArgumentParser(description="Ejecuta un archivo YAML compatible para Prasmia")
    parser.add_argument("--yaml", type=str, required=True, help="Ruta hacia el archivo YAML")
    parser.add_argument("--entry", type=str, required=False, help="Nodo desde el que se inicia la ejecución")
    parser.add_argument("--validate-only", action="store_true", help="Solo valida el YAML sin ejecutarlo")
    parser.add_argument("--ver-cli", action="store_true", help="Para ejecutar logs en cli")
    return parser.parse_args()
    
def main() -> None:
    """
    Punto de entrada principal del script.

    - Parsea argumentos.
    - Valida la ruta del YAML.
    - Carga el pipeline y verifica su estructura.
    - Si `--validate-only` está presente, finaliza tras la validación.
    - Ejecuta el pipeline desde el nodo especificado (o el nodo de entrada por defecto).

    Maneja errores de forma centralizada e imprime mensajes amigables por consola.
    """
    start_time = time.time()
    args = parse_args()
    cargar_envars()
    
    logger = Logger(os.getenv("log_dir", None)).get_logger() if not args.ver_cli else Logger(os.getenv("log_dir", None), True).get_logger()
    
    try:
        
        yaml_path = validate_file_path(args.yaml, (".yaml", ".yml"))
        
        pipelineloader = PipelineLoader()
        pipelineloader.logger = logger

        engine, default_entry, name = pipelineloader.build_pipeline_from_yaml(yaml_path)
        engine.logger = logger

        logger.info(f"[START] Se inicializa pipeline {name} con YAML: {yaml_path}")

        if args.validate_only:
            logger.info("[VALIDATE] Validación exitosa del YAML. Ejecución omitida por --validate-only.")
            logger.info("[Validación] El flujo fue validado correctamente. No se ejecutó.")
            return

        entry_node = args.entry or default_entry
        logger.info(f"[RUN] Iniciando desde nodo: {entry_node}")
        
        logger.info("[logger] Procesando Flujo...")
        engine.run(entry_node)
        elapsed_time = time.time() - start_time

        logger.info(f"[COMPLETE] Ejecución de pipeline finalizada. Tiempo total de ejecución: {elapsed_time:.2f} segundos")

    except Exception as e:
        logger.exception(f"[ERROR] Error inesperado, detalle técnico: {e}")
        exit(1)

if __name__ == "__main__":
    main()