import os
import yaml
from pathlib import Path

def cargar_envars():
    """
        Carga las rutas y las variables de entorno que se tenga en el archivo "envpaths.yaml" 
        como variables de entorno para la ejecución del pipeline.

        Returns:
        -----
            Rutas marcadas como variables de entorno 
    """
    config_path = Path(__file__).parent / "envpaths.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    running_env = os.getenv("RUNNING_ENV", "local")
    env_paths = data.get("paths", {}).get(running_env)
    if env_paths:
        for key, path in env_paths.items():
            os.environ[key] = str(path)  

def validate_file_path(path: str, extensions: tuple) -> str:
    """
        Verifica que el archivo proporcionado exista y tenga una extensión válida.

        Args:
        -----
            path (str): Ruta al archivo (puede contener `~` como home).

        Returns:
        -----
            str: Ruta expandida y validada del archivo indicado.

        Raises:
        ------
            FileNotFoundError: Si el archivo no existe.
            ValueError: Si la extensión del archivo no es de la que se indica en extensions.
    """
    
    path = Path(path).resolve()

    if not path.exists():
        raise FileNotFoundError(f"El archivo no fue encontrado: {path}")
    if path.suffix not in extensions:
        raise ValueError(f"El archivo debe tener extensión {extensions}")
    return path


