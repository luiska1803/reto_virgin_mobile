import pkgutil 
import inspect
import importlib
from typing import Dict, Type
from src.pipeline_engine.NodesEngine import Node

# Caché de clases ya cargadas
NODE_CLASSES: Dict[str, Type[Node]] = {}
NODE_MODULES: Dict[str, str] = {}

# Se coloca las rutas de importación generales de las clases, acá no se importa nada todavía
PACKAGES = [
    "src.modulos",
]

def discover_node_modules(packages: list[str]) -> Dict[str, str]:
    """
    Descubre dinámicamente todos los nodos en el paquete dado.
    Retorna un diccionario {NombreClase: ruta_modulo}.

    return:
    ----- 
        Diccionario de los nodos, sin cargar. 

        NODE_MODULES = {
            "ExcelReader"   : "src.submodulos.ExcelReader",
            "ExcelWriter"   : "src.submodulos.ExcelWriter",
            "CSVReader"     : "src.submodulos.CSVReader",
            ...
        }

    """
    discovered = {}
    for package in packages:
        # Se carga el paquete base
        module = importlib.import_module(package)

        # Se recorre todos los submódulos / modulos
        for _, modname, ispkg in pkgutil.walk_packages(module.__path__, package + "."):
            if ispkg:
                continue
            mod = importlib.import_module(modname)

            # Acá unicamente busca clases que se hereden de Node (solo importara clases con metodo run)
            for name, obj in inspect.getmembers(mod, inspect.isclass):
                if issubclass(obj, Node) and obj is not Node:
                    discovered[name] = modname
    return discovered


NODE_MODULES.update(discover_node_modules(PACKAGES))

def get_node_class(node_type: str) -> Type[Node]:
    """
        Devuelve la clase de un nodo, importándola dinámicamente
        solo cuando se necesite.
    """
    cls = NODE_CLASSES.get(node_type)

    # Validación del caché
    if cls is None or not isinstance(cls, type):
        if node_type not in NODE_MODULES:
            raise ValueError(f"Tipo de nodo no soportado: {node_type}")

        module_name = NODE_MODULES[node_type]
        module = importlib.import_module(module_name)

        cls = getattr(module, node_type)
        if not isinstance(cls, type):
            raise TypeError(f"{node_type} en {module_name} no es una clase válida, se obtuvo: {type(cls)}")
        
        NODE_CLASSES[node_type] = cls

    return cls

