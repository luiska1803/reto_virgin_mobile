import importlib
import pkgutil
import inspect
from typing import Dict, Type

from config.llm_config.llm_models import SUPPORTED_MODELS
from src.submodulos.llm.base_llm import BaseLLM

# --------------------------------------------------------------------------- #
# Registro dinámico (permite instalar paquetes externos que aporten proveedores)
# --------------------------------------------------------------------------- #

# Caché de clases ya cargadas
_PROVIDERS: Dict[str, Type[BaseLLM]] = {}
PROVIDERS_CLASSES: Dict[str, str] = {}

# Se coloca las rutas de importación generales de los providers, acá no se importa nada todavía
PACKAGES = [
    "src.submodulos.llm.providers",
]

def discover_provider_supported(packages: list[str]) -> Dict[str, str]:
    """
    Descubre dinámicamente todos los providers registrados en el paquete dado.
    Retorna un diccionario {NombreClase: ruta_modulo}.

    return:
    ----- 
        Diccionario de los providers, sin cargar. 

        _PROVIDERS = {
            "OpenaiProvider"   : "src.submodulos.llm.providers.OpenaiProvider",
            "BedrockProvider"  : "src.submodulos.llm.providers.BedrockProvider,
            ...
        }

    """
    discovered = {}
    for package in packages:
        # Se carga el paquete base
        module = importlib.import_module(package)

        # Se recorre todos los providers 
        for _, modname, ispkg in pkgutil.walk_packages(module.__path__, package + "."):
            if ispkg:
                continue
            mod = importlib.import_module(modname)

            # Acá unicamente busca clases que se hereden de BaseLLM (solo importara llm con metodo invoke)
            for name, obj in inspect.getmembers(mod, inspect.isclass):
                if issubclass(obj, BaseLLM) and obj is not BaseLLM:
                    discovered[name] = modname
    return discovered

PROVIDERS_CLASSES.update(discover_provider_supported(PACKAGES))

def get_provider_class(provider_name: str) -> Type[BaseLLM]:
    """
        Devuelve la clase de un provider, importándola dinámicamente
        solo cuando se necesite.
    """
    cls = _PROVIDERS.get(provider_name)

    # Validación del caché
    if cls is None or not isinstance(cls, type):
        if provider_name not in PROVIDERS_CLASSES:
            raise ValueError(f"El provider no esta soportado: {provider_name}")

        module_name = PROVIDERS_CLASSES[provider_name]
        module = importlib.import_module(module_name)

        cls = getattr(module, provider_name)
        if not isinstance(cls, type):
            raise TypeError(f"{provider_name} en {module_name} no es una clase válida, se obtuvo: {type(cls)}")
        
        _PROVIDERS[provider_name] = cls

    return cls

# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #

def is_model_supported(provider_name: str, model_name: str) -> bool: 
    return model_name in SUPPORTED_MODELS.get(provider_name.lower(), {})

def get_llm(model_name: str, provider_name: str, config: dict) -> BaseLLM:
    """
    Devuelve **una instancia** de la sub-clase `BaseLLM` adecuada.

    Args
    ----
    model_name     :  Nombre del modelo (ej. ``"gpt-4o"``)
    provider_name  :  Proveedor (``"openai"``, ``"bedrock"``, …)
    config         :  Diccionario de parámetros del usuario.
    """

    provider_name = provider_name.lower()

    # 1) ¿Proveedor registrado?
    provider_cls = get_provider_class(provider_name.capitalize() + "Provider")

    # 2) ¿Modelo soportado?
    if not is_model_supported(provider_name, model_name):
        raise ValueError(
            f"Modelo '{model_name}' no está soportado por el proveedor '{provider_name}'."
        )

    # 3) Mezclar config por-defecto + config de usuario
    default_cfg = SUPPORTED_MODELS[provider_name][model_name]
    merged_cfg  = {**default_cfg, **config}

    # 4) Filtrar sólo los parámetros válidos para ese proveedor
    allowed = SUPPORTED_MODELS[provider_name]['VALID_PARAMS']
    safe_cfg = {k: v for k, v in merged_cfg.items() if k in allowed}

    # 5) Instanciar la clase (¡sin .create()!)
    return provider_cls(model_name, **safe_cfg)
        