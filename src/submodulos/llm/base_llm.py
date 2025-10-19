from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

PromptLike = Union[str, List[Dict[str, str]]]

class BaseLLM(ABC):
    """
    Contrato común y mínimo para cualquier modelo de lenguaje (LLM o chat-model).
    Las sub-clases deben implementar al menos `invoke`.

    Métodos opcionales:
      • embed      -> sólo si el mismo proveedor expone embeddings.
      • get_num_tokens -> utilitario para conteo aproximado de tokens.
    """

    def __init__(self, model_name: str, **config: Any):
        self.model_name = model_name
        self.config: Dict[str, Any] = config

    # ──────────────────────────────────────────
    # Métodos obligatorios
    # ──────────────────────────────────────────
    @abstractmethod
    def invoke(self, prompt: PromptLike) -> str:
        """
        Ejecuta el modelo y devuelve la respuesta final en texto plano.

        Args:
            prompt:  Puede ser:
                     • un string (prompt estilo completion),
                     • o una lista de mensajes dict [{"role": "user", ...}]
                       para modelos de chat.

        Returns:
            Texto generado por el modelo (str).
        """
        ...

    # ──────────────────────────────────────────
    # Métodos opcionales / helper
    # ──────────────────────────────────────────
    def embed(self, text: Union[str, List[str]]) -> List[float]:
        """Convierte texto a embedding.  Levanta error si el proveedor no lo soporta."""
        raise NotImplementedError("Embeddings no soportados por este modelo ↓ usa BaseEmbedding")

    def get_num_tokens(self, text: str) -> int:   # noqa: D401
        """Devuelve un conteo rápido de tokens (heurístico)"""
        return len(text.split())