from __future__ import annotations

from typing import Any, Dict, List, Sequence, Union, cast

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_aws.llms import BedrockLLM  # wrapper de LangChain-AWS

from src.submodulos.llm.base_llm import BaseLLM, PromptLike

__all__ = ["BedrockProvider"]


class BedrockProvider(BaseLLM):
    """
    Proveedor Bedrock compatible con la interfaz `BaseLLM`.

    Parámetros esperados en **config:

    | clave                     | tipo      | por defecto | descripción                            |
    |---------------------------|-----------|-------------|----------------------------------------|
    | region_name               | str       | —           | Región AWS (ej. `"us-east-1"`)         |
    | credentials_profile_name  | str       | None        | Perfil AWS en ~/.aws/credentials       |
    | temperature               | float     | 0.7         | Creatividad del modelo                 |
    | top_k                     | int       | 250         | Top-k para sampling (depende del modelo)|
    | model_kwargs              | dict      | {}          | Otros kwargs que admita BedrockLLM     |
    """

    # ──────────────────────────────────────────
    # Construcción
    # ──────────────────────────────────────────
    def __init__(self, model_name: str, **config: Any) -> None:
        super().__init__(model_name, **config)

        self._llm = BedrockLLM(
            model_id=model_name,
            region_name=config["region_name"],
            credentials_profile_name=config.get("credentials_profile_name", None),
            temperature=config.get("temperature", 0.7),
            top_k=config.get("top_k", 250),
            model_kwargs=config.get("model_kwargs", {}),
        )

    # ──────────────────────────────────────────
    # Métodos obligatorios de BaseLLM
    # ──────────────────────────────────────────
    def invoke(self, prompt: PromptLike) -> str:
        """Ejecuta el modelo y devuelve **solo el texto** generado."""
        messages = self._normalize_prompt(prompt)
        response = self._llm.invoke(messages)
        return cast(AIMessage, response).content  # type: ignore[attr-defined]

    # Embeddings NO soportados por ahora
    # (Si más adelante Bedrock abre embeddings, añade aquí la lógica)
    # def embed(...):

    # ──────────────────────────────────────────
    # Utilidades internas
    # ──────────────────────────────────────────
    @staticmethod
    def _normalize_prompt(
        prompt: PromptLike,
    ) -> Sequence[Union[HumanMessage, AIMessage, SystemMessage]]:
        """
        Convierte `PromptLike` (str o lista de dicts) en la lista de mensajes
        que espera LangChain.
        """
        if isinstance(prompt, str):
            return [HumanMessage(content=prompt)]

        role_map: Dict[str, type] = {
            "user": HumanMessage,
            "assistant": AIMessage,
            "system": SystemMessage,
        }
        msgs: List[Union[HumanMessage, AIMessage, SystemMessage]] = []
        for m in prompt:
            role_cls = role_map.get(m.get("role", "user").lower(), HumanMessage)
            msgs.append(role_cls(content=m["content"]))
        return msgs