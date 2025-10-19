from __future__ import annotations

from typing import Any, Dict, List, Sequence, Union, cast

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from tiktoken import encoding_for_model # type: ignore

from src.submodulos.llm.base_llm import BaseLLM, PromptLike

all = ["OpenAIProvider"]

class OpenaiProvider(BaseLLM):
    """Proveedor OpenAI compatible con la interfaz BaseLLM."""

    # ------------------------------------------------------------------ #
    # Construcción
    # ------------------------------------------------------------------ #
    def __init__(self, model_name: str, **config: Any) -> None:
        """
        Args
        ----
        model_name:
            Identificador del modelo (“gpt-4o”, “gpt-3.5-turbo-0125”…)
        **config:
            - api_key              (obligatorio)
            - temperature          (float, 0.7)
            - max_tokens           (int,   512)
            - streaming            (bool,  False)
            - embedding_model      (str,   “text-embedding-3-small”)
        """
        super().__init__(model_name, **config)

        self._chat = ChatOpenAI(
            model=model_name,
            temperature=config.get("temperature", 0.7),
            max_tokens=config.get("max_tokens", 512),
            streaming=config.get("streaming", False),
            api_key=config["api_key"],
        )

        self._embeddings: OpenAIEmbeddings | None = None

    # ------------------------------------------------------------------ #
    # Métodos requeridos por BaseLLM
    # ------------------------------------------------------------------ #
    def invoke(self, prompt: PromptLike) -> str:
        """
        Ejecuta el modelo y devuelve **solo el texto** de la respuesta.

        * ``prompt`` puede ser una cadena o una lista de mensajes estilo
        `{"role": "user", "content": "…"}`
        """
        messages = self._normalize_prompt(prompt)
        response = self._chat.invoke(messages)
        return cast(AIMessage, response).content  # type: ignore[attr-defined]

    # Opcional – embeddings si el usuario los necesita
    def embed(self, text: Union[str, List[str]]) -> List[float] | List[List[float]]:
        """Genera embeddings usando el modelo indicado en ``embedding_model``."""
        if self._embeddings is None:
            self._embeddings = OpenAIEmbeddings(
                model=self.config.get("embedding_model", "text-embedding-3-small"),
                api_key=self.config["api_key"],
            )

        if isinstance(text, str):
            return self._embeddings.embed_query(text)
        return self._embeddings.embed_documents(text)

    # Conteo de tokens
    def get_num_tokens(self, text: str) -> int:
        try:
            enc = encoding_for_model(self.model_name)
            return len(enc.encode(text))
        except Exception:  # pragma: no cover
            return super().get_num_tokens(text)

    # ------------------------------------------------------------------ #
    # Utils internos
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_prompt(
        prompt: PromptLike,
    ) -> Sequence[Union[HumanMessage, AIMessage, SystemMessage]]:
        """Convierte ``PromptLike`` a lista de `langchain_core.messages`."""
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