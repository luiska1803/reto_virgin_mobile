import json
import re
import string
from typing import Any, Dict
from langchain.schema import Document

from src.pipeline_engine.NodesEngine import BaseNode
from src.submodulos.llm.llm_registry import get_llm

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)

class LLMNode(BaseNode):
    """
    Nodo que invoca un modelo LLM a partir de un prompt dinámico y configurable.

    Espera una plantilla de prompt en config["prompt_template"], así como:
      - config["model_name"]: nombre del modelo (ej. "gpt-4")
      - config["provider"]: proveedor del modelo ("openai", "bedrock")
      - otras configuraciones requeridas por el modelo

    Ejemplo de YAML:
    ----------------
    - name: extractor_codigos
      type: LLMNode
      outputs: [siguiente_nodo]
      params:
        model_name: gpt-4
        provider: openai
        api_key: ${OPENAI_API_KEY}
        temperature: 0.3
        prompt_template: |
          Analiza el siguiente contenido:
          {page_content}

    Este nodo detecta entradas tipo dict, langchain Document o texto plano.
    """
    output_type = Dict[str, Any]

    def __init__(self, name: str, config: dict = None):
        super().__init__(name, config)
        self.logger = None
        model_name = self.config.get("model_name")
        provider = self.config.get("provider")
        model_config = self.config
        self.prompt_template = self.config.get("prompt_template")

        if not self.prompt_template:
            if self.logger:
                self.logger.error(f"[{self.name}] No se proporcionó 'prompt_template' en la configuración del nodo")
            raise ValueError(f"[{self.name}] No se proporcionó 'prompt_template' en la configuración del nodo")
        
        elif len(self.prompt_template.strip()) < 10:
            if self.logger:
                self.logger.error(f"[{self.name}] No se proporcionó 'prompt_template' en la configuración del nodo")
            raise ValueError(
                f"[{self.name}] 'prompt_template' es demasiado corto o vacío. Debe contener contenido significativo para generar un prompt."
            )
        
        self.llm = get_llm(model_name, provider, model_config)
        if self.logger:
            self.logger.info(f"[{self.name}] Inicializado LLMNode con modelo {model_name} ({provider})")

    def run(self, input_data: Any, context: Dict = None):
        if self.logger:
            self.logger.debug(f"[{self.name}] Ejecutando LLMNode con input: {input_data}")

        context = self._build_context(input_data)
        if self.logger:
            self.logger.debug(f"[{self.name}] Contexto construido: {context}")

        prompt = self._format_prompt(context)

        try:
            response = self._invoke_model(prompt)
            output = self._build_output(response, context)
        except Exception as e:
            if self.logger:
                self.logger.exception(f"[{self.name}] Error ejecutando el modelo: {e}")
            raise RuntimeError(f"[{self.name}] Error ejecutando el modelo: {e}")

        if self.logger:
            self.logger.debug(f"[{self.name}] Salida del nodo: {output}")

        return self._flatten_context(output)

    def _build_context(self, input_data: Any) -> Dict[str, Any]:
        if isinstance(input_data, dict):
            return input_data
        elif isinstance(input_data, Document):
            return {
                "page_content": input_data.page_content,
                **input_data.metadata
            }
        elif isinstance(input_data, list) and all(isinstance(d, Document) for d in input_data):
            return {
                "page_content": "\n\n".join(d.page_content for d in input_data),
                "metadatas": [d.metadata for d in input_data]
            }
        elif hasattr(input_data, "to_string"):
            return {"data": input_data.to_string(index=False)}
        else:
            return {"data": str(input_data)}

    def _format_prompt(self, context: Dict[str, Any]) -> str:
        formatter = string.Formatter()
        required_fields = {field for _, field, _, _ in formatter.parse(self.prompt_template) if field}
        missing_fields = required_fields - context.keys()

        if missing_fields:
            if self.logger:
                self.logger.error(f"[Error] Faltan campos requeridos en el contexto para el nodo {self.name}: {missing_fields}")
            raise ValueError(f"[{self.name}] Faltan campos requeridos en el contexto: {missing_fields}")

        prompt = self.prompt_template.format(**context)

        if self.logger:
            self.logger.debug(f"[{self.name}] Prompt generado:\n{prompt}")

        return prompt

    def _invoke_model(self, prompt: str) -> str:
        if self.logger:
            self.logger.debug(f"[{self.name}] Enviando prompt al modelo...")
        return self.llm.invoke(prompt)

    def _build_output(self, response: Any, context: Dict) -> Dict[str, Any]:
        result_str = str(response).strip()
        clean = self.config.get("clean_json_fence", True)

        if clean:
            match = _FENCE_RE.search(result_str)
            if match:
                result_str = match.group(1)

        try:
            parsed = json.loads(result_str)
            if self.logger:
                self.logger.debug(f"[{self.name}] Salida parseada correctamente como JSON")
            if isinstance(parsed, dict):
                return self._flatten_context({**parsed, "output": parsed, **context})
            return {"output": parsed, **context}
        except json.JSONDecodeError:
            if self.logger:
                self.logger.info(f"[{self.name}] No se pudo parsear la salida como JSON. Se devuelve texto plano.")
            return {"output": result_str, **context}
    
    def _flatten_context(self, context):
        if not isinstance(context, dict):
            return context

        flat = {}
        for key, value in context.items():
            if isinstance(value, dict):
                nested = self._flatten_context(value)
                flat.update(nested)
            else:
                flat[key] = value
        return flat


        

        