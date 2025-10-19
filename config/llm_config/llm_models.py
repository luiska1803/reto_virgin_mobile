SUPPORTED_MODELS = {
    "openai": {
        "VALID_PARAMS": {
            "temperature", "max_tokens", "streaming", "api_key", "embedding_model"
        },
        "gpt-4o": {
            "max_tokens": 4096,
            "temperature": 0.7,
            "streaming": False,
            "description": "GPT 4o",
            "api_key": "api_key" 
        }
    },
    "bedrock": {
        "VALID_PARAMS": {
            "region_name", "credentials_profile_name", "model_kwargs","top_k", "temperature"
        },
        "mistral.mixtral-8x7b-instruct-v0:1": {
            "description": "Mistral 7 billones"
        },
        "deepseek.r1-v1:0": {
            "description": "Deep Seek R1"
        }
    }
}