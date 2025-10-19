pipeline_schema = {
    "pipeline": {
        "type": "dict",
        "schema": {
            "name": {"type": "string", "required": True},  
            "entrypoint": {"type": "string", "required": True},
            "nodes": {
                "type": "list",
                "schema": {
                    "type": "dict",
                    "schema": {
                        "name": {"type": "string", "required": True},
                        "type": {"type": "string", "required": True},
                        "params": {
                            "type": "dict",
                            "required": False,
                            "schema": {
                                "config": {"type": "dict", "required": False, "allow_unknown": True},
                            },
                            "allow_unknown": True
                        },
                        "outputs": {
                            "type": "list",
                            "schema": {"type": "string"},
                            "required": False
                        }
                    }
                }
            }
        }
    }
}

