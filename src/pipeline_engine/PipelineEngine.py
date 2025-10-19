import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

class PipelineEngine:
    """
    Motor de ejecución de pipelines tipo DAG.

    Características:
    - Siempre requiere un nodo de entrada (entry_name).
    - Nodo de entrada puede ejecutarse sin inputs iniciales.
    - Soporta nodos con múltiples inputs (required_inputs).
    - Maneja defer_output y finalize().
    - Mantiene paralelismo usando ThreadPoolExecutor.
    """
    def __init__(self, max_workers=5):
        self.nodes = {}
        self.max_workers = max_workers
        self.logger = None
        self.node_input_buffer = defaultdict(dict)
        self.lock = threading.Lock()

    def add_node(self, node):
        self.nodes[node.name] = node

    def run_node(self, node, input_name=None, input_value=None):
        """
        Ejecuta un nodo cuando tiene todos los inputs necesarios.
        """
        with self.lock:
            required = getattr(node, "required_inputs", None)

            # Guardamos input si viene de otro nodo
            if input_name is not None:
                self.node_input_buffer[node.name][input_name] = input_value

            # Nodo sin inputs o ya tiene todos sus inputs requeridos
            if not required:
                # Nodo sin inputs recibe None o dict con un solo input
                run_inputs = {} if input_name is None else {input_name: input_value}
                execute = True
            else:
                execute = all(k in self.node_input_buffer[node.name] for k in required)
                if execute:
                    run_inputs = {k: self.node_input_buffer[node.name][k] for k in required}
                else:
                    return  # aún faltan inputs, esperamos

        # Ejecutar nodo
        if execute:
            if self.logger:
                self.logger.info(f"[NODE_START] Ejecutando nodo: {node.name}")
                self.logger.info(f"[NODE_INPUT - {node.name}]: {run_inputs}")

            result = node.run(run_inputs if required else None)

            if self.logger:
                self.logger.info(f"[NODE_OUTPUT - {node.name}]: {result}")

            # Limpiar buffer del nodo
            self.node_input_buffer[node.name].clear()

            # Manejo de defer_output
            if result is None:
                if getattr(node, "defer_output", False):
                    if self.logger:
                        self.logger.info(f"[{node.name}] Salida diferida. Se ejecutará en finalize().")
                    return
                else:
                    if self.logger:
                        self.logger.info(f"[{node.name}] No devolvió resultados. Rama detenida.")
                    return

            # Propagar outputs a nodos hijos
            for output_node in node.outputs:
                if isinstance(result, list):
                    # Cada elemento debe ser tuple (input_name, value) para nodo hijo
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        for item in result:
                            executor.submit(self.run_node, output_node, item[0], item[1])
                elif isinstance(result, dict):
                    # Cada clave será input_name para nodo hijo
                    for k, v in result.items():
                        self.run_node(output_node, k, v)
                else:
                    # Valor simple para nodo hijo sin requerir nombre
                    self.run_node(output_node, None, result)

    def run(self, entry_name, input_data=None, wait=True):
        """
        Inicia la ejecución del pipeline.

        Args:
            entry_name (str): Nodo de entrada obligatorio.
            input_data (dict): Datos iniciales para el nodo de entrada. Puede ser None.
            wait (bool): Esperar a que termine el pipeline antes de continuar.
        """
        node = self.nodes[entry_name]

        for n in self.nodes.values():
            n.logger = self.logger

        if self.logger:
            self.logger.info(f"[RUN_START] Flujo iniciado desde nodo: {entry_name}")

        threads = []

        # Ejecutar nodo de entrada con input_data
        if input_data:
            for k, v in input_data.items():
                t = threading.Thread(target=self.run_node, args=(node, k, v), name=f"{entry_name}-{k}")
                t.start()
                threads.append(t)
                
        else:
            # No hay inputs, ejecutar nodo de entrada con None
            t = threading.Thread(target=self.run_node, args=(node,), name=entry_name)
            t.start()
            threads.append(t)

        if wait:
            for t in threads:
                t.join()

            # Ejecutar nodos con finalize
            for node in self.nodes.values():
                if hasattr(node, "finalize"):
                    final_output = node.finalize()
                    if final_output:
                        for output_node in node.outputs:
                            for k, v in final_output.items():
                                self.run_node(output_node, k, v)

        if self.logger:
            self.logger.info("[RUN_COMPLETE] Ejecución del pipeline completada")
