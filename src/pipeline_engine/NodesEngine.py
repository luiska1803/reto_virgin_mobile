from abc import ABC, abstractmethod
from typing import Dict, Any

class Node(ABC):
    """
    Clase base abstracta para todos los nodos del sistema de flujos.

    Cada nodo representa una unidad de procesamiento que recibe datos de entrada,
    realiza una operación y envía los resultados a uno o más nodos de salida.

    Atributos:
        name (str): Nombre único del nodo.
        inputs (list): Lista de nodos que envían datos a este nodo.
        outputs (list): Lista de nodos a los que este nodo envía datos.

    Métodos:
        add_input(node): Agrega un nodo a la lista de entradas si no está ya presente.
        add_output(node): Agrega un nodo a las salidas y registra este nodo como entrada en el otro.
        run(data): Método abstracto que debe implementar la lógica de procesamiento.
    """
    def __init__(self, name: str):
        """
        Inicializa un nodo con un nombre dado.

        Args:
            name (str): Nombre único para identificar el nodo.

        Raises:
            ValueError: Si no se proporciona un nombre válido.
        """
        if not name:
            raise ValueError("El nodo debe tener un nombre")
        self.name = name
        self.inputs = []
        self.outputs = []
        self.logger = None

    def add_input(self, node):
        """
        Agrega un nodo a la lista de entradas si aún no está registrado.

        Args:
            node (Node): Nodo que proporciona datos de entrada a este nodo.
        """
        if node not in self.inputs:
            self.inputs.append(node)

    def add_output(self, node):
        """
        Agrega un nodo a la lista de salidas y también registra este nodo como entrada en el nodo destino.

        Args:
            node (Node): Nodo que recibirá los datos procesados por este nodo.
        """
        if node not in self.outputs:
            self.outputs.append(node)
            node.add_input(self)

    @abstractmethod
    def run(self, data: Any) -> Any:
        """
        Ejecuta la lógica principal del nodo.

        Este método debe ser implementado por todas las subclases.

        Args:
            data (Any): Datos de entrada que el nodo debe procesar.

        Returns:
            Any: Resultado del procesamiento, que será enviado a los nodos de salida.
        """
        pass

    def __repr__(self):
        """
        Representación en cadena del nodo, útil para depuración.

        Returns:
            str: Representación en formato "<Node nombre>".
        """
        return f"<Node {self.name}>"

class BaseNode(Node):
    """
    Clase base para nodos concretos que heredan de `Node`.

    Proporciona una implementación común para nodos configurables con un diccionario de parámetros.

    Atributos adicionales:
        config (Dict[str, Any]): Configuración del nodo proporcionada por el usuario.
        defer_output (bool): Si es True, el nodo no envía datos inmediatamente y usará `finalize()`.
    """
    def __init__(self, name: str, config: Dict[str, Any] = None):
        """
        Inicializa un nodo base con nombre y configuración opcional.

        Args:
            name (str): Nombre único del nodo.
            config (Dict[str, Any], opcional): Diccionario de configuración personalizada.
        """
        super().__init__(name)
        self.config = config or {}
        self.defer_output = False

    def run(self, data: Any) -> Any:
        """
        Método `run` por defecto, que debe ser sobreescrito por subclases (metodos / submetodos).

        Args:
            data (Any): Datos de entrada.

        Raises:
            NotImplementedError: Siempre, indicando que debe ser implementado por subclases.
        """
        raise NotImplementedError("Cada Nodo debe implementar un método run")