import sys
import logging
from datetime import datetime
from pathlib import Path

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

class Logger:

    def __init__(self, log_path: str = None, ver_cli: bool = False, reuse_window: int = 5):
        self.log_path = log_path
        self.ver_cli = ver_cli
        self.reuse_window= reuse_window
    
    def get_logger(self) -> logging.Logger:
        """
        Crea un logger que escribe en archivo .log dentro de la carpeta logs/.
        Si existe un log creado dentro de los últimos `reuse_window` segundos,
        reutiliza ese mismo archivo.
        """
        LOG_DIR = Path(__file__).parent / "logs" if not self.log_path else Path(self.log_path).resolve() 
        LOG_DIR.mkdir(exist_ok=True)

        # Buscar logs existentes recientes
        log_files = sorted(LOG_DIR.glob("pipeline_*.log"), reverse=True)
        now = datetime.now()

        LOG_FILE = None
        for file in log_files:
            try:
                # Extraer el timestamp del nombre del archivo
                ts_str = file.name.replace("pipeline_", "").replace(".log", "")
                ts = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                diff = (now - ts).total_seconds()

                # Si el archivo fue creado hace menos de N segundos, reutilizarlo
                if diff <= self.reuse_window:
                    LOG_FILE = file
                    break
            except ValueError:
                continue

        # Si no hay log reciente, crear uno nuevo
        if LOG_FILE is None:
            LOG_FILE = LOG_DIR / f"pipeline_{RUN_ID}.log"

        logger = logging.getLogger("pipeline_logger")
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            if self.ver_cli:
                ch = logging.StreamHandler(sys.stdout)
                ch.setLevel(logging.INFO)
                ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
                logger.addHandler(ch)

            fh = logging.FileHandler(LOG_FILE, mode="a")  # append, no overwrite
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            logger.addHandler(fh)

        return logger