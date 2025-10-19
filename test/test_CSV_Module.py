import pytest
import polars as pl
from pathlib import Path
from src.modulos.CSV_Module import CSVReaderNode, CSVWriterNode


@pytest.fixture
def tmp_csv(tmp_path):
    """Crea un CSV temporal para pruebas."""
    file_path = tmp_path / "test.csv"
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df.write_csv(file_path)
    return file_path


@pytest.fixture
def mock_logger():
    """Logger dummy para silenciar logs en test."""
    class DummyLogger:
        def info(self, msg): pass
        def debug(self, msg): pass
        def exception(self, msg): pass
    return DummyLogger()


################### TESTS del nodo de Lectura ########################

def test_csv_reader_single_file(tmp_csv, mock_logger):
    """Lee un archivo CSV simple."""
    node = CSVReaderNode("LectorSimple", {"file_paths": [str(tmp_csv)]})
    node.logger = mock_logger
    result = node.run()

    assert "data" in result
    df = result["data"]
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (3, 2)
    assert list(df.columns) == ["a", "b"]


def test_csv_reader_lazy_mode(tmp_csv, mock_logger):
    """Lee usando modo lazy (scan_csv)."""
    node = CSVReaderNode("LectorLazy", {"file_paths": [str(tmp_csv)], "lazy_mode": True})
    node.logger = mock_logger

    result = node.run()
    assert "data" in result
    assert hasattr(result["data"], "collect")  # LazyFrame


def test_csv_reader_chunk_mode(tmp_csv, mock_logger):
    """Lee en modo chunk (n_rows parciales)."""
    node = CSVReaderNode(
        "LectorChunk",
        {"file_paths": [str(tmp_csv)], "chunk_mode": True, "chunksize": 2}
    )
    node.logger = mock_logger
    result = node.run()
    df = result["data"]
    assert df.shape[0] <= 2


def test_csv_reader_with_folder(tmp_path, mock_logger):
    """Lee todos los CSVs de una carpeta."""
    # Crear 2 CSVs
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    f1 = tmp_path / "f1.csv"
    f2 = tmp_path / "f2.csv"
    df.write_csv(f1)
    df.write_csv(f2)

    node = CSVReaderNode("LectorFolder", {"folder_path": str(tmp_path)})
    node.logger = mock_logger

    result = node.run()
    assert isinstance(result["data"], pl.DataFrame)


def test_csv_reader_excluir_files(tmp_path, mock_logger):
    """Excluye archivos indicados en excluir_files."""
    df = pl.DataFrame({"a": [1, 2]})
    f1 = tmp_path / "f1.csv"
    f2 = tmp_path / "f2.csv"
    df.write_csv(f1)
    df.write_csv(f2)

    node = CSVReaderNode(
        "LectorExclusion",
        {"folder_path": str(tmp_path), "excluir_files": ["f2.csv"]}
    )
    node.logger = mock_logger
    result = node.run()
    df_res = result["data"]
    assert isinstance(df_res, pl.DataFrame)


def test_csv_reader_file_not_found(mock_logger):
    """Lanza error si el archivo no existe."""
    node = CSVReaderNode("LectorError", {"file_paths": ["inexistente.csv"]})
    node.logger = mock_logger
    with pytest.raises(RuntimeError):
        node.run()


def test_csv_reader_missing_config():
    """Lanza error si no se definen rutas."""
    node = CSVReaderNode("LectorVacio", {})
    with pytest.raises(ValueError, match="Debes definir 'file_paths' o 'folder_path'"):
        node.run()



######################### TESTS del nodo de Escritura ################################

def test_csv_writer_from_dataframe(tmp_path, mock_logger):
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    file_path = tmp_path / "output.csv"
    node = CSVWriterNode("EscritorDF", {"file_path": str(file_path)})
    node.logger = mock_logger

    result = node.run({"data": df})
    assert file_path.exists()
    assert result["output_path"].endswith(".csv")


def test_csv_writer_from_list(tmp_path, mock_logger):
    data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    file_path = tmp_path / "list_data.csv"
    node = CSVWriterNode("EscritorLista", {"file_path": str(file_path)})
    node.logger = mock_logger

    result = node.run({"data": data})
    assert Path(result["output_path"]).exists()


def test_csv_writer_from_dict(tmp_path, mock_logger):
    data = {"a": 1, "b": "z"}
    file_path = tmp_path / "dict_data.csv"
    node = CSVWriterNode("EscritorDict", {"file_path": str(file_path)})
    node.logger = mock_logger
    result = node.run({"data": data})
    assert Path(result["output_path"]).exists()


def test_csv_writer_from_generator(tmp_path, mock_logger):
    """Escribe usando un generador de DataFrames."""
    def gen():
        for i in range(2):
            yield pl.DataFrame({"a": [i]})
    file_path = tmp_path / "gen.csv"

    node = CSVWriterNode("EscritorGen", {"file_path": str(file_path)})
    node.logger = mock_logger
    result = node.run({"data": gen()})
    assert Path(result["output_path"]).exists()


def test_csv_writer_invalid_type(tmp_path, mock_logger):
    """Tipo no soportado lanza TypeError."""
    file_path = tmp_path / "error.csv"
    node = CSVWriterNode("EscritorError", {"file_path": str(file_path)})
    node.logger = mock_logger

    with pytest.raises(RuntimeError):
        node.run({"data": 1234})  # tipo inválido


def test_csv_writer_missing_path(mock_logger):
    """Falta file_path debe lanzar error."""
    node = CSVWriterNode("EscritorSinPath", {})
    node.logger = mock_logger
    with pytest.raises(ValueError, match="Falta 'file_path'"):
        node.run({"data": pl.DataFrame({"a": [1]})})
