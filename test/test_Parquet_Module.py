import pytest
import polars as pl
from unittest.mock import patch
from src.modulos.Parquet_Module import ParquetReaderNode, ParquetWriterNode


# Mock logger simple - Para no generar logs reales
def mock_logger():
    class DummyLogger:
        def info(self, msg): pass
        def debug(self, msg): pass
        def warning(self, msg): pass
        def error(self, msg): pass
        def exception(self, msg): pass
    return DummyLogger()


######################### Test de Lectura ######################################

def test_parquet_reader_missing_config():
    node = ParquetReaderNode("Reader", {})
    with pytest.raises(ValueError, match="Debes definir 'file_paths' o 'folder_path'"):
        node.run()


@patch("src.modulos.Parquet_Module.os.path.exists", return_value=False)
def test_parquet_reader_file_not_found(mock_exists):
    config = {"file_paths": ["nonexistent.parquet"]}
    node = ParquetReaderNode("Reader", config)
    with pytest.raises(RuntimeError, match="leyendo archivo Parquet"):
        node.run()


@patch("src.modulos.Parquet_Module.pl.read_parquet")
@patch("src.modulos.Parquet_Module.os.path.exists", return_value=True)
def test_parquet_reader_basic(mock_exists, mock_read, tmp_path):
    df_mock = pl.DataFrame({"a": [1, 2, 3]})
    mock_read.return_value = df_mock
    file = tmp_path / "sample.parquet"
    file.touch()

    config = {"file_paths": [str(file)]}
    node = ParquetReaderNode("Reader", config)
    result = node.run()
    assert "data" in result
    assert isinstance(result["data"], pl.DataFrame)


######################### Test de escritura ######################################

@patch("src.modulos.Parquet_Module.pl.DataFrame.write_parquet")
def test_parquet_writer_list_input(mock_write, tmp_path):
    file = tmp_path / "data.parquet"
    node = ParquetWriterNode("Writer", {"file_path": str(file)})
    data = [{"a": 1}, {"a": 2}]
    result = node.run({"data": data})
    mock_write.assert_called_once()
    assert "output_path" in result


@patch("src.modulos.Parquet_Module.pl.DataFrame.write_parquet")
def test_parquet_writer_dict_input(mock_write, tmp_path):
    file = tmp_path / "data2.parquet"
    node = ParquetWriterNode("Writer", {"file_path": str(file)})
    data = {"a": 5}
    result = node.run({"data": data})
    mock_write.assert_called_once()
    assert "output_path" in result


def test_parquet_writer_invalid_type():
    node = ParquetWriterNode("Writer", {"file_path": "invalid"})
    node.logger = mock_logger()
    with pytest.raises(RuntimeError, match="Tipo de entrada no soportado"):
        node.run({"data": "not_a_dataframe"})


@patch("src.modulos.Parquet_Module.pl.read_parquet")
@patch("src.modulos.Parquet_Module.pl.concat")
@patch("src.modulos.Parquet_Module.pl.DataFrame.write_parquet")
@patch("src.modulos.Parquet_Module.os.path.exists", return_value=True)
def test_parquet_writer_append_mode(mock_exists, mock_write, mock_concat, mock_read, tmp_path):
    file = tmp_path / "append.parquet"
    df_existing = pl.DataFrame({"a": [1]})
    df_new = pl.DataFrame({"a": [2]})
    mock_read.return_value = df_existing
    mock_concat.return_value = pl.DataFrame({"a": [1, 2]})

    node = ParquetWriterNode("Writer", {"file_path": str(file), "mode": "a"})
    node.logger = mock_logger()

    result = node.run({"data": df_new})
    assert "output_path" in result
    mock_read.assert_called_once_with(str(file))
    mock_concat.assert_called_once()
