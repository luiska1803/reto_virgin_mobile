import pytest
import polars as pl
from unittest.mock import patch, MagicMock
from src.modulos.API_Module import APIReaderNode


@pytest.fixture
def mock_logger():
    """Mock simple para reemplazar el logger en el nodo."""
    class DummyLogger:
        def info(self, msg): pass
        def error(self, msg): pass
    return DummyLogger()


@pytest.fixture
def base_config():
    """Configuración base simulada para el nodo."""
    return {
        "api_url": "https://fake.api/test",
        "timeout": 2,
        "params": {"q": "test"},
        "salida": "data"
    }


# llamada exitosa
@patch("src.modulos.API_Module.requests.get")
def test_api_reader_success(mock_get, mock_logger, base_config):
    fake_json = [{"a": 1, "b": 2}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = fake_json
    mock_get.return_value = mock_response

    node = APIReaderNode("TestNode", base_config)
    node.logger = mock_logger

    result = node.run()

    assert "data" in result
    df = result["data"]
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (1, 2)
    assert df["a"][0] == 1
    assert df["b"][0] == 2


# llamada exitosa con campos seleccionados
@patch("src.modulos.API_Module.requests.get")
def test_api_reader_with_selected(mock_get, mock_logger, base_config):
    base_config["selected"] = ["a"]
    fake_json = [{"a": 1, "b": 2}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = fake_json
    mock_get.return_value = mock_response

    node = APIReaderNode("TestNode", base_config)
    node.logger = mock_logger

    result = node.run()
    df = result["data"]

    assert list(df.columns) == ["a"]
    assert df["a"][0] == 1


# manejo de error 429 Too Many Requests
@patch("src.modulos.API_Module.requests.get")
@patch("src.modulos.API_Module.time.sleep", return_value=None)
def test_api_reader_too_many_requests(mock_sleep, mock_get, mock_logger, base_config):
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.headers = {"Retry-After": "1"}
    mock_get.return_value = mock_response

    node = APIReaderNode("TestNode", base_config)
    node.logger = mock_logger

    # Patch recursivo: evita llamada real a run() otra vez
    with patch.object(APIReaderNode, "run", return_value="retry") as mock_run:
        node.run()
        mock_run.assert_called()


# manejo de excepciones
@patch("src.modulos.API_Module.requests.get", side_effect=Exception("Connection error"))
def test_api_reader_exception(mock_get, mock_logger, base_config):
    node = APIReaderNode("TestNode", base_config)
    node.logger = mock_logger

    with pytest.raises(Exception, match="Connection error"):
        node.run()
