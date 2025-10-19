import pytest
import polars as pl
from src.modulos.Utility_Module import (
    FilterNode,
    DropDuplicateNode,
    DropNullteNode,
    RenameColumnsNode,
    CastColumnsNode,
    DropColumnsNode,
)


# Mock logger simple - Para no generar logs reales
class DummyLogger:
    def info(self, *args): pass
    def debug(self, *args): pass
    def warning(self, *args): pass
    def error(self, *args): pass


############### test del nodo FilterNode ########################

def test_filternode_basic_filter():
    df = pl.DataFrame({"edad": [20, 35], "pais": ["Colombia", "Peru"]})
    config = {"condition": 'pl.col("edad") > 25'}
    node = FilterNode("FiltroEdad", config)
    result = node.run({"data": df})
    assert "data" in result
    assert result["data"].shape == (1, 2)
    assert result["data"]["edad"][0] == 35


def test_filternode_missing_condition():
    node = FilterNode("Filtro", {})
    with pytest.raises(ValueError, match="Falta 'condition'"):
        node.run({"data": pl.DataFrame()})


def test_filternode_invalid_type():
    node = FilterNode("Filtro", {"condition": "pl.col('x') > 0"})
    with pytest.raises(TypeError):
        node.run({"data": "not_a_dataframe"})


def test_filternode_invalid_expression():
    node = FilterNode("Filtro", {"condition": "1 + 'a'"})
    df = pl.DataFrame({"x": [1, 2]})
    with pytest.raises(RuntimeError, match="Error evaluando"):
        node.run({"data": df})


############### test del nodo DropDuplicateNode ########################

def test_dropduplicatenode_remove_dupes():
    df = pl.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
    node = DropDuplicateNode("DropDup", {"columnas": ["a"]})
    result = node.run({"data": df})
    assert len(result["data"]) == 2
    assert isinstance(result["data"], pl.DataFrame)


def test_dropduplicatenode_all_columns():
    df = pl.DataFrame({"a": [1, 1], "b": [2, 2]})
    node = DropDuplicateNode("DropDup", {})
    result = node.run({"data": df})
    assert len(result["data"]) == 1


def test_dropduplicatenode_invalid_type():
    node = DropDuplicateNode("DropDup", {})
    with pytest.raises(TypeError):
        node.run({"data": [1, 2, 3]})


############### test del nodo DropNullteNode ########################

def test_dropnulltenode_drop_and_fillna():
    df = pl.DataFrame({
        "a": [1, None, 3],
        "b": ["x", None, "z"]
    })
    config = {"columnas": ["a"], "fillna": {"b": "default"}}
    node = DropNullteNode("DropNulos", config)
    result = node.run({"data": df})
    assert result["data"]["b"].to_list() == ["x", "z"]
    assert not result["data"]["a"].is_null().any()


def test_dropnulltenode_drop_all_true():
    df = pl.DataFrame({
        "a": [1, None, 3],
        "b": [None, None, "ok"]
    })
    node = DropNullteNode("DropAll", {"drop_all": True})
    result = node.run({"data": df})
    assert len(result["data"]) == 1
    assert result["data"]["b"][0] == "ok"


def test_dropnulltenode_invalid_type():
    node = DropNullteNode("DropNulos", {})
    with pytest.raises(TypeError):
        node.run({"data": "invalid"})


############### test del nodo RenameColumnsNode ########################

def test_renamecolumnsnode_basic():
    df = pl.DataFrame({"old": [1, 2]})
    node = RenameColumnsNode("Renombrar", {"rename_map": {"old": "nuevo"}})
    result = node.run({"data": df})
    assert "nuevo" in result["data"].columns


def test_renamecolumnsnode_missing_config():
    node = RenameColumnsNode("Renombrar", {})
    with pytest.raises(ValueError):
        node.run({"data": pl.DataFrame()})


def test_renamecolumnsnode_invalid_type():
    node = RenameColumnsNode("Renombrar", {"rename_map": {"a": "b"}})
    with pytest.raises(TypeError):
        node.run({"data": 123})


############### test del nodo CastColumnsNode ########################

def test_castcolumnsnode_basic_types():
    df = pl.DataFrame({"a": ["1", "2"], "b": ["true", "false"]})
    config = {"cast_map": {"a": "int", "b": "bool"}}
    node = CastColumnsNode("Caster", config)
    result = node.run({"data": df})
    df_out = result["data"]
    assert df_out["a"].dtype == pl.Int64
    assert df_out["b"].dtype == pl.Boolean


def test_castcolumnsnode_unsupported_type():
    df = pl.DataFrame({"a": ["x", "y"]})
    node = CastColumnsNode("Caster", {"cast_map": {"a": "unsupported"}})
    node.logger = DummyLogger()
    result = node.run({"data": df})
    assert "a" in result["data"].columns


def test_castcolumnsnode_missing_column():
    df = pl.DataFrame({"a": ["1"]})
    node = CastColumnsNode("Caster", {"cast_map": {"x": "int"}})
    node.logger = DummyLogger()
    result = node.run({"data": df})
    assert "a" in result["data"].columns  # columna no eliminada


############### test del nodo DropColumnsNode ########################

def test_dropcolumnsnode_remove_columns():
    df = pl.DataFrame({"a": [1], "b": [2]})
    node = DropColumnsNode("DropCols", {"columnas": ["b"]})
    result = node.run({"data": df})
    assert "b" not in result["data"].columns


def test_dropcolumnsnode_no_columns_specified_logs_warning():
    df = pl.DataFrame({"a": [1]})
    node = DropColumnsNode("DropCols", {})
    node.logger = DummyLogger()
    result = node.run({"data": df})
    assert "a" in result["data"].columns


def test_dropcolumnsnode_invalid_type():
    node = DropColumnsNode("DropCols", {"columnas": ["a"]})
    with pytest.raises(TypeError):
        node.run({"data": "invalid"})
