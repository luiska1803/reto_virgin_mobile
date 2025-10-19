import polars as pl

from src.modulos.Transform_Module import (
    MergeDataNode,
    getHolidaysNode,
    clearMessagesNode,
    GetCampaignPerformanceNode,
)


class DummyLogger:
    def __init__(self):
        self.logs = []
    def info(self, msg): self.logs.append(("INFO", msg))
    def warning(self, msg): self.logs.append(("WARN", msg))
    def error(self, msg): self.logs.append(("ERROR", msg))
    def debug(self, msg): self.logs.append(("DEBUG", msg))


################### Test del Nodo MergeDataNode #####################

def test_merge_data_node_inner_join():
    df1 = pl.DataFrame({"id": [1, 2, 3], "val1": ["a", "b", "c"]})
    df2 = pl.DataFrame({"id": [2, 3, 4], "val2": ["x", "y", "z"]})

    node = MergeDataNode("merge_test", {"on_merge": "id", "how": "inner"})
    node.logger = DummyLogger()

    result = node.run({"data_1": df1, "data_2": df2})["data"]

    assert isinstance(result, pl.DataFrame)
    assert result.shape == (2, 3)
    assert "val2" in result.columns
    assert set(result["id"].to_list()) == {2, 3}


################### Test del Nodo getHolidaysNode #####################

def test_get_holidays_node_marks_festive_dates():
    df1 = pl.DataFrame({"fecha": ["2025-01-01", "2025-01-02", "2025-01-03"]})
    df2 = pl.DataFrame({"festivos": ["2025-01-01", "2025-01-06"]})

    node = getHolidaysNode("festivos_test", {
        "list_col_dates": ["fecha"],
        "col_holidays": "festivos"
    })
    node.logger = DummyLogger()

    result = node.run({"data_1": df1, "data_2": df2})["data"]

    assert "fecha_holiday" in result.columns
    assert result["fecha_holiday"].to_list() == [True, False, False]


################### Test del Nodo clearMessagesNode #####################

def test_clear_messages_node_filters_campaigns_and_clients():
    df1 = pl.DataFrame({
        "campaign_id": [1, 2, 3, 4],
        "client_id": [10, 20, 30, 40],
        "message": ["a", "b", "c", "d"]
    })
    df2 = pl.DataFrame({"id": [1, 3]})
    df3 = pl.DataFrame({"client_id": [10, 30]})

    node = clearMessagesNode("clear_test")
    node.logger = DummyLogger()

    result = node.run({"data_1": df1, "data_2": df2, "data_3": df3})["data"]

    assert result.shape[0] == 2
    assert set(result["campaign_id"].to_list()) == {1, 3}
    assert set(result["client_id"].to_list()) == {10, 30}


################### Test del Nodo GetCampaignPerformanceNode #####################

def test_get_campaign_performance_node_computes_metrics():
    df = pl.DataFrame({
        "campaign_id": [1, 1, 2, 2, 2],
        "message_id": [11, 12, 21, 22, 23],
        "is_opened": [1, 0, 1, 1, 0],
        "is_clicked": [1, 0, 1, 0, 0],
        "is_purchased": [1, 0, 0, 0, 0],
        "is_unsubscribed": [0, 1, 0, 0, 1],
        "is_hard_bounced": [0, 0, 0, 1, 0],
        "is_soft_bounced": [0, 0, 1, 0, 0],
    })

    node = GetCampaignPerformanceNode("performance_test")
    node.logger = DummyLogger()

    result = node.run({"data": df})["data"]

    assert "open_rate" in result.columns
    assert "click_rate" in result.columns
    assert result.filter(pl.col("campaign_id") == 1)["total_sent"].item() == 2
    assert result.shape[0] == 2
    assert all(result["open_rate"].to_list())  # > 0
