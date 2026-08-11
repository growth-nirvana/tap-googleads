"""Tests for config-defined GAQL streams."""

from __future__ import annotations

import json

import pytest

from tap_googleads.custom_streams import (
    _render_gaql,
    build_custom_streams,
    build_schema,
    flatten,
    load_field_map,
    make_custom_stream_class,
    parse_select_fields,
)
from tap_googleads.streams import ReportsStream


FIELD_MAP = {
    "campaign.id": {"data_type": "INT64", "is_repeated": False},
    "campaign.name": {"data_type": "STRING", "is_repeated": False},
    "metrics.cost_micros": {"data_type": "INT64", "is_repeated": False},
    "metrics.ctr": {"data_type": "DOUBLE", "is_repeated": False},
    "segments.date": {"data_type": "DATE", "is_repeated": False},
    "campaign.labels": {"data_type": "STRING", "is_repeated": True},
}


class TestParseSelectFields:
    def test_basic(self):
        gaql = "SELECT campaign.id, campaign.name FROM campaign"
        assert parse_select_fields(gaql) == ["campaign.id", "campaign.name"]

    def test_multiline_with_trailing_newlines(self):
        gaql = """
        SELECT
            campaign.id,
            campaign.name,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '2024-01-01' AND '2024-01-31'
        """
        assert parse_select_fields(gaql) == [
            "campaign.id",
            "campaign.name",
            "metrics.cost_micros",
        ]

    def test_strips_line_comments(self):
        gaql = """
        SELECT
            campaign.id,        -- primary key
            campaign.name       -- display name
        FROM campaign
        """
        assert parse_select_fields(gaql) == ["campaign.id", "campaign.name"]

    def test_tolerates_trailing_comma(self):
        gaql = "SELECT campaign.id, campaign.name, FROM campaign"
        assert parse_select_fields(gaql) == ["campaign.id", "campaign.name"]

    def test_case_insensitive(self):
        gaql = "select campaign.id, campaign.name from campaign"
        assert parse_select_fields(gaql) == ["campaign.id", "campaign.name"]

    def test_missing_from_raises(self):
        with pytest.raises(ValueError):
            parse_select_fields("SELECT campaign.id")


class TestFlatten:
    def test_single_segment_single_word(self):
        assert flatten("customer.id") == "customer__id"

    def test_camel_cases_within_segment(self):
        assert (
            flatten("campaign.ad_serving_optimization_status")
            == "campaign__adServingOptimizationStatus"
        )

    def test_camel_cases_resource_name(self):
        assert flatten("geo_target_constant.id") == "geoTargetConstant__id"

    def test_metrics_field(self):
        assert flatten("metrics.cost_micros") == "metrics__costMicros"

    def test_deep_nesting(self):
        assert (
            flatten("customer_client.client_customer")
            == "customerClient__clientCustomer"
        )


class TestBuildSchema:
    def test_maps_known_types(self):
        schema = build_schema(
            ["campaign.id", "metrics.ctr", "segments.date"],
            FIELD_MAP,
        )
        props = schema["properties"]
        assert props["campaign__id"]["type"] == ["string", "null"]
        assert props["metrics__ctr"]["type"] == ["number", "null"]
        assert props["segments__date"]["type"] == ["string", "null"]

    def test_repeated_becomes_array(self):
        schema = build_schema(["campaign.labels"], FIELD_MAP)
        prop = schema["properties"]["campaign__labels"]
        assert prop["type"] == ["array", "null"]
        assert prop["items"] == {"type": "string"}

    def test_unknown_field_defaults_to_string(self, caplog):
        with caplog.at_level("WARNING"):
            schema = build_schema(["campaign.made_up_field"], FIELD_MAP)
        assert schema["properties"]["campaign__madeUpField"]["type"] == [
            "string",
            "null",
        ]
        assert any("missing for 1 field" in rec.message for rec in caplog.records)

    def test_always_adds_customer_id_and_run_id(self):
        schema = build_schema(["campaign.id"], FIELD_MAP)
        assert "customer_id" in schema["properties"]
        assert "run_id" in schema["properties"]


class TestRenderGaql:
    def test_substitutes_placeholders(self):
        template = "SELECT x FROM y WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'"
        out = _render_gaql(
            template,
            {"start_date": "2024-01-01", "end_date": "2024-02-01"},
        )
        assert "'2024-01-01'" in out
        assert "'2024-02-01'" in out
        assert "{" not in out

    def test_leaves_other_braces_alone(self):
        template = "SELECT x FROM y WHERE foo = '{bar}' AND z = '{start_date}'"
        out = _render_gaql(template, {"start_date": "2024-01-01"})
        assert "'{bar}'" in out
        assert "'2024-01-01'" in out


class TestMakeCustomStreamClass:
    def _entry(self, **overrides):
        base = {
            "name": "my_campaign_report",
            "gaql": (
                "SELECT campaign.id, campaign.name, metrics.cost_micros, segments.date "
                "FROM campaign "
                "WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'"
            ),
        }
        base.update(overrides)
        return base

    def test_basic_class_shape(self):
        cls = make_custom_stream_class(self._entry(), FIELD_MAP)
        assert issubclass(cls, ReportsStream)
        assert cls.name == "stream_my_campaign_report"
        assert cls.replication_key is None
        assert cls.records_jsonpath == "$.results[*]"
        assert cls.primary_keys == []
        assert "campaign__id" in cls.schema["properties"]

    def test_auto_prefix_preserves_existing_stream_prefix(self):
        entry = self._entry(name="stream_already_prefixed")
        cls = make_custom_stream_class(entry, FIELD_MAP)
        assert cls.name == "stream_already_prefixed"

    def test_primary_keys_pass_through(self):
        entry = self._entry(primary_keys=["campaign__id", "segments__date"])
        cls = make_custom_stream_class(entry, FIELD_MAP)
        assert cls.primary_keys == ["campaign__id", "segments__date"]

    def test_missing_gaql_raises(self):
        with pytest.raises(ValueError):
            make_custom_stream_class({"name": "x"}, FIELD_MAP)

    def test_missing_name_raises(self):
        with pytest.raises(ValueError):
            make_custom_stream_class({"gaql": "SELECT x FROM y"}, FIELD_MAP)

    def test_gaql_is_a_property(self):
        cls = make_custom_stream_class(self._entry(), FIELD_MAP)
        assert isinstance(cls.__dict__["gaql"], property)


class TestBuildCustomStreams:
    def test_empty_list_returns_empty(self):
        assert build_custom_streams([], FIELD_MAP) == []

    def test_duplicate_names_raise(self):
        entry = {
            "name": "dupe",
            "gaql": "SELECT campaign.id FROM campaign",
        }
        with pytest.raises(ValueError, match="Duplicate"):
            build_custom_streams([entry, dict(entry)], FIELD_MAP)


class TestLoadFieldMap:
    def test_missing_file_returns_empty_dict(self, tmp_path, caplog):
        path = tmp_path / "does_not_exist.json"
        with caplog.at_level("WARNING"):
            result = load_field_map(path)
        assert result == {}

    def test_invalid_json_returns_empty_dict(self, tmp_path, caplog):
        path = tmp_path / "bad.json"
        path.write_text("{ not valid json")
        with caplog.at_level("WARNING"):
            result = load_field_map(path)
        assert result == {}

    def test_loads_valid_json(self, tmp_path):
        path = tmp_path / "ok.json"
        path.write_text(json.dumps(FIELD_MAP))
        assert load_field_map(path) == FIELD_MAP
