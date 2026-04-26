"""Support for user-defined GAQL streams declared in tap configuration.

Users can add entries to the ``custom_streams`` config option and those entries
are turned into real Singer streams at discovery time, without writing Python
or JSON schema files. Each custom stream is a per-customer full-table report
that runs as a child of ``CustomerHierarchyStream``.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from tap_googleads.streams import ReportsStream

logger = logging.getLogger(__name__)

FIELD_METADATA_PATH = Path(__file__).parent / "google_ads_fields.json"

_TYPE_MAP = {
    "INT32": "string",
    "INT64": "string",
    "UINT64": "string",
    "STRING": "string",
    "RESOURCE_NAME": "string",
    "ENUM": "string",
    "MESSAGE": "string",
    "DATE": "string",
    "DATE_TIME": "string",
    "DOUBLE": "number",
    "FLOAT": "number",
    "BOOLEAN": "boolean",
}

_SELECT_RE = re.compile(
    r"SELECT\s+(?P<fields>.+?)\s+FROM\s+",
    re.IGNORECASE | re.DOTALL,
)


def parse_select_fields(gaql: str) -> List[str]:
    """Return the list of fully-qualified field names selected by ``gaql``.

    Tolerates whitespace, newlines, trailing commas, and ``--`` line comments.
    Raises ``ValueError`` when no SELECT ... FROM clause is found.
    """
    cleaned = "\n".join(line.split("--", 1)[0] for line in gaql.splitlines())
    match = _SELECT_RE.search(cleaned)
    if not match:
        raise ValueError(f"Could not locate SELECT ... FROM clause in GAQL: {gaql!r}")
    raw = match.group("fields")
    return [f.strip() for f in raw.split(",") if f.strip()]


def _snake_to_camel(snake: str) -> str:
    parts = snake.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def flatten(field: str) -> str:
    """Convert a GAQL field name to the flattened JSON schema key.

    Example: ``campaign.ad_serving_optimization_status`` becomes
    ``campaign__adServingOptimizationStatus``.
    """
    return "__".join(_snake_to_camel(seg) for seg in field.strip().split("."))


def load_field_map(path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Load the bundled Google Ads field metadata map.

    Returns an empty dict (with a single warning) if the file is missing or
    invalid. Callers should treat missing entries as "unknown, default to
    nullable string".
    """
    path = path or FIELD_METADATA_PATH
    if not path.exists():
        logger.warning(
            "Google Ads field metadata file not found at %s; custom_streams "
            "schemas will default to nullable strings. Run "
            "scripts/generate_field_metadata.py to generate typed metadata.",
            path,
        )
        return {}
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "Failed to read Google Ads field metadata from %s (%s); "
            "falling back to all-string schemas.",
            path,
            exc,
        )
        return {}
    if not isinstance(data, dict):
        logger.warning(
            "Google Ads field metadata at %s is not a JSON object; ignoring.",
            path,
        )
        return {}
    return data


def _schema_property_for(
    field: str,
    field_map: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Any], bool]:
    """Return (json_schema_property, is_missing_from_map)."""
    meta = field_map.get(field)
    if meta is None:
        return {"type": ["string", "null"]}, True
    data_type = meta.get("data_type", "STRING")
    json_type = _TYPE_MAP.get(data_type, "string")
    base = {"type": [json_type, "null"]}
    if meta.get("is_repeated"):
        return {"type": ["array", "null"], "items": {"type": json_type}}, False
    return base, False


def build_schema(
    fields: List[str],
    field_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a Singer JSON schema for a list of GAQL SELECT fields.

    Adds ``customer_id`` (injected by ``ReportsStream.get_records``) and
    ``run_id`` (injected by the base ``GoogleAdsStream``).
    """
    properties: Dict[str, Any] = {}
    missing: List[str] = []
    for field in fields:
        prop, is_missing = _schema_property_for(field, field_map)
        properties[flatten(field)] = prop
        if is_missing:
            missing.append(field)
    if missing:
        logger.warning(
            "Google Ads field metadata missing for %d field(s) (%s); "
            "these properties default to nullable strings. "
            "Run scripts/generate_field_metadata.py to populate typed metadata.",
            len(missing),
            ", ".join(missing),
        )
    properties.setdefault("customer_id", {"type": ["string", "null"]})
    properties.setdefault("run_id", {"type": ["string", "null"]})
    return {"type": "object", "properties": properties}


def _normalize_stream_name(name: Optional[str]) -> str:
    if not name or not name.strip():
        raise ValueError("custom_streams entry is missing 'name'")
    name = name.strip()
    return name if name.startswith("stream_") else f"stream_{name}"


def _render_gaql(template: str, config: Dict[str, Any]) -> str:
    """Substitute ``{start_date}`` / ``{end_date}`` in a GAQL template.

    Uses simple string replacement (not ``str.format``) so unrelated ``{}``
    characters in the query are left alone.
    """
    return (
        template
        .replace("{start_date}", str(config.get("start_date", "")))
        .replace("{end_date}", str(config.get("end_date", "")))
    )


def make_custom_stream_class(
    entry: Dict[str, Any],
    field_map: Dict[str, Dict[str, Any]],
) -> type:
    """Build a ``ReportsStream`` subclass from a ``custom_streams`` config entry.

    The class is created with ``type(...)`` so Singer SDK receives a real
    class with fixed attributes; the ``gaql`` property is evaluated lazily so
    config-dependent substitutions work per request.
    """
    gaql_template = entry.get("gaql")
    if not gaql_template or not str(gaql_template).strip():
        raise ValueError(
            f"custom_streams entry {entry.get('name')!r} is missing a GAQL query"
        )

    stream_name = _normalize_stream_name(entry.get("name"))
    fields = parse_select_fields(gaql_template)
    schema = build_schema(fields, field_map)
    primary_keys = list(entry.get("primary_keys") or [])

    def _gaql(self) -> str:
        return _render_gaql(gaql_template, self.config)

    cls_name = "CustomStream_" + re.sub(r"\W+", "_", stream_name)
    return type(
        cls_name,
        (ReportsStream,),
        {
            "__doc__": f"User-defined GAQL stream {stream_name!r}.",
            "name": stream_name,
            "primary_keys": primary_keys,
            "replication_key": None,
            "schema": schema,
            "records_jsonpath": "$.results[*]",
            "gaql": property(_gaql),
        },
    )


def build_custom_streams(
    entries: List[Dict[str, Any]],
    field_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[type]:
    """Turn a list of config entries into a list of stream classes."""
    if not entries:
        return []
    if field_map is None:
        field_map = load_field_map()
    classes: List[type] = []
    seen: set = set()
    for entry in entries:
        cls = make_custom_stream_class(entry, field_map)
        if cls.name in seen:
            raise ValueError(
                f"Duplicate custom_streams entry name: {cls.name!r}"
            )
        seen.add(cls.name)
        classes.append(cls)
    return classes
