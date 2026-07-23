"""Deterministic extraction of (x, y) time-series points from a JSON API
response.

Shared by TWO callers so the detection that *routes* an endpoint and the
detection that *extracts* its data can never drift:

  * the generation-time probe in resource-service
    (`from wrapper_runtime.shared.api_extract import detect_mapping`) decides
    whether an endpoint is "simple enough" to wrap without AI, and
  * the deterministic API wrapper template at runtime
    (`from api_extract import extract_points`, PYTHONPATH=shared) pulls the
    points out the exact same way.

CRITICAL: timestamps are parsed at FULL precision (date *and* time). An earlier
AI-generated wrapper truncated each reading to its calendar day, so ~144
intraday sensor readings collapsed onto one x with conflicting y values and the
whole batch was rejected by the data-collector validator. Preserving the time
component keeps every reading on its own x.

This module must NOT import the wrapper-runtime `config` module: it has to load
cleanly from both the FastAPI app and the wrapper subprocess, which resolve the
name `config` to two different files.
"""
import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Field-name hints (lowercased substring match), best first. Detection prefers
# a hinted field but falls back to value-shape sniffing when nothing matches.
_DATE_HINTS = (
    "timestamp", "datetime", "date_time", "date", "time", "data",
    "fecha", "dia", "periodo", "period", "year", "ano", "ts", "when",
)
_VALUE_HINTS = (
    "value", "valor", "measurement", "measure", "reading", "amount",
    "average", "media", "total", "count", "qty", "quantity", "y", "val",
)
# Keys that look like value columns by shape but are almost never the metric.
_ID_SUFFIXES = ("id", "_id", "uuid", "code", "codigo")

# Container keys that commonly hold the records array, best first.
_LIST_HINTS = (
    "data", "results", "result", "items", "records", "measurements",
    "rows", "values", "series", "observations", "entries", "list", "content",
)

_MIN_HIT_RATIO = 0.6  # a field must parse for >= 60% of sampled rows
_SAMPLE = 40          # rows sampled for detection


def to_float(value: Any) -> Optional[float]:
    """Parse a JSON scalar into a float, tolerating EU/EN number formats and
    currency/percent symbols. Returns None for blanks / non-numbers / bools."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return None if (isinstance(value, float) and math.isnan(value)) else float(value)
        except (ValueError, TypeError):
            return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("\xa0", "").replace(" ", "").replace("€", "").replace("%", "").replace("$", "")
    has_comma, has_dot = "," in s, "." in s
    if has_comma and has_dot:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")  # EU: 1.234,56
        else:
            s = s.replace(",", "")                       # EN: 1,234.56
    elif has_comma:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_x(value: Any) -> Optional[str]:
    """Parse a scalar into a FULL-precision ISO-8601 string, or None.

    Handles ISO datetimes (with/without tz), common date strings, bare 4-digit
    years (→ YYYY-01-01) and epoch seconds/milliseconds. Plain small numbers
    (e.g. a sensor reading of 45.9) are intentionally NOT treated as epochs."""
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        iv = int(value)
        if float(iv) == float(value) and 1900 <= iv <= 2100:
            return f"{iv:04d}-01-01T00:00:00"
        return _epoch_to_iso(iv)

    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        if len(s) == 4:
            return f"{int(s):04d}-01-01T00:00:00"
        if len(s) in (10, 13):
            iso = _epoch_to_iso(int(s))
            if iso:
                return iso
    # Require something date-shaped: a separator or month name. Avoids pandas
    # heroically parsing arbitrary tokens (ids, plain ints) into dates.
    if not any(c in s for c in "-/:") and not _has_month_word(s):
        return None
    ts = pd.to_datetime(s, errors="coerce")
    if ts is None or pd.isna(ts):
        ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if ts is None or pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _epoch_to_iso(iv: int) -> Optional[str]:
    a = abs(iv)
    if 10 ** 12 <= a < 10 ** 14:      # milliseconds
        unit = "ms"
    elif 10 ** 8 <= a < 10 ** 11:     # seconds (≈ 1973 .. 5138)
        unit = "s"
    else:
        return None
    ts = pd.to_datetime(iv, unit=unit, errors="coerce")
    return None if pd.isna(ts) else ts.strftime("%Y-%m-%dT%H:%M:%S")


def _has_month_word(s: str) -> bool:
    low = s.lower()
    return any(m in low for m in (
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct",
        "nov", "dec", "fev", "abr", "mai", "ago", "set", "out", "dez",
    ))


def _walk_lists(node: Any, prefix: str = "", depth: int = 0):
    """Yield (dotted_path, list_of_dicts) for every list of objects reachable
    within a shallow depth. Hinted container keys are visited first so a payload
    like {"data": [...], "meta": {...}} prefers "data"."""
    if depth > 3:
        return
    if isinstance(node, list):
        if node and isinstance(node[0], dict):
            yield prefix, node
        return
    if isinstance(node, dict):
        keys = list(node.keys())
        keys.sort(key=lambda k: (_LIST_HINTS.index(k.lower()) if k.lower() in _LIST_HINTS else len(_LIST_HINTS)))
        for k in keys:
            child_path = f"{prefix}.{k}" if prefix else k
            yield from _walk_lists(node[k], child_path, depth + 1)


def dig(payload: Any, path: Optional[str]) -> Any:
    """Navigate a dotted path ('' or None → payload itself)."""
    if not path:
        return payload
    node = payload
    for part in path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return None
    return node


def find_records(payload: Any, preferred_path: Optional[str] = None) -> Tuple[Optional[str], List[dict]]:
    """Locate the array of record objects. Honors preferred_path when it points
    at a non-empty list of dicts; otherwise picks the largest hinted list."""
    if preferred_path:
        node = dig(payload, preferred_path)
        if isinstance(node, list) and node and isinstance(node[0], dict):
            return preferred_path, node
    candidates = list(_walk_lists(payload))
    if not candidates:
        return None, []
    # Prefer hinted container keys (kept first by _walk_lists), then size.
    best_path, best_list = candidates[0]
    for path, lst in candidates:
        if len(lst) > len(best_list) * 2:  # a clearly bigger array wins
            best_path, best_list = path, lst
    return best_path, best_list


def _hint_rank(key: str, hints: Tuple[str, ...]) -> int:
    low = key.lower()
    for i, h in enumerate(hints):
        if h in low:
            return i
    return len(hints)


def detect_fields(records: List[dict],
                  date_field: Optional[str] = None,
                  value_field: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """Pick the (date_field, value_field) for a list of record dicts.

    Honors any field the caller already pinned. For the rest, scores each key by
    how reliably its values parse as a date / number across a sample, tie-broken
    by name hints."""
    sample = [r for r in records[:_SAMPLE] if isinstance(r, dict)]
    if not sample:
        return date_field, value_field
    keys: List[str] = []
    for r in sample:
        for k in r.keys():
            if k not in keys:
                keys.append(k)

    def hit_ratio(key, parser) -> float:
        seen = hits = 0
        for r in sample:
            if key not in r or r[key] is None:
                continue
            seen += 1
            if parser(r[key]) is not None:
                hits += 1
        return (hits / seen) if seen else 0.0

    if not date_field:
        scored = [
            (hit_ratio(k, parse_x), _hint_rank(k, _DATE_HINTS), k)
            for k in keys
        ]
        scored = [s for s in scored if s[0] >= _MIN_HIT_RATIO]
        # highest parse ratio, then best name hint
        scored.sort(key=lambda s: (-s[0], s[1]))
        if scored:
            date_field = scored[0][2]

    if not value_field:
        scored = []
        for k in keys:
            if k == date_field:
                continue
            low = k.lower()
            if any(low == s or low.endswith(s) for s in _ID_SUFFIXES):
                continue  # never pick an id-shaped field as the metric
            ratio = hit_ratio(k, to_float)
            if ratio >= _MIN_HIT_RATIO:
                scored.append((ratio, _hint_rank(k, _VALUE_HINTS), k))
        scored.sort(key=lambda s: (s[1], -s[0]))  # prefer a hinted name first
        if scored:
            value_field = scored[0][2]

    return date_field, value_field


def extract_points(payload: Any, data_path: Optional[str],
                   date_field: str, value_field: str) -> List[Dict[str, Any]]:
    """Turn a JSON payload into [{'x': iso, 'y': float}, ...] using a known
    mapping. Records that don't yield both an x and a y are skipped."""
    records = dig(payload, data_path)
    if not isinstance(records, list):
        return []
    points: List[Dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        x = parse_x(rec.get(date_field))
        y = to_float(rec.get(value_field))
        if x is None or y is None:
            continue
        points.append({"x": x, "y": y})
    return points


def detect_mapping(payload: Any,
                   preferred_path: Optional[str] = None,
                   date_field: Optional[str] = None,
                   value_field: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Auto-detect {data_path, date_field, value_field} for a payload, honoring
    any hints the caller pinned. Returns None when the shape isn't a flat list
    of records with a parseable date + numeric value (caller should fall back to
    AI generation)."""
    path, records = find_records(payload, preferred_path)
    if not records:
        return None
    df, vf = detect_fields(records, date_field, value_field)
    if not df or not vf:
        return None
    points = extract_points(payload, path, df, vf)
    if not points:
        return None
    return {
        "data_path": path or "",
        "date_field": df,
        "value_field": vf,
        "sample_points": len(points),
    }
