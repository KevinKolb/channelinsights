from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
CATALOG_VERSION = 1

LIST_FIELDS = {
    "aliases",
    "verticals",
    "segments",
    "member_segments",
    "partner_companies",
    "partner_company_sources",
    "products",
    "preferred_pos",
    "preferred_pos_sources",
    "sources",
    "workspace_files",
    "tags",
}

REPLACE_LIST_FIELDS = {
    "preferred_pos",
    "preferred_pos_sources",
    "partner_companies",
    "partner_company_sources",
}

REPLACE_IF_DIFFERENT_FIELDS = {
    "partner_company_last_checked",
    "partner_company_note",
    "preferred_pos_confidence",
    "preferred_pos_last_checked",
    "preferred_pos_note",
    "preferred_pos_status",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_name(name: str) -> str:
    value = (name or "").lower()
    value = re.sub(r"[^\w\s&/+.-]", " ", value)
    value = re.sub(
        r"\b(the|incorporated|inc|corporation|corp|company|co|llc|ltd|limited)\b",
        " ",
        value,
    )
    return re.sub(r"\s+", " ", value).strip()


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", normalize_name(value))
    return slug.strip("-")


def unique_strings(values: Iterable[Any]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return sorted(result, key=str.casefold)


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def record_key(record: Dict[str, Any]) -> str:
    country = (record.get("country") or "United States").strip().casefold()
    return f"{normalize_name(record.get('name', ''))}|{country}"


def load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def extract_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if not isinstance(payload, dict):
        return []

    for collection_key in ("providers", "buying_groups", "items", "entities"):
        collection = payload.get(collection_key)
        if isinstance(collection, list):
            return [row for row in collection if isinstance(row, dict)]

    north_america = payload.get("North America")
    if not isinstance(north_america, dict):
        return []

    records: List[Dict[str, Any]] = []
    for country, country_payload in north_america.items():
        if not isinstance(country_payload, dict):
            continue
        for collection_key in ("entities", "providers", "buying_groups", "manufacturers"):
            for row in country_payload.get(collection_key, []):
                if not isinstance(row, dict):
                    continue
                hydrated = dict(row)
                hydrated.setdefault("country", country)
                records.append(hydrated)
    return records


def normalize_record(record: Dict[str, Any], default_source: str, now: str) -> Dict[str, Any]:
    normalized = dict(record)
    normalized["name"] = str(normalized.get("name", "")).strip()
    if not normalized["name"]:
        raise ValueError("Record is missing a name")

    normalized["country"] = str(
        normalized.get("country")
        or normalized.get("hq_country")
        or "United States"
    ).strip()
    source_values = [
        value
        for value in as_list(normalized.get("sources"))
        if str(value).strip().casefold() != "existing_file"
    ]
    if default_source:
        source_values.append(default_source)
    normalized["sources"] = unique_strings(source_values)
    normalized["aliases"] = unique_strings(as_list(normalized.get("aliases")))
    normalized["workspace_files"] = unique_strings(as_list(normalized.get("workspace_files")))
    normalized["first_seen"] = normalized.get("first_seen") or now
    normalized["last_seen"] = now
    normalized["id"] = normalized.get("id") or slugify(
        f"{normalized['name']}-{normalized['country']}"
    )

    for key in LIST_FIELDS:
        if key in normalized:
            normalized[key] = unique_strings(as_list(normalized[key]))

    return normalized


def merge_records(
    existing_records: List[Dict[str, Any]],
    incoming_records: List[Dict[str, Any]],
    default_source: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    now = utc_now_iso()
    merged: Dict[str, Dict[str, Any]] = {}

    for record in existing_records:
        normalized = normalize_record(record, default_source="", now=now)
        merged[record_key(normalized)] = normalized

    stats = {"added": 0, "updated": 0, "unchanged": 0}

    for record in incoming_records:
        normalized = normalize_record(record, default_source=default_source, now=now)
        key = record_key(normalized)

        if key not in merged:
            merged[key] = normalized
            stats["added"] += 1
            continue

        current = merged[key]
        changed = False
        merged_record = dict(current)

        for field, value in normalized.items():
            if field in {"first_seen", "last_seen"}:
                continue

            if field in REPLACE_LIST_FIELDS:
                replacement = unique_strings(as_list(value))
                if replacement != current.get(field, []):
                    merged_record[field] = replacement
                    changed = True
                continue

            if field in LIST_FIELDS:
                combined = unique_strings(as_list(current.get(field)) + as_list(value))
                if combined != current.get(field, []):
                    changed = True
                merged_record[field] = combined
                continue

            if field == "website":
                current_value = str(current.get("website", "")).strip()
                new_value = str(value or "").strip()
                if not current_value and new_value:
                    merged_record["website"] = new_value
                    changed = True
                elif current_value and new_value and current_value != new_value:
                    alternates = unique_strings(
                        as_list(current.get("alternate_websites")) + [new_value]
                    )
                    if alternates != current.get("alternate_websites", []):
                        merged_record["alternate_websites"] = alternates
                        changed = True
                continue

            if field == "notes":
                current_value = str(current.get("notes", "")).strip()
                new_value = str(value or "").strip()
                if not current_value and new_value:
                    merged_record["notes"] = new_value
                    changed = True
                elif current_value and new_value and new_value not in current_value:
                    merged_record["notes"] = f"{current_value} | {new_value}"
                    changed = True
                continue

            if field in REPLACE_IF_DIFFERENT_FIELDS:
                if current.get(field) != value and value not in (None, "", [], {}):
                    merged_record[field] = value
                    changed = True
                continue

            current_value = current.get(field)
            if current_value in (None, "", [], {}) and value not in (None, "", [], {}):
                merged_record[field] = value
                changed = True

        merged_record["first_seen"] = current.get("first_seen") or normalized["first_seen"]
        merged_record["last_seen"] = now
        merged[key] = merged_record
        stats["updated" if changed else "unchanged"] += 1

    ordered = sorted(merged.values(), key=lambda row: (row.get("name", "").casefold(), row.get("country", "").casefold()))
    return ordered, stats


def load_existing_records(paths: Iterable[Path]) -> List[Dict[str, Any]]:
    collected: Dict[str, Dict[str, Any]] = {}
    for path in paths:
        payload = load_json(path)
        if payload is None:
            continue
        for record in extract_records(payload):
            if not isinstance(record, dict) or not record.get("name"):
                continue
            key = record_key(record)
            collected[key] = dict(record)
    return list(collected.values())


def load_incremental_records(path: Path, collection_key: str) -> List[Dict[str, Any]]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        return []

    metadata = payload.get("metadata", {})
    if metadata.get("catalog_version") != CATALOG_VERSION:
        return []

    collection = payload.get(collection_key)
    if not isinstance(collection, list):
        return []

    return [row for row in collection if isinstance(row, dict) and row.get("name")]


def save_catalog(
    output_path: Path,
    collection_key: str,
    records: List[Dict[str, Any]],
    script_name: str,
    stats: Dict[str, int],
    seed_count: int,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "metadata": {
            "catalog_version": CATALOG_VERSION,
            "entity_type": collection_key,
            "updated_at": utc_now_iso(),
            "generated_by": script_name,
            "merge_strategy": "incremental merge on normalized name + country",
            "preserves_existing_records": True,
            "seed_count": seed_count,
            "total_count": len(records),
            "added_this_run": stats["added"],
            "updated_this_run": stats["updated"],
            "unchanged_this_run": stats["unchanged"],
        },
        collection_key: records,
    }
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
