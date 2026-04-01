from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
MAK_DIR = Path(__file__).resolve().parent


def default_output_path() -> Path:
    return MAK_DIR / "mak.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a makers catalog from existing local partner and brand data."
    )
    parser.add_argument(
        "--output",
        default=str(default_output_path()),
        help="Output JSON path. Defaults to mak/mak.json.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve mak/index.html locally and expose POST /run to regenerate mak.json.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for --serve mode. Defaults to 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8767,
        help="Port for --serve mode. Defaults to 8767.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    return "".join(character.lower() for character in value if character.isalnum())


def build_brand_index(brands_payload: dict) -> dict[str, list[dict]]:
    index: dict[str, list[dict]] = {}
    for row in brands_payload.get("brands", {}).get("all", []):
        parent = row.get("parent_company")
        brand_name = row.get("brand_name")
        if not parent or not brand_name:
            continue
        index.setdefault(normalize_name(parent), []).append(row)
    return index


def maker_subtype_glossary(partner_type_payload: dict) -> dict[str, dict]:
    for group in partner_type_payload.get("partner_types", []):
        if group.get("type") != "Makers":
            continue
        glossary = {}
        for subtype in group.get("subtypes", []):
            sub_type = subtype.get("sub_type")
            if not sub_type:
                continue
            glossary[sub_type] = {
                "short_name": subtype.get("short_name"),
                "description": subtype.get("description"),
                "incentives": subtype.get("incentives"),
            }
        return glossary
    return {}


def distinct_brand_names(rows: list[dict], maker_name: str) -> list[str]:
    maker_key = normalize_name(maker_name)
    seen: set[str] = set()
    names: list[str] = []

    for row in rows:
        brand_name = row.get("brand_name")
        if not brand_name:
            continue
        brand_key = normalize_name(brand_name)
        if not brand_key or brand_key == maker_key or brand_key in seen:
            continue
        seen.add(brand_key)
        names.append(brand_name)

    return sorted(names)


def iter_makers(partners_payload: dict):
    for country, payload in partners_payload.get("North America", {}).items():
        for item in payload.get("manufacturers", []):
            if item.get("category") == "Makers":
                yield country, item


def build_catalog_payload() -> dict:
    partners_payload = load_json(DATA_DIR / "partners.json")
    brands_payload = load_json(DATA_DIR / "brands.json")
    partner_types_payload = load_json(DATA_DIR / "partner_type.json")

    brand_index = build_brand_index(brands_payload)
    subtype_glossary = maker_subtype_glossary(partner_types_payload)

    makers: list[dict] = []

    for sequence, (country, item) in enumerate(iter_makers(partners_payload), start=1):
        name = item.get("name")
        if not name:
            continue

        matched_brand_rows = brand_index.get(normalize_name(name), [])
        brand_names = distinct_brand_names(matched_brand_rows, name)
        subtypes = item.get("sub_type") or []

        makers.append(
            {
                "id": f"mak-{sequence:03d}",
                "name": name,
                "country": country,
                "website": item.get("website") or item.get("wikipedia_url"),
                "headquarters": item.get("headquarters"),
                "manufacturer_type": item.get("type"),
                "subtypes": subtypes,
                "subtype_details": [
                    {
                        "sub_type": subtype,
                        **subtype_glossary.get(subtype, {}),
                    }
                    for subtype in subtypes
                ],
                "source": item.get("source"),
                "revenue_usd": item.get("revenue_usd"),
                "notes": item.get("notes"),
                "brands": brand_names,
                "brand_count": len(brand_names),
                "brand_source": "data/brands.json",
            }
        )

    makers.sort(key=lambda maker: (maker.get("country") or "", maker.get("name") or ""))

    unique_brand_names = sorted(
        {
            brand
            for maker in makers
            for brand in maker.get("brands", [])
            if brand
        }
    )
    unique_types = sorted(
        {
            maker.get("manufacturer_type")
            for maker in makers
            if maker.get("manufacturer_type")
        }
    )
    unique_subtypes = sorted(
        {
            subtype
            for maker in makers
            for subtype in maker.get("subtypes", [])
            if subtype
        }
    )
    unique_countries = sorted(
        {
            maker.get("country")
            for maker in makers
            if maker.get("country")
        }
    )

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "generated_by": "mak/mak.py",
            "derived_from": [
                "data/partners.json",
                "data/brands.json",
                "data/partner_type.json",
            ],
            "total_count": len(makers),
            "countries": len(unique_countries),
            "maker_types": len(unique_types),
            "maker_subtypes": len(unique_subtypes),
            "mapped_brand_entries": len(unique_brand_names),
            "makers_with_brand_portfolios": sum(1 for maker in makers if maker.get("brand_count")),
        },
        "makers": makers,
    }


def generate_catalog(output_path: Path) -> dict:
    payload = build_catalog_payload()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    metadata = payload.get("metadata", {})
    return {
        "ok": True,
        "output": str(output_path),
        "metadata": metadata,
        "total_count": metadata.get("total_count", len(payload.get("makers", []))),
    }


class MakHandler(SimpleHTTPRequestHandler):
    output_path = default_output_path()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(MAK_DIR), **kwargs)

    def log_message(self, *_args):
        pass

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        self.path = parsed.path or "/"
        if self.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/run":
            self.send_json({"ok": False, "error": "Not found"}, status=404)
            return

        try:
            result = generate_catalog(Path(self.output_path).resolve())
            self.send_json(result)
        except Exception as exc:
            self.send_json({"ok": False, "error": str(exc)}, status=500)


def serve(host: str, port: int, output_path: Path) -> None:
    MakHandler.output_path = output_path
    server = ThreadingHTTPServer((host, port), MakHandler)
    print(f"Serving makers at http://{host}:{port}")
    print("POST /run regenerates mak.json")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server.")
    finally:
        server.server_close()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output).resolve()

    if args.serve:
        serve(args.host, args.port, output_path)
        return

    result = generate_catalog(output_path)
    metadata = result["metadata"]
    print(f"Saved {result['total_count']} makers to {output_path}")
    print(
        "Mapped "
        f"{metadata.get('mapped_brand_entries', 0)} brand entries across "
        f"{metadata.get('makers_with_brand_portfolios', 0)} maker portfolios"
    )


if __name__ == "__main__":
    main()
