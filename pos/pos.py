from __future__ import annotations

import argparse
import json
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"
POS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from _catalog_merge import DATA_DIR, load_existing_records, load_incremental_records, merge_records, save_catalog


SEED_PROVIDERS = [
    {
        "name": "STORIS",
        "country": "United States",
        "website": "https://www.storis.com",
        "aliases": ["STORIS POS", "STORIS ERP"],
        "products": ["ERP", "POS"],
        "verticals": ["Appliances", "Furniture", "Mattress"],
        "tags": ["appliance", "furniture", "specialty retail"],
        "notes": "Retail management platform commonly used by furniture, appliance, and mattress dealers.",
    },
    {
        "name": "ePASS",
        "country": "United States",
        "website": "https://www.ecisolutions.com",
        "aliases": ["ECI ePASS", "ePass"],
        "products": ["ERP", "POS"],
        "verticals": ["Appliances", "Furniture", "Retail"],
        "tags": ["appliance", "dealer hub", "partner-led integration"],
        "vendor": "ECI Software Solutions",
        "notes": "Dealer-facing retail POS and ERP product in the ECI portfolio.",
    },
    {
        "name": "Windward Software",
        "country": "Canada",
        "website": "https://www.windwardsoftware.com",
        "aliases": ["System Five", "Windward", "Windward System Five"],
        "products": ["ERP", "POS"],
        "verticals": ["Appliances", "Home goods", "Retail"],
        "tags": ["erp", "inventory", "rest api"],
        "notes": "Retail ERP and POS platform with an API surface commonly used in specialty retail deployments.",
    },
    {
        "name": "Celerant Technology",
        "country": "United States",
        "website": "https://www.celerant.com",
        "aliases": ["Celerant"],
        "products": ["Back office", "Ecommerce", "POS"],
        "verticals": ["Retail", "Specialty retail"],
        "tags": ["omnichannel", "retail"],
        "notes": "Retail commerce platform with point-of-sale, ecommerce, and back-office tooling.",
    },
    {
        "name": "FDM4",
        "country": "Canada",
        "website": "https://www.fdm4.com",
        "aliases": ["FDM4 International"],
        "products": ["ERP", "POS"],
        "verticals": ["Furniture", "Retail"],
        "tags": ["furniture", "erp"],
        "notes": "ERP and POS provider with strength in furniture and specialty retail workflows.",
    },
    {
        "name": "Lightspeed Retail",
        "country": "Canada",
        "website": "https://www.lightspeedhq.com",
        "aliases": ["Lightspeed", "Lightspeed POS"],
        "products": ["Ecommerce", "Payments", "POS"],
        "verticals": ["Retail"],
        "tags": ["cloud", "api"],
        "notes": "Cloud retail POS platform with a broad integration ecosystem.",
    },
    {
        "name": "Shopify POS",
        "country": "Canada",
        "website": "https://www.shopify.com/pos",
        "aliases": ["Shopify Point of Sale"],
        "products": ["Ecommerce", "POS"],
        "verticals": ["Retail"],
        "tags": ["cloud", "commerce"],
        "notes": "Shopify's in-store POS offering for retailers already operating on Shopify.",
    },
    {
        "name": "Heartland Retail",
        "country": "United States",
        "website": "https://www.heartland.us",
        "aliases": ["Heartland", "Heartland POS"],
        "products": ["Payments", "POS"],
        "verticals": ["Retail"],
        "tags": ["payments", "retail"],
        "notes": "Retail POS platform offered alongside Heartland payment services.",
    },
    {
        "name": "NCR Counterpoint",
        "country": "United States",
        "website": "https://www.ncrvoyix.com",
        "aliases": ["Counterpoint", "NCR Retail"],
        "products": ["Inventory", "POS"],
        "verticals": ["Retail"],
        "tags": ["inventory", "retail"],
        "notes": "Retail POS and inventory platform in the NCR product family.",
    },
    {
        "name": "Retail Pro",
        "country": "United States",
        "website": "https://www.retailpro.com",
        "aliases": ["Retail Pro Prism"],
        "products": ["Back office", "POS"],
        "verticals": ["Retail"],
        "tags": ["global", "retail"],
        "notes": "Retail POS platform with enterprise and specialty retail deployments.",
    },
    {
        "name": "Oracle Retail Xstore",
        "country": "United States",
        "website": "https://www.oracle.com/retail/",
        "aliases": ["Oracle Xstore", "Xstore"],
        "products": ["Enterprise POS"],
        "verticals": ["Retail"],
        "tags": ["enterprise", "retail"],
        "notes": "Enterprise retail POS platform in Oracle's retail software suite.",
    },
    {
        "name": "MicroBiz",
        "country": "United States",
        "website": "https://microbiz.com",
        "aliases": ["MicroBiz POS"],
        "products": ["Inventory", "POS"],
        "verticals": ["Retail"],
        "tags": ["small business", "retail"],
        "notes": "Retail POS and inventory system oriented to specialty merchants.",
    },
]


def default_output_path() -> Path:
    return POS_DIR / "pos.json"


def legacy_output_path() -> Path:
    return DATA_DIR / "pos.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an incremental POS catalog without dropping existing records."
    )
    parser.add_argument(
        "--output",
        default=str(default_output_path()),
        help="Output JSON path. Defaults to pos/pos.json.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve pos/index.html locally and expose POST /run to regenerate pos.json.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for --serve mode. Defaults to 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8766,
        help="Port for --serve mode. Defaults to 8766.",
    )
    return parser.parse_args()


def load_base_records(output_path: Path) -> list[dict]:
    existing_records = load_incremental_records(output_path, "providers")
    if existing_records:
        return existing_records

    legacy_path = legacy_output_path().resolve()
    if output_path.resolve() == legacy_path:
        return []

    return load_existing_records([legacy_path])


def generate_catalog(output_path: Path) -> dict:
    existing_records = load_base_records(output_path)
    merged_records, stats = merge_records(
        existing_records=existing_records,
        incoming_records=SEED_PROVIDERS,
        default_source="curated_pos_provider_seed",
    )
    save_catalog(
        output_path=output_path,
        collection_key="providers",
        records=merged_records,
        script_name="pos/pos.py",
        stats=stats,
        seed_count=len(SEED_PROVIDERS),
    )

    with output_path.open("r", encoding="utf-8") as handle:
        saved_payload = json.load(handle)

    return {
        "ok": True,
        "output": str(output_path),
        "stats": stats,
        "metadata": saved_payload.get("metadata", {}),
        "total_count": len(merged_records),
    }


class PosHandler(SimpleHTTPRequestHandler):
    output_path = default_output_path()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(POS_DIR), **kwargs)

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
    PosHandler.output_path = output_path
    server = ThreadingHTTPServer((host, port), PosHandler)
    print(f"Serving POS systems at http://{host}:{port}")
    print("POST /run regenerates pos.json")
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
    stats = result["stats"]
    print(f"Saved {result['total_count']} POS providers to {output_path}")
    print(f"Added {stats['added']}, updated {stats['updated']}, unchanged {stats['unchanged']}")


if __name__ == "__main__":
    main()
