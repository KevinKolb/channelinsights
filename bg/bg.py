from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"
BG_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from _catalog_merge import load_incremental_records, merge_records, save_catalog


SEED_BUYING_GROUPS = [
    {
        "name": "BrandSource",
        "country": "United States",
        "website": "https://www.brandsource.com",
        "aliases": ["AVB BrandSource"],
        "member_segments": ["Appliances", "Electronics", "Furniture"],
        "tags": ["buying group", "independent dealers", "marketing"],
        "parent_company": "AVB Marketing",
        "notes": "Nationwide buying and marketing organization for independent dealers.",
    },
    {
        "name": "Nationwide Marketing Group",
        "country": "United States",
        "website": "https://www.nationwidegroup.org",
        "aliases": ["NMG", "Nationwide"],
        "member_segments": ["Appliances", "Consumer electronics", "Furniture"],
        "tags": ["buying group", "independent dealers"],
        "notes": "Large independent dealer buying group with national scale.",
    },
    {
        "name": "CANTREX Nationwide",
        "country": "Canada",
        "website": "https://www.cantrex.com",
        "aliases": ["Cantrex", "Cantrex Nationwide"],
        "member_segments": ["Appliances", "Consumer electronics", "Furniture"],
        "tags": ["buying group", "canada"],
        "parent_company": "Nationwide Marketing Group",
        "notes": "Canadian dealer buying group operating under the Nationwide umbrella.",
    },
    {
        "name": "AVB",
        "country": "United States",
        "website": "https://www.avbmarketing.com/",
        "aliases": ["AVB Marketing"],
        "member_segments": ["Appliances", "Electronics", "Furniture"],
        "tags": ["buying group", "parent organization"],
        "notes": "Parent organization behind BrandSource and related dealer programs.",
    },
    {
        "name": "PRO Group",
        "country": "United States",
        "website": "https://www.progroup.net",
        "aliases": ["PRO"],
        "member_segments": ["Appliances", "Consumer electronics", "Furniture"],
        "tags": ["buying group", "independent dealers"],
        "notes": "Member-driven buying group serving independent retailers.",
    },
    {
        "name": "MEGA Group USA",
        "country": "United States",
        "website": "https://www.megagroupusa.com",
        "aliases": ["MEGA Group"],
        "member_segments": ["Independent dealers"],
        "tags": ["buying group", "member-owned"],
        "notes": "Independent dealer buying group operating in the United States.",
    },
    {
        "name": "Mega Group Canada",
        "country": "Canada",
        "website": "https://www.megagroup.ca/",
        "aliases": ["MEGA Group Canada"],
        "member_segments": ["Independent dealers"],
        "tags": ["buying group", "canada", "member-owned"],
        "notes": "Canadian independent dealer buying group.",
    },
    {
        "name": "NECO Alliance",
        "country": "United States",
        "website": "https://www.necoalliance.com",
        "member_segments": ["Independent dealers"],
        "tags": ["buying cooperative", "regional"],
        "notes": "Regional buying cooperative used by independent retail dealers.",
    },
]


def utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


POS_SIGNALS_BY_GROUP = {
    "AVB": {
        "preferred_pos": [
            "ARC",
            "ePASS",
            "PROFITsystems",
            "STORIS",
            "SYNC",
            "WhirlWind",
            "Windward",
        ],
        "preferred_pos_status": "official_program_stack",
        "preferred_pos_confidence": "high",
        "preferred_pos_note": (
            "AVB publicly positions ARC as its most integrated POS while also maintaining "
            "a broader BrandSource-compatible stack of partner platforms."
        ),
        "preferred_pos_sources": [
            "https://yoursourcenews.com/2025/03/avb-announces-arc/",
            "https://yoursourcenews.com/2025/03/avb-pushes-the-digital-envelope/",
            "https://yoursourcenews.com/2022/03/avb-streamlines-retail-ops/",
            "https://yoursourcenews.com/2024/09/sync-back-to-basics/",
        ],
    },
    "BrandSource": {
        "preferred_pos": [
            "ARC",
            "ePASS",
            "PROFITsystems",
            "STORIS",
            "SYNC",
            "WhirlWind",
            "Windward",
        ],
        "preferred_pos_status": "featured_partner_stack",
        "preferred_pos_confidence": "high",
        "preferred_pos_note": (
            "BrandSource does not present a single exclusive POS. Public messaging instead points "
            "to an approved stack led by AVB's ARC plus integrated partner platforms."
        ),
        "preferred_pos_sources": [
            "https://yoursourcenews.com/2025/03/avb-announces-arc/",
            "https://yoursourcenews.com/2025/03/avb-pushes-the-digital-envelope/",
            "https://yoursourcenews.com/2022/08/connected-pos-systems-critical-to-success-meekings/",
            "https://www.storis.com/news/brandsource-partners-with-storis/",
        ],
    },
    "CANTREX Nationwide": {
        "preferred_pos": [
            "ePASS",
            "Furniture Wizard",
            "Myriad Software",
            "Oracle NetSuite",
            "PROFITsystems",
            "STORIS",
            "Smartwerks",
            "WhirlWind",
        ],
        "preferred_pos_status": "inferred_from_parent_programs",
        "preferred_pos_confidence": "medium",
        "preferred_pos_note": (
            "CANTREX does not publish its own POS roster, but its public positioning is tightly tied "
            "to Nationwide programs, so this list is inferred from Nationwide's approved and integrated POS stack."
        ),
        "preferred_pos_sources": [
            "https://www.cantrex.com/",
            "https://www.cantrex.com/nationwide-learning-academy/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-and-storis-solidify-25-year-pos-partnership/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-signs-point-of-sale-partnership-with-myriad-software/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-offers-enhanced-website-experience-with-centerpoint/",
        ],
    },
    "Mega Group Canada": {
        "preferred_pos": [
            "Concept Cameleon",
            "PROFITsystems",
            "Windward",
        ],
        "preferred_pos_status": "member_service_signal",
        "preferred_pos_confidence": "medium",
        "preferred_pos_note": (
            "Mega Group Canada's public software signal comes from its Expert Accounting content, "
            "which names Windward, Profit, Concept Cameleon, and QuickBooks. The POS list here excludes "
            "QuickBooks because it is accounting-first, not a dealer POS."
        ),
        "preferred_pos_sources": [
            "https://www.megagroup.ca/fr/cycle-de-vie-dune-entreprise-partie-2-demarrage/",
        ],
    },
    "MEGA Group USA": {
        "preferred_pos": [
            "ePASS",
            "Furniture Wizard",
            "Myriad Software",
            "Oracle NetSuite",
            "PROFITsystems",
            "STORIS",
            "Smartwerks",
            "WhirlWind",
        ],
        "preferred_pos_status": "inferred_from_parent_programs",
        "preferred_pos_confidence": "medium",
        "preferred_pos_note": (
            "MEGA Group USA was folded into Nationwide. The current POS signal is therefore inferred "
            "from Nationwide's approved and PriMetrix-integrated POS ecosystem."
        ),
        "preferred_pos_sources": [
            "https://www.nationwidegroup.org/nationwide-marketing-group-50-years-united/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-and-storis-solidify-25-year-pos-partnership/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-signs-point-of-sale-partnership-with-myriad-software/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-offers-enhanced-website-experience-with-centerpoint/",
        ],
    },
    "Nationwide Marketing Group": {
        "preferred_pos": [
            "ePASS",
            "Furniture Wizard",
            "Myriad Software",
            "Oracle NetSuite",
            "PROFITsystems",
            "STORIS",
            "Smartwerks",
            "WhirlWind",
        ],
        "preferred_pos_status": "approved_partner_ecosystem",
        "preferred_pos_confidence": "high",
        "preferred_pos_note": (
            "Nationwide publicly describes an approved and integrated POS ecosystem rather than a single preferred platform. "
            "STORIS and Myriad are formal partners; centerpoint and PriMetrix materials point to a broader supported stack."
        ),
        "preferred_pos_sources": [
            "https://www.nationwidegroup.org/nationwide-marketing-group-and-storis-solidify-25-year-pos-partnership/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-signs-point-of-sale-partnership-with-myriad-software/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-launches-new-oracle-netsuite-program-for-independent-retail-channel/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-offers-enhanced-website-experience-with-centerpoint/",
            "https://www.nationwidegroup.org/wp-content/uploads/2023/05/23_03_NMG_PriMetrixConsentForm_SellSheet_B.pdf",
        ],
    },
    "NECO Alliance": {
        "preferred_pos": [
            "ePASS",
        ],
        "preferred_pos_status": "dealer_signal_only",
        "preferred_pos_confidence": "low",
        "preferred_pos_note": (
            "No formal alliance-wide POS program was found. The clearest current public signal is ePASS in AVB coverage of "
            "NECO-aligned dealers using HUB inventory integration, so treat this as directional rather than definitive."
        ),
        "preferred_pos_sources": [
            "https://yoursourcenews.com/2025/06/the-hub-club/",
            "https://yoursourcenews.com/2025/06/sealing-the-deal-with-digital/",
        ],
    },
    "PRO Group": {
        "preferred_pos": [],
        "preferred_pos_status": "no_clear_public_preference",
        "preferred_pos_confidence": "low",
        "preferred_pos_note": (
            "No current public PRO Group POS preference or approved-platform program was found in the available sources."
        ),
        "preferred_pos_sources": [],
    },
}


COMPANY_SIGNALS_BY_GROUP = {
    "AVB": {
        "partner_companies": [
            "BrandSource",
            "HFA Buying Source",
            "Mega Group Canada",
            "ProSource",
            "TRIB Group",
        ],
        "partner_company_note": (
            "Public AVB materials explicitly describe these affiliate organizations as part of the AVB network."
        ),
        "partner_company_sources": [
            "https://yoursourcenews.com/about-us/",
        ],
    },
    "BrandSource": {
        "partner_companies": [
            "Citi Retail Services",
            "STORIS",
            "Synchrony",
            "TD Retail Card Services",
            "WhirlWind",
            "Windward",
        ],
        "partner_company_note": (
            "This list combines BrandSource's named financing relationships with publicly promoted platform partners."
        ),
        "partner_company_sources": [
            "https://www.brandsource.com/brandsource-financing",
            "https://www.brandsource.com/contact/",
            "https://www.storis.com/news/brandsource-partners-with-storis/",
            "https://yoursourcenews.com/2022/08/connected-pos-systems-critical-to-success-meekings/",
        ],
    },
    "CANTREX Nationwide": {
        "partner_companies": [
            "Amisco",
            "Amplis",
            "Armstrong",
            "Beaudoin",
            "Bosch",
            "Canon",
            "Comerco",
            "Danby",
            "Decor-Rest",
            "Elran",
            "Fisher & Paykel",
            "Flexiti",
            "Fujifilm",
            "GE",
            "Gentec",
            "Guardsman",
            "Hisense",
            "Ingram Micro",
            "LG",
            "Mannington",
            "Mirabel",
            "Moneris",
            "Napoleon",
            "Nikon",
            "Palliser",
            "Panasonic",
            "Primco",
            "Samsung",
            "Shaw Floors",
            "Sonos",
            "Sony",
            "Springwall",
            "Stirling Marathon",
            "TD SYNNEX",
            "Tempur Sealy",
            "Whirlpool",
            "Yamaha",
        ],
        "partner_company_note": (
            "Derived from the clearly named vendor logos on Cantrex's official Vendor Partners page. "
            "Ambiguous logo-only entries were left out."
        ),
        "partner_company_sources": [
            "https://www.cantrex.com/vendor-partners/",
        ],
    },
    "MEGA Group USA": {
        "partner_companies": [
            "AT&T",
            "DSI Systems",
            "Google Nest",
            "Myriad Software",
            "Oracle NetSuite",
            "STORIS",
        ],
        "partner_company_note": (
            "MEGA Group USA now sits inside Nationwide, so this company list is inferred from Nationwide's official "
            "member stories and technology partnerships."
        ),
        "partner_company_sources": [
            "https://www.nationwidegroup.org/nationwide-marketing-group-50-years-united/",
            "https://www.nationwidegroup.org/success-stories/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-forms-industrys-first-connected-home-division/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-and-storis-solidify-25-year-pos-partnership/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-signs-point-of-sale-partnership-with-myriad-software/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-launches-new-oracle-netsuite-program-for-independent-retail-channel/",
        ],
    },
    "Mega Group Canada": {
        "partner_companies": [
            "Concept Cameleon",
            "PROFITsystems",
            "Windward",
        ],
        "partner_company_note": (
            "Mega Group Canada's public company-level signal is clearest in its software guidance content; "
            "its broader vendor roster is described publicly but not named company-by-company on the pages used here."
        ),
        "partner_company_sources": [
            "https://www.megagroup.ca/fr/cycle-de-vie-dune-entreprise-partie-2-demarrage/",
            "https://www.megagroup.ca/momentum-2023-a-triumph-in-montreal/momentum-highlight-1-5/",
        ],
    },
    "Nationwide Marketing Group": {
        "partner_companies": [
            "AT&T",
            "DSI Systems",
            "Google Nest",
            "Joe's TV & Appliance",
            "King and Bunny's",
            "Myriad Software",
            "Oracle NetSuite",
            "Pay-Less Furniture and Appliances",
            "Snooze Mattress Co.",
            "STORIS",
            "Sweet Dreams",
            "XSENSOR",
        ],
        "partner_company_note": (
            "This list mixes member companies highlighted on Nationwide's Success Stories page with named technology "
            "and service partners from official Nationwide announcements."
        ),
        "partner_company_sources": [
            "https://www.nationwidegroup.org/success-stories/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-forms-industrys-first-connected-home-division/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-and-storis-solidify-25-year-pos-partnership/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-signs-point-of-sale-partnership-with-myriad-software/",
            "https://www.nationwidegroup.org/nationwide-marketing-group-launches-new-oracle-netsuite-program-for-independent-retail-channel/",
        ],
    },
}

CANONICAL_GROUP_WEBSITES = {
    "AVB": {
        "website": "https://www.avbmarketing.com/",
        "alternate_websites": ["https://www.avb.com"],
    },
    "Mega Group Canada": {
        "website": "https://www.megagroup.ca/",
        "alternate_websites": ["https://www.megagroupcanada.com"],
    },
}


def with_pos_signals(group: dict, checked_on: str) -> dict:
    record = dict(group)
    pos_signal = POS_SIGNALS_BY_GROUP.get(record["name"])
    if not pos_signal:
        record["preferred_pos"] = []
        record["preferred_pos_status"] = "no_clear_public_preference"
        record["preferred_pos_confidence"] = "low"
        record["preferred_pos_note"] = (
            "No clear public POS preference was found for this buying group in the current dataset."
        )
        record["preferred_pos_sources"] = []
        record["preferred_pos_last_checked"] = checked_on
        return record

    record.update(pos_signal)
    record["preferred_pos_last_checked"] = checked_on
    return record


def with_company_signals(group: dict, checked_on: str) -> dict:
    record = dict(group)
    company_signal = COMPANY_SIGNALS_BY_GROUP.get(record["name"], {})
    record["partner_companies"] = company_signal.get("partner_companies", [])
    record["partner_company_sources"] = company_signal.get("partner_company_sources", [])
    record["partner_company_last_checked"] = checked_on

    if company_signal.get("partner_company_note"):
        record["partner_company_note"] = company_signal["partner_company_note"]

    return record


def apply_canonical_website_fixes(records: list[dict]) -> list[dict]:
    fixed_records: list[dict] = []

    for record in records:
        updated = dict(record)
        canonical = CANONICAL_GROUP_WEBSITES.get(updated.get("name"))
        if not canonical:
            fixed_records.append(updated)
            continue

        current_website = str(updated.get("website", "")).strip()
        canonical_website = canonical["website"]
        alternate_value = updated.get("alternate_websites") or []
        alternate_websites = (
            list(alternate_value)
            if isinstance(alternate_value, list)
            else [str(alternate_value).strip()]
        )
        alternate_websites = [url for url in alternate_websites if url]

        for legacy_url in canonical.get("alternate_websites", []):
            if legacy_url and legacy_url not in alternate_websites:
                alternate_websites.append(legacy_url)

        if current_website and current_website != canonical_website and current_website not in alternate_websites:
            alternate_websites.append(current_website)

        updated["website"] = canonical_website
        if alternate_websites:
            updated["alternate_websites"] = sorted(set(alternate_websites), key=str.casefold)

        fixed_records.append(updated)

    return fixed_records


def build_seed_records() -> list[dict]:
    checked_on = utc_today_iso()
    enriched_groups: list[dict] = []

    for group in SEED_BUYING_GROUPS:
        enriched = with_pos_signals(group, checked_on)
        enriched = with_company_signals(enriched, checked_on)
        enriched_groups.append(enriched)

    return enriched_groups


def default_output_path() -> Path:
    return BG_DIR / "bg.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an incremental buying-group catalog without dropping existing records."
    )
    parser.add_argument(
        "--output",
        default=str(default_output_path()),
        help="Output JSON path. Defaults to bg/bg.json.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Serve bg/index.html locally and expose POST /run to regenerate bg.json.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for --serve mode. Defaults to 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port for --serve mode. Defaults to 8765.",
    )
    return parser.parse_args()


def generate_catalog(output_path: Path) -> dict:
    incoming_records = build_seed_records()
    existing_records = load_incremental_records(output_path, "buying_groups")
    merged_records, stats = merge_records(
        existing_records=existing_records,
        incoming_records=incoming_records,
        default_source="curated_buying_group_seed",
    )
    merged_records = apply_canonical_website_fixes(merged_records)
    save_catalog(
        output_path=output_path,
        collection_key="buying_groups",
        records=merged_records,
        script_name="bg/bg.py",
        stats=stats,
        seed_count=len(incoming_records),
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


class BgHandler(SimpleHTTPRequestHandler):
    output_path = default_output_path()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BG_DIR), **kwargs)

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
    BgHandler.output_path = output_path
    server = ThreadingHTTPServer((host, port), BgHandler)
    print(f"Serving buying groups at http://{host}:{port}")
    print("POST /run regenerates bg.json")
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
    print(f"Saved {result['total_count']} buying groups to {output_path}")
    print(f"Added {stats['added']}, updated {stats['updated']}, unchanged {stats['unchanged']}")


if __name__ == "__main__":
    main()
