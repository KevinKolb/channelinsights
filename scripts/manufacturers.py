"""
Generate manufacturers.json with North American appliance manufacturers.
Data Source: Wikidata (https://www.wikidata.org/) - CC0 1.0
Usage: python scripts/manufacturers.py
"""
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
import requests


def fetch_manufacturers_from_wikidata() -> Optional[List[Dict]]:
    """Fetch appliance manufacturers from Wikidata SPARQL endpoint."""
    endpoint = "https://query.wikidata.org/sparql"
    query = """
    SELECT DISTINCT ?manufacturer ?manufacturerLabel ?countryLabel ?websiteUrl WHERE {
      # Find companies that produce home appliances or are appliance manufacturers
      {
        ?manufacturer wdt:P452 wd:Q327992.  # industry: home appliances
      } UNION {
        ?manufacturer wdt:P1056 wd:Q751797.  # product: home appliance
      } UNION {
        ?manufacturer wdt:P31 wd:Q1137062.  # instance of: appliance manufacturer
      }

      # Must be from North America (US, Canada, or Mexico)
      ?manufacturer wdt:P17 ?country.
      FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))

      # Optional: Get website
      OPTIONAL { ?manufacturer wdt:P856 ?websiteUrl. }

      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?countryLabel ?manufacturerLabel
    LIMIT 100
    """

    headers = {
        'User-Agent': 'ChannelInsights/1.0 (https://github.com/channelinsights; educational)',
        'Accept': 'application/json'
    }

    try:
        print("Fetching appliance manufacturers from Wikidata...")
        response = requests.get(
            endpoint,
            params={'query': query, 'format': 'json'},
            headers=headers,
            timeout=30
        )
        response.raise_for_status()

        data = response.json()
        results = data.get('results', {}).get('bindings', [])

        if not results:
            print("Warning: No manufacturers found")
            return None

        manufacturers = []
        for result in results:
            manufacturer = {
                'name': result['manufacturerLabel']['value'],
                'country': result['countryLabel']['value'],
                'wikidata_id': result['manufacturer']['value'].split('/')[-1]
            }
            if 'websiteUrl' in result:
                manufacturer['website'] = result['websiteUrl']['value']
            manufacturers.append(manufacturer)

        print(f"Found {len(manufacturers)} manufacturers")
        return manufacturers

    except requests.RequestException as e:
        print(f"Error fetching data from Wikidata: {e}")
        return None


def get_hardcoded_data() -> List[Dict]:
    """Fallback data when Wikidata is unavailable (manually curated as of 2024)."""
    return [
        {
            "name": "Whirlpool Corporation",
            "country": "United States",
            "website": "https://www.whirlpoolcorp.com"
        },
        {
            "name": "General Electric Appliances",
            "country": "United States",
            "website": "https://www.geappliances.com"
        },
        {
            "name": "Electrolux",
            "country": "United States",
            "website": "https://www.electrolux.com"
        },
        {
            "name": "Maytag",
            "country": "United States",
            "website": "https://www.maytag.com"
        },
        {
            "name": "KitchenAid",
            "country": "United States",
            "website": "https://www.kitchenaid.com"
        },
        {
            "name": "Frigidaire",
            "country": "United States",
            "website": "https://www.frigidaire.com"
        },
        {
            "name": "Kenmore",
            "country": "United States",
            "website": "https://www.kenmore.com"
        },
        {
            "name": "Viking Range",
            "country": "United States",
            "website": "https://www.vikingrange.com"
        },
        {
            "name": "Sub-Zero",
            "country": "United States",
            "website": "https://www.subzero-wolf.com"
        },
        {
            "name": "Wolf",
            "country": "United States",
            "website": "https://www.subzero-wolf.com"
        },
        {
            "name": "Bosch",
            "country": "United States",
            "website": "https://www.bosch-home.com"
        },
        {
            "name": "Thermador",
            "country": "United States",
            "website": "https://www.thermador.com"
        },
        {
            "name": "Jenn-Air",
            "country": "United States",
            "website": "https://www.jennair.com"
        },
        {
            "name": "Amana",
            "country": "United States",
            "website": "https://www.amana.com"
        },
        {
            "name": "Speed Queen",
            "country": "United States",
            "website": "https://www.speedqueen.com"
        },
        {
            "name": "Mabe",
            "country": "Mexico",
            "website": "https://www.mabe.com.mx"
        },
        {
            "name": "Acros",
            "country": "Mexico"
        }
    ]


def organize_by_country(manufacturers: List[Dict]) -> Dict[str, Dict]:
    """Organize manufacturers by country (similar to states.py structure)."""
    organized = {
        "United States": {"manufacturers": []},
        "Canada": {"manufacturers": []},
        "Mexico": {"manufacturers": []}
    }

    for manufacturer in manufacturers:
        country = manufacturer['country']
        if country in organized:
            mfr_data = {k: v for k, v in manufacturer.items() if k != 'country'}
            organized[country]["manufacturers"].append(mfr_data)

    for country in organized:
        organized[country]["manufacturers"].sort(key=lambda x: x['name'])

    return organized


def generate_manufacturers_data() -> Dict:
    """Generate manufacturers data with automatic fallback to hardcoded data."""
    manufacturers = fetch_manufacturers_from_wikidata()

    if not manufacturers:
        print("Using hardcoded fallback data...")
        manufacturers = get_hardcoded_data()
        data_source = "hardcoded"
    else:
        print("Successfully fetched data from Wikidata")
        data_source = "Wikidata"

    by_country = organize_by_country(manufacturers)
    total_count = sum(len(country_data["manufacturers"]) for country_data in by_country.values())

    return {
        "metadata": {
            "date_pulled": datetime.now(timezone.utc).isoformat(),
            "source": data_source,
            "total_manufacturers": total_count,
            "license": "CC0 1.0",
            "license_url": "https://creativecommons.org/publicdomain/zero/1.0/"
        },
        "North America": by_country
    }


def main():
    """Generate and save manufacturers.json to data directory."""
    print("Generating North American appliance manufacturers data...")
    data = generate_manufacturers_data()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    os.makedirs(data_dir, exist_ok=True)

    output_file = os.path.join(data_dir, 'manufacturers.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    north_america = data['North America']
    print(f"\nSuccessfully generated {output_file}")
    print(f"  - Data pulled: {data['metadata']['date_pulled']}")
    print(f"  - Source: {data['metadata']['source']}")
    print(f"  - Total manufacturers: {data['metadata']['total_manufacturers']}")
    print(f"  - United States: {len(north_america['United States']['manufacturers'])} manufacturers")
    print(f"  - Canada: {len(north_america['Canada']['manufacturers'])} manufacturers")
    print(f"  - Mexico: {len(north_america['Mexico']['manufacturers'])} manufacturers")


if __name__ == "__main__":
    main()
