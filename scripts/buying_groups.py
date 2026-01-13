"""
Generate buying_groups.json with North American appliance buying groups.
Data Source: Wikidata (https://www.wikidata.org/) - CC0 1.0
Usage: python scripts/buying_groups.py
"""
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
import requests


def fetch_buying_groups_from_wikidata() -> Optional[List[Dict]]:
    """Fetch buying groups and purchasing cooperatives."""
    endpoint = "https://query.wikidata.org/sparql"
    query = """
    SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
      {
        ?entity wdt:P31 wd:Q4508.
      } UNION {
        ?entity wdt:P452 wd:Q215353.
      }
      ?entity wdt:P17 ?country.
      FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
      OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    LIMIT 100
    """

    headers = {
        'User-Agent': 'ChannelInsights/1.0 (https://github.com/channelinsights; educational)',
        'Accept': 'application/json'
    }

    try:
        print("Fetching buying groups from Wikidata...")
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
            print("Warning: No buying groups found")
            return None

        entities = []
        for result in results:
            entity = {
                'name': result['entityLabel']['value'],
                'country': result['countryLabel']['value'],
                'wikidata_id': result['entity']['value'].split('/')[-1]
            }
            if 'websiteUrl' in result:
                entity['website'] = result['websiteUrl']['value']
            entities.append(entity)

        print(f"Found {len(entities)} buying groups")
        return entities

    except requests.RequestException as e:
        print(f"Error fetching buying groups from Wikidata: {e}")
        return None


def get_hardcoded_buying_groups() -> List[Dict]:
    """Hardcoded buying group data."""
    return [
        {"name": "Nationwide Marketing Group", "country": "United States", "website": "https://www.nationwidegrp.com"},
        {"name": "BrandSource", "country": "United States", "website": "https://www.brandsource.com"},
        {"name": "PRO Group", "country": "United States", "website": "https://www.progroup.net"},
        {"name": "AVB Buying Group", "country": "United States", "website": "https://www.avbbg.com"}
    ]


def organize_by_country(entities: List[Dict]) -> Dict[str, Dict]:
    """Organize entities by country."""
    organized = {
        "United States": {"entities": []},
        "Canada": {"entities": []},
        "Mexico": {"entities": []}
    }

    for entity in entities:
        country = entity['country']
        if country in organized:
            entity_data = {k: v for k, v in entity.items() if k != 'country'}
            organized[country]["entities"].append(entity_data)

    for country in organized:
        organized[country]["entities"].sort(key=lambda x: x['name'])

    return organized


def generate_buying_groups_data() -> Dict:
    """Generate buying groups data with automatic fallback to hardcoded data."""
    entities = fetch_buying_groups_from_wikidata()

    if not entities:
        print("Using hardcoded fallback data...")
        entities = get_hardcoded_buying_groups()
        data_source = "hardcoded"
    else:
        print("Successfully fetched data from Wikidata")
        data_source = "Wikidata"

    by_country = organize_by_country(entities)
    total_count = sum(len(country_data["entities"]) for country_data in by_country.values())

    return {
        "metadata": {
            "date_pulled": datetime.now(timezone.utc).isoformat(),
            "source": data_source,
            "total_count": total_count,
            "license": "CC0 1.0",
            "license_url": "https://creativecommons.org/publicdomain/zero/1.0/"
        },
        "North America": by_country
    }


def main():
    """Generate and save buying_groups.json to data directory."""
    print("Generating North American buying groups data...")
    data = generate_buying_groups_data()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    os.makedirs(data_dir, exist_ok=True)

    output_file = os.path.join(data_dir, 'buying_groups.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    north_america = data['North America']
    print(f"\nSuccessfully generated {output_file}")
    print(f"  - Data pulled: {data['metadata']['date_pulled']}")
    print(f"  - Source: {data['metadata']['source']}")
    print(f"  - Total: {data['metadata']['total_count']}")
    print(f"  - United States: {len(north_america['United States']['entities'])}")
    print(f"  - Canada: {len(north_america['Canada']['entities'])}")
    print(f"  - Mexico: {len(north_america['Mexico']['entities'])}")


if __name__ == "__main__":
    main()
