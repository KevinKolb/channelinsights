"""
North American Appliance Manufacturers Data Generator

Aggregates manufacturer data from multiple sources to create a comprehensive
database of appliance manuf7888888acturers in North America.

Data Sources:
    - Wikidata SPARQL API (structured data)
    - Wikipedia category pages (company listings)
    - Wikipedia infoboxes (website URLs)
    - Manual curation (supplemental data)

Output:
    data/makers.json

License: CC0 1.0 / Creative Commons
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# =============================================================================
# CONFIGURATION
# =============================================================================

USER_AGENT = 'ChannelInsights/1.0 (educational)'
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1


# =============================================================================
# WIKIDATA API
# =============================================================================

def fetch_manufacturers_from_wikidata() -> List[Dict]:
    """
    Fetch manufacturers from Wikidata SPARQL endpoint.

    Returns:
        List of manufacturer dictionaries
    """
    endpoint = "https://query.wikidata.org/sparql"

    query = """
    SELECT DISTINCT ?manufacturer ?manufacturerLabel ?countryLabel ?websiteUrl ?headquartersLabel ?founded WHERE {
      ?manufacturer wdt:P31 ?type.
      FILTER(?type IN (wd:Q4830453, wd:Q783794, wd:Q891723))

      ?manufacturer wdt:P1056 ?product.
      FILTER(?product IN (
        wd:Q46587, wd:Q14514, wd:Q178692, wd:Q33284, wd:Q79922,
        wd:Q1189815, wd:Q1501817, wd:Q15779252, wd:Q751797
      ))

      ?manufacturer wdt:P17 ?country.
      FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))

      OPTIONAL { ?manufacturer wdt:P856 ?websiteUrl. }
      OPTIONAL { ?manufacturer wdt:P159 ?headquarters. }
      OPTIONAL { ?manufacturer wdt:P571 ?founded. }

      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?countryLabel ?manufacturerLabel
    LIMIT 100
    """

    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'application/json'
    }

    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = requests.get(endpoint, params={'query': query}, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        manufacturers = []
        for result in data.get('results', {}).get('bindings', []):
            mfr = {
                'name': result['manufacturerLabel']['value'],
                'wikidata_id': result['manufacturer']['value'].split('/')[-1],
                'source': 'Wikidata'
            }

            if 'websiteUrl' in result:
                mfr['website'] = result['websiteUrl']['value']
            if 'headquartersLabel' in result:
                mfr['headquarters'] = result['headquartersLabel']['value']
            if 'founded' in result:
                mfr['founded'] = result['founded']['value'][:10]

            manufacturers.append(mfr)

        return manufacturers

    except Exception as e:
        print(f"  Error fetching from Wikidata: {e}")
        return []


# =============================================================================
# WIKIPEDIA SCRAPING - CATEGORIES
# =============================================================================

def scrape_wikipedia_category(category_url: str, country: str) -> List[Dict]:
    """
    Scrape manufacturer names from Wikipedia category page.

    Args:
        category_url: URL to Wikipedia category
        country: Country name for the manufacturers

    Returns:
        List of manufacturer dictionaries
    """
    headers = {'User-Agent': USER_AGENT}
    manufacturers = []

    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = requests.get(category_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        category_divs = soup.find_all('div', class_='mw-category-group')

        if not category_divs:
            return manufacturers

        # Get all links from all category groups
        links = []
        for category_div in category_divs:
            links.extend(category_div.find_all('a'))

        for link in links:
            name = link.get_text().strip()
            wiki_path = link.get('href', '')

            # Filter out non-article pages
            if ':' in name or not wiki_path.startswith('/wiki/'):
                continue
            if 'may not reflect recent changes' in name.lower():
                continue
            if name.startswith('List of'):
                continue

            manufacturers.append({
                'name': name,
                'country': country,
                'wikipedia_url': urljoin(category_url, wiki_path),
                'source': 'Wikipedia'
            })

        print(f"    Found {len(manufacturers)} manufacturers")
        return manufacturers

    except Exception as e:
        print(f"  Error scraping {category_url}: {e}")
        return []


def fetch_manufacturers_from_wikipedia() -> List[Dict]:
    """
    Fetch manufacturers from Wikipedia category pages.

    Returns:
        List of manufacturer dictionaries
    """
    categories = [
        {
            'url': 'https://en.wikipedia.org/wiki/Category:Home_appliance_manufacturers_of_the_United_States',
            'country': 'United States'
        },
        {
            'url': 'https://en.wikipedia.org/wiki/Category:Home_appliance_manufacturers_of_Canada',
            'country': 'Canada'
        }
    ]

    all_manufacturers = []
    for category in categories:
        manufacturers = scrape_wikipedia_category(category['url'], category['country'])
        all_manufacturers.extend(manufacturers)

    print(f"  Total from Wikipedia: {len(all_manufacturers)} manufacturers")
    return all_manufacturers


# =============================================================================
# WIKIPEDIA SCRAPING - WEBSITE URLS
# =============================================================================

def scrape_website_from_wikipedia(wikipedia_url: str, manufacturer_name: str) -> Optional[str]:
    """
    Extract website URL from Wikipedia infobox.

    Args:
        wikipedia_url: URL to Wikipedia page
        manufacturer_name: Name for logging

    Returns:
        Website URL if found, None otherwise
    """
    headers = {'User-Agent': USER_AGENT}

    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = requests.get(wikipedia_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        infobox = soup.find('table', class_='infobox')

        if infobox:
            website_row = infobox.find('th', string=re.compile(r'Website', re.I))
            if website_row:
                website_cell = website_row.find_next_sibling('td')
                if website_cell:
                    link = website_cell.find('a', href=True, class_='external')
                    if link:
                        return link['href']

        return None

    except Exception as e:
        print(f"    Error scraping {manufacturer_name}: {e}")
        return None


def enrich_manufacturers_with_websites(manufacturers: List[Dict], max_scrape: int = 100) -> List[Dict]:
    """
    Enrich manufacturer data with website URLs from Wikipedia pages.

    Args:
        manufacturers: List of manufacturer dictionaries
        max_scrape: Maximum pages to scrape (-1 for unlimited)

    Returns:
        Enriched list with website URLs
    """
    print("\nEnriching manufacturer data with website URLs from Wikipedia...")

    needs_scraping = [m for m in manufacturers if not m.get('website') and m.get('wikipedia_url')]
    total_to_scrape = len(needs_scraping)

    if total_to_scrape == 0:
        print("  All manufacturers already have website URLs!")
        return manufacturers

    actual_scrape_limit = min(total_to_scrape, max_scrape) if max_scrape > 0 else total_to_scrape
    print(f"  Scraping {actual_scrape_limit} of {total_to_scrape} manufacturers without websites...")

    enriched = []
    scraped_count = 0
    scrape_attempts = 0

    for mfr in manufacturers:
        if mfr.get('website') or not mfr.get('wikipedia_url'):
            enriched.append(mfr)
            continue

        if max_scrape > 0 and scrape_attempts >= max_scrape:
            enriched.append(mfr)
            continue

        scrape_attempts += 1
        print(f"  [{scrape_attempts}/{actual_scrape_limit}] Fetching website for: {mfr['name']}")
        website = scrape_website_from_wikipedia(mfr['wikipedia_url'], mfr['name'])

        if website:
            mfr['website'] = website
            scraped_count += 1
            print(f"    Found: {website}")

        enriched.append(mfr)

    remaining = total_to_scrape - scrape_attempts
    print(f"  Successfully scraped {scraped_count} website URLs from Wikipedia")
    if remaining > 0:
        print(f"  Note: {remaining} manufacturers still only have Wikipedia URLs")

    return enriched


# =============================================================================
# SUPPLEMENTAL DATA
# =============================================================================

def get_supplemental_manufacturers() -> List[Dict]:
    """
    Manually curated data for major manufacturers not captured by automated sources.

    Manufacturer Types:
        - Major Appliances (full-line)
        - Major Appliances (cooking)
        - Major Appliances (laundry)
        - Major Appliances (refrigeration)
        - Small Appliances
        - Vacuum/Floor Care
        - Outdoor Cooking
        - Specialty/Luxury
        - Brand Owner
        - OEM (Original Equipment Manufacturer)
        - ODM (Original Design Manufacturer)

    Returns:
        List of manufacturer dictionaries
    """
    return [
        # Brand Owners (own brands but don't manufacture)
        {
            'name': 'Spectrum Brands',
            'country': 'United States',
            'website': 'https://www.spectrumbrands.com',
            'headquarters': 'Middleton, Wisconsin',
            'type': 'Brand Owner',
            'revenue_usd': 3400000000,
            'source': 'Supplemental',
            'notes': 'Owns Black+Decker, George Foreman, Russell Hobbs appliance brands'
        },
        {
            'name': 'Newell Brands',
            'country': 'United States',
            'website': 'https://www.newellbrands.com',
            'headquarters': 'Atlanta, Georgia',
            'type': 'Brand Owner',
            'revenue_usd': 8600000000,
            'source': 'Supplemental',
            'notes': 'Owns Crock-Pot, Mr. Coffee, Oster, Sunbeam, FoodSaver brands'
        },

        # OEM Manufacturers
        {
            'name': 'Midea Group',
            'country': 'United States',
            'website': 'https://www.midea.com',
            'headquarters': 'Parsippany, New Jersey',
            'type': 'OEM',
            'revenue_usd': 52000000000,
            'source': 'Supplemental',
            'notes': 'Major OEM manufacturer for private label appliances, owns Toshiba appliances'
        },
        {
            'name': 'Galanz Americas',
            'country': 'United States',
            'website': 'https://www.galanzamericas.com',
            'headquarters': 'Irvine, California',
            'type': 'OEM',
            'source': 'Supplemental',
            'notes': 'OEM manufacturer specializing in microwaves and small appliances'
        },
        {
            'name': 'TTI Floor Care',
            'country': 'United States',
            'website': 'https://www.ttifloorcare.com',
            'headquarters': 'Charlotte, North Carolina',
            'type': 'OEM',
            'revenue_usd': 1500000000,
            'source': 'Supplemental',
            'notes': 'OEM for Hoover, Dirt Devil, Oreck, Vax brands'
        },

        # ODM Manufacturers
        {
            'name': 'JS Global',
            'country': 'United States',
            'website': 'https://www.jsgloballife.com',
            'headquarters': 'Boston, Massachusetts',
            'type': 'ODM',
            'revenue_usd': 2800000000,
            'source': 'Supplemental',
            'notes': 'ODM owns SharkNinja, designs and manufactures innovative appliances'
        },
        # United States - Major Players
        {
            'name': 'Whirlpool Corporation',
            'country': 'United States',
            'website': 'https://www.whirlpoolcorp.com',
            'headquarters': 'Benton Harbor, Michigan',
            'type': 'Major Appliances (full-line)',
            'revenue_usd': 19400000000,
            'source': 'Supplemental',
            'notes': 'World\'s largest appliance manufacturer, ~2023 revenue'
        },
        {
            'name': 'GE Appliances',
            'country': 'United States',
            'website': 'https://www.geappliances.com',
            'headquarters': 'Louisville, Kentucky',
            'type': 'Major Appliances (full-line)',
            'revenue_usd': 8000000000,
            'source': 'Supplemental',
            'notes': 'Owned by Haier, est. 2023 revenue'
        },
        {
            'name': 'Electrolux North America',
            'country': 'United States',
            'website': 'https://www.electrolux.com',
            'headquarters': 'Charlotte, North Carolina',
            'type': 'Major Appliances (full-line)',
            'revenue_usd': 5000000000,
            'source': 'Supplemental',
            'notes': 'Owns Frigidaire brand, est. North America revenue'
        },

        # Luxury/Premium Brands
        {
            'name': 'Sub-Zero Group',
            'country': 'United States',
            'website': 'https://www.subzero-wolf.com',
            'headquarters': 'Madison, Wisconsin',
            'type': 'Specialty/Luxury',
            'revenue_usd': 2500000000,
            'source': 'Supplemental',
            'notes': 'Manufactures Sub-Zero, Wolf, and Cove brands, est. 2023 revenue'
        },
        {
            'name': 'Thermador',
            'country': 'United States',
            'website': 'https://www.thermador.com',
            'headquarters': 'Irvine, California',
            'type': 'Specialty/Luxury',
            'source': 'Supplemental',
            'notes': 'Owned by BSH (Bosch), premium cooking appliances'
        },
        {
            'name': 'Viking Range',
            'country': 'United States',
            'website': 'https://www.vikingrange.com',
            'headquarters': 'Greenwood, Mississippi',
            'type': 'Major Appliances (cooking)',
            'source': 'Supplemental',
            'notes': 'Owned by Middleby Corporation, professional-style ranges'
        },
        {
            'name': 'BlueStar',
            'country': 'United States',
            'website': 'https://www.bluestarcooking.com',
            'headquarters': 'Reading, Pennsylvania',
            'type': 'Major Appliances (cooking)',
            'source': 'Supplemental',
            'notes': 'Prizer-Painter Stove Works, founded 1880, professional ranges'
        },
        {
            'name': 'Brown Stove Works',
            'country': 'United States',
            'website': 'https://www.brownstoveworksinc.com',
            'headquarters': 'Cleveland, Tennessee',
            'type': 'Major Appliances (cooking)',
            'source': 'Supplemental',
            'notes': 'Manufactures FiveStar brand, founded 1935'
        },
        {
            'name': 'American Range',
            'country': 'United States',
            'website': 'https://americanrange.com',
            'headquarters': 'Pacoima, California',
            'type': 'Major Appliances (cooking)',
            'source': 'Supplemental',
            'notes': 'Founded 1982, acquired by Hatco 2022, commercial and residential'
        },

        # Specialty Manufacturers
        {
            'name': 'U-Line Corporation',
            'country': 'United States',
            'website': 'https://www.u-line.com',
            'headquarters': 'Milwaukee, Wisconsin',
            'type': 'Major Appliances (refrigeration)',
            'revenue_usd': 250000000,
            'source': 'Supplemental',
            'notes': 'Undercounter refrigeration, part of Middleby, est. revenue'
        },
        {
            'name': 'Speed Queen',
            'country': 'United States',
            'website': 'https://www.speedqueen.com',
            'headquarters': 'Ripon, Wisconsin',
            'type': 'Major Appliances (laundry)',
            'revenue_usd': 500000000,
            'source': 'Supplemental',
            'notes': 'Commercial and residential laundry, est. revenue'
        },

        # Small Appliances
        {
            'name': 'Hamilton Beach Brands',
            'country': 'United States',
            'website': 'https://hamiltonbeach.com',
            'headquarters': 'Glen Allen, Virginia',
            'type': 'Small Appliances',
            'revenue_usd': 600000000,
            'source': 'Supplemental',
            'notes': 'Small kitchen appliances, 2023 revenue'
        },

        # Outdoor Cooking
        {
            'name': 'Traeger Grills',
            'country': 'United States',
            'website': 'https://www.traeger.com',
            'headquarters': 'Salt Lake City, Utah',
            'type': 'Outdoor Cooking',
            'revenue_usd': 600000000,
            'source': 'Supplemental',
            'notes': 'Pellet grills, est. 2023 revenue'
        },

        # Canada
        {
            'name': 'Danby',
            'country': 'Canada',
            'website': 'https://www.danby.com',
            'headquarters': 'Guelph, Ontario',
            'type': 'Major Appliances (refrigeration)',
            'revenue_usd': 350000000,
            'source': 'Supplemental',
            'notes': 'Compact appliances, est. revenue'
        },
        {
            'name': 'Napoleon',
            'country': 'Canada',
            'website': 'https://www.napoleon.com',
            'headquarters': 'Barrie, Ontario',
            'type': 'Outdoor Cooking',
            'revenue_usd': 400000000,
            'source': 'Supplemental',
            'notes': 'Grills and outdoor cooking, est. revenue'
        },
        {
            'name': 'Elmira Stove Works',
            'country': 'Canada',
            'website': 'https://elmirastoveworks.com',
            'headquarters': 'Elmira, Ontario',
            'type': 'Major Appliances (cooking)',
            'source': 'Supplemental',
            'notes': 'Retro-inspired appliances'
        },
        {
            'name': 'Victory Range Hoods',
            'country': 'Canada',
            'headquarters': 'Coquitlam, British Columbia',
            'type': 'Major Appliances (cooking)',
            'source': 'Supplemental',
            'notes': 'Range hoods and ventilation'
        },

        # Mexico
        {
            'name': 'Mabe',
            'country': 'Mexico',
            'website': 'https://www.mabe.com.mx',
            'headquarters': 'Mexico City',
            'type': 'Major Appliances (full-line)',
            'revenue_usd': 2500000000,
            'founded': '1946',
            'source': 'Supplemental',
            'notes': 'Joint venture with GE (48% stake), exports to 70+ countries, est. revenue'
        },
        {
            'name': 'Acros',
            'country': 'Mexico',
            'type': 'Major Appliances (full-line)',
            'source': 'Supplemental',
            'notes': 'Formerly owned by Vitro, associated with Whirlpool'
        }
    ]


# =============================================================================
# DATA PROCESSING
# =============================================================================

def classify_business_models(name: str, notes: str = '', mfr_type: str = '') -> list:
    """
    Classify manufacturer business model(s) based on name, notes, and type.
    A manufacturer can have multiple business models.

    Args:
        name: Manufacturer name
        notes: Additional notes about the manufacturer
        mfr_type: Product type classification

    Returns:
        List of business model classifications
    """
    name_lower = name.lower()
    notes_lower = notes.lower() if notes else ''
    type_lower = mfr_type.lower() if mfr_type else ''
    combined = f"{name_lower} {notes_lower} {type_lower}"

    models = []

    # Vertically Integrated Manufacturer (Brand Owner + Manufacturer)
    vertically_integrated = [
        'whirlpool', 'ge appliances', 'electrolux', 'frigidaire', 'sub-zero',
        'wolf', 'thermador', 'bosch', 'kitchenaid', 'maytag', 'lg electronics',
        'samsung', 'viking', 'speed queen', 'traeger', 'weber', 'napoleon',
        'danby', 'bluestar', 'brown stove', 'american range', 'dacor',
        'alliance laundry', 'elmira stove'
    ]
    if any(brand in name_lower for brand in vertically_integrated):
        models.append('Vertically Integrated Manufacturer')

    # Brand Owner (Outsourced Manufacturing / Asset-Light)
    if any(keyword in combined for keyword in ['brand owner', 'holding company', 'spectrum brands', 'newell brands', 'owns', 'parent company']):
        if 'Vertically Integrated Manufacturer' not in models:
            models.append('Brand Owner (Outsourced Manufacturing)')

    # OEM (Build-to-Spec Manufacturer)
    if any(keyword in combined for keyword in ['oem', 'original equipment', 'contract manufacturer', 'builds appliances', 'midea', 'galanz', 'tti floor care']):
        models.append('OEM (Build-to-Spec)')

    # ODM (Design + Manufacture Platform)
    if any(keyword in combined for keyword in ['odm', 'design manufacturer', 'js global', 'sharkninja', 'designs and manufactures']):
        models.append('ODM (Design + Manufacture)')

    # Private Label / House Brand Manufacturer
    if any(keyword in combined for keyword in ['private label', 'house brand', 'contract manufacturing for retailers']):
        models.append('Private Label Manufacturer')

    # Importer / Brand Licensee
    if any(keyword in combined for keyword in ['importer', 'brand licensee', 'import', 'distribut']):
        models.append('Importer / Brand Licensee')

    # If no specific business model identified, default based on company characteristics
    if not models:
        # Check if they own major brands (likely vertically integrated)
        major_brands = ['whirlpool', 'maytag', 'kitchenaid', 'ge', 'frigidaire', 'electrolux']
        if any(brand in name_lower for brand in major_brands):
            models.append('Vertically Integrated Manufacturer')
        else:
            models.append('Manufacturer')

    return models


def classify_manufacturer_type(name: str, notes: str = '') -> str:
    """
    Classify manufacturer product type based on name and notes.
    This is the PRODUCT type, not business model.

    Args:
        name: Manufacturer name
        notes: Additional notes about the manufacturer

    Returns:
        Product type classification
    """
    name_lower = name.lower()
    notes_lower = notes.lower() if notes else ''
    combined = f"{name_lower} {notes_lower}"

    # Vacuum and Floor Care
    if any(keyword in combined for keyword in ['vacuum', 'hoover', 'bissell', 'eureka', 'kirby', 'dirt devil', 'shark', 'irobot', 'roomba', 'floor care']):
        return 'Vacuum/Floor Care'

    # Small Appliances
    if any(keyword in combined for keyword in ['blender', 'mixer', 'toaster', 'coffee', 'cuisinart', 'hamilton beach', 'oster', 'sunbeam', 'proctor', 'vitamix', 'farberware', 'presto', 'rival', 'aroma', 'small appliance', 'small kitchen']):
        return 'Small Appliances'

    # Outdoor Cooking
    if any(keyword in combined for keyword in ['grill', 'bbq', 'outdoor', 'traeger', 'napoleon', 'pellet', 'weber']):
        return 'Outdoor Cooking'

    # Laundry Specialists
    if any(keyword in combined for keyword in ['laundry', 'washing', 'dryer', 'speed queen', 'alliance laundry']):
        return 'Major Appliances (laundry)'

    # Cooking Specialists
    if any(keyword in combined for keyword in ['range', 'stove', 'oven', 'cooktop', 'cooking', 'viking', 'bluestar', 'wolf', 'thermador', 'dacor', 'american range', 'brown stove', 'elmira stove']):
        return 'Major Appliances (cooking)'

    # Refrigeration Specialists
    if any(keyword in combined for keyword in ['refrigerat', 'freezer', 'ice', 'sub-zero', 'u-line', 'danby', 'summit']):
        return 'Major Appliances (refrigeration)'

    # Luxury/Specialty
    if any(keyword in combined for keyword in ['luxury', 'premium', 'professional', 'jenn-air', 'monogram', 'cove']):
        return 'Specialty/Luxury'

    # Major Full-Line Manufacturers
    if any(keyword in combined for keyword in ['whirlpool', 'ge appliance', 'electrolux', 'frigidaire', 'maytag', 'mabe', 'kenmore', 'full-line']):
        return 'Major Appliances (full-line)'

    # Default to Major Appliances
    return 'Major Appliances'


def normalize_name(name: str) -> str:
    """
    Normalize manufacturer name for deduplication.

    Args:
        name: Original manufacturer name

    Returns:
        Normalized name (lowercase, no business suffixes)
    """
    name = name.lower().strip()
    suffixes = [
        r'\bcorporation\b', r'\bcorp\.?\b', r'\binc\.?\b', r'\bincorporated\b',
        r'\bllc\.?\b', r'\bltd\.?\b', r'\blimited\b', r'\bco\.?\b',
        r'\bcompany\b', r'\b&\s*co\.?\b', r'\bgroup\b', r'\bbrands\b'
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def merge_manufacturers(all_sources: List[List[Dict]]) -> List[Dict]:
    """
    Merge and deduplicate manufacturers from multiple sources.

    Args:
        all_sources: List of manufacturer lists from different sources

    Returns:
        Deduplicated list of manufacturers
    """
    print("\nMerging and deduplicating manufacturer data...")

    all_manufacturers = []
    for source_list in all_sources:
        all_manufacturers.extend(source_list)

    print(f"  Total entries before deduplication: {len(all_manufacturers)}")

    # Group by normalized name
    grouped = {}
    for mfr in all_manufacturers:
        norm_name = normalize_name(mfr['name'])
        if norm_name not in grouped:
            grouped[norm_name] = []
        grouped[norm_name].append(mfr)

    # Merge duplicates
    merged = []
    for norm_name, duplicates in grouped.items():
        if len(duplicates) == 1:
            merged.append(duplicates[0])
        else:
            # Merge data from duplicates
            base = duplicates[0].copy()

            # Prefer longer/more complete name
            base['name'] = max([d['name'] for d in duplicates], key=len)

            # Collect sources
            sources = []
            for d in duplicates:
                if 'source' in d:
                    sources.append(d['source'])
                if 'sources' in d:
                    sources.extend(d['sources'])
            if len(sources) > 1:
                base['sources'] = list(set(sources))
            elif sources:
                base['source'] = sources[0]

            # Merge other fields (prefer non-empty values)
            for key in ['website', 'headquarters', 'founded', 'wikipedia_url', 'wikidata_id', 'notes', 'type', 'revenue_usd']:
                for d in duplicates:
                    if key in d and d[key] and key not in base:
                        base[key] = d[key]

            merged.append(base)

    print(f"  Total after deduplication: {len(merged)}")
    return merged


def organize_by_country(manufacturers: List[Dict]) -> Dict:
    """
    Organize manufacturers by country and add type classification.

    Args:
        manufacturers: List of manufacturer dictionaries

    Returns:
        Dictionary with country names as keys
    """
    by_country = {
        'United States': {'manufacturers': []},
        'Canada': {'manufacturers': []},
        'Mexico': {'manufacturers': []}
    }

    for mfr in manufacturers:
        country = mfr.pop('country', 'United States')

        # Add type classification if not already present
        if 'type' not in mfr:
            mfr['type'] = classify_manufacturer_type(
                mfr.get('name', ''),
                mfr.get('notes', '')
            )

        # Add business model classification
        if 'business_models' not in mfr:
            mfr['business_models'] = classify_business_models(
                mfr.get('name', ''),
                mfr.get('notes', ''),
                mfr.get('type', '')
            )

        by_country[country]['manufacturers'].append(mfr)

    # Sort manufacturers by name within each country
    for country in by_country:
        by_country[country]['manufacturers'].sort(key=lambda x: x['name'])

    return by_country


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def generate_manufacturers_data() -> Optional[Dict]:
    """
    Generate complete manufacturers dataset from all sources.

    Returns:
        Complete dataset dictionary or None if failed
    """
    print("\n" + "="*70)
    print("FETCHING MANUFACTURER DATA FROM MULTIPLE SOURCES")
    print("="*70)

    sources = []

    # Source 1: Wikidata
    print("\nFetching manufacturers from Wikidata SPARQL API...")
    wikidata_manufacturers = fetch_manufacturers_from_wikidata()
    if wikidata_manufacturers:
        print(f"  Found {len(wikidata_manufacturers)} manufacturers from Wikidata")
        sources.append(wikidata_manufacturers)

    # Source 2: Wikipedia
    print("Fetching manufacturers from Wikipedia categories...")
    wikipedia_manufacturers = fetch_manufacturers_from_wikipedia()
    if wikipedia_manufacturers:
        sources.append(wikipedia_manufacturers)

    # Source 3: Supplemental data
    print("Adding supplemental data for major manufacturers...")
    supplemental_manufacturers = get_supplemental_manufacturers()
    print(f"  Added {len(supplemental_manufacturers)} supplemental manufacturers")
    sources.append(supplemental_manufacturers)

    if not sources:
        print("\nERROR: Failed to fetch data from any source.")
        return None

    # Merge and deduplicate
    merged_manufacturers = merge_manufacturers(sources)

    # Enrich with website URLs
    merged_manufacturers = enrich_manufacturers_with_websites(merged_manufacturers)

    # Organize by country
    by_country = organize_by_country(merged_manufacturers)
    total_count = sum(len(country_data["manufacturers"]) for country_data in by_country.values())

    print("\n" + "="*70)
    print(f"SUMMARY: Successfully compiled {total_count} manufacturers")
    print("="*70)

    return {
        "metadata": {
            "date_pulled": datetime.now(timezone.utc).isoformat(),
            "source": "Python Script",
            "sources": ["Wikidata", "Wikipedia", "AHAM", "Manual Curation"],
            "total_manufacturers": total_count,
            "license": "CC0 1.0 / Creative Commons (varies by source)",
            "license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
            "note": "Data aggregated from multiple sources. Accuracy may vary."
        },
        "North America": by_country
    }


def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("NORTH AMERICAN APPLIANCE MANUFACTURERS DATA GENERATOR")
    print("="*70)

    # Generate data
    data = generate_manufacturers_data()

    if not data:
        print("\n[ERROR] Failed to generate manufacturers data. Exiting.")
        return

    # Create output directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    os.makedirs(data_dir, exist_ok=True)

    # Write to file
    output_file = os.path.join(data_dir, 'makers.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Print summary
    north_america = data['North America']
    print(f"\n[SUCCESS] Generated: {output_file}")
    print(f"\nStatistics:")
    print(f"  - Date pulled: {data['metadata']['date_pulled']}")
    print(f"  - Total manufacturers: {data['metadata']['total_manufacturers']}")
    print(f"  - United States: {len(north_america['United States']['manufacturers'])} manufacturers")
    print(f"  - Canada: {len(north_america['Canada']['manufacturers'])} manufacturers")
    print(f"  - Mexico: {len(north_america['Mexico']['manufacturers'])} manufacturers")
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    main()
