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
    data/partners.json

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

def get_supplemental_partners() -> List[Dict]:
    """
    Comprehensive partner data across all partner types.

    Partner Types (from partner_type.json):
        Makers: Vertically Integrated, Brand Owner, OEM, ODM, Private Label, Importer
        Middle: Distributor, Buying Group, Rep Firm, 3PL
        Sellers: Retailer, Dealer, Franchisee
        Projects: Designer, Builder, GC, Installer, Service Provider
        People: Sales Associate, Influencer

    Returns:
        List of partner dictionaries
    """
    return [
        # =========================================================================
        # MAKERS - Vertically Integrated Manufacturers
        # =========================================================================
        {
            'name': 'Whirlpool Corporation',
            'country': 'United States',
            'website': 'https://www.whirlpoolcorp.com',
            'headquarters': 'Benton Harbor, Michigan',
            'type': 'Major Appliances (full-line)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 19400000000,
            'source': 'Supplemental',
            'notes': 'World\'s largest appliance manufacturer. Owns Whirlpool, Maytag, KitchenAid, Amana, JennAir'
        },
        {
            'name': 'GE Appliances',
            'country': 'United States',
            'website': 'https://www.geappliances.com',
            'headquarters': 'Louisville, Kentucky',
            'type': 'Major Appliances (full-line)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 8000000000,
            'source': 'Supplemental',
            'notes': 'Owned by Haier. Owns GE, Monogram, Café, Profile, Hotpoint brands'
        },
        {
            'name': 'Electrolux North America',
            'country': 'United States',
            'website': 'https://www.electrolux.com',
            'headquarters': 'Charlotte, North Carolina',
            'type': 'Major Appliances (full-line)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 5000000000,
            'source': 'Supplemental',
            'notes': 'Owns Frigidaire, Electrolux brands'
        },
        {
            'name': 'BSH Home Appliances',
            'country': 'United States',
            'website': 'https://www.bsh-group.com',
            'headquarters': 'Irvine, California',
            'type': 'Major Appliances (full-line)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 4500000000,
            'source': 'Supplemental',
            'notes': 'Owns Bosch, Thermador, Gaggenau brands in North America'
        },
        {
            'name': 'LG Electronics USA',
            'country': 'United States',
            'website': 'https://www.lg.com/us',
            'headquarters': 'Englewood Cliffs, New Jersey',
            'type': 'Major Appliances (full-line)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 7000000000,
            'source': 'Supplemental',
            'notes': 'Major appliance manufacturer with US production in Tennessee'
        },
        {
            'name': 'Samsung Electronics America',
            'country': 'United States',
            'website': 'https://www.samsung.com/us',
            'headquarters': 'Ridgefield Park, New Jersey',
            'type': 'Major Appliances (full-line)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 6500000000,
            'source': 'Supplemental',
            'notes': 'Major appliance manufacturer with US production in South Carolina'
        },
        {
            'name': 'Sub-Zero Group',
            'country': 'United States',
            'website': 'https://www.subzero-wolf.com',
            'headquarters': 'Madison, Wisconsin',
            'type': 'Specialty/Luxury',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 2500000000,
            'source': 'Supplemental',
            'notes': 'Premium appliances. Owns Sub-Zero, Wolf, Cove brands. Made in USA'
        },
        {
            'name': 'Viking Range',
            'country': 'United States',
            'website': 'https://www.vikingrange.com',
            'headquarters': 'Greenwood, Mississippi',
            'type': 'Major Appliances (cooking)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'source': 'Supplemental',
            'notes': 'Owned by Middleby Corporation. Professional-style ranges'
        },
        {
            'name': 'BlueStar',
            'country': 'United States',
            'website': 'https://www.bluestarcooking.com',
            'headquarters': 'Reading, Pennsylvania',
            'type': 'Major Appliances (cooking)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'source': 'Supplemental',
            'notes': 'Prizer-Painter Stove Works. Professional ranges made in USA since 1880'
        },
        {
            'name': 'Speed Queen',
            'country': 'United States',
            'website': 'https://www.speedqueen.com',
            'headquarters': 'Ripon, Wisconsin',
            'type': 'Major Appliances (laundry)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 500000000,
            'source': 'Supplemental',
            'notes': 'Alliance Laundry Systems brand. Commercial and residential laundry'
        },
        {
            'name': 'Traeger Grills',
            'country': 'United States',
            'website': 'https://www.traeger.com',
            'headquarters': 'Salt Lake City, Utah',
            'type': 'Outdoor Cooking',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 600000000,
            'source': 'Supplemental',
            'notes': 'Pellet grill pioneer'
        },
        {
            'name': 'Weber-Stephen Products',
            'country': 'United States',
            'website': 'https://www.weber.com',
            'headquarters': 'Palatine, Illinois',
            'type': 'Outdoor Cooking',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 1800000000,
            'source': 'Supplemental',
            'notes': 'Leading grill manufacturer'
        },
        {
            'name': 'Danby',
            'country': 'Canada',
            'website': 'https://www.danby.com',
            'headquarters': 'Guelph, Ontario',
            'type': 'Major Appliances (refrigeration)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 350000000,
            'source': 'Supplemental',
            'notes': 'Compact and specialty appliances'
        },
        {
            'name': 'Napoleon',
            'country': 'Canada',
            'website': 'https://www.napoleon.com',
            'headquarters': 'Barrie, Ontario',
            'type': 'Outdoor Cooking',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 400000000,
            'source': 'Supplemental',
            'notes': 'Grills, fireplaces, HVAC'
        },
        {
            'name': 'Mabe',
            'country': 'Mexico',
            'website': 'https://www.mabe.com.mx',
            'headquarters': 'Mexico City',
            'type': 'Major Appliances (full-line)',
            'sub_type': ['Vertically Integrated Manufacturer'],
            'revenue_usd': 2500000000,
            'source': 'Supplemental',
            'notes': 'Joint venture with GE. Major Latin American manufacturer'
        },

        # =========================================================================
        # MAKERS - Brand Owners (Outsourced Manufacturing)
        # =========================================================================
        {
            'name': 'Spectrum Brands',
            'country': 'United States',
            'website': 'https://www.spectrumbrands.com',
            'headquarters': 'Middleton, Wisconsin',
            'type': 'Brand Owner',
            'sub_type': ['Brand Owner'],
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
            'sub_type': ['Brand Owner'],
            'revenue_usd': 8600000000,
            'source': 'Supplemental',
            'notes': 'Owns Crock-Pot, Mr. Coffee, Oster, Sunbeam, FoodSaver brands'
        },
        {
            'name': 'Hamilton Beach Brands',
            'country': 'United States',
            'website': 'https://hamiltonbeach.com',
            'headquarters': 'Glen Allen, Virginia',
            'type': 'Small Appliances',
            'sub_type': ['Brand Owner'],
            'revenue_usd': 600000000,
            'source': 'Supplemental',
            'notes': 'Small kitchen appliances'
        },

        # =========================================================================
        # MAKERS - OEM (Build-to-Spec)
        # =========================================================================
        {
            'name': 'Midea Group',
            'country': 'United States',
            'website': 'https://www.midea.com',
            'headquarters': 'Parsippany, New Jersey',
            'type': 'OEM',
            'sub_type': ['OEM'],
            'revenue_usd': 52000000000,
            'source': 'Supplemental',
            'notes': 'Major OEM for private label appliances. Owns Toshiba appliances'
        },
        {
            'name': 'Galanz Americas',
            'country': 'United States',
            'website': 'https://www.galanzamericas.com',
            'headquarters': 'Irvine, California',
            'type': 'OEM',
            'sub_type': ['OEM'],
            'source': 'Supplemental',
            'notes': 'OEM specializing in microwaves and small appliances'
        },
        {
            'name': 'TTI Floor Care',
            'country': 'United States',
            'website': 'https://www.ttifloorcare.com',
            'headquarters': 'Charlotte, North Carolina',
            'type': 'OEM',
            'sub_type': ['OEM'],
            'revenue_usd': 1500000000,
            'source': 'Supplemental',
            'notes': 'OEM for Hoover, Dirt Devil, Oreck brands'
        },

        # =========================================================================
        # MAKERS - ODM (Design + Manufacture)
        # =========================================================================
        {
            'name': 'JS Global (SharkNinja)',
            'country': 'United States',
            'website': 'https://www.sharkninja.com',
            'headquarters': 'Needham, Massachusetts',
            'type': 'ODM',
            'sub_type': ['ODM'],
            'revenue_usd': 4000000000,
            'source': 'Supplemental',
            'notes': 'Designs and manufactures Shark and Ninja brands'
        },

        # =========================================================================
        # MIDDLE - Distributors / Wholesalers
        # =========================================================================
        {
            'name': 'DERA (Distributor Efficiency & Resource Alliance)',
            'country': 'United States',
            'website': 'https://www.dera.com',
            'headquarters': 'Dallas, Texas',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'source': 'Supplemental',
            'notes': 'Major appliance distributor alliance'
        },
        {
            'name': '!"Almo Corporation',
            'country': 'United States',
            'website': 'https://www.almo.com',
            'headquarters': 'Philadelphia, Pennsylvania',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'revenue_usd': 2500000000,
            'source': 'Supplemental',
            'notes': 'Major appliance and consumer electronics distributor'
        },
        {
            'name': 'D&H Distributing',
            'country': 'United States',
            'website': 'https://www.dandh.com',
            'headquarters': 'Harrisburg, Pennsylvania',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'revenue_usd': 7000000000,
            'source': 'Supplemental',
            'notes': 'Technology and appliance distributor'
        },
        {
            'name': 'Pacific Sales',
            'country': 'United States',
            'website': 'https://www.pacificsales.com',
            'headquarters': 'Torrance, California',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'source': 'Supplemental',
            'notes': 'Premium appliance distributor. Best Buy subsidiary'
        },
        {
            'name': 'Warners\' Stellian',
            'country': 'United States',
            'website': 'https://www.warnersstellian.com',
            'headquarters': 'Saint Paul, Minnesota',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'source': 'Supplemental',
            'notes': 'Regional appliance distributor and retailer'
        },
        {
            'name': 'Marcone Supply',
            'country': 'United States',
            'website': 'https://www.marcone.com',
            'headquarters': 'Saint Louis, Missouri',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'revenue_usd': 1000000000,
            'source': 'Supplemental',
            'notes': 'Appliance parts distributor'
        },
        {
            'name': 'Reliable Parts',
            'country': 'United States',
            'website': 'https://www.reliableparts.com',
            'headquarters': 'Hayward, California',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'source': 'Supplemental',
            'notes': 'Appliance parts distributor'
        },
        {
            'name': 'DERA Canada',
            'country': 'Canada',
            'website': 'https://www.dera.ca',
            'headquarters': 'Toronto, Ontario',
            'type': 'Distributor',
            'sub_type': ['Distributor / Wholesaler'],
            'source': 'Supplemental',
            'notes': 'Canadian appliance distributor alliance'
        },

        # =========================================================================
        # MIDDLE - Buying Groups / Coops
        # =========================================================================
        {
            'name': 'Nationwide Marketing Group (NMG)',
            'country': 'United States',
            'website': 'https://www.nationwidegroup.org',
            'headquarters': 'Winston-Salem, North Carolina',
            'type': 'Buying Group',
            'sub_type': ['Buying Group / Coop'],
            'source': 'Supplemental',
            'notes': 'Largest buying group. 5,000+ independent retailers'
        },
        {
            'name': 'BrandSource',
            'country': 'United States',
            'website': 'https://www.brandsource.com',
            'headquarters': 'Anaheim, California',
            'type': 'Buying Group',
            'sub_type': ['Buying Group / Coop'],
            'source': 'Supplemental',
            'notes': 'Major buying group. ~4,000 member locations'
        },
        {
            'name': 'MEGA Group USA',
            'country': 'United States',
            'website': 'https://www.megagroupusa.com',
            'headquarters': 'Dallas, Texas',
            'type': 'Buying Group',
            'sub_type': ['Buying Group / Coop'],
            'source': 'Supplemental',
            'notes': 'Independent dealer buying group'
        },
        {
            'name': 'AVB BrandSource',
            'country': 'United States',
            'website': 'https://www.avb.com',
            'headquarters': 'Anaheim, California',
            'type': 'Buying Group',
            'sub_type': ['Buying Group / Coop'],
            'source': 'Supplemental',
            'notes': 'Appliance, furniture, electronics buying group'
        },
        {
            'name': 'NECO Alliance',
            'country': 'United States',
            'website': 'https://www.necoalliance.com',
            'headquarters': 'Various',
            'type': 'Buying Group',
            'sub_type': ['Buying Group / Coop'],
            'source': 'Supplemental',
            'notes': 'Regional buying cooperative'
        },
        {
            'name': 'CANTREX Nationwide',
            'country': 'Canada',
            'website': 'https://www.cantrex.com',
            'headquarters': 'Mississauga, Ontario',
            'type': 'Buying Group',
            'sub_type': ['Buying Group / Coop'],
            'source': 'Supplemental',
            'notes': 'Canadian buying group. Part of Nationwide'
        },
        {
            'name': 'Mega Group Canada',
            'country': 'Canada',
            'website': 'https://www.megagroupcanada.com',
            'headquarters': 'Vancouver, British Columbia',
            'type': 'Buying Group',
            'sub_type': ['Buying Group / Coop'],
            'source': 'Supplemental',
            'notes': 'Canadian independent dealer buying group'
        },

        # =========================================================================
        # MIDDLE - Rep Firms / Manufacturer's Reps
        # =========================================================================
        {
            'name': 'Springboard Brand & Creative Strategy',
            'country': 'United States',
            'website': 'https://www.springboardbrands.com',
            'headquarters': 'Dallas, Texas',
            'type': 'Rep Firm',
            'sub_type': ['Rep Firm / Manufacturer\'s Rep'],
            'source': 'Supplemental',
            'notes': 'Appliance manufacturer rep firm'
        },
        {
            'name': 'The Hartman Company',
            'country': 'United States',
            'website': 'https://www.hartmanco.com',
            'headquarters': 'Chicago, Illinois',
            'type': 'Rep Firm',
            'sub_type': ['Rep Firm / Manufacturer\'s Rep'],
            'source': 'Supplemental',
            'notes': 'Midwest appliance rep firm'
        },
        {
            'name': 'Thomas Associates',
            'country': 'United States',
            'headquarters': 'Various',
            'type': 'Rep Firm',
            'sub_type': ['Rep Firm / Manufacturer\'s Rep'],
            'source': 'Supplemental',
            'notes': 'Regional manufacturer rep firm'
        },

        # =========================================================================
        # SELLERS - Retailers (Big Box / Regional / Online)
        # =========================================================================
        {
            'name': 'Best Buy',
            'country': 'United States',
            'website': 'https://www.bestbuy.com',
            'headquarters': 'Richfield, Minnesota',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'revenue_usd': 46000000000,
            'source': 'Supplemental',
            'notes': 'Major electronics and appliance retailer. ~1,000 stores'
        },
        {
            'name': 'The Home Depot',
            'country': 'United States',
            'website': 'https://www.homedepot.com',
            'headquarters': 'Atlanta, Georgia',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'revenue_usd': 157000000000,
            'source': 'Supplemental',
            'notes': 'Largest home improvement retailer. Major appliance seller'
        },
        {
            'name': 'Lowe\'s',
            'country': 'United States',
            'website': 'https://www.lowes.com',
            'headquarters': 'Mooresville, North Carolina',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'revenue_usd': 86000000000,
            'source': 'Supplemental',
            'notes': 'Major home improvement and appliance retailer'
        },
        {
            'name': 'Costco',
            'country': 'United States',
            'website': 'https://www.costco.com',
            'headquarters': 'Issaquah, Washington',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'revenue_usd': 242000000000,
            'source': 'Supplemental',
            'notes': 'Warehouse club with major appliance sales'
        },
        {
            'name': 'Amazon',
            'country': 'United States',
            'website': 'https://www.amazon.com',
            'headquarters': 'Seattle, Washington',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'revenue_usd': 575000000000,
            'source': 'Supplemental',
            'notes': 'Largest online retailer. Growing appliance category'
        },
        {
            'name': 'Nebraska Furniture Mart',
            'country': 'United States',
            'website': 'https://www.nfm.com',
            'headquarters': 'Omaha, Nebraska',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'source': 'Supplemental',
            'notes': 'Berkshire Hathaway company. Major appliance retailer'
        },
        {
            'name': 'RC Willey',
            'country': 'United States',
            'website': 'https://www.rcwilley.com',
            'headquarters': 'Salt Lake City, Utah',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'source': 'Supplemental',
            'notes': 'Berkshire Hathaway company. Western US appliance retailer'
        },
        {
            'name': 'Canadian Tire',
            'country': 'Canada',
            'website': 'https://www.canadiantire.ca',
            'headquarters': 'Toronto, Ontario',
            'type': 'Retailer',
            'sub_type': ['Retailer'],
            'revenue_usd': 12000000000,
            'source': 'Supplemental',
            'notes': 'Major Canadian retailer with appliances'
        },

        # =========================================================================
        # SELLERS - Dealers / Independent Showrooms
        # =========================================================================
        {
            'name': 'Abt Electronics',
            'country': 'United States',
            'website': 'https://www.abt.com',
            'headquarters': 'Glenview, Illinois',
            'type': 'Dealer',
            'sub_type': ['Dealer / Independent Showroom'],
            'revenue_usd': 500000000,
            'source': 'Supplemental',
            'notes': 'Premium independent appliance dealer'
        },
        {
            'name': 'P.C. Richard & Son',
            'country': 'United States',
            'website': 'https://www.pcrichard.com',
            'headquarters': 'Farmingdale, New York',
            'type': 'Dealer',
            'sub_type': ['Dealer / Independent Showroom'],
            'revenue_usd': 800000000,
            'source': 'Supplemental',
            'notes': 'Northeast US appliance and electronics dealer'
        },
        {
            'name': 'Yale Appliance',
            'country': 'United States',
            'website': 'https://www.yaleappliance.com',
            'headquarters': 'Boston, Massachusetts',
            'type': 'Dealer',
            'sub_type': ['Dealer / Independent Showroom'],
            'source': 'Supplemental',
            'notes': 'Premium appliance dealer. Strong content marketing'
        },
        {
            'name': 'Albert Lee Appliance',
            'country': 'United States',
            'website': 'https://www.quiteapossibly.com',
            'headquarters': 'Seattle, Washington',
            'type': 'Dealer',
            'sub_type': ['Dealer / Independent Showroom'],
            'source': 'Supplemental',
            'notes': 'Pacific Northwest premium dealer'
        },
        {
            'name': 'Trail Appliances',
            'country': 'Canada',
            'website': 'https://www.trailappliances.com',
            'headquarters': 'Vancouver, British Columbia',
            'type': 'Dealer',
            'sub_type': ['Dealer / Independent Showroom'],
            'source': 'Supplemental',
            'notes': 'Western Canada premium appliance dealer'
        },
        {
            'name': 'Tasco Appliances',
            'country': 'Canada',
            'website': 'https://www.tasco.ca',
            'headquarters': 'Toronto, Ontario',
            'type': 'Dealer',
            'sub_type': ['Dealer / Independent Showroom'],
            'source': 'Supplemental',
            'notes': 'Ontario premium appliance dealer'
        },

        # =========================================================================
        # SELLERS - Franchisees
        # =========================================================================
        {
            'name': 'Sears Hometown Stores',
            'country': 'United States',
            'website': 'https://www.searshometownstores.com',
            'headquarters': 'Various',
            'type': 'Franchisee',
            'sub_type': ['Franchisee'],
            'source': 'Supplemental',
            'notes': 'Franchised appliance stores'
        },

        # =========================================================================
        # PROJECTS - Builders / Developers
        # =========================================================================
        {
            'name': 'D.R. Horton',
            'country': 'United States',
            'website': 'https://www.drhorton.com',
            'headquarters': 'Arlington, Texas',
            'type': 'Builder',
            'sub_type': ['Builder / Developer'],
            'revenue_usd': 36000000000,
            'source': 'Supplemental',
            'notes': 'Largest US homebuilder. Major appliance buyer'
        },
        {
            'name': 'Lennar',
            'country': 'United States',
            'website': 'https://www.lennar.com',
            'headquarters': 'Miami, Florida',
            'type': 'Builder',
            'sub_type': ['Builder / Developer'],
            'revenue_usd': 34000000000,
            'source': 'Supplemental',
            'notes': 'Major national homebuilder'
        },
        {
            'name': 'PulteGroup',
            'country': 'United States',
            'website': 'https://www.pultegroupinc.com',
            'headquarters': 'Atlanta, Georgia',
            'type': 'Builder',
            'sub_type': ['Builder / Developer'],
            'revenue_usd': 16000000000,
            'source': 'Supplemental',
            'notes': 'National homebuilder. Pulte Homes, Del Webb, Centex brands'
        },
        {
            'name': 'NVR Inc.',
            'country': 'United States',
            'website': 'https://www.nvrinc.com',
            'headquarters': 'Reston, Virginia',
            'type': 'Builder',
            'sub_type': ['Builder / Developer'],
            'revenue_usd': 10000000000,
            'source': 'Supplemental',
            'notes': 'Ryan Homes, NVHomes brands'
        },
        {
            'name': 'Toll Brothers',
            'country': 'United States',
            'website': 'https://www.tollbrothers.com',
            'headquarters': 'Fort Washington, Pennsylvania',
            'type': 'Builder',
            'sub_type': ['Builder / Developer'],
            'revenue_usd': 10000000000,
            'source': 'Supplemental',
            'notes': 'Luxury homebuilder. Premium appliance packages'
        },
        {
            'name': 'Mattamy Homes',
            'country': 'Canada',
            'website': 'https://www.mattamyhomes.com',
            'headquarters': 'Toronto, Ontario',
            'type': 'Builder',
            'sub_type': ['Builder / Developer'],
            'source': 'Supplemental',
            'notes': 'Largest privately owned homebuilder in North America'
        },

        # =========================================================================
        # PROJECTS - Authorized Service Providers
        # =========================================================================
        {
            'name': 'A&E Factory Service',
            'country': 'United States',
            'website': 'https://www.aefactoryservice.com',
            'headquarters': 'Various',
            'type': 'Service Provider',
            'sub_type': ['Authorized Service Provider'],
            'source': 'Supplemental',
            'notes': 'Major appliance service provider. Transformco company'
        },
        {
            'name': 'Mr. Appliance',
            'country': 'United States',
            'website': 'https://www.mrappliance.com',
            'headquarters': 'Waco, Texas',
            'type': 'Service Provider',
            'sub_type': ['Authorized Service Provider'],
            'source': 'Supplemental',
            'notes': 'Appliance repair franchise. Neighborly brand'
        },
        {
            'name': 'Sears Home Services',
            'country': 'United States',
            'website': 'https://www.searshomeservices.com',
            'headquarters': 'Various',
            'type': 'Service Provider',
            'sub_type': ['Authorized Service Provider'],
            'source': 'Supplemental',
            'notes': 'Appliance repair and service network'
        },

        # =========================================================================
        # PROJECTS - Installers
        # =========================================================================
        {
            'name': 'Installation Made Easy (IME)',
            'country': 'United States',
            'website': 'https://www.imehome.com',
            'headquarters': 'Minneapolis, Minnesota',
            'type': 'Installer',
            'sub_type': ['Installer / Delivery & Install Partner'],
            'source': 'Supplemental',
            'notes': 'Major appliance installation network'
        },
        {
            'name': 'ServiceLive',
            'country': 'United States',
            'website': 'https://www.servicelive.com',
            'headquarters': 'Troy, Michigan',
            'type': 'Installer',
            'sub_type': ['Installer / Delivery & Install Partner'],
            'source': 'Supplemental',
            'notes': 'Installation and service marketplace'
        },

        # =========================================================================
        # MIDDLE - 3PL / Logistics Providers
        # =========================================================================
        {
            'name': 'XPO Logistics',
            'country': 'United States',
            'website': 'https://www.xpo.com',
            'headquarters': 'Greenwich, Connecticut',
            'type': '3PL',
            'sub_type': ['3PL / Logistics Provider'],
            'revenue_usd': 7700000000,
            'source': 'Supplemental',
            'notes': 'Major last-mile delivery provider for appliances'
        },
        {
            'name': 'J.B. Hunt Transport',
            'country': 'United States',
            'website': 'https://www.jbhunt.com',
            'headquarters': 'Lowell, Arkansas',
            'type': '3PL',
            'sub_type': ['3PL / Logistics Provider'],
            'revenue_usd': 15000000000,
            'source': 'Supplemental',
            'notes': 'Final mile delivery services'
        },
        {
            'name': 'Ryder System',
            'country': 'United States',
            'website': 'https://www.ryder.com',
            'headquarters': 'Miami, Florida',
            'type': '3PL',
            'sub_type': ['3PL / Logistics Provider'],
            'revenue_usd': 11000000000,
            'source': 'Supplemental',
            'notes': 'Last mile and white glove delivery services'
        },
        {
            'name': 'MXD Group',
            'country': 'United States',
            'website': 'https://www.mxdgroup.com',
            'headquarters': 'St. Louis, Missouri',
            'type': '3PL',
            'sub_type': ['3PL / Logistics Provider'],
            'source': 'Supplemental',
            'notes': 'Appliance last-mile delivery specialist'
        }
    ]


# =============================================================================
# DATA PROCESSING
# =============================================================================

def load_partner_types() -> Dict:
    """
    Load partner types from partner_type.json.

    Returns:
        Dictionary with sub_type names as keys and their data as values
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    partner_type_file = os.path.join(data_dir, 'partner_type.json')

    try:
        with open(partner_type_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Extract Makers subtypes into a lookup dict
        subtypes = {}
        for partner_type in data.get('partner_types', []):
            if partner_type.get('type') == 'Makers':
                for subtype in partner_type.get('subtypes', []):
                    subtypes[subtype['sub_type']] = subtype
        return subtypes
    except Exception as e:
        print(f"  Warning: Could not load partner_type.json: {e}")
        return {}


# Load partner types at module level
PARTNER_SUBTYPES = None


def get_partner_subtypes() -> Dict:
    """Get partner subtypes, loading from file if needed."""
    global PARTNER_SUBTYPES
    if PARTNER_SUBTYPES is None:
        PARTNER_SUBTYPES = load_partner_types()
    return PARTNER_SUBTYPES


def classify_sub_type(name: str, notes: str = '', mfr_type: str = '') -> List[str]:
    """
    Classify manufacturer sub_type based on name, notes, and type.
    Uses sub_type values from partner_type.json.

    Args:
        name: Manufacturer name
        notes: Additional notes about the manufacturer
        mfr_type: Product type classification

    Returns:
        List of sub_type classifications from partner_type.json
    """
    subtypes = get_partner_subtypes()
    valid_subtypes = list(subtypes.keys())

    name_lower = name.lower()
    notes_lower = notes.lower() if notes else ''
    type_lower = mfr_type.lower() if mfr_type else ''
    combined = f"{name_lower} {notes_lower} {type_lower}"

    result = []

    # Vertically Integrated Manufacturer
    vertically_integrated = [
        'whirlpool', 'ge appliances', 'electrolux', 'frigidaire', 'sub-zero',
        'wolf', 'thermador', 'bosch', 'kitchenaid', 'maytag', 'lg electronics',
        'samsung', 'viking', 'speed queen', 'traeger', 'weber', 'napoleon',
        'danby', 'bluestar', 'brown stove', 'american range', 'dacor',
        'alliance laundry', 'elmira stove'
    ]
    if any(brand in name_lower for brand in vertically_integrated):
        if 'Vertically Integrated Manufacturer' in valid_subtypes:
            result.append('Vertically Integrated Manufacturer')

    # Brand Owner
    if any(keyword in combined for keyword in ['brand owner', 'holding company', 'spectrum brands', 'newell brands', 'owns', 'parent company']):
        if 'Vertically Integrated Manufacturer' not in result:
            if 'Brand Owner' in valid_subtypes:
                result.append('Brand Owner')

    # OEM
    if any(keyword in combined for keyword in ['oem', 'original equipment', 'contract manufacturer', 'builds appliances', 'midea', 'galanz', 'tti floor care']):
        if 'OEM' in valid_subtypes:
            result.append('OEM')

    # ODM
    if any(keyword in combined for keyword in ['odm', 'design manufacturer', 'js global', 'sharkninja', 'designs and manufactures']):
        if 'ODM' in valid_subtypes:
            result.append('ODM')

    # Private Label / House Brand Manufacturer
    if any(keyword in combined for keyword in ['private label', 'house brand', 'contract manufacturing for retailers']):
        if 'Private Label / House Brand Manufacturer' in valid_subtypes:
            result.append('Private Label / House Brand Manufacturer')

    # Importer / Brand Licensee
    if any(keyword in combined for keyword in ['importer', 'brand licensee', 'import', 'distribut']):
        if 'Importer / Brand Licensee' in valid_subtypes:
            result.append('Importer / Brand Licensee')

    # Default: if no match, assign Vertically Integrated Manufacturer for major brands
    if not result:
        major_brands = ['whirlpool', 'maytag', 'kitchenaid', 'ge', 'frigidaire', 'electrolux']
        if any(brand in name_lower for brand in major_brands):
            if 'Vertically Integrated Manufacturer' in valid_subtypes:
                result.append('Vertically Integrated Manufacturer')
        else:
            # Default to first available subtype or 'Vertically Integrated Manufacturer'
            if 'Vertically Integrated Manufacturer' in valid_subtypes:
                result.append('Vertically Integrated Manufacturer')
            elif valid_subtypes:
                result.append(valid_subtypes[0])

    return result


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
        if 'sub_type' not in mfr:
            mfr['sub_type'] = classify_sub_type(
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
    print("Adding supplemental partners data...")
    supplemental_partners = get_supplemental_partners()
    print(f"  Added {len(supplemental_partners)} supplemental partners")
    sources.append(supplemental_partners)

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
    output_file = os.path.join(data_dir, 'partners.json')
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
