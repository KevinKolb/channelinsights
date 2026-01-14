"""
North American Appliance Brands Data Generator

Analyzes makers.json to extract and organize brands owned by each manufacturer.
Creates a comprehensive database of appliance brands and their parent companies.

Data Sources:
    - makers.json (parent companies)
    - Manual brand research and curation
    - Known brand ownership relationships

Output:
    data/brands.json

License: CC0 1.0 / Creative Commons
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional


# =============================================================================
# BRAND OWNERSHIP DATABASE
# =============================================================================

BRAND_RELATIONSHIPS = {
    # Whirlpool Corporation Brands
    'Whirlpool Corporation': [
        {'name': 'Whirlpool', 'type': 'Major Appliances (full-line)', 'notes': 'Flagship brand'},
        {'name': 'Maytag', 'type': 'Major Appliances (full-line)', 'notes': 'Acquired 2006'},
        {'name': 'KitchenAid', 'type': 'Major Appliances (full-line)', 'notes': 'Premium brand'},
        {'name': 'Jenn-Air', 'type': 'Specialty/Luxury', 'notes': 'Luxury cooking appliances'},
        {'name': 'Amana', 'type': 'Major Appliances (full-line)', 'notes': 'Value brand'},
        {'name': 'Roper', 'type': 'Major Appliances (full-line)', 'notes': 'Value brand'},
        {'name': 'Admiral', 'type': 'Major Appliances', 'notes': 'Value brand'},
        {'name': 'Magic Chef', 'type': 'Major Appliances', 'notes': 'Value brand'},
        {'name': 'Gladiator', 'type': 'Major Appliances', 'notes': 'Garage and storage'},
    ],

    # GE Appliances (Haier)
    'GE Appliances': [
        {'name': 'GE', 'type': 'Major Appliances (full-line)', 'notes': 'Main brand'},
        {'name': 'GE Profile', 'type': 'Major Appliances (full-line)', 'notes': 'Mid-tier premium'},
        {'name': 'GE Café', 'type': 'Specialty/Luxury', 'notes': 'Premium customizable'},
        {'name': 'Monogram', 'type': 'Specialty/Luxury', 'notes': 'Ultra-luxury built-in'},
        {'name': 'Haier', 'type': 'Major Appliances (full-line)', 'notes': 'Parent company brand'},
        {'name': 'Hotpoint', 'type': 'Major Appliances', 'notes': 'Value brand (some markets)'},
    ],

    # Electrolux North America
    'Electrolux North America': [
        {'name': 'Electrolux', 'type': 'Major Appliances (full-line)', 'notes': 'Main brand'},
        {'name': 'Frigidaire', 'type': 'Major Appliances (full-line)', 'notes': 'Mass market'},
        {'name': 'Frigidaire Gallery', 'type': 'Major Appliances (full-line)', 'notes': 'Mid-tier'},
        {'name': 'Frigidaire Professional', 'type': 'Major Appliances (full-line)', 'notes': 'Premium'},
        {'name': 'Tappan', 'type': 'Major Appliances', 'notes': 'Value brand'},
        {'name': 'White-Westinghouse', 'type': 'Major Appliances', 'notes': 'Value brand'},
        {'name': 'Kelvinator', 'type': 'Major Appliances', 'notes': 'Value brand'},
        {'name': 'Gibson', 'type': 'Major Appliances', 'notes': 'Value brand'},
    ],

    # Sub-Zero Group
    'Sub-Zero Group': [
        {'name': 'Sub-Zero', 'type': 'Major Appliances (refrigeration)', 'notes': 'Luxury refrigeration'},
        {'name': 'Wolf', 'type': 'Major Appliances (cooking)', 'notes': 'Luxury cooking'},
        {'name': 'Cove', 'type': 'Major Appliances', 'notes': 'Luxury dishwashers'},
    ],

    # BSH Home Appliances (Bosch)
    'Thermador': [
        {'name': 'Thermador', 'type': 'Specialty/Luxury', 'notes': 'Owned by BSH (Bosch & Siemens)'},
        {'name': 'Bosch', 'type': 'Major Appliances (full-line)', 'notes': 'Parent company (German)'},
        {'name': 'Gaggenau', 'type': 'Specialty/Luxury', 'notes': 'Ultra-luxury (German)'},
    ],

    # Middleby Corporation
    'Viking Range': [
        {'name': 'Viking', 'type': 'Major Appliances (cooking)', 'notes': 'Professional-style ranges'},
        {'name': 'U-Line', 'type': 'Major Appliances (refrigeration)', 'notes': 'Undercounter refrigeration'},
        {'name': 'Lynx', 'type': 'Outdoor Cooking', 'notes': 'Outdoor kitchen equipment'},
    ],

    # Spectrum Brands
    'Spectrum Brands': [
        {'name': 'Black+Decker', 'type': 'Small Appliances', 'notes': 'Small kitchen appliances'},
        {'name': 'George Foreman', 'type': 'Small Appliances', 'notes': 'Grills and cooking'},
        {'name': 'Russell Hobbs', 'type': 'Small Appliances', 'notes': 'Kettles and small appliances'},
        {'name': 'Emeril', 'type': 'Small Appliances', 'notes': 'Celebrity chef brand'},
    ],

    # Newell Brands
    'Newell Brands': [
        {'name': 'Crock-Pot', 'type': 'Small Appliances', 'notes': 'Slow cookers'},
        {'name': 'Mr. Coffee', 'type': 'Small Appliances', 'notes': 'Coffee makers'},
        {'name': 'Oster', 'type': 'Small Appliances', 'notes': 'Blenders and appliances'},
        {'name': 'Sunbeam', 'type': 'Small Appliances', 'notes': 'Small appliances'},
        {'name': 'FoodSaver', 'type': 'Small Appliances', 'notes': 'Vacuum sealers'},
    ],

    # SharkNinja
    'SharkNinja': [
        {'name': 'Shark', 'type': 'Vacuum/Floor Care', 'notes': 'Vacuum cleaners'},
        {'name': 'Ninja', 'type': 'Small Appliances', 'notes': 'Blenders and kitchen appliances'},
    ],

    # Mabe (Joint Venture with GE)
    'Mabe': [
        {'name': 'Mabe', 'type': 'Major Appliances (full-line)', 'notes': 'Main brand'},
        {'name': 'GE (Latin America)', 'type': 'Major Appliances (full-line)', 'notes': 'Manufactured by Mabe'},
        {'name': 'Easy', 'type': 'Major Appliances', 'notes': 'Value brand'},
        {'name': 'Acros', 'type': 'Major Appliances', 'notes': 'Mexican brand'},
    ],

    # Single-Brand Manufacturers
    'Hamilton Beach Brands': [
        {'name': 'Hamilton Beach', 'type': 'Small Appliances', 'notes': 'Main brand'},
        {'name': 'Proctor Silex', 'type': 'Small Appliances', 'notes': 'Value brand'},
    ],

    'Traeger Grills': [
        {'name': 'Traeger', 'type': 'Outdoor Cooking', 'notes': 'Wood pellet grills'},
    ],

    'Napoleon': [
        {'name': 'Napoleon', 'type': 'Outdoor Cooking', 'notes': 'Grills and fireplaces'},
    ],

    'Danby': [
        {'name': 'Danby', 'type': 'Major Appliances (refrigeration)', 'notes': 'Compact appliances'},
    ],

    'Speed Queen': [
        {'name': 'Speed Queen', 'type': 'Major Appliances (laundry)', 'notes': 'Commercial-grade laundry'},
    ],

    'BlueStar': [
        {'name': 'BlueStar', 'type': 'Major Appliances (cooking)', 'notes': 'Professional ranges'},
    ],

    'Brown Stove Works': [
        {'name': 'FiveStar', 'type': 'Major Appliances (cooking)', 'notes': 'Professional ranges'},
    ],

    'American Range': [
        {'name': 'American Range', 'type': 'Major Appliances (cooking)', 'notes': 'Commercial and residential'},
    ],

    'Elmira Stove Works': [
        {'name': 'Elmira', 'type': 'Major Appliances (cooking)', 'notes': 'Retro-inspired ranges'},
        {'name': 'Northstar', 'type': 'Major Appliances (cooking)', 'notes': 'Retro refrigerators'},
    ],

    'IRobot': [
        {'name': 'Roomba', 'type': 'Vacuum/Floor Care', 'notes': 'Robot vacuums'},
        {'name': 'Braava', 'type': 'Vacuum/Floor Care', 'notes': 'Robot mops'},
    ],

    'Bissell': [
        {'name': 'Bissell', 'type': 'Vacuum/Floor Care', 'notes': 'Carpet cleaners and vacuums'},
    ],

    'The Hoover Company': [
        {'name': 'Hoover', 'type': 'Vacuum/Floor Care', 'notes': 'Vacuum cleaners'},
    ],

    'Eureka (company)': [
        {'name': 'Eureka', 'type': 'Vacuum/Floor Care', 'notes': 'Vacuum cleaners'},
    ],

    'Kirby Company': [
        {'name': 'Kirby', 'type': 'Vacuum/Floor Care', 'notes': 'Premium vacuum systems'},
    ],

    'Dirt Devil': [
        {'name': 'Dirt Devil', 'type': 'Vacuum/Floor Care', 'notes': 'Budget vacuum cleaners'},
    ],

    'Cuisinart': [
        {'name': 'Cuisinart', 'type': 'Small Appliances', 'notes': 'Food processors and kitchen appliances'},
    ],

    'Vitamix': [
        {'name': 'Vitamix', 'type': 'Small Appliances', 'notes': 'High-performance blenders'},
    ],

    'Blendtec': [
        {'name': 'Blendtec', 'type': 'Small Appliances', 'notes': 'Professional blenders'},
    ],
}


# =============================================================================
# DATA GENERATION
# =============================================================================

def load_manufacturers_data() -> Dict:
    """
    Load makers.json data.

    Returns:
        Manufacturers data dictionary
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    manufacturers_file = os.path.join(data_dir, 'makers.json')

    with open(manufacturers_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def generate_brands_data() -> Dict:
    """
    Generate comprehensive brands dataset from manufacturers data.

    Returns:
        Complete brands dataset dictionary
    """
    print("\n" + "="*70)
    print("GENERATING BRANDS DATA FROM MANUFACTURERS")
    print("="*70)

    # Load manufacturers
    manufacturers_data = load_manufacturers_data()
    print(f"\nLoaded {manufacturers_data['metadata']['total_manufacturers']} manufacturers")

    # Organize brands
    all_brands = []
    brands_by_manufacturer = {}

    for country, country_data in manufacturers_data['North America'].items():
        for manufacturer in country_data['manufacturers']:
            mfr_name = manufacturer['name']

            # Check if we have brand information for this manufacturer
            brands = BRAND_RELATIONSHIPS.get(mfr_name, [])

            # If no brands defined, create a self-branded entry
            if not brands:
                brands = [{
                    'name': mfr_name,
                    'type': manufacturer.get('type', 'Major Appliances'),
                    'notes': 'Self-branded manufacturer'
                }]

            # Add manufacturer info to each brand
            for brand in brands:
                brand_entry = {
                    'brand_name': brand['name'],
                    'parent_company': mfr_name,
                    'country': country,
                    'type': brand['type'],
                    'notes': brand.get('notes', ''),
                }

                # Add optional fields from manufacturer
                if 'headquarters' in manufacturer:
                    brand_entry['headquarters'] = manufacturer['headquarters']
                if 'website' in manufacturer:
                    brand_entry['parent_website'] = manufacturer['website']
                if 'revenue_usd' in manufacturer:
                    brand_entry['parent_revenue_usd'] = manufacturer['revenue_usd']

                all_brands.append(brand_entry)

            brands_by_manufacturer[mfr_name] = brands

    # Sort brands alphabetically
    all_brands.sort(key=lambda x: x['brand_name'])

    # Group by country
    brands_by_country = {
        'United States': [],
        'Canada': [],
        'Mexico': []
    }

    for brand in all_brands:
        brands_by_country[brand['country']].append(brand)

    # Group by type
    brands_by_type = {}
    for brand in all_brands:
        brand_type = brand['type']
        if brand_type not in brands_by_type:
            brands_by_type[brand_type] = []
        brands_by_type[brand_type].append(brand)

    print(f"Generated {len(all_brands)} brands from {len(brands_by_manufacturer)} manufacturers")
    print(f"\nBreakdown by country:")
    for country, brands in brands_by_country.items():
        print(f"  - {country}: {len(brands)} brands")

    print(f"\nBreakdown by type:")
    for brand_type, brands in sorted(brands_by_type.items()):
        print(f"  - {brand_type}: {len(brands)} brands")

    return {
        "metadata": {
            "date_generated": datetime.now(timezone.utc).isoformat(),
            "source": "Python Script",
            "derived_from": "makers.json",
            "total_brands": len(all_brands),
            "total_parent_companies": len(brands_by_manufacturer),
            "license": "CC0 1.0 / Creative Commons",
            "license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
            "note": "Brand ownership data compiled from manufacturer relationships and public sources."
        },
        "brands": {
            "all": all_brands,
            "by_country": brands_by_country,
            "by_type": brands_by_type,
            "by_manufacturer": brands_by_manufacturer
        }
    }


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function."""
    print("\n" + "="*70)
    print("NORTH AMERICAN APPLIANCE BRANDS DATA GENERATOR")
    print("="*70)

    # Generate data
    data = generate_brands_data()

    # Create output directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    os.makedirs(data_dir, exist_ok=True)

    # Write to file
    output_file = os.path.join(data_dir, 'brands.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n[SUCCESS] Generated: {output_file}")
    print(f"\nStatistics:")
    print(f"  - Date generated: {data['metadata']['date_generated']}")
    print(f"  - Total brands: {data['metadata']['total_brands']}")
    print(f"  - Parent companies: {data['metadata']['total_parent_companies']}")
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    main()
