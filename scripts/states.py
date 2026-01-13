"""
Generate states.json with North American administrative divisions.
Data Source: GeoNames (https://www.geonames.org/) - CC BY 4.0 License
Usage: python scripts/states.py
"""
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    import pgeocode
    PGEOCODE_AVAILABLE = True
except ImportError:
    PGEOCODE_AVAILABLE = False
    print("Warning: pgeocode not installed. Install with: pip install pgeocode")
    print("Falling back to hardcoded data...\n")

CANADIAN_TERRITORIES = ['Northwest Territo', 'Nunavut', 'Yukon']


def fetch_states_from_pgeocode(country_code: str, country_name: str) -> Optional[List[str]]:
    """Fetch administrative divisions from GeoNames via pgeocode."""
    if not PGEOCODE_AVAILABLE:
        return None

    try:
        print(f"Fetching data for {country_name}...")
        nomi = pgeocode.Nominatim(country_code.lower())

        if not hasattr(nomi, '_data_frame') or nomi._data_frame is None:
            print(f"Warning: No data found for {country_name}")
            return None

        df = nomi._data_frame
        if 'state_name' not in df.columns:
            print(f"Warning: No state data available for {country_name}")
            return None

        states = [s for s in df['state_name'].dropna().unique() if s and isinstance(s, str)]
        states.sort()

        print(f"Found {len(states)} divisions for {country_name}")
        return states

    except Exception as e:
        print(f"Error fetching data for {country_name}: {e}")
        return None


def categorize_canadian_divisions(divisions: List[str]) -> Dict[str, List[str]]:
    """Categorize Canadian divisions into provinces and territories."""
    provinces = []
    territories = []

    for division in divisions:
        if any(keyword in division for keyword in CANADIAN_TERRITORIES):
            territories.append(division)
        else:
            provinces.append(division)

    return {'provinces': provinces, 'territories': territories}


def fetch_north_american_states() -> Optional[Dict[str, Dict]]:
    """Fetch administrative divisions for US, Canada, and Mexico from GeoNames."""
    if not PGEOCODE_AVAILABLE:
        return None

    result = {}

    # United States
    us_states = fetch_states_from_pgeocode('US', 'United States')
    if us_states:
        result['United States'] = {'states': us_states}

    # Canada
    ca_divisions = fetch_states_from_pgeocode('CA', 'Canada')
    if ca_divisions:
        result['Canada'] = categorize_canadian_divisions(ca_divisions)

    # Mexico
    mx_states = fetch_states_from_pgeocode('MX', 'Mexico')
    if mx_states:
        result['Mexico'] = {'states': mx_states}

    return result if result else None


def get_hardcoded_data() -> Dict[str, Dict]:
    """Fallback data when GeoNames is unavailable (manually curated as of 2024)."""
    return {
        "United States": {
            "states": [
                "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
                "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
                "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
                "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
                "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
                "New Hampshire", "New Jersey", "New Mexico", "New York",
                "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
                "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
                "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
                "West Virginia", "Wisconsin", "Wyoming"
            ]
        },
        "Canada": {
            "provinces": [
                "Alberta", "British Columbia", "Manitoba", "New Brunswick",
                "Newfoundland and Labrador", "Nova Scotia", "Ontario",
                "Prince Edward Island", "Quebec", "Saskatchewan"
            ],
            "territories": ["Northwest Territories", "Nunavut", "Yukon"]
        },
        "Mexico": {
            "states": [
                "Aguascalientes", "Baja California", "Baja California Sur",
                "Campeche", "Chiapas", "Chihuahua", "Coahuila", "Colima",
                "Durango", "Guanajuato", "Guerrero", "Hidalgo", "Jalisco",
                "México", "Michoacán", "Morelos", "Nayarit", "Nuevo León",
                "Oaxaca", "Puebla", "Querétaro", "Quintana Roo", "San Luis Potosí",
                "Sinaloa", "Sonora", "Tabasco", "Tamaulipas", "Tlaxcala",
                "Veracruz", "Yucatán", "Zacatecas", "Mexico City"
            ]
        }
    }


def generate_states_data() -> Dict:
    """Generate states data with automatic fallback to hardcoded data."""
    countries_data = fetch_north_american_states()

    if not countries_data:
        print("Using hardcoded fallback data...")
        countries_data = get_hardcoded_data()
        data_source = "hardcoded"
    else:
        print("Successfully fetched data from GeoNames")
        data_source = "GeoNames"

    # Wrap in North America object with metadata
    return {
        "metadata": {
            "date_pulled": datetime.now(timezone.utc).isoformat(),
            "source": data_source,
            "license": "CC BY 4.0",
            "license_url": "https://creativecommons.org/licenses/by/4.0/"
        },
        "North America": countries_data
    }


def main():
    """Generate and save states.json to data directory."""
    print("Generating North American states data...")
    data = generate_states_data()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    os.makedirs(data_dir, exist_ok=True)

    output_file = os.path.join(data_dir, 'states.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    north_america = data['North America']
    print(f"\nSuccessfully generated {output_file}")
    print(f"  - Data pulled: {data['metadata']['date_pulled']}")
    print(f"  - Source: {data['metadata']['source']}")
    print(f"  - United States: {len(north_america['United States']['states'])} states")
    print(f"  - Canada: {len(north_america['Canada']['provinces'])} provinces, "
          f"{len(north_america['Canada']['territories'])} territories")
    print(f"  - Mexico: {len(north_america['Mexico']['states'])} states")


if __name__ == "__main__":
    main()
