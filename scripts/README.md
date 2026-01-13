# Scripts Directory

This directory contains utility scripts for generating and managing data for the Channel Insights project.

## states.py

Generates `states.json` containing geographical administrative divisions for North American countries.

### Data Source

This script uses data from **GeoNames** (https://www.geonames.org/), a free geographical database licensed under Creative Commons Attribution 4.0.

- **License**: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Attribution**: GeoNames geographical database (https://www.geonames.org/)
- **Access Method**: pgeocode Python library
- **Data Files**: Downloaded from https://download.geonames.org/export/zip/

### Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run the script
python scripts/states.py
```

### Output

Creates `data/states.json` with the following structure:

```json
{
  "United States": {
    "states": ["Alabama", "Alaska", ...]
  },
  "Canada": {
    "provinces": ["Alberta", "British Columbia", ...],
    "territories": ["Northwest Territory", "Nunavut Territory", "Yukon"]
  },
  "Mexico": {
    "states": ["Aguascalientes", "Baja California", ...]
  }
}
```

### Features

- **Automatic data fetching**: Downloads latest GeoNames data via pgeocode
- **Local caching**: Data is cached locally after first download
- **Offline capable**: Works offline after initial download
- **Fallback support**: Uses hardcoded data if GeoNames is unavailable
- **No API key required**: No registration or authentication needed

### Data Coverage

- **United States**: 52 entries (50 states + DC + territories)
- **Canada**: 10 provinces + 3 territories
- **Mexico**: 32 states

### Updating Data

Simply run the script again to refresh the data from GeoNames. The pgeocode library will use its cached data, which is typically updated monthly.

To force a fresh download:
```bash
# Clear pgeocode cache (location varies by OS)
# Linux/Mac: rm -rf ~/.cache/pgeocode/
# Windows: del /s %USERPROFILE%\.cache\pgeocode\

# Run script
python scripts/states.py
```

### Data Quality

- **Primary source**: GeoNames postal code database (most current)
- **Fallback source**: Manually curated hardcoded data (as of 2024)
- **Update frequency**: On-demand (run script when updates needed)

### License Compliance

When using this data, you should provide attribution to GeoNames:

> This product uses data from GeoNames (https://www.geonames.org/), licensed under CC BY 4.0.
