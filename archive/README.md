# Channel Insights

A comprehensive data visualization platform for the North American appliance ecosystem, displaying geographic divisions, manufacturers, and all entity types involved in appliance distribution and sales.

## Features

- **States View**: Browse all US states, Canadian provinces/territories, and Mexican states
- **Manufacturers View**: Explore appliance manufacturers operating in North America
- **Channel Entities**: Comprehensive coverage of all entity types in the channel ecosystem:
  - **Distributors** - Wholesale companies that supply dealers and retailers
  - **Buying Groups** - Cooperative organizations that pool purchasing power
  - **Dealers** - Retail stores that sell directly to consumers
  - **POS Providers** - Point-of-sale system vendors
  - **Incentive Platforms** - Rebate and rewards management software
  - **Sales Agencies** - Independent sales representatives and rep organizations
  - **Integrators** - System integrators and technology solution providers
  - **Service Providers** - Installation, maintenance, and repair networks
  - **eCommerce Platforms** - Online marketplaces and digital commerce solutions
- **Channel Diagrams**: Entity relationship diagrams, Salesforce mappings, personas, and lifecycle flows

## Data Sources

- **States Data**: GeoNames (CC BY 4.0) via pgeocode library
- **Manufacturers Data**: Wikidata (CC0 1.0) via SPARQL queries
- **Entities Data**: Wikidata (CC0 1.0) via SPARQL queries
- All scripts include hardcoded fallback data for reliability

## Installation

```bash
# Install Python dependencies
pip install -r requirements.txt
```

## Usage

### Generate Data

```bash
# Generate states data
python scripts/states.py

# Generate manufacturers data
python scripts/manufacturers.py

# Generate channel entity data (individual scripts for each type)
python scripts/distributors.py
python scripts/buying_groups.py
python scripts/dealers.py
python scripts/pos_providers.py
python scripts/incentive_platforms.py
python scripts/sales_agencies.py
python scripts/integrators.py
python scripts/service_providers.py
python scripts/ecommerce_platforms.py
```

### View the Application

Open `index.html` in a web browser to explore the data with:
- Search filtering
- Country filtering
- Entity type filtering (for Entities view)
- Multiple sort options (alphabetical, reverse, by length)
- **Download functionality** - Export filtered data in CSV, JSON, or Excel format

## Project Structure

```
channelinsights/
├── index.html              # Main data browser interface
├── map.html               # Channel relationship diagrams
├── app.js                 # JavaScript application logic
├── style.css              # Swiss design styling
├── requirements.txt       # Python dependencies
├── data/                  # Generated JSON data files
│   ├── states.json
│   ├── manufacturers.json
│   ├── distributors.json
│   ├── buying_groups.json
│   ├── dealers.json
│   ├── pos_providers.json
│   ├── incentive_platforms.json
│   ├── sales_agencies.json
│   ├── integrators.json
│   ├── service_providers.json
│   └── ecommerce_platforms.json
└── scripts/               # Data generation scripts
    ├── states.py
    ├── manufacturers.py
    ├── distributors.py
    ├── buying_groups.py
    ├── dealers.py
    ├── pos_providers.py
    ├── incentive_platforms.py
    ├── sales_agencies.py
    ├── integrators.py
    ├── service_providers.py
    └── ecommerce_platforms.py
```

## Design

Built with Swiss design principles:
- Minimalist black and white color scheme
- Helvetica typography
- Grid-based layouts
- Heavy borders for visual structure
- Clean, functional interface

## License

Data licenses vary by source:
- GeoNames data: CC BY 4.0
- Wikidata data: CC0 1.0

Code is provided as-is for educational purposes.

## About

Channel Insights is a 360insights project for visualizing the North American appliance distribution ecosystem.
