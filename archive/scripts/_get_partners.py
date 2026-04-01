"""
North American Channel Partners Data Generator

Aggregates partner data from multiple sources to create comprehensive
databases of appliance industry partners in North America.

Data Sources:
    - Wikidata SPARQL API (structured data)
    - Wikipedia category pages (company listings)
    - Wikipedia infoboxes (website URLs)
    - Manual curation (supplemental data)

Output Files:
    data/partners.json - Manufacturers and business partners
    data/brands.json - Brand ownership relationships
    data/buying_groups.json - Buying groups/cooperatives
    data/dealers.json - Appliance dealers/retailers
    data/distributors.json - Distributors/wholesalers
    data/ecommerce_platforms.json - eCommerce platforms
    data/incentive_platforms.json - Incentive/rebate platforms
    data/incentive_program_types.json - Incentive program type reference
    data/integrators.json - System integrators
    data/pos_providers.json - POS system providers
    data/sales_agencies.json - Sales agencies/rep firms
    data/service_providers.json - Service/installation providers
    data/states.json - North American administrative divisions

License: CC0 1.0 / Creative Commons

Usage: python scripts/_get_partners.py [--all | --partners | --brands | ...]
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin
import argparse

import requests
from bs4 import BeautifulSoup

try:
    import pgeocode
    PGEOCODE_AVAILABLE = True
except ImportError:
    PGEOCODE_AVAILABLE = False


# =============================================================================
# CONFIGURATION
# =============================================================================

USER_AGENT = 'ChannelInsights/1.0 (educational)'
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'data')


# =============================================================================
# COMMON UTILITY FUNCTIONS
# =============================================================================

def organize_by_country(entities: List[Dict], key: str = 'entities',
                        category: Optional[str] = None, sub_type: Optional[str] = None) -> Dict[str, Dict]:
    """Organize entities by country, optionally adding category and sub_type."""
    organized = {
        "United States": {key: []},
        "Canada": {key: []},
        "Mexico": {key: []}
    }

    for entity in entities:
        country = entity.get('country', 'United States')
        if country in organized:
            entity_data = {k: v for k, v in entity.items() if k != 'country'}
            # Add category and sub_type if provided
            if category and 'category' not in entity_data:
                entity_data['category'] = category
            if sub_type and 'sub_type' not in entity_data:
                entity_data['sub_type'] = sub_type
            organized[country][key].append(entity_data)

    for country in organized:
        organized[country][key].sort(key=lambda x: x.get('name', ''))

    return organized


def normalize_name(name: str) -> str:
    """Normalize manufacturer name for deduplication."""
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


def save_json(data: Dict, filename: str) -> str:
    """Save data to JSON file in data directory."""
    os.makedirs(DATA_DIR, exist_ok=True)
    output_file = os.path.join(DATA_DIR, filename)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return output_file


def create_metadata(source: str, total_count: int = None, license_type: str = "CC0 1.0") -> Dict:
    """Create standard metadata block."""
    meta = {
        "date_pulled": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "license": license_type,
        "license_url": "https://creativecommons.org/publicdomain/zero/1.0/"
    }
    if total_count is not None:
        meta["total_count"] = total_count
    return meta


# =============================================================================
# WIKIDATA SPARQL QUERIES
# =============================================================================

def fetch_from_wikidata(query: str, entity_type: str) -> Optional[List[Dict]]:
    """Generic Wikidata SPARQL fetch function."""
    endpoint = "https://query.wikidata.org/sparql"
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'application/json'
    }

    try:
        print(f"Fetching {entity_type} from Wikidata...")
        response = requests.get(
            endpoint,
            params={'query': query, 'format': 'json'},
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        results = data.get('results', {}).get('bindings', [])

        if not results:
            print(f"Warning: No {entity_type} found")
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

        print(f"Found {len(entities)} {entity_type}")
        return entities

    except requests.RequestException as e:
        print(f"Error fetching {entity_type} from Wikidata: {e}")
        return None


# =============================================================================
# HARDCODED DATA - BUYING GROUPS
# =============================================================================

def get_hardcoded_buying_groups() -> List[Dict]:
    """Hardcoded buying group data."""
    return [
        {"name": "Nationwide Marketing Group", "country": "United States", "website": "https://www.nationwidegrp.com"},
        {"name": "BrandSource", "country": "United States", "website": "https://www.brandsource.com"},
        {"name": "PRO Group", "country": "United States", "website": "https://www.progroup.net"},
        {"name": "AVB Buying Group", "country": "United States", "website": "https://www.avbbg.com"}
    ]


# =============================================================================
# HARDCODED DATA - DEALERS
# =============================================================================

def get_hardcoded_dealers() -> List[Dict]:
    """Hardcoded dealer data."""
    return [
        {"name": "Best Buy", "country": "United States", "website": "https://www.bestbuy.com"},
        {"name": "Home Depot", "country": "United States", "website": "https://www.homedepot.com"},
        {"name": "Lowe's", "country": "United States", "website": "https://www.lowes.com"},
        {"name": "Costco", "country": "United States", "website": "https://www.costco.com"},
        {"name": "Abt Electronics", "country": "United States", "website": "https://www.abt.com"},
        {"name": "P.C. Richard & Son", "country": "United States", "website": "https://www.pcrichard.com"},
        {"name": "Leon's", "country": "Canada", "website": "https://www.leons.ca"},
        {"name": "The Brick", "country": "Canada", "website": "https://www.thebrick.com"}
    ]


# =============================================================================
# HARDCODED DATA - DISTRIBUTORS
# =============================================================================

def get_hardcoded_distributors() -> List[Dict]:
    """Hardcoded distributor data."""
    return [
        {"name": "Ferguson Enterprises", "country": "United States", "website": "https://www.ferguson.com"},
        {"name": "AD (Affiliated Distributors)", "country": "United States", "website": "https://www.adhq.com"},
        {"name": "Marcone", "country": "United States", "website": "https://www.marcone.com"},
        {"name": "United Appliance Parts", "country": "United States", "website": "https://www.unitedapp.com"}
    ]


# =============================================================================
# HARDCODED DATA - ECOMMERCE PLATFORMS
# =============================================================================

def get_hardcoded_ecommerce_platforms() -> List[Dict]:
    """Hardcoded eCommerce platforms data."""
    return [
        # Major Online Marketplaces
        {"name": "Amazon", "country": "United States", "website": "https://www.amazon.com"},
        {"name": "eBay", "country": "United States", "website": "https://www.ebay.com"},
        {"name": "Walmart Marketplace", "country": "United States", "website": "https://marketplace.walmart.com"},
        {"name": "Best Buy Marketplace", "country": "United States", "website": "https://www.bestbuy.com"},
        {"name": "Target Plus", "country": "United States", "website": "https://www.target.com"},
        {"name": "Wayfair", "country": "United States", "website": "https://www.wayfair.com"},
        {"name": "Overstock", "country": "United States", "website": "https://www.overstock.com"},
        # eCommerce Platform Software
        {"name": "Shopify", "country": "Canada", "website": "https://www.shopify.com"},
        {"name": "BigCommerce", "country": "United States", "website": "https://www.bigcommerce.com"},
        {"name": "Magento (Adobe Commerce)", "country": "United States", "website": "https://magento.com"},
        {"name": "WooCommerce (Automattic)", "country": "United States", "website": "https://woocommerce.com"},
        {"name": "Salesforce Commerce Cloud", "country": "United States", "website": "https://www.salesforce.com/commerce"},
        # B2B eCommerce Platforms
        {"name": "OroCommerce", "country": "United States", "website": "https://www.orocommerce.com"},
        {"name": "Logicbroker", "country": "United States", "website": "https://www.logicbroker.com"},
        {"name": "Mirakl", "country": "United States", "website": "https://www.mirakl.com"},
        {"name": "ChannelAdvisor", "country": "United States", "website": "https://www.channeladvisor.com"},
        {"name": "Kibo Commerce", "country": "United States", "website": "https://www.kibocommerce.com"},
        {"name": "Elastic Path", "country": "Canada", "website": "https://www.elasticpath.com"},
        # Marketplace Management
        {"name": "Feedonomics (BigCommerce)", "country": "United States", "website": "https://www.feedonomics.com"},
        {"name": "Zentail", "country": "United States", "website": "https://www.zentail.com"},
        {"name": "Sellbrite", "country": "United States", "website": "https://www.sellbrite.com"},
        {"name": "Linnworks", "country": "United States", "website": "https://www.linnworks.com"},
        # Headless/API-first Commerce
        {"name": "commercetools", "country": "United States", "website": "https://www.commercetools.com"},
        {"name": "Fabric", "country": "United States", "website": "https://www.fabric.inc"},
        {"name": "VTEX", "country": "United States", "website": "https://www.vtex.com"},
        # Social Commerce
        {"name": "Meta Shops (Facebook/Instagram)", "country": "United States", "website": "https://www.facebook.com/business/shops"},
        {"name": "TikTok Shop", "country": "United States", "website": "https://www.tiktok.com/business/shopping"},
        {"name": "Pinterest Shopping", "country": "United States", "website": "https://www.pinterest.com/business"}
    ]


# =============================================================================
# HARDCODED DATA - INCENTIVE PLATFORMS
# =============================================================================

def get_hardcoded_incentive_platforms() -> List[Dict]:
    """Hardcoded incentive platform data."""
    return [
        # Channel Incentive & Rebate Management
        {"name": "360insights", "country": "Canada", "website": "https://www.360insights.com"},
        {"name": "Channelscaler", "country": "United States", "website": "https://channelscaler.com"},
        {"name": "e2open", "country": "United States", "website": "https://www.e2open.com"},
        {"name": "Enable", "country": "United States", "website": "https://www.enable.com"},
        {"name": "Vendavo", "country": "United States", "website": "https://www.vendavo.com"},
        {"name": "Dash Solutions", "country": "United States", "website": "https://dashsolutions.com"},
        {"name": "Incentit", "country": "United States", "website": "https://incentit.com"},
        {"name": "ZiftONE", "country": "United States", "website": "https://www.ziftone.com"},
        {"name": "Vistex", "country": "United States", "website": "https://www.vistex.com"},
        {"name": "Model N", "country": "United States", "website": "https://www.modeln.com"},
        # Reward & Incentive Fulfillment
        {"name": "Blackhawk Network", "country": "United States", "website": "https://www.blackhawknetwork.com"},
        {"name": "Xoxoday", "country": "United States", "website": "https://www.xoxoday.com"},
        {"name": "Tremendous", "country": "United States", "website": "https://www.tremendous.com"},
        {"name": "Tango Card", "country": "United States", "website": "https://www.tangocard.com"},
        {"name": "Rybbon", "country": "United States", "website": "https://www.rybbon.net"},
        {"name": "InComm Incentives", "country": "United States", "website": "https://www.incommincentives.com"},
        {"name": "BI WORLDWIDE", "country": "United States", "website": "https://www.biworldwide.com"},
        {"name": "The Incentive Group", "country": "United States", "website": "https://www.incentivegroup.com"},
        # Sales Incentive Compensation
        {"name": "Performio", "country": "United States", "website": "https://www.performio.co"},
        {"name": "Varicent (IBM)", "country": "United States", "website": "https://www.varicent.com"},
        {"name": "Optymyze", "country": "United States", "website": "https://www.optymyze.com"},
        {"name": "Salesforce (Incentive Compensation)", "country": "United States", "website": "https://www.salesforce.com"},
        # Channel Partner Platforms
        {"name": "Impartner", "country": "United States", "website": "https://www.impartner.com"},
        {"name": "Allbound", "country": "United States", "website": "https://www.allbound.com"},
        {"name": "Zinfi", "country": "United States", "website": "https://www.zinfi.com"},
        {"name": "Channeltivity", "country": "United States", "website": "https://www.channeltivity.com"}
    ]


# =============================================================================
# HARDCODED DATA - INCENTIVE PROGRAM TYPES
# =============================================================================

def get_incentive_program_types() -> Dict[str, Dict]:
    """Reference data for types of incentive programs."""
    return {
        "SPIF (Sales Performance Incentive Fund)": {
            "abbreviation": "SPIF",
            "definition": "Short-term sales incentives that reward individual sales representatives for selling specific products or achieving targets within a defined period.",
            "typical_duration": "30-90 days",
            "target_audience": "Sales associates, retail staff",
            "payment_type": "Cash, gift cards, prizes",
            "examples": [
                "Sell 10 units of Product X, earn $50 per unit",
                "First to sell new model gets $500 bonus",
                "Monthly contest for highest unit sales"
            ]
        },
        "Rebate": {
            "abbreviation": "Rebate",
            "definition": "Post-purchase incentives that return money to buyers (dealers or consumers) after proof of purchase is submitted and validated.",
            "typical_duration": "Ongoing or promotional periods",
            "target_audience": "Dealers, consumers, distributors",
            "payment_type": "Check, ACH, prepaid card, credit memo",
            "examples": [
                "Consumer mail-in rebate: $100 back on appliance purchase",
                "Dealer volume rebate: 2% back on quarterly purchases over $50K",
                "Instant rebate at point of sale"
            ]
        },
        "MDF (Market Development Funds)": {
            "abbreviation": "MDF",
            "definition": "Cooperative marketing funds provided by manufacturers to channel partners for local marketing activities that promote the manufacturer's products.",
            "typical_duration": "Annual or quarterly allocation",
            "target_audience": "Dealers, distributors, resellers",
            "payment_type": "Reimbursement after proof of activity",
            "examples": [
                "Co-op advertising in local newspaper",
                "Funding for in-store displays and signage",
                "Support for dealer open house events"
            ]
        },
        "Co-op (Cooperative Advertising)": {
            "abbreviation": "Co-op",
            "definition": "Shared advertising costs between manufacturers and dealers, typically as a percentage of purchases or sales.",
            "typical_duration": "Ongoing, accrued over time",
            "target_audience": "Dealers, retailers",
            "payment_type": "Accrual-based reimbursement",
            "examples": [
                "Manufacturer pays 50% of local TV ad costs",
                "Earn 3% of purchases as co-op advertising credits",
                "Quarterly co-op funds for digital marketing"
            ]
        },
        "STA (Sell-Through Allowance)": {
            "abbreviation": "STA",
            "definition": "Incentives paid to dealers based on products sold to end customers, not just purchased from distributor.",
            "typical_duration": "Quarterly or program-based",
            "target_audience": "Dealers, retailers",
            "payment_type": "Per-unit payment or percentage",
            "examples": [
                "$25 per unit sold through to consumer",
                "Extra 5% margin on units registered with consumers",
                "Tiered payments based on quarterly sell-through volume"
            ]
        },
        "Volume Incentive": {
            "abbreviation": "Volume",
            "definition": "Tiered rewards based on purchase or sales volume over a defined period.",
            "typical_duration": "Quarterly, annual",
            "target_audience": "Dealers, distributors, buying groups",
            "payment_type": "Retroactive rebate, tiered discount",
            "examples": [
                "Hit $100K purchases: earn 2% rebate, $250K: earn 4%",
                "Year-end volume bonus for exceeding target",
                "Progressive discount as volume increases"
            ]
        },
        "Growth Incentive": {
            "abbreviation": "Growth",
            "definition": "Rewards for year-over-year growth, encouraging partners to expand their business.",
            "typical_duration": "Annual, quarterly",
            "target_audience": "Dealers, distributors",
            "payment_type": "Bonus payment, increased margin",
            "examples": [
                "10% YoY growth = 1% bonus on total annual purchases",
                "Growth acceleration bonus for exceeding 20% increase",
                "Quarterly growth incentive on new product lines"
            ]
        },
        "New Product Introduction (NPI)": {
            "abbreviation": "NPI",
            "definition": "Special incentives to drive adoption and sales of newly launched products.",
            "typical_duration": "60-180 days (launch period)",
            "target_audience": "Sales associates, dealers",
            "payment_type": "Higher margins, bonus payments, prizes",
            "examples": [
                "Double SPIF on new model for first 90 days",
                "Free unit after selling 5 of new product",
                "Launch contest with travel prize for top seller"
            ]
        },
        "Display Allowance": {
            "abbreviation": "Display",
            "definition": "Payments to dealers for featuring products in prominent floor displays.",
            "typical_duration": "Monthly, seasonal",
            "target_audience": "Dealers, retailers",
            "payment_type": "Fixed payment per display",
            "examples": [
                "$500/month for maintaining endcap display",
                "Display fee for featuring products in showroom",
                "Seasonal display bonus for holiday merchandising"
            ]
        },
        "Training Incentive": {
            "abbreviation": "Training",
            "definition": "Rewards for completing product training, certification programs, or educational requirements.",
            "typical_duration": "Ongoing",
            "target_audience": "Sales associates, dealer staff",
            "payment_type": "Certification bonuses, increased SPIFs",
            "examples": [
                "$100 bonus for completing product certification",
                "Certified reps earn 25% higher SPIF rates",
                "Training completion unlocks access to premium programs"
            ]
        },
        "Bundle Incentive": {
            "abbreviation": "Bundle",
            "definition": "Enhanced incentives for selling product combinations or complete solutions.",
            "typical_duration": "Promotional periods",
            "target_audience": "Sales associates, dealers",
            "payment_type": "Bonus per bundle sold",
            "examples": [
                "Sell appliance + extended warranty = extra $50 SPIF",
                "Complete kitchen package earns 3x normal incentive",
                "Bundle bonus for selling installation with product"
            ]
        },
        "Demo/Floor Model Allowance": {
            "abbreviation": "Demo",
            "definition": "Support for dealers to purchase and display floor models or demo units.",
            "typical_duration": "As needed",
            "target_audience": "Dealers, showrooms",
            "payment_type": "Discounted units, allowance payment",
            "examples": [
                "50% off first unit for floor display",
                "Demo allowance covers cost of display model",
                "Annual floor model refresh program"
            ]
        }
    }


# =============================================================================
# HARDCODED DATA - INTEGRATORS
# =============================================================================

def get_hardcoded_integrators() -> List[Dict]:
    """Hardcoded integrators data."""
    return [
        # Major Technology System Integrators
        {"name": "Accenture", "country": "United States", "website": "https://www.accenture.com"},
        {"name": "Deloitte Digital", "country": "United States", "website": "https://www.deloitte.com/digital"},
        {"name": "IBM Global Services", "country": "United States", "website": "https://www.ibm.com/services"},
        {"name": "Cognizant", "country": "United States", "website": "https://www.cognizant.com"},
        {"name": "Capgemini", "country": "United States", "website": "https://www.capgemini.com"},
        {"name": "Wipro", "country": "United States", "website": "https://www.wipro.com"},
        {"name": "Infosys", "country": "United States", "website": "https://www.infosys.com"},
        # Smart Home/Building Integrators
        {"name": "Control4 (Snap One)", "country": "United States", "website": "https://www.control4.com"},
        {"name": "Crestron", "country": "United States", "website": "https://www.crestron.com"},
        {"name": "Savant", "country": "United States", "website": "https://www.savant.com"},
        {"name": "ELAN", "country": "United States", "website": "https://www.elanhomesystems.com"},
        {"name": "Josh.ai", "country": "United States", "website": "https://www.josh.ai"},
        # Audio/Video Integration
        {"name": "AVIXA", "country": "United States", "website": "https://www.avixa.org"},
        {"name": "CEDIA (Custom Electronic Design & Installation Association)", "country": "United States", "website": "https://www.cedia.org"},
        # IoT/Industrial Integrators
        {"name": "PTC", "country": "United States", "website": "https://www.ptc.com"},
        {"name": "Rockwell Automation", "country": "United States", "website": "https://www.rockwellautomation.com"},
        {"name": "Honeywell Building Technologies", "country": "United States", "website": "https://www.honeywell.com"},
        {"name": "Johnson Controls", "country": "United States", "website": "https://www.johnsoncontrols.com"},
        {"name": "Siemens Building Technologies", "country": "United States", "website": "https://www.siemens.com/building"},
        # Appliance/HVAC Integration
        {"name": "ADT Commercial", "country": "United States", "website": "https://www.adt.com/commercial"},
        {"name": "Carrier", "country": "United States", "website": "https://www.carrier.com"},
        # Canadian Integrators
        {"name": "CGI Group", "country": "Canada", "website": "https://www.cgi.com"},
        {"name": "OpenText", "country": "Canada", "website": "https://www.opentext.com"}
    ]


# =============================================================================
# HARDCODED DATA - POS PROVIDERS
# =============================================================================

def get_hardcoded_pos_providers() -> List[Dict]:
    """Hardcoded POS provider data."""
    return [
        {"name": "Square", "country": "United States", "website": "https://squareup.com"},
        {"name": "Clover", "country": "United States", "website": "https://www.clover.com"},
        {"name": "Lightspeed", "country": "Canada", "website": "https://www.lightspeedhq.com"},
        {"name": "Toast", "country": "United States", "website": "https://pos.toasttab.com"},
        {"name": "Shopify POS", "country": "Canada", "website": "https://www.shopify.com/pos"}
    ]


# =============================================================================
# HARDCODED DATA - SALES AGENCIES
# =============================================================================

def get_hardcoded_sales_agencies() -> List[Dict]:
    """Hardcoded sales agencies data."""
    return [
        {"name": "Manufacturers' Agents National Association (MANA)", "country": "United States", "website": "https://www.manaonline.org"},
        {"name": "Commercial Service Association (CSA)", "country": "United States", "website": "https://www.csa.com"},
        {"name": "Repfabric", "country": "United States", "website": "https://www.repfabric.com"},
        {"name": "RepHunter", "country": "United States", "website": "https://www.rephunter.net"},
        {"name": "Manufacturers Representatives Educational Research Foundation", "country": "United States", "website": "https://www.mrerf.org"},
        {"name": "Alliance of Technology Service Providers", "country": "United States", "website": "https://www.theallianceoftsp.org"},
        {"name": "TechRep Solutions", "country": "United States", "website": "https://www.techrepsolutions.com"},
        {"name": "Canadian Professional Sales Association", "country": "Canada", "website": "https://www.cpsa.com"},
        {"name": "Sales Talent Agency", "country": "Canada", "website": "https://www.salestalent.com"}
    ]


# =============================================================================
# HARDCODED DATA - SERVICE PROVIDERS
# =============================================================================

def get_hardcoded_service_providers() -> List[Dict]:
    """Hardcoded service providers data."""
    return [
        # Major Appliance Service Networks
        {"name": "Asurion", "country": "United States", "website": "https://www.asurion.com"},
        {"name": "ServiceBench", "country": "United States", "website": "https://www.servicebench.com"},
        {"name": "Servicelution (Assurant)", "country": "United States", "website": "https://www.servicelution.com"},
        {"name": "Service Experts", "country": "United States", "website": "https://www.serviceexperts.com"},
        {"name": "ARS/Rescue Rooter", "country": "United States", "website": "https://www.ars.com"},
        # Warranty Service Providers
        {"name": "AmTrust Financial Services", "country": "United States", "website": "https://www.amtrustfinancial.com"},
        {"name": "Cinch Home Services", "country": "United States", "website": "https://www.cinchhomeservices.com"},
        {"name": "HomeServe USA", "country": "United States", "website": "https://www.homeserveusa.com"},
        {"name": "American Home Shield", "country": "United States", "website": "https://www.ahs.com"},
        {"name": "First American Home Warranty", "country": "United States", "website": "https://www.firstam.com"},
        # Field Service Management
        {"name": "ServiceTitan", "country": "United States", "website": "https://www.servicetitan.com"},
        {"name": "Housecall Pro", "country": "United States", "website": "https://www.housecallpro.com"},
        {"name": "FieldEdge (Xplor)", "country": "United States", "website": "https://www.fieldedge.com"},
        {"name": "ServiceMax (PTC)", "country": "United States", "website": "https://www.servicemax.com"},
        {"name": "Jobber", "country": "Canada", "website": "https://getjobber.com"},
        # HVAC Service Networks
        {"name": "Nexstar Network", "country": "United States", "website": "https://www.nexstarnetwork.com"},
        {"name": "Service Nation Alliance", "country": "United States", "website": "https://www.servicenation.com"},
        {"name": "Service Roundtable", "country": "United States", "website": "https://www.serviceroundtable.com"},
        # Installation & Repair
        {"name": "Mr. Appliance (Neighborly)", "country": "United States", "website": "https://www.mrappliance.com"},
        {"name": "Appliance Repair Depot", "country": "United States", "website": "https://www.appliancerepairdepot.com"},
        {"name": "Sears Home Services", "country": "United States", "website": "https://www.searshomeservices.com"},
        # Parts Distribution
        {"name": "PartsSource", "country": "United States", "website": "https://www.partssource.com"},
        {"name": "PartSelect", "country": "United States", "website": "https://www.partselect.com"},
        {"name": "RepairClinic", "country": "United States", "website": "https://www.repairclinic.com"}
    ]


# =============================================================================
# HARDCODED DATA - STATES
# =============================================================================

CANADIAN_TERRITORIES = ['Northwest Territo', 'Nunavut', 'Yukon']


def get_hardcoded_states() -> Dict[str, Dict]:
    """Fallback data for North American administrative divisions."""
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


# =============================================================================
# HARDCODED DATA - BRAND RELATIONSHIPS
# =============================================================================

BRAND_RELATIONSHIPS = {
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
    'GE Appliances': [
        {'name': 'GE', 'type': 'Major Appliances (full-line)', 'notes': 'Main brand'},
        {'name': 'GE Profile', 'type': 'Major Appliances (full-line)', 'notes': 'Mid-tier premium'},
        {'name': 'GE Café', 'type': 'Specialty/Luxury', 'notes': 'Premium customizable'},
        {'name': 'Monogram', 'type': 'Specialty/Luxury', 'notes': 'Ultra-luxury built-in'},
        {'name': 'Haier', 'type': 'Major Appliances (full-line)', 'notes': 'Parent company brand'},
        {'name': 'Hotpoint', 'type': 'Major Appliances', 'notes': 'Value brand (some markets)'},
    ],
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
    'Sub-Zero Group': [
        {'name': 'Sub-Zero', 'type': 'Major Appliances (refrigeration)', 'notes': 'Luxury refrigeration'},
        {'name': 'Wolf', 'type': 'Major Appliances (cooking)', 'notes': 'Luxury cooking'},
        {'name': 'Cove', 'type': 'Major Appliances', 'notes': 'Luxury dishwashers'},
    ],
    'Thermador': [
        {'name': 'Thermador', 'type': 'Specialty/Luxury', 'notes': 'Owned by BSH'},
        {'name': 'Bosch', 'type': 'Major Appliances (full-line)', 'notes': 'Parent company (German)'},
        {'name': 'Gaggenau', 'type': 'Specialty/Luxury', 'notes': 'Ultra-luxury (German)'},
    ],
    'Viking Range': [
        {'name': 'Viking', 'type': 'Major Appliances (cooking)', 'notes': 'Professional-style ranges'},
        {'name': 'U-Line', 'type': 'Major Appliances (refrigeration)', 'notes': 'Undercounter refrigeration'},
        {'name': 'Lynx', 'type': 'Outdoor Cooking', 'notes': 'Outdoor kitchen equipment'},
    ],
    'Spectrum Brands': [
        {'name': 'Black+Decker', 'type': 'Small Appliances', 'notes': 'Small kitchen appliances'},
        {'name': 'George Foreman', 'type': 'Small Appliances', 'notes': 'Grills and cooking'},
        {'name': 'Russell Hobbs', 'type': 'Small Appliances', 'notes': 'Kettles and small appliances'},
        {'name': 'Emeril', 'type': 'Small Appliances', 'notes': 'Celebrity chef brand'},
    ],
    'Newell Brands': [
        {'name': 'Crock-Pot', 'type': 'Small Appliances', 'notes': 'Slow cookers'},
        {'name': 'Mr. Coffee', 'type': 'Small Appliances', 'notes': 'Coffee makers'},
        {'name': 'Oster', 'type': 'Small Appliances', 'notes': 'Blenders and appliances'},
        {'name': 'Sunbeam', 'type': 'Small Appliances', 'notes': 'Small appliances'},
        {'name': 'FoodSaver', 'type': 'Small Appliances', 'notes': 'Vacuum sealers'},
    ],
    'SharkNinja': [
        {'name': 'Shark', 'type': 'Vacuum/Floor Care', 'notes': 'Vacuum cleaners'},
        {'name': 'Ninja', 'type': 'Small Appliances', 'notes': 'Blenders and kitchen appliances'},
    ],
    'Mabe': [
        {'name': 'Mabe', 'type': 'Major Appliances (full-line)', 'notes': 'Main brand'},
        {'name': 'GE (Latin America)', 'type': 'Major Appliances (full-line)', 'notes': 'Manufactured by Mabe'},
        {'name': 'Easy', 'type': 'Major Appliances', 'notes': 'Value brand'},
        {'name': 'Acros', 'type': 'Major Appliances', 'notes': 'Mexican brand'},
    ],
    'Hamilton Beach Brands': [
        {'name': 'Hamilton Beach', 'type': 'Small Appliances', 'notes': 'Main brand'},
        {'name': 'Proctor Silex', 'type': 'Small Appliances', 'notes': 'Value brand'},
    ],
    'Traeger Grills': [{'name': 'Traeger', 'type': 'Outdoor Cooking', 'notes': 'Wood pellet grills'}],
    'Napoleon': [{'name': 'Napoleon', 'type': 'Outdoor Cooking', 'notes': 'Grills and fireplaces'}],
    'Danby': [{'name': 'Danby', 'type': 'Major Appliances (refrigeration)', 'notes': 'Compact appliances'}],
    'Speed Queen': [{'name': 'Speed Queen', 'type': 'Major Appliances (laundry)', 'notes': 'Commercial-grade laundry'}],
    'BlueStar': [{'name': 'BlueStar', 'type': 'Major Appliances (cooking)', 'notes': 'Professional ranges'}],
    'Brown Stove Works': [{'name': 'FiveStar', 'type': 'Major Appliances (cooking)', 'notes': 'Professional ranges'}],
    'American Range': [{'name': 'American Range', 'type': 'Major Appliances (cooking)', 'notes': 'Commercial and residential'}],
    'Elmira Stove Works': [
        {'name': 'Elmira', 'type': 'Major Appliances (cooking)', 'notes': 'Retro-inspired ranges'},
        {'name': 'Northstar', 'type': 'Major Appliances (cooking)', 'notes': 'Retro refrigerators'},
    ],
    'IRobot': [
        {'name': 'Roomba', 'type': 'Vacuum/Floor Care', 'notes': 'Robot vacuums'},
        {'name': 'Braava', 'type': 'Vacuum/Floor Care', 'notes': 'Robot mops'},
    ],
    'Bissell': [{'name': 'Bissell', 'type': 'Vacuum/Floor Care', 'notes': 'Carpet cleaners and vacuums'}],
    'The Hoover Company': [{'name': 'Hoover', 'type': 'Vacuum/Floor Care', 'notes': 'Vacuum cleaners'}],
    'Eureka (company)': [{'name': 'Eureka', 'type': 'Vacuum/Floor Care', 'notes': 'Vacuum cleaners'}],
    'Kirby Company': [{'name': 'Kirby', 'type': 'Vacuum/Floor Care', 'notes': 'Premium vacuum systems'}],
    'Dirt Devil': [{'name': 'Dirt Devil', 'type': 'Vacuum/Floor Care', 'notes': 'Budget vacuum cleaners'}],
    'Cuisinart': [{'name': 'Cuisinart', 'type': 'Small Appliances', 'notes': 'Food processors and kitchen appliances'}],
    'Vitamix': [{'name': 'Vitamix', 'type': 'Small Appliances', 'notes': 'High-performance blenders'}],
    'Blendtec': [{'name': 'Blendtec', 'type': 'Small Appliances', 'notes': 'Professional blenders'}],
}


# =============================================================================
# CATEGORY MAPPING - Maps sub_type to main category
# =============================================================================

CATEGORY_MAP = {
    # Makers
    'Vertically Integrated Manufacturer': 'Makers',
    'Brand Owner': 'Makers',
    'OEM': 'Makers',
    'ODM': 'Makers',
    'Private Label / House Brand Manufacturer': 'Makers',
    'Importer / Brand Licensee': 'Makers',
    # Middle
    'Distributor / Wholesaler': 'Middle',
    'Buying Group / Coop': 'Middle',
    "Rep Firm / Manufacturer's Rep": 'Middle',
    '3PL / Logistics Provider': 'Middle',
    'Rebate Management / Incentive Platform': 'Middle',
    # Sellers
    'Retailer': 'Sellers',
    'Dealer / Independent Showroom': 'Sellers',
    'Franchisee': 'Sellers',
    # Projects
    'Designer / Specifier': 'Projects',
    'Builder / Developer': 'Projects',
    'General Contractor (GC)': 'Projects',
    'Remodeler / Renovation Contractor': 'Projects',
    'Installer / Delivery & Install Partner': 'Projects',
    'Authorized Service Provider': 'Projects',
    # People
    'Sales Associate': 'People',
    'Influencer / Affiliate': 'People',
}


def get_category_from_sub_type(sub_types: List[str]) -> str:
    """Get the main category from a list of sub_types."""
    if not sub_types:
        return 'Makers'  # Default
    for sub_type in sub_types:
        if sub_type in CATEGORY_MAP:
            return CATEGORY_MAP[sub_type]
    return 'Makers'  # Default


# =============================================================================
# HARDCODED DATA - SUPPLEMENTAL PARTNERS (COMPREHENSIVE)
# =============================================================================

def get_supplemental_partners() -> List[Dict]:
    """Comprehensive partner data across all partner types."""
    return [
        # MAKERS - Vertically Integrated Manufacturers
        {'name': 'Whirlpool Corporation', 'country': 'United States', 'website': 'https://www.whirlpoolcorp.com',
         'headquarters': 'Benton Harbor, Michigan', 'type': 'Major Appliances (full-line)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 19400000000, 'source': 'Supplemental',
         'notes': "World's largest appliance manufacturer. Owns Whirlpool, Maytag, KitchenAid, Amana, JennAir"},
        {'name': 'GE Appliances', 'country': 'United States', 'website': 'https://www.geappliances.com',
         'headquarters': 'Louisville, Kentucky', 'type': 'Major Appliances (full-line)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 8000000000, 'source': 'Supplemental',
         'notes': 'Owned by Haier. Owns GE, Monogram, Café, Profile, Hotpoint brands'},
        {'name': 'Electrolux North America', 'country': 'United States', 'website': 'https://www.electrolux.com',
         'headquarters': 'Charlotte, North Carolina', 'type': 'Major Appliances (full-line)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 5000000000, 'source': 'Supplemental',
         'notes': 'Owns Frigidaire, Electrolux brands'},
        {'name': 'BSH Home Appliances', 'country': 'United States', 'website': 'https://www.bsh-group.com',
         'headquarters': 'Irvine, California', 'type': 'Major Appliances (full-line)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 4500000000, 'source': 'Supplemental',
         'notes': 'Owns Bosch, Thermador, Gaggenau brands in North America'},
        {'name': 'LG Electronics USA', 'country': 'United States', 'website': 'https://www.lg.com/us',
         'headquarters': 'Englewood Cliffs, New Jersey', 'type': 'Major Appliances (full-line)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 7000000000, 'source': 'Supplemental',
         'notes': 'Major appliance manufacturer with US production in Tennessee'},
        {'name': 'Samsung Electronics America', 'country': 'United States', 'website': 'https://www.samsung.com/us',
         'headquarters': 'Ridgefield Park, New Jersey', 'type': 'Major Appliances (full-line)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 6500000000, 'source': 'Supplemental',
         'notes': 'Major appliance manufacturer with US production in South Carolina'},
        {'name': 'Sub-Zero Group', 'country': 'United States', 'website': 'https://www.subzero-wolf.com',
         'headquarters': 'Madison, Wisconsin', 'type': 'Specialty/Luxury',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 2500000000, 'source': 'Supplemental',
         'notes': 'Premium appliances. Owns Sub-Zero, Wolf, Cove brands. Made in USA'},
        {'name': 'Viking Range', 'country': 'United States', 'website': 'https://www.vikingrange.com',
         'headquarters': 'Greenwood, Mississippi', 'type': 'Major Appliances (cooking)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'source': 'Supplemental',
         'notes': 'Owned by Middleby Corporation. Professional-style ranges'},
        {'name': 'BlueStar', 'country': 'United States', 'website': 'https://www.bluestarcooking.com',
         'headquarters': 'Reading, Pennsylvania', 'type': 'Major Appliances (cooking)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'source': 'Supplemental',
         'notes': 'Prizer-Painter Stove Works. Professional ranges made in USA since 1880'},
        {'name': 'Speed Queen', 'country': 'United States', 'website': 'https://www.speedqueen.com',
         'headquarters': 'Ripon, Wisconsin', 'type': 'Major Appliances (laundry)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 500000000, 'source': 'Supplemental',
         'notes': 'Alliance Laundry Systems brand. Commercial and residential laundry'},
        {'name': 'Traeger Grills', 'country': 'United States', 'website': 'https://www.traeger.com',
         'headquarters': 'Salt Lake City, Utah', 'type': 'Outdoor Cooking',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 600000000, 'source': 'Supplemental',
         'notes': 'Pellet grill pioneer'},
        {'name': 'Weber-Stephen Products', 'country': 'United States', 'website': 'https://www.weber.com',
         'headquarters': 'Palatine, Illinois', 'type': 'Outdoor Cooking',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 1800000000, 'source': 'Supplemental',
         'notes': 'Leading grill manufacturer'},
        {'name': 'Danby', 'country': 'Canada', 'website': 'https://www.danby.com',
         'headquarters': 'Guelph, Ontario', 'type': 'Major Appliances (refrigeration)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 350000000, 'source': 'Supplemental',
         'notes': 'Compact and specialty appliances'},
        {'name': 'Napoleon', 'country': 'Canada', 'website': 'https://www.napoleon.com',
         'headquarters': 'Barrie, Ontario', 'type': 'Outdoor Cooking',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 400000000, 'source': 'Supplemental',
         'notes': 'Grills, fireplaces, HVAC'},
        {'name': 'Mabe', 'country': 'Mexico', 'website': 'https://www.mabe.com.mx',
         'headquarters': 'Mexico City', 'type': 'Major Appliances (full-line)',
         'sub_type': ['Vertically Integrated Manufacturer'], 'revenue_usd': 2500000000, 'source': 'Supplemental',
         'notes': 'Joint venture with GE. Major Latin American manufacturer'},

        # MAKERS - Brand Owners
        {'name': 'Spectrum Brands', 'country': 'United States', 'website': 'https://www.spectrumbrands.com',
         'headquarters': 'Middleton, Wisconsin', 'type': 'Brand Owner', 'sub_type': ['Brand Owner'],
         'revenue_usd': 3400000000, 'source': 'Supplemental',
         'notes': 'Owns Black+Decker, George Foreman, Russell Hobbs appliance brands'},
        {'name': 'Newell Brands', 'country': 'United States', 'website': 'https://www.newellbrands.com',
         'headquarters': 'Atlanta, Georgia', 'type': 'Brand Owner', 'sub_type': ['Brand Owner'],
         'revenue_usd': 8600000000, 'source': 'Supplemental',
         'notes': 'Owns Crock-Pot, Mr. Coffee, Oster, Sunbeam, FoodSaver brands'},
        {'name': 'Hamilton Beach Brands', 'country': 'United States', 'website': 'https://hamiltonbeach.com',
         'headquarters': 'Glen Allen, Virginia', 'type': 'Small Appliances', 'sub_type': ['Brand Owner'],
         'revenue_usd': 600000000, 'source': 'Supplemental', 'notes': 'Small kitchen appliances'},

        # MAKERS - OEM
        {'name': 'Midea Group', 'country': 'United States', 'website': 'https://www.midea.com',
         'headquarters': 'Parsippany, New Jersey', 'type': 'OEM', 'sub_type': ['OEM'],
         'revenue_usd': 52000000000, 'source': 'Supplemental',
         'notes': 'Major OEM for private label appliances. Owns Toshiba appliances'},
        {'name': 'Galanz Americas', 'country': 'United States', 'website': 'https://www.galanzamericas.com',
         'headquarters': 'Irvine, California', 'type': 'OEM', 'sub_type': ['OEM'], 'source': 'Supplemental',
         'notes': 'OEM specializing in microwaves and small appliances'},
        {'name': 'TTI Floor Care', 'country': 'United States', 'website': 'https://www.ttifloorcare.com',
         'headquarters': 'Charlotte, North Carolina', 'type': 'OEM', 'sub_type': ['OEM'],
         'revenue_usd': 1500000000, 'source': 'Supplemental', 'notes': 'OEM for Hoover, Dirt Devil, Oreck brands'},

        # MAKERS - ODM
        {'name': 'JS Global (SharkNinja)', 'country': 'United States', 'website': 'https://www.sharkninja.com',
         'headquarters': 'Needham, Massachusetts', 'type': 'ODM', 'sub_type': ['ODM'],
         'revenue_usd': 4000000000, 'source': 'Supplemental',
         'notes': 'Designs and manufactures Shark and Ninja brands'},

        # MIDDLE - Distributors
        {'name': 'DERA (Distributor Efficiency & Resource Alliance)', 'country': 'United States',
         'website': 'https://www.dera.com', 'headquarters': 'Dallas, Texas', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'source': 'Supplemental',
         'notes': 'Major appliance distributor alliance'},
        {'name': 'Almo Corporation', 'country': 'United States', 'website': 'https://www.almo.com',
         'headquarters': 'Philadelphia, Pennsylvania', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'revenue_usd': 2500000000, 'source': 'Supplemental',
         'notes': 'Major appliance and consumer electronics distributor'},
        {'name': 'D&H Distributing', 'country': 'United States', 'website': 'https://www.dandh.com',
         'headquarters': 'Harrisburg, Pennsylvania', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'revenue_usd': 7000000000, 'source': 'Supplemental',
         'notes': 'Technology and appliance distributor'},
        {'name': 'Pacific Sales', 'country': 'United States', 'website': 'https://www.pacificsales.com',
         'headquarters': 'Torrance, California', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'source': 'Supplemental',
         'notes': 'Premium appliance distributor. Best Buy subsidiary'},
        {'name': "Warners' Stellian", 'country': 'United States', 'website': 'https://www.warnersstellian.com',
         'headquarters': 'Saint Paul, Minnesota', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'source': 'Supplemental',
         'notes': 'Regional appliance distributor and retailer'},
        {'name': 'Marcone Supply', 'country': 'United States', 'website': 'https://www.marcone.com',
         'headquarters': 'Saint Louis, Missouri', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'revenue_usd': 1000000000, 'source': 'Supplemental',
         'notes': 'Appliance parts distributor'},
        {'name': 'Reliable Parts', 'country': 'United States', 'website': 'https://www.reliableparts.com',
         'headquarters': 'Hayward, California', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'source': 'Supplemental',
         'notes': 'Appliance parts distributor'},
        {'name': 'DERA Canada', 'country': 'Canada', 'website': 'https://www.dera.ca',
         'headquarters': 'Toronto, Ontario', 'type': 'Distributor',
         'sub_type': ['Distributor / Wholesaler'], 'source': 'Supplemental',
         'notes': 'Canadian appliance distributor alliance'},

        # MIDDLE - Buying Groups
        {'name': 'Nationwide Marketing Group (NMG)', 'country': 'United States',
         'website': 'https://www.nationwidegroup.org', 'headquarters': 'Winston-Salem, North Carolina',
         'type': 'Buying Group', 'sub_type': ['Buying Group / Coop'], 'source': 'Supplemental',
         'notes': 'Largest buying group. 5,000+ independent retailers'},
        {'name': 'BrandSource', 'country': 'United States', 'website': 'https://www.brandsource.com',
         'headquarters': 'Anaheim, California', 'type': 'Buying Group', 'sub_type': ['Buying Group / Coop'],
         'source': 'Supplemental', 'notes': 'Major buying group. ~4,000 member locations'},
        {'name': 'MEGA Group USA', 'country': 'United States', 'website': 'https://www.megagroupusa.com',
         'headquarters': 'Dallas, Texas', 'type': 'Buying Group', 'sub_type': ['Buying Group / Coop'],
         'source': 'Supplemental', 'notes': 'Independent dealer buying group'},
        {'name': 'AVB BrandSource', 'country': 'United States', 'website': 'https://www.avb.com',
         'headquarters': 'Anaheim, California', 'type': 'Buying Group', 'sub_type': ['Buying Group / Coop'],
         'source': 'Supplemental', 'notes': 'Appliance, furniture, electronics buying group'},
        {'name': 'NECO Alliance', 'country': 'United States', 'website': 'https://www.necoalliance.com',
         'headquarters': 'Various', 'type': 'Buying Group', 'sub_type': ['Buying Group / Coop'],
         'source': 'Supplemental', 'notes': 'Regional buying cooperative'},
        {'name': 'CANTREX Nationwide', 'country': 'Canada', 'website': 'https://www.cantrex.com',
         'headquarters': 'Mississauga, Ontario', 'type': 'Buying Group', 'sub_type': ['Buying Group / Coop'],
         'source': 'Supplemental', 'notes': 'Canadian buying group. Part of Nationwide'},
        {'name': 'Mega Group Canada', 'country': 'Canada', 'website': 'https://www.megagroupcanada.com',
         'headquarters': 'Vancouver, British Columbia', 'type': 'Buying Group', 'sub_type': ['Buying Group / Coop'],
         'source': 'Supplemental', 'notes': 'Canadian independent dealer buying group'},

        # MIDDLE - Rep Firms
        {'name': 'Springboard Brand & Creative Strategy', 'country': 'United States',
         'website': 'https://www.springboardbrands.com', 'headquarters': 'Dallas, Texas', 'type': 'Rep Firm',
         'sub_type': ["Rep Firm / Manufacturer's Rep"], 'source': 'Supplemental',
         'notes': 'Appliance manufacturer rep firm'},
        {'name': 'The Hartman Company', 'country': 'United States', 'website': 'https://www.hartmanco.com',
         'headquarters': 'Chicago, Illinois', 'type': 'Rep Firm',
         'sub_type': ["Rep Firm / Manufacturer's Rep"], 'source': 'Supplemental',
         'notes': 'Midwest appliance rep firm'},
        {'name': 'Thomas Associates', 'country': 'United States', 'headquarters': 'Various', 'type': 'Rep Firm',
         'sub_type': ["Rep Firm / Manufacturer's Rep"], 'source': 'Supplemental',
         'notes': 'Regional manufacturer rep firm'},

        # SELLERS - Retailers
        {'name': 'Best Buy', 'country': 'United States', 'website': 'https://www.bestbuy.com',
         'headquarters': 'Richfield, Minnesota', 'type': 'Retailer', 'sub_type': ['Retailer'],
         'revenue_usd': 46000000000, 'source': 'Supplemental',
         'notes': 'Major electronics and appliance retailer. ~1,000 stores'},
        {'name': 'The Home Depot', 'country': 'United States', 'website': 'https://www.homedepot.com',
         'headquarters': 'Atlanta, Georgia', 'type': 'Retailer', 'sub_type': ['Retailer'],
         'revenue_usd': 157000000000, 'source': 'Supplemental',
         'notes': 'Largest home improvement retailer. Major appliance seller'},
        {"name": "Lowe's", 'country': 'United States', 'website': 'https://www.lowes.com',
         'headquarters': 'Mooresville, North Carolina', 'type': 'Retailer', 'sub_type': ['Retailer'],
         'revenue_usd': 86000000000, 'source': 'Supplemental',
         'notes': 'Major home improvement and appliance retailer'},
        {'name': 'Costco', 'country': 'United States', 'website': 'https://www.costco.com',
         'headquarters': 'Issaquah, Washington', 'type': 'Retailer', 'sub_type': ['Retailer'],
         'revenue_usd': 242000000000, 'source': 'Supplemental',
         'notes': 'Warehouse club with major appliance sales'},
        {'name': 'Amazon', 'country': 'United States', 'website': 'https://www.amazon.com',
         'headquarters': 'Seattle, Washington', 'type': 'Retailer', 'sub_type': ['Retailer'],
         'revenue_usd': 575000000000, 'source': 'Supplemental',
         'notes': 'Largest online retailer. Growing appliance category'},
        {'name': 'Nebraska Furniture Mart', 'country': 'United States', 'website': 'https://www.nfm.com',
         'headquarters': 'Omaha, Nebraska', 'type': 'Retailer', 'sub_type': ['Retailer'], 'source': 'Supplemental',
         'notes': 'Berkshire Hathaway company. Major appliance retailer'},
        {'name': 'RC Willey', 'country': 'United States', 'website': 'https://www.rcwilley.com',
         'headquarters': 'Salt Lake City, Utah', 'type': 'Retailer', 'sub_type': ['Retailer'],
         'source': 'Supplemental', 'notes': 'Berkshire Hathaway company. Western US appliance retailer'},
        {'name': 'Canadian Tire', 'country': 'Canada', 'website': 'https://www.canadiantire.ca',
         'headquarters': 'Toronto, Ontario', 'type': 'Retailer', 'sub_type': ['Retailer'],
         'revenue_usd': 12000000000, 'source': 'Supplemental',
         'notes': 'Major Canadian retailer with appliances'},

        # SELLERS - Dealers
        {'name': 'Abt Electronics', 'country': 'United States', 'website': 'https://www.abt.com',
         'headquarters': 'Glenview, Illinois', 'type': 'Dealer',
         'sub_type': ['Dealer / Independent Showroom'], 'revenue_usd': 500000000, 'source': 'Supplemental',
         'notes': 'Premium independent appliance dealer'},
        {'name': 'P.C. Richard & Son', 'country': 'United States', 'website': 'https://www.pcrichard.com',
         'headquarters': 'Farmingdale, New York', 'type': 'Dealer',
         'sub_type': ['Dealer / Independent Showroom'], 'revenue_usd': 800000000, 'source': 'Supplemental',
         'notes': 'Northeast US appliance and electronics dealer'},
        {'name': 'Yale Appliance', 'country': 'United States', 'website': 'https://www.yaleappliance.com',
         'headquarters': 'Boston, Massachusetts', 'type': 'Dealer',
         'sub_type': ['Dealer / Independent Showroom'], 'source': 'Supplemental',
         'notes': 'Premium appliance dealer. Strong content marketing'},
        {'name': 'Albert Lee Appliance', 'country': 'United States', 'website': 'https://www.quiteapossibly.com',
         'headquarters': 'Seattle, Washington', 'type': 'Dealer',
         'sub_type': ['Dealer / Independent Showroom'], 'source': 'Supplemental',
         'notes': 'Pacific Northwest premium dealer'},
        {'name': 'Trail Appliances', 'country': 'Canada', 'website': 'https://www.trailappliances.com',
         'headquarters': 'Vancouver, British Columbia', 'type': 'Dealer',
         'sub_type': ['Dealer / Independent Showroom'], 'source': 'Supplemental',
         'notes': 'Western Canada premium appliance dealer'},
        {'name': 'Tasco Appliances', 'country': 'Canada', 'website': 'https://www.tasco.ca',
         'headquarters': 'Toronto, Ontario', 'type': 'Dealer',
         'sub_type': ['Dealer / Independent Showroom'], 'source': 'Supplemental',
         'notes': 'Ontario premium appliance dealer'},

        # SELLERS - Franchisees
        {'name': 'Sears Hometown Stores', 'country': 'United States',
         'website': 'https://www.searshometownstores.com', 'headquarters': 'Various', 'type': 'Franchisee',
         'sub_type': ['Franchisee'], 'source': 'Supplemental', 'notes': 'Franchised appliance stores'},

        # PROJECTS - Builders
        {'name': 'D.R. Horton', 'country': 'United States', 'website': 'https://www.drhorton.com',
         'headquarters': 'Arlington, Texas', 'type': 'Builder', 'sub_type': ['Builder / Developer'],
         'revenue_usd': 36000000000, 'source': 'Supplemental',
         'notes': 'Largest US homebuilder. Major appliance buyer'},
        {'name': 'Lennar', 'country': 'United States', 'website': 'https://www.lennar.com',
         'headquarters': 'Miami, Florida', 'type': 'Builder', 'sub_type': ['Builder / Developer'],
         'revenue_usd': 34000000000, 'source': 'Supplemental', 'notes': 'Major national homebuilder'},
        {'name': 'PulteGroup', 'country': 'United States', 'website': 'https://www.pultegroupinc.com',
         'headquarters': 'Atlanta, Georgia', 'type': 'Builder', 'sub_type': ['Builder / Developer'],
         'revenue_usd': 16000000000, 'source': 'Supplemental',
         'notes': 'National homebuilder. Pulte Homes, Del Webb, Centex brands'},
        {'name': 'NVR Inc.', 'country': 'United States', 'website': 'https://www.nvrinc.com',
         'headquarters': 'Reston, Virginia', 'type': 'Builder', 'sub_type': ['Builder / Developer'],
         'revenue_usd': 10000000000, 'source': 'Supplemental', 'notes': 'Ryan Homes, NVHomes brands'},
        {'name': 'Toll Brothers', 'country': 'United States', 'website': 'https://www.tollbrothers.com',
         'headquarters': 'Fort Washington, Pennsylvania', 'type': 'Builder', 'sub_type': ['Builder / Developer'],
         'revenue_usd': 10000000000, 'source': 'Supplemental',
         'notes': 'Luxury homebuilder. Premium appliance packages'},
        {'name': 'Mattamy Homes', 'country': 'Canada', 'website': 'https://www.mattamyhomes.com',
         'headquarters': 'Toronto, Ontario', 'type': 'Builder', 'sub_type': ['Builder / Developer'],
         'source': 'Supplemental', 'notes': 'Largest privately owned homebuilder in North America'},

        # PROJECTS - Service Providers
        {'name': 'A&E Factory Service', 'country': 'United States', 'website': 'https://www.aefactoryservice.com',
         'headquarters': 'Various', 'type': 'Service Provider', 'sub_type': ['Authorized Service Provider'],
         'source': 'Supplemental', 'notes': 'Major appliance service provider. Transformco company'},
        {'name': 'Mr. Appliance', 'country': 'United States', 'website': 'https://www.mrappliance.com',
         'headquarters': 'Waco, Texas', 'type': 'Service Provider', 'sub_type': ['Authorized Service Provider'],
         'source': 'Supplemental', 'notes': 'Appliance repair franchise. Neighborly brand'},
        {'name': 'Sears Home Services', 'country': 'United States', 'website': 'https://www.searshomeservices.com',
         'headquarters': 'Various', 'type': 'Service Provider', 'sub_type': ['Authorized Service Provider'],
         'source': 'Supplemental', 'notes': 'Appliance repair and service network'},

        # PROJECTS - Installers
        {'name': 'Installation Made Easy (IME)', 'country': 'United States', 'website': 'https://www.imehome.com',
         'headquarters': 'Minneapolis, Minnesota', 'type': 'Installer',
         'sub_type': ['Installer / Delivery & Install Partner'], 'source': 'Supplemental',
         'notes': 'Major appliance installation network'},
        {'name': 'ServiceLive', 'country': 'United States', 'website': 'https://www.servicelive.com',
         'headquarters': 'Troy, Michigan', 'type': 'Installer',
         'sub_type': ['Installer / Delivery & Install Partner'], 'source': 'Supplemental',
         'notes': 'Installation and service marketplace'},

        # MIDDLE - 3PL
        {'name': 'XPO Logistics', 'country': 'United States', 'website': 'https://www.xpo.com',
         'headquarters': 'Greenwich, Connecticut', 'type': '3PL', 'sub_type': ['3PL / Logistics Provider'],
         'revenue_usd': 7700000000, 'source': 'Supplemental',
         'notes': 'Major last-mile delivery provider for appliances'},
        {'name': 'J.B. Hunt Transport', 'country': 'United States', 'website': 'https://www.jbhunt.com',
         'headquarters': 'Lowell, Arkansas', 'type': '3PL', 'sub_type': ['3PL / Logistics Provider'],
         'revenue_usd': 15000000000, 'source': 'Supplemental', 'notes': 'Final mile delivery services'},
        {'name': 'Ryder System', 'country': 'United States', 'website': 'https://www.ryder.com',
         'headquarters': 'Miami, Florida', 'type': '3PL', 'sub_type': ['3PL / Logistics Provider'],
         'revenue_usd': 11000000000, 'source': 'Supplemental',
         'notes': 'Last mile and white glove delivery services'},
        {'name': 'MXD Group', 'country': 'United States', 'website': 'https://www.mxdgroup.com',
         'headquarters': 'St. Louis, Missouri', 'type': '3PL', 'sub_type': ['3PL / Logistics Provider'],
         'source': 'Supplemental', 'notes': 'Appliance last-mile delivery specialist'}
    ]


# =============================================================================
# WIKIDATA QUERIES
# =============================================================================

def fetch_manufacturers_from_wikidata() -> List[Dict]:
    """Fetch manufacturers from Wikidata SPARQL endpoint with proper field mapping."""
    endpoint = "https://query.wikidata.org/sparql"
    query = """
    SELECT DISTINCT ?manufacturer ?manufacturerLabel ?countryLabel ?websiteUrl ?headquartersLabel ?founded WHERE {
      ?manufacturer wdt:P31 ?type. FILTER(?type IN (wd:Q4830453, wd:Q783794, wd:Q891723))
      ?manufacturer wdt:P1056 ?product.
      FILTER(?product IN (wd:Q46587, wd:Q14514, wd:Q178692, wd:Q33284, wd:Q79922,
        wd:Q1189815, wd:Q1501817, wd:Q15779252, wd:Q751797))
      ?manufacturer wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
      OPTIONAL { ?manufacturer wdt:P856 ?websiteUrl. }
      OPTIONAL { ?manufacturer wdt:P159 ?headquarters. }
      OPTIONAL { ?manufacturer wdt:P571 ?founded. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    } ORDER BY ?countryLabel ?manufacturerLabel LIMIT 100
    """

    headers = {'User-Agent': USER_AGENT, 'Accept': 'application/json'}

    try:
        print("Fetching manufacturers from Wikidata...")
        time.sleep(RATE_LIMIT_DELAY)
        response = requests.get(endpoint, params={'query': query, 'format': 'json'},
                                headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        manufacturers = []
        for result in data.get('results', {}).get('bindings', []):
            mfr = {
                'name': result['manufacturerLabel']['value'],
                'country': result['countryLabel']['value'],
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

        print(f"Found {len(manufacturers)} manufacturers from Wikidata")
        return manufacturers

    except Exception as e:
        print(f"  Error fetching manufacturers from Wikidata: {e}")
        return []


def get_wikidata_query(entity_type: str) -> str:
    """Get SPARQL query for specific entity type."""
    queries = {
        'buying_groups': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P31 wd:Q4508. } UNION { ?entity wdt:P452 wd:Q215353. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'dealers': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P452 wd:Q216107. } UNION { ?entity wdt:P31 wd:Q508380. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'distributors': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P452 wd:Q178561. } UNION { ?entity wdt:P31 wd:Q1266946. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'ecommerce_platforms': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P31 wd:Q843895. } UNION { ?entity wdt:P452 wd:Q484652. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'incentive_platforms': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P452 wd:Q7397. } UNION { ?entity wdt:P31 wd:Q1616075. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'integrators': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P31 wd:Q1058914. } UNION { ?entity wdt:P452 wd:Q11661. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'pos_providers': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P452 wd:Q7091182. } UNION { ?entity wdt:P1056 wd:Q1172284. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'sales_agencies': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P31 wd:Q891723. } UNION { ?entity wdt:P452 wd:Q4830453. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """,
        'service_providers': """
            SELECT DISTINCT ?entity ?entityLabel ?countryLabel ?websiteUrl WHERE {
              { ?entity wdt:P31 wd:Q1664720. } UNION { ?entity wdt:P452 wd:Q7406919. }
              ?entity wdt:P17 ?country. FILTER(?country IN (wd:Q30, wd:Q16, wd:Q96))
              OPTIONAL { ?entity wdt:P856 ?websiteUrl. }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            } LIMIT 100
        """
    }
    return queries.get(entity_type, '')


# =============================================================================
# WIKIPEDIA SCRAPING
# =============================================================================

def scrape_wikipedia_category(category_url: str, country: str) -> List[Dict]:
    """Scrape manufacturer names from Wikipedia category page."""
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

        links = []
        for category_div in category_divs:
            links.extend(category_div.find_all('a'))

        for link in links:
            name = link.get_text().strip()
            wiki_path = link.get('href', '')

            if ':' in name or not wiki_path.startswith('/wiki/'):
                continue
            if 'may not reflect recent changes' in name.lower() or name.startswith('List of'):
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
    """Fetch manufacturers from Wikipedia category pages."""
    categories = [
        {'url': 'https://en.wikipedia.org/wiki/Category:Home_appliance_manufacturers_of_the_United_States',
         'country': 'United States'},
        {'url': 'https://en.wikipedia.org/wiki/Category:Home_appliance_manufacturers_of_Canada',
         'country': 'Canada'}
    ]

    all_manufacturers = []
    for category in categories:
        manufacturers = scrape_wikipedia_category(category['url'], category['country'])
        all_manufacturers.extend(manufacturers)

    print(f"  Total from Wikipedia: {len(all_manufacturers)} manufacturers")
    return all_manufacturers


def scrape_website_from_wikipedia(wikipedia_url: str, manufacturer_name: str) -> Optional[str]:
    """Extract website URL from Wikipedia infobox."""
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
    """Enrich manufacturer data with website URLs from Wikipedia pages."""
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

    print(f"  Successfully scraped {scraped_count} website URLs from Wikipedia")
    return enriched


# =============================================================================
# STATES DATA (pgeocode)
# =============================================================================

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


# =============================================================================
# CLASSIFICATION FUNCTIONS
# =============================================================================

def classify_manufacturer_type(name: str, notes: str = '') -> str:
    """Classify manufacturer product type based on name and notes."""
    name_lower = name.lower()
    notes_lower = notes.lower() if notes else ''
    combined = f"{name_lower} {notes_lower}"

    if any(keyword in combined for keyword in ['vacuum', 'hoover', 'bissell', 'eureka', 'kirby', 'dirt devil', 'shark', 'irobot', 'roomba', 'floor care']):
        return 'Vacuum/Floor Care'
    if any(keyword in combined for keyword in ['blender', 'mixer', 'toaster', 'coffee', 'cuisinart', 'hamilton beach', 'oster', 'sunbeam', 'proctor', 'vitamix', 'small appliance']):
        return 'Small Appliances'
    if any(keyword in combined for keyword in ['grill', 'bbq', 'outdoor', 'traeger', 'napoleon', 'pellet', 'weber']):
        return 'Outdoor Cooking'
    if any(keyword in combined for keyword in ['laundry', 'washing', 'dryer', 'speed queen', 'alliance laundry']):
        return 'Major Appliances (laundry)'
    if any(keyword in combined for keyword in ['range', 'stove', 'oven', 'cooktop', 'cooking', 'viking', 'bluestar', 'wolf', 'thermador']):
        return 'Major Appliances (cooking)'
    if any(keyword in combined for keyword in ['refrigerat', 'freezer', 'ice', 'sub-zero', 'u-line', 'danby']):
        return 'Major Appliances (refrigeration)'
    if any(keyword in combined for keyword in ['luxury', 'premium', 'professional', 'jenn-air', 'monogram', 'cove']):
        return 'Specialty/Luxury'
    if any(keyword in combined for keyword in ['whirlpool', 'ge appliance', 'electrolux', 'frigidaire', 'maytag', 'mabe', 'full-line']):
        return 'Major Appliances (full-line)'

    return 'Major Appliances'


def classify_sub_type(name: str, notes: str = '', mfr_type: str = '') -> List[str]:
    """Classify manufacturer sub_type based on name, notes, and type."""
    name_lower = name.lower()
    notes_lower = notes.lower() if notes else ''
    combined = f"{name_lower} {notes_lower} {mfr_type.lower() if mfr_type else ''}"

    result = []

    vertically_integrated = [
        'whirlpool', 'ge appliances', 'electrolux', 'frigidaire', 'sub-zero',
        'wolf', 'thermador', 'bosch', 'kitchenaid', 'maytag', 'lg electronics',
        'samsung', 'viking', 'speed queen', 'traeger', 'weber', 'napoleon',
        'danby', 'bluestar', 'brown stove', 'american range', 'dacor'
    ]
    if any(brand in name_lower for brand in vertically_integrated):
        result.append('Vertically Integrated Manufacturer')

    if any(keyword in combined for keyword in ['brand owner', 'holding company', 'spectrum brands', 'newell brands']):
        if 'Vertically Integrated Manufacturer' not in result:
            result.append('Brand Owner')

    if any(keyword in combined for keyword in ['oem', 'original equipment', 'midea', 'galanz', 'tti floor care']):
        result.append('OEM')

    if any(keyword in combined for keyword in ['odm', 'design manufacturer', 'js global', 'sharkninja']):
        result.append('ODM')

    if not result:
        result.append('Vertically Integrated Manufacturer')

    return result


# =============================================================================
# DATA GENERATION FUNCTIONS
# =============================================================================

def generate_simple_data(entity_type: str, hardcoded_func, filename: str, description: str,
                         category: Optional[str] = None, sub_type: Optional[str] = None) -> Dict:
    """Generate simple entity data with Wikidata + hardcoded fallback."""
    print(f"\nGenerating {description} data...")

    query = get_wikidata_query(entity_type)
    entities = fetch_from_wikidata(query, entity_type) if query else None

    if not entities:
        print("Using hardcoded fallback data...")
        entities = hardcoded_func()
        data_source = "hardcoded"
    else:
        print("Successfully fetched data from Wikidata")
        data_source = "Wikidata"

    by_country = organize_by_country(entities, category=category, sub_type=sub_type)
    total_count = sum(len(country_data["entities"]) for country_data in by_country.values())

    data = {
        "metadata": create_metadata(data_source, total_count),
        "North America": by_country
    }

    output_file = save_json(data, filename)
    print(f"Successfully generated {output_file}")
    print(f"  - Total: {total_count}")

    return data


def generate_buying_groups_data() -> Dict:
    return generate_simple_data('buying_groups', get_hardcoded_buying_groups,
                                'buying_groups.json', 'buying groups',
                                category='Middle', sub_type='Buying Group / Coop')


def generate_dealers_data() -> Dict:
    return generate_simple_data('dealers', get_hardcoded_dealers,
                                'dealers.json', 'dealers',
                                category='Sellers', sub_type='Dealer / Independent Showroom')


def generate_distributors_data() -> Dict:
    return generate_simple_data('distributors', get_hardcoded_distributors,
                                'distributors.json', 'distributors',
                                category='Middle', sub_type='Distributor / Wholesaler')


def generate_ecommerce_platforms_data() -> Dict:
    print("\nGenerating eCommerce platforms data...")
    print("Using curated industry data...")
    entities = get_hardcoded_ecommerce_platforms()

    by_country = organize_by_country(entities, category='Sellers', sub_type='Retailer')
    total_count = sum(len(country_data["entities"]) for country_data in by_country.values())

    data = {
        "metadata": create_metadata("Industry Research (2026)", total_count),
        "North America": by_country
    }

    output_file = save_json(data, 'ecommerce_platforms.json')
    print(f"Successfully generated {output_file}")
    return data


def generate_incentive_platforms_data() -> Dict:
    print("\nGenerating incentive platforms data...")
    print("Using curated industry data...")
    entities = get_hardcoded_incentive_platforms()

    by_country = organize_by_country(entities, category='Middle', sub_type='Rebate Management / Incentive Platform')
    total_count = sum(len(country_data["entities"]) for country_data in by_country.values())

    data = {
        "metadata": create_metadata("Industry Research (2026)", total_count),
        "North America": by_country
    }

    output_file = save_json(data, 'incentive_platforms.json')
    print(f"Successfully generated {output_file}")
    return data


def generate_incentive_program_types_data() -> Dict:
    print("\nGenerating incentive program types reference data...")
    program_types = get_incentive_program_types()

    data = {
        "metadata": {
            "date_created": datetime.now(timezone.utc).isoformat(),
            "description": "Reference guide to common incentive program types used in channel sales",
            "total_types": len(program_types),
            "purpose": "Educational reference - not a list of software platforms"
        },
        "program_types": program_types
    }

    output_file = save_json(data, 'incentive_program_types.json')
    print(f"Successfully generated {output_file}")
    return data


def generate_integrators_data() -> Dict:
    print("\nGenerating integrators data...")
    print("Using curated industry data...")
    entities = get_hardcoded_integrators()

    by_country = organize_by_country(entities, category='Projects', sub_type='Installer / Delivery & Install Partner')
    total_count = sum(len(country_data["entities"]) for country_data in by_country.values())

    data = {
        "metadata": create_metadata("Industry Research (2026)", total_count),
        "North America": by_country
    }

    output_file = save_json(data, 'integrators.json')
    print(f"Successfully generated {output_file}")
    return data


def generate_pos_providers_data() -> Dict:
    return generate_simple_data('pos_providers', get_hardcoded_pos_providers,
                                'pos_providers.json', 'POS providers',
                                category='Middle', sub_type='Distributor / Wholesaler')


def generate_sales_agencies_data() -> Dict:
    print("\nGenerating sales agencies data...")
    print("Using curated industry data...")
    entities = get_hardcoded_sales_agencies()

    by_country = organize_by_country(entities, category='Middle', sub_type="Rep Firm / Manufacturer's Rep")
    total_count = sum(len(country_data["entities"]) for country_data in by_country.values())

    data = {
        "metadata": create_metadata("Industry Research (2026)", total_count),
        "North America": by_country
    }

    output_file = save_json(data, 'sales_agencies.json')
    print(f"Successfully generated {output_file}")
    return data


def generate_service_providers_data() -> Dict:
    print("\nGenerating service providers data...")
    print("Using curated industry data...")
    entities = get_hardcoded_service_providers()

    by_country = organize_by_country(entities, category='Projects', sub_type='Authorized Service Provider')
    total_count = sum(len(country_data["entities"]) for country_data in by_country.values())

    data = {
        "metadata": create_metadata("Industry Research (2026)", total_count),
        "North America": by_country
    }

    output_file = save_json(data, 'service_providers.json')
    print(f"Successfully generated {output_file}")
    return data


def generate_states_data() -> Dict:
    print("\nGenerating states data...")

    countries_data = None
    if PGEOCODE_AVAILABLE:
        result = {}
        us_states = fetch_states_from_pgeocode('US', 'United States')
        if us_states:
            result['United States'] = {'states': us_states}
        ca_divisions = fetch_states_from_pgeocode('CA', 'Canada')
        if ca_divisions:
            result['Canada'] = categorize_canadian_divisions(ca_divisions)
        mx_states = fetch_states_from_pgeocode('MX', 'Mexico')
        if mx_states:
            result['Mexico'] = {'states': mx_states}
        countries_data = result if result else None

    if not countries_data:
        print("Using hardcoded fallback data...")
        countries_data = get_hardcoded_states()
        data_source = "hardcoded"
    else:
        print("Successfully fetched data from GeoNames")
        data_source = "GeoNames"

    data = {
        "metadata": {
            "date_pulled": datetime.now(timezone.utc).isoformat(),
            "source": data_source,
            "license": "CC BY 4.0",
            "license_url": "https://creativecommons.org/licenses/by/4.0/"
        },
        "North America": countries_data
    }

    output_file = save_json(data, 'states.json')
    print(f"Successfully generated {output_file}")
    return data


def generate_brands_data() -> Dict:
    print("\nGenerating brands data...")

    # Load partners data
    partners_file = os.path.join(DATA_DIR, 'partners.json')
    try:
        with open(partners_file, 'r', encoding='utf-8') as f:
            manufacturers_data = json.load(f)
        print(f"Loaded {manufacturers_data['metadata']['total_manufacturers']} manufacturers")
    except FileNotFoundError:
        print("Warning: partners.json not found. Run generate_partners_data() first.")
        manufacturers_data = {'North America': {'United States': {'manufacturers': []},
                                                'Canada': {'manufacturers': []},
                                                'Mexico': {'manufacturers': []}},
                             'metadata': {'total_manufacturers': 0}}

    all_brands = []
    brands_by_manufacturer = {}

    for country, country_data in manufacturers_data.get('North America', {}).items():
        for manufacturer in country_data.get('manufacturers', []):
            mfr_name = manufacturer['name']
            brands = BRAND_RELATIONSHIPS.get(mfr_name, [])

            if not brands:
                brands = [{
                    'name': mfr_name,
                    'type': manufacturer.get('type', 'Major Appliances'),
                    'notes': 'Self-branded manufacturer'
                }]

            for brand in brands:
                brand_entry = {
                    'brand_name': brand['name'],
                    'parent_company': mfr_name,
                    'country': country,
                    'type': brand['type'],
                    'notes': brand.get('notes', ''),
                }
                if 'headquarters' in manufacturer:
                    brand_entry['headquarters'] = manufacturer['headquarters']
                if 'website' in manufacturer:
                    brand_entry['parent_website'] = manufacturer['website']
                if 'revenue_usd' in manufacturer:
                    brand_entry['parent_revenue_usd'] = manufacturer['revenue_usd']

                all_brands.append(brand_entry)

            brands_by_manufacturer[mfr_name] = brands

    all_brands.sort(key=lambda x: x['brand_name'])

    brands_by_country = {'United States': [], 'Canada': [], 'Mexico': []}
    for brand in all_brands:
        brands_by_country[brand['country']].append(brand)

    brands_by_type = {}
    for brand in all_brands:
        brand_type = brand['type']
        if brand_type not in brands_by_type:
            brands_by_type[brand_type] = []
        brands_by_type[brand_type].append(brand)

    data = {
        "metadata": {
            "date_generated": datetime.now(timezone.utc).isoformat(),
            "source": "Python Script",
            "derived_from": "partners.json",
            "total_brands": len(all_brands),
            "total_parent_companies": len(brands_by_manufacturer),
            "license": "CC0 1.0 / Creative Commons",
            "license_url": "https://creativecommons.org/publicdomain/zero/1.0/"
        },
        "brands": {
            "all": all_brands,
            "by_country": brands_by_country,
            "by_type": brands_by_type,
            "by_manufacturer": brands_by_manufacturer
        }
    }

    output_file = save_json(data, 'brands.json')
    print(f"Successfully generated {output_file}")
    print(f"  - Total brands: {len(all_brands)}")
    return data


def generate_partners_data() -> Optional[Dict]:
    """Generate complete partners dataset from all sources."""
    print("\n" + "=" * 70)
    print("GENERATING PARTNERS DATA")
    print("=" * 70)

    sources = []

    # Source 1: Wikidata
    wikidata_manufacturers = fetch_manufacturers_from_wikidata()
    if wikidata_manufacturers:
        sources.append(wikidata_manufacturers)

    # Source 2: Wikipedia
    print("\nFetching manufacturers from Wikipedia categories...")
    wikipedia_manufacturers = fetch_manufacturers_from_wikipedia()
    if wikipedia_manufacturers:
        sources.append(wikipedia_manufacturers)

    # Source 3: Supplemental data
    print("\nAdding supplemental partners data...")
    supplemental_partners = get_supplemental_partners()
    print(f"  Added {len(supplemental_partners)} supplemental partners")
    sources.append(supplemental_partners)

    if not sources:
        print("\nERROR: Failed to fetch data from any source.")
        return None

    # Merge and deduplicate
    print("\nMerging and deduplicating...")
    all_manufacturers = []
    for source_list in sources:
        all_manufacturers.extend(source_list)

    grouped = {}
    for mfr in all_manufacturers:
        norm_name = normalize_name(mfr['name'])
        if norm_name not in grouped:
            grouped[norm_name] = []
        grouped[norm_name].append(mfr)

    merged = []
    for norm_name, duplicates in grouped.items():
        if len(duplicates) == 1:
            merged.append(duplicates[0])
        else:
            base = duplicates[0].copy()
            base['name'] = max([d['name'] for d in duplicates], key=len)
            for key in ['website', 'headquarters', 'founded', 'wikipedia_url', 'wikidata_id', 'notes', 'type', 'revenue_usd']:
                for d in duplicates:
                    if key in d and d[key] and key not in base:
                        base[key] = d[key]
            merged.append(base)

    print(f"  Total after deduplication: {len(merged)}")

    # Enrich with websites
    merged = enrich_manufacturers_with_websites(merged)

    # Organize by country
    by_country = {'United States': {'manufacturers': []}, 'Canada': {'manufacturers': []}, 'Mexico': {'manufacturers': []}}

    for mfr in merged:
        country = mfr.pop('country', 'United States')
        if 'type' not in mfr:
            mfr['type'] = classify_manufacturer_type(mfr.get('name', ''), mfr.get('notes', ''))
        if 'sub_type' not in mfr:
            mfr['sub_type'] = classify_sub_type(mfr.get('name', ''), mfr.get('notes', ''), mfr.get('type', ''))
        # Add category based on sub_type
        if 'category' not in mfr:
            mfr['category'] = get_category_from_sub_type(mfr.get('sub_type', []))
        by_country[country]['manufacturers'].append(mfr)

    for country in by_country:
        by_country[country]['manufacturers'].sort(key=lambda x: x['name'])

    total_count = sum(len(country_data["manufacturers"]) for country_data in by_country.values())

    data = {
        "metadata": {
            "date_pulled": datetime.now(timezone.utc).isoformat(),
            "source": "Python Script",
            "sources": ["Wikidata", "Wikipedia", "Manual Curation"],
            "total_manufacturers": total_count,
            "license": "CC0 1.0 / Creative Commons",
            "license_url": "https://creativecommons.org/publicdomain/zero/1.0/"
        },
        "North America": by_country
    }

    output_file = save_json(data, 'partners.json')
    print(f"\nSuccessfully generated {output_file}")
    print(f"  - Total manufacturers: {total_count}")
    return data


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def generate_all():
    """Generate all data files."""
    print("\n" + "=" * 70)
    print("NORTH AMERICAN CHANNEL PARTNERS DATA GENERATOR")
    print("=" * 70)

    generate_states_data()
    generate_buying_groups_data()
    generate_dealers_data()
    generate_distributors_data()
    generate_ecommerce_platforms_data()
    generate_incentive_platforms_data()
    generate_incentive_program_types_data()
    generate_integrators_data()
    generate_pos_providers_data()
    generate_sales_agencies_data()
    generate_service_providers_data()
    generate_partners_data()
    generate_brands_data()

    print("\n" + "=" * 70)
    print("ALL DATA FILES GENERATED SUCCESSFULLY")
    print("=" * 70 + "\n")


def main():
    """Main entry point with command line arguments."""
    parser = argparse.ArgumentParser(description='Generate channel partner data files')
    parser.add_argument('--all', action='store_true', help='Generate all data files')
    parser.add_argument('--partners', action='store_true', help='Generate partners.json')
    parser.add_argument('--brands', action='store_true', help='Generate brands.json')
    parser.add_argument('--buying-groups', action='store_true', help='Generate buying_groups.json')
    parser.add_argument('--dealers', action='store_true', help='Generate dealers.json')
    parser.add_argument('--distributors', action='store_true', help='Generate distributors.json')
    parser.add_argument('--ecommerce', action='store_true', help='Generate ecommerce_platforms.json')
    parser.add_argument('--incentive-platforms', action='store_true', help='Generate incentive_platforms.json')
    parser.add_argument('--incentive-types', action='store_true', help='Generate incentive_program_types.json')
    parser.add_argument('--integrators', action='store_true', help='Generate integrators.json')
    parser.add_argument('--pos', action='store_true', help='Generate pos_providers.json')
    parser.add_argument('--sales-agencies', action='store_true', help='Generate sales_agencies.json')
    parser.add_argument('--service-providers', action='store_true', help='Generate service_providers.json')
    parser.add_argument('--states', action='store_true', help='Generate states.json')

    args = parser.parse_args()

    # If no args provided, generate all
    if not any(vars(args).values()):
        generate_all()
        return

    if args.all:
        generate_all()
        return

    if args.states:
        generate_states_data()
    if args.buying_groups:
        generate_buying_groups_data()
    if args.dealers:
        generate_dealers_data()
    if args.distributors:
        generate_distributors_data()
    if args.ecommerce:
        generate_ecommerce_platforms_data()
    if args.incentive_platforms:
        generate_incentive_platforms_data()
    if args.incentive_types:
        generate_incentive_program_types_data()
    if args.integrators:
        generate_integrators_data()
    if args.pos:
        generate_pos_providers_data()
    if args.sales_agencies:
        generate_sales_agencies_data()
    if args.service_providers:
        generate_service_providers_data()
    if args.partners:
        generate_partners_data()
    if args.brands:
        generate_brands_data()


if __name__ == "__main__":
    main()
