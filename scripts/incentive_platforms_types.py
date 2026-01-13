"""
Generate incentive_program_types.json - a reference list of incentive program types.

This file contains definitions and examples of different types of incentive programs
used in channel sales and distribution, not the software platforms that manage them.

Data Sources:
- Industry standard incentive program terminology
- Common channel sales practices

Usage: python scripts/incentive_platforms_types.py
"""
import json
import os
from datetime import datetime, timezone
from typing import Dict, List


def get_incentive_program_types() -> Dict[str, Dict]:
    """Reference data for types of incentive programs used in channel sales."""
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
            "definition": "Incentives paid to dealers based on products sold to end customers, not just purchased from distributor (rewards sell-through, not stock).",
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
            "definition": "Tiered rewards based on purchase or sales volume over a defined period, encouraging larger order quantities.",
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
            "definition": "Rewards for year-over-year growth, encouraging partners to expand their business with the manufacturer.",
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
            "definition": "Special incentives to drive adoption and sales of newly launched products during market introduction period.",
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
            "definition": "Payments to dealers for featuring products in prominent floor displays, endcaps, or featured positions.",
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
            "definition": "Enhanced incentives for selling product combinations, accessories, or complete solutions together.",
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
            "definition": "Support for dealers to purchase and display floor models, demo units, or samples in-store.",
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


def generate_incentive_program_types_data() -> Dict:
    """Generate reference data for incentive program types."""
    print("Generating incentive program types reference data...")

    program_types = get_incentive_program_types()

    return {
        "metadata": {
            "date_created": datetime.now(timezone.utc).isoformat(),
            "description": "Reference guide to common incentive program types used in channel sales",
            "total_types": len(program_types),
            "purpose": "Educational reference - not a list of software platforms"
        },
        "program_types": program_types
    }


def main():
    """Generate and save incentive_program_types.json to data directory."""
    print("Generating incentive program types reference...")
    data = generate_incentive_program_types_data()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data')
    os.makedirs(data_dir, exist_ok=True)

    output_file = os.path.join(data_dir, 'incentive_program_types.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nSuccessfully generated {output_file}")
    print(f"  - Total program types: {data['metadata']['total_types']}")
    print("\nProgram types included:")
    for program_type in data['program_types'].keys():
        print(f"  - {program_type}")


if __name__ == "__main__":
    main()
