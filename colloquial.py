"""
Colloquial Payer Override System
================================

This module handles payer-specific overrides for 835 EDI processing. It provides a
centralized location for managing non-standard behaviors from specific payers,
allowing the main parser (parser_835.py) and validation (validation.py) to remain
standard-compliant while accommodating real-world variations.

================================================================================
HOW TO IMPLEMENT A NEW PAYER (Instructions for Future Claudes)
================================================================================

STEP 1: GATHER PAYER INFORMATION
--------------------------------
Before adding a new payer, you need:
  a) TRN03 value - The Originating Company Identifier from the TRN segment
     - PRIMARY identifier for most payers (Medi-Cal, commercial)
     - Found in: TRN*1*{trace_number}*{TRN03_VALUE}
     - Example: TRN*1*123456789*1999999999 → TRN03 = "1999999999"

  b) ISA06 value - The Interchange Sender ID from the ISA segment
     - PRIMARY identifier for batch senders (eMedNY, clearinghouses)
     - Found in: ISA*00*...*ZZ*{ISA06_VALUE}*...
     - Example: ISA*00*...*ZZ*EMEDNYBAT*... → ISA06 = "EMEDNYBAT"

  c) Payer Name - The exact string from N1*PR segment
     - FALLBACK identifier for all payers
     - Found in: N1*PR*{PAYER_NAME}*...
     - Example: N1*PR*MEDI CAL FISCAL INTERMEDIARY*... → "MEDI CAL FISCAL INTERMEDIARY"

  d) Known quirks/variations from standard X12:
     - Do they use leading zeros in CARC codes? (e.g., "0012" instead of "12")
     - Do they use generic payer IDs? (e.g., "999999")
     - Any payer-specific RARC codes?
     - Any different meanings for standard reference qualifier codes?

STEP 2: ADD ENTRY TO PAYER_REGISTRY
-----------------------------------
Add a new entry to the PAYER_REGISTRY dictionary (below). Use this template:

    "PAYER_KEY": {
        "description": "Human-readable payer name",
        "identifiers": {
            "trn03": ["originating_company_id"],      # Primary for most payers
            "isa06": ["batch_sender_id"],             # Primary for batch senders (eMedNY)
            "payer_name": ["EXACT PAYER NAME"]        # Fallback for all payers
        },
        "normalize_carc_codes": False,    # True if payer uses leading zeros
        "validation_overrides": {
            "allow_generic_payer_id": False,  # True if payer uses generic IDs
        },
        "parsing_rules": {},              # Reserved for future parsing adjustments
        "dictionary_overrides": {
            "reference_qualifiers": {
                # "CODE": "Payer-specific meaning"
            },
            "priority_rarc_codes": [],    # Payer-specific RARC codes to highlight
        },
        "notes": [
            "Documentation notes about this payer's quirks"
        ]
    },

STEP 3: ADD PAYER-SPECIFIC CODES TO dictionary.py (if needed)
-------------------------------------------------------------
If the payer uses codes not in the standard dictionary:
  - Add them to the appropriate function in dictionary.py
  - Prefix description with [STATE] for state-specific codes
  - Example: 'N908': '[CA] Alert: This claim was paid using a state supplemental payment'

STEP 4: NO CHANGES NEEDED TO parser_835.py OR validation.py
----------------------------------------------------
The main program and validation already call this module:
  - parser_835.py calls identify_payer() and normalize_carc_code()
  - validation.py calls normalize_carc_code() for CARC validation
  - Payer-specific lookups use get_payer_reference_qualifier_description(), etc.

ARCHITECTURE NOTES
------------------
- This system is PAYER-LEVEL, not state-level
- Each payer is independent (LA Care doesn't inherit from Medi-Cal)
- Multiple payers can share the same codes from dictionary.py
- Payer-specific meanings override standard meanings via dictionary_overrides

EXAMPLE: Adding eMedNY (New York Medicaid)
------------------------------------------
    "EMEDNY": {
        "description": "New York State Medicaid (eMedNY)",
        "identifiers": {
            "isa06": ["EMEDNYBAT"],              # Primary - batch sender ID
            "trn03": [],                          # Not used by eMedNY
            "payer_name": ["NYSDOH"]              # Fallback
        },
        "normalize_carc_codes": False,
        "validation_overrides": {},
        "parsing_rules": {},
        "dictionary_overrides": {
            "reference_qualifiers": {
                "9A": "eMedNY Rate Code",         # Override standard meaning
            },
            "priority_rarc_codes": ["N426", "N427", "N428", "N429"]
        },
        "notes": [
            "REF*9A contains eMedNY rate codes, not repriced claim references",
            "Uses NY-specific RARC codes (N426, N427, N428, N429)"
        ]
    },

================================================================================
CURRENT CAPABILITIES VS FUTURE EXTENSIBILITY
================================================================================

WHAT IS CURRENTLY IMPLEMENTED:
- normalize_carc_codes: True/False     → Strips leading zeros from CARC codes
- dictionary_overrides.reference_qualifiers → Payer-specific REF code meanings
- dictionary_overrides.priority_rarc_codes  → Highlight payer-specific RARCs
- validation_overrides.allow_generic_payer_id → Skip generic payer ID warnings

WHAT IS STUBBED BUT NOT YET WIRED UP:
- parsing_rules: {}                    → Exists in registry but parser_835.py doesn't read it
- Other dictionary code types          → Only reference_qualifiers implemented

IF A PAYER HAS QUIRKS BEYOND CURRENT CAPABILITIES:
--------------------------------------------------------------------------------
To handle different meanings for OTHER code types (PLB, entity IDs, dates, etc.):

1. Expand dictionary_overrides in PAYER_REGISTRY:
   "dictionary_overrides": {
       "reference_qualifiers": { ... },      # Already works
       "plb_adjustment_codes": { "WO": "Different meaning" },    # Add this
       "entity_identifiers": { "82": "Different meaning" },      # Add this
       "date_qualifiers": { "472": "Different meaning" },        # Add this
       "claim_status_codes": { ... },        # Add this
       "rarc_codes": { ... },                # Add this
       "carc_codes": { ... },                # Add this
   }

2. Create corresponding lookup functions in this file:
   def get_payer_plb_description(payer_key, code): ...
   def get_payer_entity_description(payer_key, code): ...
   # Pattern matches get_payer_reference_qualifier_description()

3. Update parser_835.py and validation.py to call the new functions

To handle CUSTOM PARSING (different loops, segments, element positions):

1. Define parsing_rules in PAYER_REGISTRY:
   "parsing_rules": {
       "clp_element_count": 14,              # If payer uses non-standard element count
       "skip_loop_2110": True,               # If payer omits certain loops
       "custom_segment_handler": "func_name" # For truly custom parsing
   }

2. Add hooks in parser_835.py that check get_parsing_rules() before processing
3. Implement handler functions for custom logic

NOTE: The framework supports this extensibility - the hooks just need to be
connected when a payer requires it. Don't over-engineer until needed.

================================================================================
END OF IMPLEMENTATION GUIDE
================================================================================

Payer Identification Strategy:
- Primary: TRN03 (Originating Company Identifier) - most reliable
- Secondary: N1*PR Payer Name (exact match)

Usage:
    from colloquial import identify_payer, get_payer_config, normalize_carc_code

    payer_key = identify_payer(trn03="1999999999", payer_name="MEDI CAL FISCAL INTERMEDIARY")
    if payer_key:
        config = get_payer_config(payer_key)
        if config.get('normalize_carc_codes'):
            code = normalize_carc_code(code)
"""

import logging

import dictionary

logger = logging.getLogger(__name__)


# =============================================================================
# PAYER REGISTRY
# =============================================================================
# Each payer entry contains:
#   - identifiers: dict with 'trn03' (list) and 'payer_name' (list) for identification
#   - normalize_carc_codes: bool - whether to strip leading zeros from CARC codes
#   - validation_overrides: dict - payer-specific validation rule adjustments
#   - parsing_rules: dict - payer-specific parsing behavior
#   - description: str - human-readable payer description

PAYER_REGISTRY = {
    "MEDI_CAL": {
        "description": "California Medi-Cal (Medicaid) Fiscal Intermediary",
        "identifiers": {
            # TRN03 - Originating Company Identifier (primary identification)
            "trn03": ["1999999999"],
            # N1*PR payer name - exact match (secondary identification)
            "payer_name": ["MEDI CAL FISCAL INTERMEDIARY"],
        },
        # Medi-Cal adds leading zeros to CARC codes (e.g., 0012 instead of 12)
        "normalize_carc_codes": True,
        "validation_overrides": {
            # Medi-Cal uses generic payer ID 999999 (shared with other CA payers)
            "allow_generic_payer_id": True,
        },
        "parsing_rules": {
            # Any Medi-Cal specific parsing adjustments go here
        },
        "dictionary_overrides": {
            # Payer-specific code meanings that differ from standard
            "reference_qualifiers": {
                "2U": "Payer Identification Number (Medi-Cal)",
            },
            # California-specific RARC codes that are prioritized for this payer
            "priority_rarc_codes": ["N908", "N909", "N910", "N911", "N912", "N913"],
        },
        "notes": [
            "Uses leading zeros in CARC codes (e.g., 0012, 015, 034)",
            "Payer ID is generic 999999 in N1*PR segment",
            "Processed through CERESOFT clearinghouse",
            "Uses California-specific RARC codes (N908, N909, N910)",
        ],
    },
    "EMEDNY": {
        "description": "New York State Medicaid (eMedNY)",
        "identifiers": {
            # ISA06 - Interchange Sender ID (primary identification for batch senders)
            "isa06": ["EMEDNYBAT"],
            # TRN03 - Not typically used by eMedNY
            "trn03": [],
            # N1*PR payer name - exact match (secondary identification)
            "payer_name": ["NYSDOH", "NY STATE DEPT OF HEALTH"],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {
            # eMedNY sends EMT SUPPLEMENT payments as PLB adjustments that may not balance
            # with individual CLP claims - these are lump-sum fiscal period payments
            "allow_plb_balance_discrepancy": True,
            # eMedNY frequently sends void+resubmit pairs (CLP02=22 followed by CLP02=1)
            # resulting in many "duplicate" claim IDs which are legitimate
            "allow_duplicate_claim_ids": True,
            # eMedNY may skip BPR segments for certain transaction types
            "allow_missing_bpr": True,
        },
        "parsing_rules": {
            # eMedNY uses FCN# (Financial Control Number) format in PLB03/PLB05
            # Format: LS:FCN#YYYYMMDDXXXXXX-description
            "plb_fcn_format": True,
        },
        "dictionary_overrides": {
            "reference_qualifiers": {
                "9A": "eMedNY Rate Code",  # Override standard "Repriced Claim Reference Number"
            },
            # NY-specific codes (N426-N429), general NY Medicaid (N892),
            # and retroactive rate adjustment codes per eMedNY FAQ ER04 (N689, N419)
            "priority_rarc_codes": ["N426", "N427", "N428", "N429", "N892", "N689", "N419"],
            # eMedNY claim status codes (CLP02) - per X12 835 TR3 and eMedNY guidelines
            # These are standard X12 codes; eMedNY follows the standard definitions
            "claim_status_codes": {
                "1": "Processed as Primary",
                "2": "Processed as Secondary",
                "3": "Processed as Tertiary",
                "4": "Denied",
                "19": "Processed as Primary, Forwarded to Additional Payer(s)",
                "20": "Processed as Secondary, Forwarded to Additional Payer(s)",
                "21": "Processed as Tertiary, Forwarded to Additional Payer(s)",
                "22": "Reversal of Previous Payment",  # VOID - negative amounts expected
                "23": "Not Our Claim, Forwarded to Additional Payer(s)",
            },
            # PLB adjustment reason codes per X12 835 TR3 standard
            # eMedNY uses LS for EMT SUPPLEMENT fiscal-period lump sums
            "plb_adjustment_codes": {
                "LS": "Lump Sum (passthrough, EMT Supplement, rate adjustment)",
                "WO": "Overpayment Recovery",
                "FB": "Forwarding Balance (balance carried to future remittance)",
                "CS": "Adjustment (general, see PLB03-2 for details)",
                "L6": "Interest Owed",
                "PI": "Periodic Interim Payment",
                "RA": "Retro-activity Adjustment",
            },
        },
        "notes": [
            "REF*9A contains eMedNY rate codes, not repriced claim references",
            "Uses FCN# (Financial Control Number) in PLB segments for lump-sum adjustments",
            "EMT SUPPLEMENT payments are fiscal-period lump sums, not claim-specific",
            "PLB adjustments may cause small balance discrepancies - this is expected",
            "Pending claims are in separate 'Pended Claims Report' (proprietary), not in 835",
            "CLP02=22 indicates void/reversal of previous payment (negative amounts)",
            "Same claim ID with CLP02=22 and CLP02=1 = void + resubmit pair",
            "Retroactive rate adjustments: CLP02=22 with N689, then CLP02=1 with N419 (per FAQ ER04)",
            "Uses NY-specific RARC codes (N426, N427, N428, N429, N892)",
            "TCN format: YYDDD#########TA (Year, Julian day, sequence, media type, adjustment code)",
            "Source: eMedNY Remittance Advice Guidelines and FAQ (emedny.org)",
        ],
    },
    "MS_DOM": {
        "description": "Mississippi Division of Medicaid (Title XIX)",
        "identifiers": {
            "trn03": [],
            "isa06": [],
            "payer_name": ["MSTXIX", "MISSISSIPPI DIVISION OF MEDICAID"],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {},
        "parsing_rules": {},
        "dictionary_overrides": {
            "reference_qualifiers": {},
            "priority_rarc_codes": [],
        },
        "notes": [
            "Payer ID CKMS1 (Title XIX 835)",
            "REF*G2 carries legacy/secondary provider IDs",
            "A1-A9 wound-dressing modifiers are invalid on ambulance services",
        ],
    },
    "MAGNOLIA_MS": {
        "description": "Magnolia Health (MississippiCAN)",
        "identifiers": {
            "trn03": [],
            "isa06": [],
            "payer_name": ["MAGNOLIA HEALTH", "WELLCARE OF MISSISSIPPI, INC."],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {},
        "parsing_rules": {},
        "dictionary_overrides": {"reference_qualifiers": {}, "priority_rarc_codes": []},
        "notes": ["Payer IDs 68069 (medical) / 68068 (behavioral health)"],
    },
    "MOLINA_MS": {
        "description": "Molina Healthcare of Mississippi (MississippiCAN)",
        "identifiers": {
            "trn03": [],
            "isa06": [],
            "payer_name": ["MOLINA HEALTHCARE OF MISSISSIPPI", "MOLINA HEALTHCARE MS"],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {},
        "parsing_rules": {},
        "dictionary_overrides": {"reference_qualifiers": {}, "priority_rarc_codes": []},
        "notes": ["Payer ID 77010", "ERA/835 via ECHO"],
    },
    "TRUECARE_MS": {
        "description": "TrueCare Mississippi (MississippiCAN)",
        "identifiers": {
            "trn03": [],
            "isa06": [],
            "payer_name": ["TRUECARE MISSISSIPPI", "TRUECARE MS"],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {},
        "parsing_rules": {},
        "dictionary_overrides": {"reference_qualifiers": {}, "priority_rarc_codes": []},
        "notes": ["Payer ID MSTRC (ERA 835)"],
    },
    "CIGNA_SAMBA": {
        "description": "Cigna / SAMBA Federal Employee Health Benefits",
        "identifiers": {
            "trn03": [],
            "isa06": [],
            # SAMBA processes through Cigna (Payer ID 62308)
            # SAMBA's own Payer ID is 37259
            "payer_name": ["SAMBA", "SAMBA HEALTH PLAN", "CIGNA"],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {},
        "parsing_rules": {},
        "dictionary_overrides": {
            "reference_qualifiers": {},
            "priority_rarc_codes": [],
            # Cigna uses proprietary CARC codes in addition to standard codes
            "carc_codes": {
                "NPM": "No Payment Made (Cigna proprietary)",
            },
        },
        "notes": [
            "SAMBA is a Federal Employee Health Benefits (FEHB) plan",
            "Claims processed through Cigna (Payer ID 62308)",
            "SAMBA's own Payer ID is 37259",
            "Cigna uses proprietary CARC 'NPM' = 'No Payment Made'",
            "Source: Cigna 835 Companion Guide",
        ],
    },
    "INDIANA_MEDICAID": {
        "description": "Indiana Health Coverage Programs (IHCP) - Gainwell Technologies",
        "identifiers": {
            # TRN03 - Originating Company Identifier (payer TIN preceded by '1')
            # Source: IHCP 835 Companion Guide v3.4 (August 2024)
            "trn03": ["1350000000"],
            # ISA06 - Interchange Sender ID
            # Source: IHCP TA1/999 Companion Guide
            "isa06": ["IHCP"],
            # N1*PR payer name - exact match (secondary identification)
            "payer_name": ["INDIANA HEALTH COVERAGE PROGRAMS", "IHCP", "GAINWELL TECHNOLOGIES"],
        },
        # Indiana does NOT use leading zeros in CARC codes (unlike Medi-Cal)
        # Source: IHCP 835 Companion Guide - "use the codes as defined without adding leading zeros"
        "normalize_carc_codes": False,
        "validation_overrides": {},
        "parsing_rules": {
            # SVC01-7 (Procedure Code Description) is "Not Used" by IHCP
            # SVC06 composite data elements are "not utilized" - billed/adjudicated procedure in SVC01 only
            # Source: IHCP 835 Companion Guide
            "svc01_7_not_used": True,
            "svc06_not_used": True,
        },
        "dictionary_overrides": {
            "reference_qualifiers": {},
            "priority_rarc_codes": [],
        },
        "notes": [
            "TRN03 = 1350000000 (payer TIN preceded by '1')",
            "ISA06 = IHCP",
            "SVC01-7 (Procedure Code Description) is 'Not Used'",
            "SVC06 composite data elements are 'not utilized'",
            "Billed/adjudicated procedure included in SVC01 only",
            "Modifiers (SVC01-3 to SVC01-6) are situational - empty is valid",
            "Financial cycle runs every Friday night",
            "835/ERA files posted to IHCP MOVEit (File Exchange) server after financial cycle",
            "Fiscal agent: Gainwell Technologies",
            "EDI Technical Assistance: 800-457-4584 or INXIXTradingPartner@gainwelltechnologies.com",
            "Source: IHCP 835 Companion Guide v3.4 (August 2024)",
        ],
    },
    "PROSPECT_MEDICAL": {
        "description": "Prospect Medical Systems (California)",
        "identifiers": {
            "trn03": [],
            "isa06": [],
            "payer_name": [
                "PROSPECT MEDICAL SYSTEMS",
                "PROSPECT HEALTH SOURCE",
                "PROSPECT MEDICAL SD",
                "PROSPECT MEDICAL",
            ],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {
            # Prospect encodes sequestration (CO-253) in a way that causes allowed amount
            # mismatch: Method1 (Charge-CO) includes sequestration, but Method2 (Payment+PR)
            # does not reflect it. This is expected payer behavior.
            "allow_allowed_amount_mismatch": True,
            # Prospect may have small balance discrepancies due to rounding
            "transaction_balance_tolerance": 10.00,
        },
        "parsing_rules": {},
        "dictionary_overrides": {
            "reference_qualifiers": {},
            "priority_rarc_codes": [],
        },
        "notes": [
            "Sequestration (CO-253) causes allowed amount mismatch - expected behavior",
            "Method1 (Charge-CO) includes sequestration, Method2 (Payment+PR) does not",
            "Transaction balance may be off by up to $10 due to rounding",
        ],
    },
    "EMPLOYERS_MUTUAL": {
        "description": "Employers Mutual Casualty Company (Workers' Comp)",
        "identifiers": {
            "trn03": [],
            "isa06": [],
            "payer_name": ["EMPLOYERS MUTUAL"],
        },
        "normalize_carc_codes": False,
        "validation_overrides": {
            # Workers' comp payers often have complex adjustments that may not balance
            "transaction_balance_tolerance": 1000.00,
        },
        "parsing_rules": {},
        "dictionary_overrides": {
            "reference_qualifiers": {},
            "priority_rarc_codes": [],
        },
        "notes": [
            "Workers' Compensation payer",
            "Large transaction balance tolerance due to complex WC adjustments",
        ],
    },
    # Template for adding additional payers:
    # "PAYER_KEY": {
    #     "description": "Payer Name",
    #     "identifiers": {
    #         "trn03": ["originating_company_id"],     # Primary for most payers
    #         "isa06": ["batch_sender_id"],            # Primary for batch senders (eMedNY)
    #         "payer_name": ["EXACT PAYER NAME"]       # Fallback for all
    #     },
    #     "normalize_carc_codes": False,
    #     "validation_overrides": {},
    #     "parsing_rules": {},
    #     "dictionary_overrides": {},
    #     "notes": []
    # },
}


# =============================================================================
# PAYER IDENTIFICATION FUNCTIONS
# =============================================================================


def identify_payer(trn03=None, payer_name=None, isa06=None):
    """
    Identify a payer based on TRN03, ISA06, or payer name.

    Args:
        trn03: Originating Company Identifier from TRN segment element 3
        payer_name: Payer name from N1*PR segment element 2
        isa06: Interchange Sender ID from ISA segment element 6

    Returns:
        str: Payer key (e.g., "MEDI_CAL", "EMEDNY") if identified, None otherwise

    Identification Priority:
        1. TRN03 (primary for most payers like Medi-Cal)
        2. ISA06 (primary for batch senders like eMedNY)
        3. Payer name (fallback for all payers)
    """
    # Primary identification: TRN03
    if trn03:
        trn03_clean = str(trn03).strip()
        for payer_key, config in PAYER_REGISTRY.items():
            identifiers = config.get("identifiers", {})
            if trn03_clean in identifiers.get("trn03", []):
                return payer_key

    # Secondary identification: ISA06 (for batch senders like eMedNY)
    if isa06:
        isa06_clean = str(isa06).strip()
        for payer_key, config in PAYER_REGISTRY.items():
            identifiers = config.get("identifiers", {})
            if isa06_clean in identifiers.get("isa06", []):
                return payer_key

    # Tertiary identification: Payer name (exact match)
    if payer_name:
        payer_name_clean = str(payer_name).strip().upper()
        for payer_key, config in PAYER_REGISTRY.items():
            identifiers = config.get("identifiers", {})
            payer_names = [name.upper() for name in identifiers.get("payer_name", [])]
            if payer_name_clean in payer_names:
                return payer_key

    return None


def get_payer_config(payer_key):
    """
    Get the configuration for a specific payer.

    Args:
        payer_key: Payer identifier key (e.g., "MEDI_CAL")

    Returns:
        dict: Payer configuration, or empty dict if not found
    """
    return PAYER_REGISTRY.get(payer_key, {})


def get_all_payer_keys():
    """
    Get a list of all registered payer keys.

    Returns:
        list: All payer keys in the registry
    """
    return list(PAYER_REGISTRY.keys())


def get_payer_description(payer_key):
    """
    Get human-readable description for a payer.

    Args:
        payer_key: Payer identifier key

    Returns:
        str: Payer description or "Unknown Payer"
    """
    config = get_payer_config(payer_key)
    return config.get("description", "Unknown Payer")


# =============================================================================
# CARC CODE NORMALIZATION
# =============================================================================
# Moved from dictionary.py - this is payer-specific behavior


def normalize_carc_code(code):
    """
    Normalize CARC code by stripping leading zeros if the result is valid.

    Some agencies (like Medi-Cal) incorrectly add leading zeros to CARC codes
    (e.g., 001 instead of 1, 0012 instead of 12). This function returns the
    normalized code if it maps to a valid standard code, otherwise returns
    the original.

    Args:
        code: CARC code string (may have leading zeros)

    Returns:
        str: Normalized code if valid, otherwise original code

    Examples:
        normalize_carc_code("0012") -> "12"
        normalize_carc_code("034") -> "34"
        normalize_carc_code("45") -> "45" (already valid)
        normalize_carc_code("A1") -> "A1" (alphanumeric, unchanged)
    """
    if not code:
        return code

    classifications = dictionary.get_carc_classifications()

    # If already valid, return as-is
    if code in classifications:
        return code

    # Try stripping leading zeros
    stripped = code.lstrip("0")

    # Return stripped version if it's valid and different from original
    if stripped and stripped != code and stripped in classifications:
        return stripped

    return code


def should_normalize_carc(payer_key):
    """
    Check if a payer requires CARC code normalization.

    Args:
        payer_key: Payer identifier key

    Returns:
        bool: True if payer uses non-standard CARC codes that need normalization
    """
    if not payer_key:
        return False
    config = get_payer_config(payer_key)
    return config.get("normalize_carc_codes", False)


# =============================================================================
# VALIDATION OVERRIDE FUNCTIONS
# =============================================================================


def get_validation_overrides(payer_key):
    """
    Get validation rule overrides for a specific payer.

    Args:
        payer_key: Payer identifier key

    Returns:
        dict: Validation override settings
    """
    if not payer_key:
        return {}
    config = get_payer_config(payer_key)
    return config.get("validation_overrides", {})


def allows_generic_payer_id(payer_key):
    """
    Check if payer is known to use generic payer IDs (like 999999).

    Args:
        payer_key: Payer identifier key

    Returns:
        bool: True if generic payer ID is expected for this payer
    """
    overrides = get_validation_overrides(payer_key)
    return overrides.get("allow_generic_payer_id", False)


def allows_plb_balance_discrepancy(payer_key):
    """
    Check if payer is known to have PLB adjustments that may not balance.

    Some payers (like eMedNY) send lump-sum supplemental payments (EMT SUPPLEMENT,
    rate adjustments, etc.) as PLB adjustments that are not tied to specific claims.
    These may cause small balance discrepancies that are expected behavior.

    Args:
        payer_key: Payer identifier key

    Returns:
        bool: True if PLB balance discrepancies are expected for this payer
    """
    overrides = get_validation_overrides(payer_key)
    return overrides.get("allow_plb_balance_discrepancy", False)


def allows_duplicate_claim_ids(payer_key):
    """
    Check if payer is known to send duplicate claim IDs legitimately.

    Some payers (like eMedNY) frequently send void/adjustment pairs where:
    - CLP02=22 (Reversal) with negative amounts voids the original
    - CLP02=1 (Processed as Primary) with positive amounts resubmits

    This results in the same claim ID appearing multiple times, which is
    legitimate 835 behavior for adjustments, not duplicate claims.

    Args:
        payer_key: Payer identifier key

    Returns:
        bool: True if duplicate claim IDs are expected for this payer
    """
    overrides = get_validation_overrides(payer_key)
    return overrides.get("allow_duplicate_claim_ids", False)


def allows_missing_bpr(payer_key):
    """
    Check if payer may have transactions without BPR segments.

    Some payers may send informational transactions or adjustments
    without a corresponding BPR (check/payment) segment.

    Args:
        payer_key: Payer identifier key

    Returns:
        bool: True if missing BPR is expected for this payer
    """
    overrides = get_validation_overrides(payer_key)
    return overrides.get("allow_missing_bpr", False)


def allows_allowed_amount_mismatch(payer_key):
    """
    Check if payer is known to have allowed amount calculation mismatches.

    Some payers (like Prospect Medical) encode adjustments in ways that cause
    Method1 (Charge - CO) and Method2 (Payment + PR) to not match. This is
    expected payer behavior, not a data quality issue.

    Common causes:
    - Sequestration (CO-253) included in Method1 but not reflected in Method2
    - Payer-specific adjustment encoding
    - Rounding differences

    Args:
        payer_key: Payer identifier key

    Returns:
        bool: True if allowed amount mismatches are expected for this payer
    """
    overrides = get_validation_overrides(payer_key)
    return overrides.get("allow_allowed_amount_mismatch", False)


def get_transaction_balance_tolerance(payer_key):
    """
    Get payer-specific tolerance for transaction balance validation.

    Some payers have small discrepancies in transaction balancing due to:
    - Rounding differences
    - Complex adjustment scenarios (especially Workers' Comp)
    - PLB adjustments that don't tie directly to claims

    Default tolerance is $0.01 per X12 standard.

    Args:
        payer_key: Payer identifier key

    Returns:
        float: Tolerance amount in dollars (default 0.01)
    """
    overrides = get_validation_overrides(payer_key)
    return overrides.get("transaction_balance_tolerance", 0.01)


# =============================================================================
# PARSING OVERRIDE FUNCTIONS
# =============================================================================


def get_parsing_rules(payer_key):
    """
    Get parsing rule overrides for a specific payer.

    Args:
        payer_key: Payer identifier key

    Returns:
        dict: Parsing rule settings
    """
    if not payer_key:
        return {}
    config = get_payer_config(payer_key)
    return config.get("parsing_rules", {})


def handle_custom_segment(payer_key, segment_id, elements):
    """
    Route to payer-specific segment handlers if defined.

    This allows payers to define custom parsing behavior for specific segments
    when their 835 files deviate from the standard X12 structure.

    Args:
        payer_key: Payer identifier key
        segment_id: The segment type (e.g., 'CLP', 'SVC')
        elements: The parsed segment elements

    Returns:
        dict: Parsed segment data if custom handler exists, None otherwise
    """
    if not payer_key:
        return None

    config = get_payer_config(payer_key)
    parsing_rules = config.get("parsing_rules", {})

    # Check for a custom handler for this segment type
    handler_name = parsing_rules.get(f"{segment_id.lower()}_handler")

    if handler_name:
        # Look for the handler function in this module
        import sys

        current_module = sys.modules[__name__]
        if hasattr(current_module, handler_name):
            handler = getattr(current_module, handler_name)
            return handler(segment_id, elements)

    return None  # Use standard parsing


def get_payer_segment_element_count(payer_key, segment_id):
    """
    Get expected element count for a segment if payer uses non-standard count.

    Some payers may send segments with more or fewer elements than standard.

    Args:
        payer_key: Payer identifier key
        segment_id: The segment type (e.g., 'CLP', 'SVC')

    Returns:
        int or None: Expected element count if override exists, None otherwise
    """
    if not payer_key:
        return None

    rules = get_parsing_rules(payer_key)
    return rules.get(f"{segment_id.lower()}_element_count")


# =============================================================================
# DICTIONARY OVERRIDE FUNCTIONS
# =============================================================================


def get_dictionary_overrides(payer_key):
    """
    Get dictionary overrides for a specific payer.

    Args:
        payer_key: Payer identifier key

    Returns:
        dict: Dictionary override settings
    """
    if not payer_key:
        return {}
    config = get_payer_config(payer_key)
    return config.get("dictionary_overrides", {})


def get_payer_reference_qualifier_description(payer_key, code):
    """
    Get reference qualifier description with payer-first lookup.

    If the payer has a specific meaning for this code, return that.
    Otherwise, return the standard description from dictionary.py.

    Args:
        payer_key: Payer identifier key (can be None)
        code: Reference qualifier code

    Returns:
        str: Description for the code (payer-specific if available, else standard)
    """
    # Check for payer-specific override first
    if payer_key:
        overrides = get_dictionary_overrides(payer_key)
        ref_overrides = overrides.get("reference_qualifiers", {})
        if code in ref_overrides:
            return ref_overrides[code]

    # Fall back to standard dictionary
    return dictionary.get_reference_qualifier_description(code)


def get_payer_rarc_description(payer_key, code):
    """
    Get RARC (Remittance Advice Remark Code) description with payer-first lookup.

    Some payers use state-specific RARC codes. This function returns
    the appropriate description based on payer context.

    Args:
        payer_key: Payer identifier key (can be None)
        code: RARC code

    Returns:
        str: Description for the code
    """
    # Standard lookup - dictionary already contains all codes including CA-specific
    return dictionary.get_remark_code_description(code)


def get_payer_plb_description(payer_key, code):
    """
    Get PLB adjustment code description with payer-first lookup.

    Args:
        payer_key: Payer identifier key (can be None)
        code: PLB adjustment reason code

    Returns:
        str or None: Payer-specific description if found, None otherwise
    """
    if not payer_key:
        return None
    overrides = get_dictionary_overrides(payer_key)
    return overrides.get("plb_adjustment_codes", {}).get(code)


def get_payer_entity_description(payer_key, code):
    """
    Get entity identifier description with payer-first lookup.

    Args:
        payer_key: Payer identifier key (can be None)
        code: Entity identifier code

    Returns:
        str or None: Payer-specific description if found, None otherwise
    """
    if not payer_key:
        return None
    overrides = get_dictionary_overrides(payer_key)
    return overrides.get("entity_identifiers", {}).get(code)


def get_payer_date_qualifier_description(payer_key, code):
    """
    Get date qualifier description with payer-first lookup.

    Args:
        payer_key: Payer identifier key (can be None)
        code: Date qualifier code

    Returns:
        str or None: Payer-specific description if found, None otherwise
    """
    if not payer_key:
        return None
    overrides = get_dictionary_overrides(payer_key)
    return overrides.get("date_qualifiers", {}).get(code)


def get_payer_claim_status_description(payer_key, code):
    """
    Get claim status code (CLP02) description with payer-first lookup.

    CLP02 values indicate the disposition of the claim:
    - 1 = Processed as Primary
    - 2 = Processed as Secondary
    - 4 = Denied
    - 22 = Reversal of Previous Payment (void)
    - etc.

    Some payers have specific meanings or additional codes.

    Args:
        payer_key: Payer identifier key (can be None)
        code: CLP02 claim status code

    Returns:
        str: Description for the code (payer-specific if available, else standard)
    """
    # Standard X12 835 claim status codes
    standard_codes = {
        "1": "Processed as Primary",
        "2": "Processed as Secondary",
        "3": "Processed as Tertiary",
        "4": "Denied",
        "19": "Processed as Primary, Forwarded to Additional Payer(s)",
        "20": "Processed as Secondary, Forwarded to Additional Payer(s)",
        "21": "Processed as Tertiary, Forwarded to Additional Payer(s)",
        "22": "Reversal of Previous Payment",
        "23": "Not Our Claim, Forwarded to Additional Payer(s)",
        "25": "Rejected",
    }

    # Check for payer-specific override first
    if payer_key:
        overrides = get_dictionary_overrides(payer_key)
        claim_status_codes = overrides.get("claim_status_codes", {})
        if code in claim_status_codes:
            return claim_status_codes[code]

    # Fall back to standard codes
    return standard_codes.get(code, f"Unknown Status Code ({code})")


def is_reversal_claim_status(code):
    """
    Check if a claim status code indicates a reversal/void.

    CLP02=22 means "Reversal of Previous Payment" - the claim amounts
    will typically be negative, voiding a previous payment.

    Args:
        code: CLP02 claim status code

    Returns:
        bool: True if this is a reversal/void status
    """
    return str(code) == "22"


def is_void_adjustment_pair(claims_with_same_id):
    """
    Check if a list of claims with the same ID represents a void/adjustment pair.

    A void/adjustment pair consists of:
    - One claim with CLP02=22 (Reversal) - negative amounts
    - One claim with CLP02=1, 2, or 3 (Processed) - corrected amounts

    This is legitimate 835 behavior for correcting previous payments.

    Args:
        claims_with_same_id: List of claim dicts with same claim ID

    Returns:
        bool: True if this appears to be a void/adjustment pair
    """
    if not claims_with_same_id or len(claims_with_same_id) < 2:
        return False

    statuses = [str(c.get("claim_status_code", "")) for c in claims_with_same_id]

    # Check for reversal (22) + processed (1, 2, 3) pattern
    has_reversal = "22" in statuses
    has_processed = any(s in ("1", "2", "3") for s in statuses)

    return has_reversal and has_processed


def get_payer_carc_description(payer_key, code):
    """
    Get CARC description with payer-first lookup.

    Args:
        payer_key: Payer identifier key (can be None)
        code: CARC (Claim Adjustment Reason Code)

    Returns:
        str or None: Payer-specific description if found, None otherwise
    """
    if not payer_key:
        return None
    overrides = get_dictionary_overrides(payer_key)
    return overrides.get("carc_codes", {}).get(code)


def is_payer_priority_rarc(payer_key, code):
    """
    Check if a RARC code is a priority code for this payer.

    Priority codes are payer-specific codes that should be highlighted
    or given special attention during processing.

    Args:
        payer_key: Payer identifier key
        code: RARC code

    Returns:
        bool: True if this is a priority code for the payer
    """
    if not payer_key:
        return False
    overrides = get_dictionary_overrides(payer_key)
    priority_codes = overrides.get("priority_rarc_codes", [])
    return code in priority_codes


def get_payer_specific_description(payer_key, code_type, code):
    """
    Generic payer-specific description lookup.

    This is a general-purpose function that routes to the appropriate
    payer-specific lookup based on code type.

    Args:
        payer_key: Payer identifier key (can be None)
        code_type: Type of code ('reference_qualifier', 'rarc', etc.)
        code: The code value

    Returns:
        str: Description for the code
    """
    if code_type == "reference_qualifier":
        return get_payer_reference_qualifier_description(payer_key, code)
    elif code_type == "rarc":
        return get_payer_rarc_description(payer_key, code)
    else:
        # Unknown code type - return empty string
        return ""


# =============================================================================
# EMEDNY-SPECIFIC FUNCTIONS
# =============================================================================


def parse_emedny_fcn(plb_reference):
    """
    Parse eMedNY FCN# (Financial Control Number) format from PLB segments.

    eMedNY PLB segments use a specific format for adjustment references:
    LS:FCN#YYYYMMDDXXXXXX-description

    Example:
    LS:FCN#202512161000512-EMT SUPPLEMENT 04/01-06/30/202

    Args:
        plb_reference: The PLB03 or PLB05 reference string

    Returns:
        dict: Parsed FCN information with keys:
            - adjustment_type: The adjustment type code (e.g., "LS")
            - fcn: The Financial Control Number
            - date: The date portion (YYYYMMDD)
            - sequence: The sequence number
            - description: The description text
            - is_emt_supplement: True if this is an EMT SUPPLEMENT payment
        Returns None if not in FCN# format.
    """
    import re

    if not plb_reference:
        return None

    # Pattern: XX:FCN#YYYYMMDDNNNNNN-description
    pattern = r"^(\w+):FCN#(\d{8})(\d+)-(.+)$"
    match = re.match(pattern, plb_reference)

    if not match:
        # Try simpler pattern without sequence number
        pattern2 = r"^(\w+):FCN#(\d+)-(.+)$"
        match2 = re.match(pattern2, plb_reference)
        if match2:
            adj_type, fcn_num, description = match2.groups()
            return {
                "adjustment_type": adj_type,
                "fcn": fcn_num,
                "date": fcn_num[:8] if len(fcn_num) >= 8 else None,
                "sequence": fcn_num[8:] if len(fcn_num) > 8 else None,
                "description": description,
                "is_emt_supplement": "EMT SUPPLEMENT" in description.upper(),
            }
        return None

    adj_type, date, sequence, description = match.groups()
    return {
        "adjustment_type": adj_type,
        "fcn": f"{date}{sequence}",
        "date": date,
        "sequence": sequence,
        "description": description,
        "is_emt_supplement": "EMT SUPPLEMENT" in description.upper(),
    }


def is_emt_supplement_adjustment(plb_reference):
    """
    Check if a PLB reference is an EMT SUPPLEMENT payment.

    EMT SUPPLEMENT payments are lump-sum fiscal-period payments from
    New York Medicaid for ambulance/EMS services. They are not tied
    to specific claims and may cause balance discrepancies.

    Args:
        plb_reference: The PLB03 or PLB05 reference string

    Returns:
        bool: True if this is an EMT SUPPLEMENT adjustment
    """
    if not plb_reference:
        return False
    return "EMT SUPPLEMENT" in str(plb_reference).upper()


def get_balance_discrepancy_explanation(payer_key, discrepancy_amount, plb_segments=None):
    """
    Get an explanation for a balance discrepancy based on payer characteristics.

    Some payers have known behaviors that cause balance discrepancies:
    - eMedNY: EMT SUPPLEMENT payments are lump-sum fiscal-period adjustments
    - Other payers may have similar quirks

    Args:
        payer_key: Payer identifier key
        discrepancy_amount: The discrepancy amount (expected - actual)
        plb_segments: Optional list of PLB segment data for analysis

    Returns:
        str or None: Explanation if discrepancy is expected, None otherwise
    """
    if not payer_key:
        return None

    config = get_payer_config(payer_key)

    # Check if this payer allows PLB balance discrepancies
    overrides = config.get("validation_overrides", {})
    if not overrides.get("allow_plb_balance_discrepancy"):
        return None

    # Build explanation based on payer
    if payer_key == "EMEDNY":
        explanation_parts = [
            f"Balance discrepancy of ${abs(discrepancy_amount):.2f} detected.",
            "This is expected behavior for eMedNY (NYSDOH).",
        ]

        # Check for EMT SUPPLEMENT in PLB segments
        if plb_segments:
            emt_supplements = []
            for plb in plb_segments:
                ref = plb.get("reference", "") or plb.get("adjustment_reason", "")
                if is_emt_supplement_adjustment(ref):
                    emt_supplements.append(ref)

            if emt_supplements:
                explanation_parts.append(
                    "EMT SUPPLEMENT payments are lump-sum fiscal-period adjustments "
                    "that are not tied to specific claims in this 835."
                )

        explanation_parts.append("Source: eMedNY Remittance Advice Guidelines (emedny.org)")

        return " ".join(explanation_parts)

    return None


def get_duplicate_claim_explanation(payer_key, claim_statuses):
    """
    Get an explanation for duplicate claim IDs based on payer characteristics.

    Args:
        payer_key: Payer identifier key
        claim_statuses: List of CLP02 status codes for claims with the same ID

    Returns:
        str or None: Explanation if duplicates are expected, None otherwise
    """
    if not payer_key:
        return None

    config = get_payer_config(payer_key)

    # Check if this payer allows duplicate claim IDs
    overrides = config.get("validation_overrides", {})
    if not overrides.get("allow_duplicate_claim_ids"):
        return None

    # Check for void/adjustment pattern
    statuses = [str(s) for s in claim_statuses]
    has_reversal = "22" in statuses
    has_processed = any(s in ("1", "2", "3") for s in statuses)

    if has_reversal and has_processed:
        return (
            "This claim ID appears multiple times with status 22 (Reversal) and "
            "status 1/2/3 (Processed). This is a void/adjustment pair - the original "
            "payment was voided and a corrected payment issued. This is normal 835 behavior."
        )

    if payer_key == "EMEDNY":
        return (
            "eMedNY frequently sends adjustments and corrections that result in "
            "the same claim ID appearing multiple times. Each occurrence represents "
            "a separate transaction event (original, adjustment, void, resubmit, etc.)."
        )

    return None


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def get_payer_notes(payer_key):
    """
    Get notes/documentation about a payer's non-standard behaviors.

    Args:
        payer_key: Payer identifier key

    Returns:
        list: Notes about payer-specific behaviors
    """
    config = get_payer_config(payer_key)
    return config.get("notes", [])


def is_registered_payer(payer_key):
    """
    Check if a payer key is registered in the system.

    Args:
        payer_key: Payer identifier key

    Returns:
        bool: True if payer is registered
    """
    return payer_key in PAYER_REGISTRY


def print_payer_summary():
    """
    Print a summary of all registered payers and their configurations.
    Useful for debugging and documentation.
    """
    logger.info("=" * 70)
    logger.info("REGISTERED PAYER OVERRIDES")
    logger.info("=" * 70)

    for payer_key, config in PAYER_REGISTRY.items():
        logger.info("\n%s:", payer_key)
        logger.info("  Description: %s", config.get("description", "N/A"))

        identifiers = config.get("identifiers", {})
        logger.info("  TRN03 IDs: %s", identifiers.get("trn03", []))
        logger.info("  Payer Names: %s", identifiers.get("payer_name", []))

        logger.info("  Normalize CARC Codes: %s", config.get("normalize_carc_codes", False))

        notes = config.get("notes", [])
        if notes:
            logger.info("  Notes:")
            for note in notes:
                logger.info("    - %s", note)

    logger.info("\n" + "=" * 70)


# =============================================================================
# PAYER-SPECIFIC CODE DESCRIPTIONS
# =============================================================================
# These codes are proprietary/non-standard codes used by specific payers.
# They are kept separate from the standard dictionary to clearly identify
# them as payer-specific rather than industry-standard codes.

PAYER_SPECIFIC_CARC_CODES = {
    # Clever Care proprietary codes
    "NON01": {
        "description": "Non-standard adjustment (Clever Care proprietary)",
        "payers": ["CLEVER CARE OF GOLDEN STATE INC"],
        "notes": "Proprietary CARC used by Clever Care; not a standard X12 code",
    },
    # Cigna proprietary codes (also used by SAMBA which processes through Cigna)
    "NPM": {
        "description": "No Payment Made (Cigna proprietary)",
        "payers": ["CIGNA", "SAMBA", "SAMBA HEALTH PLAN"],
        "notes": "Cigna proprietary CARC indicating no payment was issued for this claim/service. "
        "Not a standard X12 code. SAMBA (Federal Employee Health Benefits) processes "
        "claims through Cigna. Source: Cigna 835 Companion Guide.",
    },
}

PAYER_SPECIFIC_PROCEDURE_CODES = {
    # Behavioral health/Carelon proprietary codes
    "F2005": {
        "description": "Behavioral health screening/assessment (payer-specific)",
        "payers": ["CARELON HEALTH", "BEACON HEALTH OPTIONS"],
        "notes": "Proprietary procedure code for behavioral health services; not a standard HCPCS",
    },
    # Generic placeholder codes used by some payers
    "00001": {
        "description": "Miscellaneous service (payer-specific placeholder)",
        "payers": ["Various"],
        "notes": "Non-standard placeholder code used by some payers for miscellaneous services",
    },
}


def get_payer_specific_carc_description(code):
    """
    Get description for payer-specific CARC codes.

    Args:
        code: CARC code string

    Returns:
        str: Description if found, empty string otherwise
    """
    if code in PAYER_SPECIFIC_CARC_CODES:
        return PAYER_SPECIFIC_CARC_CODES[code]["description"]
    return ""


def get_payer_specific_procedure_description(code):
    """
    Get description for payer-specific procedure codes.

    Args:
        code: Procedure code string

    Returns:
        str: Description if found, empty string otherwise
    """
    if code in PAYER_SPECIFIC_PROCEDURE_CODES:
        return PAYER_SPECIFIC_PROCEDURE_CODES[code]["description"]
    return ""


def is_payer_specific_code(code, code_type="any"):
    """
    Check if a code is a known payer-specific code.

    Args:
        code: Code string to check
        code_type: 'carc', 'procedure', or 'any'

    Returns:
        bool: True if code is payer-specific
    """
    if code_type == "carc":
        return code in PAYER_SPECIFIC_CARC_CODES
    elif code_type == "procedure":
        return code in PAYER_SPECIFIC_PROCEDURE_CODES
    else:
        return code in PAYER_SPECIFIC_CARC_CODES or code in PAYER_SPECIFIC_PROCEDURE_CODES


def get_payer_specific_code_info(code):
    """
    Get full info for a payer-specific code.

    Args:
        code: Code string

    Returns:
        dict: Code info including description, payers, and notes; or None if not found
    """
    if code in PAYER_SPECIFIC_CARC_CODES:
        return {"type": "CARC", **PAYER_SPECIFIC_CARC_CODES[code]}
    if code in PAYER_SPECIFIC_PROCEDURE_CODES:
        return {"type": "procedure", **PAYER_SPECIFIC_PROCEDURE_CODES[code]}
    return None


# =============================================================================
# MODULE TESTING
# =============================================================================

if __name__ == "__main__":
    # Self-test when run directly - configure logging for console output
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    logger.info("Testing colloquial.py module...\n")

    # Test payer identification
    logger.info("Testing payer identification:")

    # Test with TRN03
    payer = identify_payer(trn03="1999999999")
    logger.info("  TRN03='1999999999' -> %s", payer)
    assert payer == "MEDI_CAL", "Should identify Medi-Cal by TRN03"

    # Test with payer name
    payer = identify_payer(payer_name="MEDI CAL FISCAL INTERMEDIARY")
    logger.info("  payer_name='MEDI CAL FISCAL INTERMEDIARY' -> %s", payer)
    assert payer == "MEDI_CAL", "Should identify Medi-Cal by name"

    # Test unknown payer
    payer = identify_payer(trn03="9999999999", payer_name="UNKNOWN PAYER")
    logger.info("  Unknown payer -> %s", payer)
    assert payer is None, "Should return None for unknown payer"

    # Test CARC normalization
    logger.info("\nTesting CARC code normalization:")
    test_codes = ["0012", "034", "015", "45", "A1", "999", ""]
    for code in test_codes:
        normalized = normalize_carc_code(code)
        logger.info("  '%s' -> '%s'", code, normalized)

    # Test should_normalize_carc
    logger.info("\nTesting should_normalize_carc:")
    logger.info("  MEDI_CAL: %s", should_normalize_carc("MEDI_CAL"))
    logger.info("  None: %s", should_normalize_carc(None))
    logger.info("  Unknown: %s", should_normalize_carc("UNKNOWN"))

    # Print payer summary
    logger.info("\n")
    print_payer_summary()

    logger.info("\nAll tests passed!")
