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
        "validation_overrides": {},
        "parsing_rules": {},
        "dictionary_overrides": {
            "reference_qualifiers": {
                "9A": "eMedNY Rate Code",  # Override standard "Repriced Claim Reference Number"
            },
            "priority_rarc_codes": ["N426", "N427", "N428", "N429", "N892"],
        },
        "notes": [
            "REF*9A contains eMedNY rate codes, not repriced claim references",
            "Uses FCN# (Financial Control Number) in PLB segments",
            "Pending claims in separate Supplemental file, not in 835",
            "Uses NY-specific RARC codes (N426, N427, N428, N429, N892)",
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
