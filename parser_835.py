
import os
import sys
import traceback
import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import csv
import dictionary
import redactor
import colloquial
import rates
from categorization import categorize_adjustment
from validation import validate_835_output, generate_validation_report, EDIFieldTracker, EDIElementPresenceTracker
from config import get_config

# Configure module logger
logger = logging.getLogger(__name__)


def configure_logging(level=logging.INFO, log_file=None, simple_format=False):
    """
    Configure logging for the 835 parser.
    
    Args:
        level: Logging level (logging.DEBUG, logging.INFO, etc.)
        log_file: Optional path to log file. If None, logs to console only.
        simple_format: If True, use simple format without timestamps (for GUI).
    """
    if simple_format:
        formatter = logging.Formatter('%(message)s')
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    # Clear existing handlers
    logger.handlers.clear()
    logger.setLevel(level)
    
    # Console handler - writes to stdout so GUI can capture it
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False


# NOTE: Trips.csv path is now configured via config.py
# Default path can be overridden via config file or environment variable EDI_TRIPS_CSV_PATH


class EDIProcessor:
    """
    Encapsulates mutable state for 835 EDI processing.
    
    Holds Fair Health rates and trip lookup data, avoiding module-level globals.
    Instantiate once per processing run and pass to functions that need this state.
    """
    
    def __init__(self):
        self.fair_health_rates = None
        self.trips_by_run = {}  # RUN -> normalized ZIP mapping
    
    def load_fair_health_rates(self, rates_file_path=None):
        """
        Load Fair Health rates from Excel file.
        
        Args:
            rates_file_path: Path to RATES.xlsx file. If None, uses configured path.
        
        Returns:
            True if rates loaded successfully, False otherwise.
        """
        if rates_file_path is None:
            # Get from configuration
            rates_file_path = get_config().rates_xlsx_path
        
        if rates_file_path is None:
            logger.info("Fair Health rates path not configured (columns will be empty)")
            return False
        
        if not os.path.exists(rates_file_path):
            logger.info("Fair Health rates file not found: %s (columns will be empty)", rates_file_path)
            return False
        
        try:
            self.fair_health_rates = rates.FairHealthRates()
            stats = self.fair_health_rates.load_from_excel(rates_file_path)
            logger.info("Loaded Fair Health rates: %s rate combinations", stats['rate_keys'])
            logger.debug("  ZIP codes: %s, HCPCS codes: %s", stats['unique_zips'], stats['unique_hcpcs'])
            return True
        except Exception as e:
            logger.warning("Failed to load Fair Health rates: %s", e)
            self.fair_health_rates = None
            return False
    
    def load_trips_csv(self, trips_csv_path=None):
        """Load Trips.csv and build RUN -> PU ZIP lookup."""
        if trips_csv_path is None:
            trips_csv_path = get_config().trips_csv_path
        
        if trips_csv_path is None:
            logger.info("Trips.csv path not configured")
            return False
        
        if not os.path.exists(trips_csv_path):
            logger.info("Trips.csv not found at %s", trips_csv_path)
            return False
        
        # Clear existing cache to prevent stale data from previous runs
        self.trips_by_run.clear()
        
        try:
            with open(trips_csv_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    run = row.get('RUN', '').strip()
                    puzip = row.get('puzip', '').strip()
                    if run and puzip:
                        normalized = normalize_zip_code(puzip)
                        if normalized:
                            self.trips_by_run[run] = normalized
        except (OSError, csv.Error) as e:
            logger.warning("Failed to read Trips.csv at %s: %s", trips_csv_path, e)
            return False
        
        logger.info("Loaded %d trip records from Trips.csv", len(self.trips_by_run))
        return True
    
    def get_fair_health_rates_for_service(self, hcpcs_code, service_date_str):
        """
        Get Fair Health rates for a service line.
        
        Args:
            hcpcs_code: The HCPCS/CPT procedure code
            service_date_str: Service date as string (YYYYMMDD or MM/DD/YY format)
        
        Returns:
            Dict mapping ZIP codes to (out_of_network, in_network) tuples
        """
        if self.fair_health_rates is None:
            return {}
        
        # Parse service date
        service_date = None
        if service_date_str:
            try:
                # Try YYYYMMDD format first
                if len(service_date_str) == 8 and service_date_str.isdigit():
                    service_date = datetime.strptime(service_date_str, '%Y%m%d').date()
                # Try MM/DD/YY format
                elif '/' in service_date_str:
                    service_date = datetime.strptime(service_date_str, '%m/%d/%y').date()
            except ValueError:
                pass
        
        return self.fair_health_rates.get_rates_for_all_zips(hcpcs_code, service_date)
    
    def get_fair_health_rate_for_zip(self, zip_code, hcpcs_code, service_date_str):
        """
        Get Fair Health rate for a specific ZIP code - O(1) lookup.
        
        Args:
            zip_code: The ZIP code to look up (int or string)
            hcpcs_code: The HCPCS/CPT procedure code
            service_date_str: Service date as string (YYYYMMDD or MM/DD/YY format)
        
        Returns:
            Tuple of (out_of_network, in_network) or None if not found
        """
        if self.fair_health_rates is None:
            return None
        
        # Convert ZIP to int
        try:
            zip_int = int(zip_code) if zip_code else None
        except (ValueError, TypeError):
            return None
        
        if zip_int is None:
            return None
        
        # Parse service date
        service_date = None
        if service_date_str:
            try:
                # Try YYYYMMDD format first
                if len(service_date_str) == 8 and service_date_str.isdigit():
                    service_date = datetime.strptime(service_date_str, '%Y%m%d').date()
                # Try MM/DD/YY format
                elif '/' in service_date_str:
                    service_date = datetime.strptime(service_date_str, '%m/%d/%y').date()
            except ValueError:
                pass
        
        return self.fair_health_rates.get_rate(zip_int, hcpcs_code, service_date)


# Module-level processor instance (singleton pattern for backward compatibility)
_processor = None


def _get_processor():
    """Get or create the module-level processor instance."""
    global _processor
    if _processor is None:
        _processor = EDIProcessor()
    return _processor


def _reset_processor():
    """Reset the processor instance (useful for testing)."""
    global _processor
    _processor = EDIProcessor()

# Display-friendly column names for CSV output
# Maps internal technical field names to human-readable display names
DISPLAY_COLUMN_NAMES = {
    # Payer Information (L1000A)
    'Payer_Name_L1000A_N1': 'PAYOR PAID',
    'Payer_Address_L1000A_N3': 'PAYOR ADDRESS',
    'Payer_Address2_L1000A_N3': 'PAYOR ADDRESS 2',
    'Payer_City_L1000A_N4': 'PAYOR CITY',
    'Payer_State_L1000A_N4': 'PAYOR STATE',
    'Payer_Zip_L1000A_N4': 'PAYOR ZIP',
    
    # Provider Information (L1000B)
    'Provider_Name_L1000B_N1': 'COMPANY',
    
    # Claim Level - CLP Segment (L2100)
    'CLM_Occurrence_L2100_CLP': 'CLAIM OCCURRENCE',
    'CLM_StatusDescr_L2100_CLP': 'IS PRIMARY',
    'CLM_FilingIndicatorDesc_L2100_CLP': 'FILING INDICATOR',
    'CLM_ChargeAmount_L2100_CLP': 'CLAIM CHARGE',
    'CLM_PaymentAmount_L2100_CLP': 'CLAIM PAYMENT',
    'CLM_PatientResponsibility_L2100_CLP': 'CLAIM PATIENT RESPONSIBILITY',
    'CLM_IsReversal_L2100_CLP': 'IS REVERSAL',
    
    # Claim Level - CAS Adjustments (L2100) - CALCULATED because we categorize from CAS segments
    'CLM_Contractual_L2100_CAS': 'CALCULATED CLAIM CONTRACTUAL',
    'CLM_Copay_L2100_CAS': 'CALCULATED CLAIM COPAY',
    'CLM_Coinsurance_L2100_CAS': 'CALCULATED CLAIM COINSURANCE',
    'CLM_Deductible_L2100_CAS': 'CALCULATED CLAIM DEDUCTIBLE',
    'CLM_Denied_L2100_CAS': 'CALCULATED CLAIM DENIED',
    'CLM_OtherAdjustments_L2100_CAS': 'CALCULATED CLAIM OTHER ADJUSTMENTS',
    'CLM_Sequestration_L2100_CAS': 'CALCULATED CLAIM SEQUESTRATION',
    'CLM_COB_L2100_CAS': 'CALCULATED CLAIM COB',
    'CLM_HCRA_L2100_CAS': 'CALCULATED CLAIM HCRA',
    'CLM_QMB_L2100_CAS': 'CALCULATED CLAIM QMB',
    'CLM_AuditFlag_L2100_CAS': 'CLAIM ADJUSTMENT AUDIT FLAG',
    
    # Claim Level - AMT Amounts (L2100)
    'CLM_InterestAmount_L2100_AMT': 'CLAIM INTEREST',
    'CLM_CoverageAmount_L2100_AMT': 'CLAIM COVERAGE',
    'CLM_PatientAmountPaid_L2100_AMT': 'CLAIM PATIENT PAID',
    'CLM_DiscountAmount_L2100_AMT': 'CLAIM DISCOUNT',
    
    # Claim Level - NM1 Patient/Subscriber (L2100)
    'CLM_MemberID_L2100_NM1': 'MEMBER ID',
    'CLM_SubscriberName_L2100_NM1': 'SUSCRIBER NAME',
    'CLM_PatientName_L2100_NM1': 'NAME',
    'CLM_SSN_L2100_NM1': 'SSN',
    'CLM_CorrectedInsuredName_L2100_NM1': 'NAME CORRECTIONS',
    'CLM_CorrectedInsuredID_L2100_NM1': 'MEMBER ID CORRECTIONS',
    
    # Claim Level - REF References (L2100)
    'CLM_PriorAuth_L2100_REF': 'AUTH',
    'CLM_OriginalRef_L2100_REF': 'ORIGINAL REFERENCE NUMBER',
    'CLM_GroupNumber_L2100_REF': 'MEMBER GROUP ID',
    'CLM_PlanName_L2100_REF': 'MEMBER PLAN ID',
    'CLM_RepricedClaimRefNumber_L2100_REF': 'CLAIM REPRICED CLAIM REF',
    'CLM_RepricedLineItemRefNumber_L2100_REF': 'CLAIM REPRICED LINE ITEM REF',
    'CLM_AmbulatoryPaymentClassification_L2100_REF': 'CLAIM APC',
    'CLM_NAICCode_L2100_REF': 'CLAIM NAIC CODE',
    
    # Claim Level - DTM Dates (L2100)
    'CLM_ServiceStartDate_L2100_DTM': 'DATE OF SERVICE',
    
    # Claim Level - LQ Remark Codes (L2100)
    'CLM_HealthcareRemarkCodes_L2100_LQ': 'CLAIM HEALTHCARE REMARK CODES',
    'CLM_HealthcareRemarkDescriptions_L2100_LQ': 'CLAIM HEALTHCARE REMARK DESCRIPTIONS',
    
    # Claim Level - QTY Quantities (L2100)
    'CLM_CoveredActual_L2100_QTY': 'CLAIM COVERED ACTUAL',
    
    # Header Level - DTM*405 Production Date
    'CHK_ProductionDate_Header_DTM405': 'PRODUCTION DATE',
    
    # Service Level - SVC Segment (L2110)
    'SVC_ProcedureCode_L2110_SVC': 'HCPCS',
    'SVC_CodeDescription_L2110_SVC': 'HCPCS DESCRIPTION',
    'SVC_ServiceLevel_L2110_SVC': 'HCPCS TYPE',
    'SVC_Modifier1_L2110_SVC': 'MODIFIERS 1',
    'SVC_Modifier2_L2110_SVC': 'MODIFIERS 2',
    'SVC_Modifiers_L2110_SVC': 'MODIFIERS',
    'SVC_ModifierDescriptions_L2110_SVC': 'MODIFIERS DESCRIPTION',
    'SVC_ChargeAmount_L2110_SVC': 'SERVICE CHARGE',
    'SVC_PaymentAmount_L2110_SVC': 'SERVICE PAYMENT',
    'SVC_Units_L2110_SVC': 'SERVICE UNITS',
    
    # Service Level - CAS Adjustments (L2110) - CALCULATED because we categorize from CAS segments
    'SVC_Contractual_L2110_CAS': 'CALCULATED CONTRACTUAL',
    'SVC_Copay_L2110_CAS': 'CALCULATED COPAY',
    'SVC_Coinsurance_L2110_CAS': 'CALCULATED COINSURANCE',
    'SVC_Deductible_L2110_CAS': 'CALCULATED DEDUCTIBLE',
    'SVC_Denied_L2110_CAS': 'CALCULATED DENIED',
    'SVC_OtherAdjustments_L2110_CAS': 'CALCULATED OTHER ADJUSTMENTS',
    'SVC_Sequestration_L2110_CAS': 'CALCULATED SEQUESTRATION',
    'SVC_COB_L2110_CAS': 'CALCULATED COB',
    'SVC_HCRA_L2110_CAS': 'CALCULATED HCRA',
    'SVC_QMB_L2110_CAS': 'CALCULATED QMB',
    'SVC_AuditFlag_L2110_CAS': 'ADJUSTMENT AUDIT FLAG',
    'SVC_Adjustments_L2110_CAS': 'SERVICE ADJUSTMENTS RAW',
    
    # Service Level - DTM Dates (L2110)
    'SVC_ServiceStartDate_L2110_DTM': 'SERVICE DATE',
    
    # Service Level - LQ Remark Codes (L2110)
    'SVC_RemarkCodes_L2110_LQ': 'SERVICE REMARK CODES',
    'SVC_RemarkDescriptions_L2110_LQ': 'SERVICE REMARK DESCRIPTIONS',
    'SVC_HealthcareRemarkCodes_L2110_LQ': 'SERVICE HEALTHCARE REMARK CODES',
    'SVC_HealthcareRemarkDescriptions_L2110_LQ': 'SERVICE HEALTHCARE REMARK DESCRIPTIONS',
    
    # Service Level - AMT Amounts (L2110)
    'SVC_AllowedAmount_L2110_AMT': 'SERVICE ALLOWED AMOUNT',
    
    # Service Level - QTY Quantities (L2110)
    'SVC_CoveredActual_L2110_QTY': 'SERVICE COVERED ACTUAL',
    
    # Service Level - REF Repricing References (L2110)
    'SVC_RepricedClaimRefNumber_L2110_REF': 'SERVICE REPRICED CLAIM REF',
    'SVC_RepricedLineItemRefNumber_L2110_REF': 'SERVICE REPRICED LINE ITEM REF',
    'SVC_AmbulatoryPaymentClassification_L2110_REF': 'SERVICE APC',
    'SVC_NAICCode_L2110_REF': 'SERVICE NAIC CODE',
    
    # Calculated Fields
    'Patient_NonCovered': 'CALCULATED PATIENT NON COVERED',
    'Patient_OtherResp': 'CALCULATED PATIENT OTHER',
    'Allowed_Amount': 'CALCULATED ALLOWED 1',
    'Allowed_Verification': 'CALCULATED ALLOWED 2',
    'EDI_MileageUnitPrice': 'SERVICE MILEAGE UNIT PRICE',
    
    # Fair Health Rate Columns (matched to trip's pickup ZIP)
    'FH_PickupZIP': 'PICK UP ZIP',
    'FH_OutOfNetwork': 'OUT OF NETWORK',
    'FH_InNetwork': 'IN NETWORK',
    'FH_OON_UnitPrice': 'ONUP',
    'FH_IN_UnitPrice': 'INUP',
    'FH_OON_Miles': 'OUT OF NETWORK MILES',
    'FH_IN_Miles': 'IN NETWORK MILES',
    'FH_OON_Final': 'OUT OF NETWORK FINAL',
    'FH_IN_Final': 'IN NETWORK FINAL',
    'FH_EffectiveUnits': 'FAIR HEALTH UNITS USED',
}


def rename_columns_for_display(row):
    """
    Rename internal column names to human-readable display names for CSV output.
    
    Args:
        row: Dictionary with internal field names as keys
        
    Returns:
        New dictionary with display-friendly column names where mappings exist,
        original names preserved for unmapped fields.
    """
    return {DISPLAY_COLUMN_NAMES.get(k, k): v for k, v in row.items()}


def load_fair_health_rates(rates_file_path=None):
    """
    Load Fair Health rates from Excel file.
    
    Delegates to the module-level EDIProcessor instance.
    
    Args:
        rates_file_path: Path to RATES.xlsx file. If None, looks for RATES.xlsx
                        in user's Desktop folder.
    
    Returns:
        True if rates loaded successfully, False otherwise.
    """
    return _get_processor().load_fair_health_rates(rates_file_path)


def normalize_zip_code(zip_str):
    """
    Normalize ZIP to 5 digits. Handles:
    - Standard 5-digit: "12206" -> "12206"
    - ZIP+4 with hyphen: "12206-1234" -> "12206"
    - Fat-fingered ZIP+4 (no hyphen): "122061234" -> "12206"
    - Empty/whitespace: "" -> None
    """
    if not zip_str:
        return None
    zip_str = str(zip_str).strip()
    if not zip_str:
        return None
    if '-' in zip_str:
        zip_str = zip_str.split('-')[0]
    digits = ''.join(c for c in zip_str if c.isdigit())
    return digits[:5] if len(digits) >= 5 else None


def load_trips_csv():
    """
    Load Trips.csv and build RUN -> PU ZIP lookup.
    
    Delegates to the module-level EDIProcessor instance.
    """
    return _get_processor().load_trips_csv()


def get_fair_health_rates_for_service(hcpcs_code, service_date_str):
    """
    Get Fair Health rates for a service line (builds dict for ALL ZIPs).
    
    NOTE: Prefer get_fair_health_rate_for_zip() for O(1) single-ZIP lookup.
    
    Delegates to the module-level EDIProcessor instance.
    
    Args:
        hcpcs_code: The HCPCS/CPT procedure code
        service_date_str: Service date as string (YYYYMMDD or MM/DD/YY format)
    
    Returns:
        Dict mapping ZIP codes to (out_of_network, in_network) tuples
    """
    return _get_processor().get_fair_health_rates_for_service(hcpcs_code, service_date_str)


def get_fair_health_rate_for_zip(zip_code, hcpcs_code, service_date_str):
    """
    Get Fair Health rate for a specific ZIP code - O(1) lookup.
    
    Delegates to the module-level EDIProcessor instance.
    
    Args:
        zip_code: The ZIP code to look up (int or string)
        hcpcs_code: The HCPCS/CPT procedure code
        service_date_str: Service date as string (YYYYMMDD or MM/DD/YY format)
    
    Returns:
        Tuple of (out_of_network, in_network) or None if not found
    """
    return _get_processor().get_fair_health_rate_for_zip(zip_code, hcpcs_code, service_date_str)


redact_string = redactor.redact_string
redact_835_segment = redactor.redact_835_segment
redact_835_file = redactor.redact_835_file
redact_csv_row = redactor.redact_csv_row
normalize_csv_row = redactor.normalize_csv_row


def format_run_number(claim_number):
    """
    Format a claim number as a RUN number by inserting a hyphen between
    the 2nd and 3rd digit.
    
    Example: 2565914 becomes 25-65914
    
    Args:
        claim_number: The patient control number / claim number
    
    Returns:
        Formatted RUN string, or empty string if claim_number is invalid
    """
    if not claim_number:
        return ''
    
    # Convert to string and strip whitespace
    claim_str = str(claim_number).strip()
    
    if len(claim_str) < 3:
        # Not enough digits to insert hyphen
        return claim_str
    
    # Insert hyphen between 2nd and 3rd character
    return claim_str[:2] + '-' + claim_str[2:]


def parse_835_file(file_path):
    """Parse 835 file
    Args:
        file_path: Path to the 835 file
    """
    with open(file_path, 'r') as f:
        content = f.read()

    content = content.rstrip()

    if not content.startswith('ISA'):
        raise ValueError("File does not start with ISA segment")

    element_delimiter = content[3]
    component_delimiter = content[104]
    segment_terminator = content[105] if len(content) > 105 else '~'

    cleaned_content = content.replace('\n', '').replace('\r', '')
    segments = [seg.strip() for seg in cleaned_content.split(segment_terminator) if seg.strip()]

    return {
        'segments': segments,
        'element_delimiter': element_delimiter,
        'component_delimiter': component_delimiter,
        'content': cleaned_content
    }


def safe_parse_segment(segment, element_delimiter, parser_func, default_return=None):
    try:
        elements = segment.split(element_delimiter)
        return parser_func(elements)
    except IndexError:
        # Expected error when element index out of range - return default silently
        return default_return if default_return is not None else {}
    except (ValueError, TypeError, KeyError) as e:
        # Known parsing errors - log at debug level
        logger.debug("Segment parse error (%s): %s", type(e).__name__, e)
        return default_return if default_return is not None else {}


def safe_element_access(elements, index, default=''):
    try:
        if index < len(elements):
            return elements[index]
        return default
    except (IndexError, TypeError):
        return default


def parse_cas(elements, segment_id='', claim_status=None, payer_key=None):
    """
    Parse CAS segment with proper handling of optional quantity elements.

    Per X12 835 spec section 1.10.2.1: CAS adjustments DECREASE payment when positive,
    INCREASE payment when negative. Amounts are preserved as-is from EDI (may be negative).

    Fix Issue 2: CAS follows pattern Reason-Amount-[Quantity] where quantity is conditional.
    Pattern: CAS*Group*Reason1*Amount1*[Qty1]*Reason2*Amount2*[Qty2]...
    Quantities (CAS04/07/10/13/16/19) are optional per X12 spec.

    Args:
        elements: Segment elements split by element delimiter
        segment_id: Context identifier (CLAIM or SERVICE)
        claim_status: Claim status code for context
        payer_key: Payer key for payer-specific overrides (e.g., CARC normalization)

    Returns:
        List of adjustment dictionaries with 'group_code', 'reason_code', 'amount', 'quantity'
    """
    cas_entries = []

    if len(elements) < 4:  # Minimum: CAS*Group*Reason*Amount
        return cas_entries

    group_code = elements[1] if len(elements) > 1 else ''

    # Per X12 spec: CAS has up to 6 adjustment trios
    # Positions: CAS01=Group, then repeating trios:
    # Trio 1: CAS02=Reason, CAS03=Amount, CAS04=Quantity (optional)
    # Trio 2: CAS05=Reason, CAS06=Amount, CAS07=Quantity (optional)
    # Trio 3: CAS08=Reason, CAS09=Amount, CAS10=Quantity (optional)
    # Trio 4: CAS11=Reason, CAS12=Amount, CAS13=Quantity (optional)
    # Trio 5: CAS14=Reason, CAS15=Amount, CAS16=Quantity (optional)
    # Trio 6: CAS17=Reason, CAS18=Amount, CAS19=Quantity (optional)
    
    # Process each trio
    trio_positions = [
        (2, 3, 4),   # Trio 1: CAS02, CAS03, CAS04
        (5, 6, 7),   # Trio 2: CAS05, CAS06, CAS07
        (8, 9, 10),  # Trio 3: CAS08, CAS09, CAS10
        (11, 12, 13), # Trio 4: CAS11, CAS12, CAS13
        (14, 15, 16), # Trio 5: CAS14, CAS15, CAS16
        (17, 18, 19)  # Trio 6: CAS17, CAS18, CAS19
    ]
    
    for reason_pos, amount_pos, qty_pos in trio_positions:
        # Check if we have at least the reason code
        if reason_pos >= len(elements):
            break
            
        reason_code_raw = elements[reason_pos].strip() if reason_pos < len(elements) else ''
        amount = elements[amount_pos].strip() if amount_pos < len(elements) else ''
        
        # Only process if we have both reason and amount
        if not reason_code_raw or not amount:
            continue
        
        # Normalize CARC code to handle payer-specific variations (e.g., Medi-Cal leading zeros)
        reason_code = colloquial.normalize_carc_code(reason_code_raw)
            
        # Quantity is optional - only include if position exists and has value
        quantity = ''
        if qty_pos < len(elements):
            potential_qty = elements[qty_pos].strip()
            # Only use it as quantity if it's numeric or empty
            # If it looks like a reason code (contains letters), skip it
            if potential_qty and not any(c.isalpha() for c in potential_qty.replace('-', '').replace('+', '')):
                try:
                    # Validate it can be parsed as a number
                    float(potential_qty)
                    quantity = potential_qty
                except (ValueError, TypeError):
                    # Not a valid number, leave quantity empty
                    pass
        
        cas_entries.append({
            'group_code': group_code,
            'group_code_desc': get_cas_group_code_description(group_code),
            'reason_code': reason_code,
            'amount': amount,
            'quantity': quantity  # May be empty string
        })

    return cas_entries


def parse_ref(elements, payer_key=None):
    """Parse REF segment with payer override support for qualifier descriptions."""
    return {
        'qualifier': elements[1] if len(elements) > 1 else '',
        'qualifier_desc': get_reference_qualifier_description(elements[1], payer_key=payer_key) if len(elements) > 1 else '',
        'ref_value': elements[2] if len(elements) > 2 else '',
        'description': elements[3] if len(elements) > 3 else ''
    }


def parse_dtm(elements, payer_key=None):
    """Parse DTM segment with payer override support for qualifier descriptions."""
    return {
        'date_qualifier': elements[1] if len(elements) > 1 else '',  
        'date_qualifier_desc': get_date_qualifier_description(elements[1], payer_key=payer_key) if len(elements) > 1 else '',
        'date_value': elements[2] if len(elements) > 2 else '',  
        'time': elements[3] if len(elements) > 3 else '',  
        'time_code': elements[4] if len(elements) > 4 else '',  
        'date_time_period_format': elements[5] if len(elements) > 5 else '',  
        'date_time_period': elements[6] if len(elements) > 6 else ''  
    }


def parse_nm1(elements, payer_key=None):
    """Parse NM1 segment with payer override support for entity descriptions."""
    return {
        'entity_id_code': elements[1] if len(elements) > 1 else '',
        'entity_id_desc': get_entity_identifier_description(elements[1], payer_key=payer_key) if len(elements) > 1 else '',
        'entity_type_qualifier': elements[2] if len(elements) > 2 else '',
        'entity_type_desc': get_entity_type_qualifier_description(elements[2]) if len(elements) > 2 else '',
        'last_name': elements[3] if len(elements) > 3 else '',
        'first_name': elements[4] if len(elements) > 4 else '',
        'middle_name': elements[5] if len(elements) > 5 else '',
        'prefix': elements[6] if len(elements) > 6 else '',
        'suffix': elements[7] if len(elements) > 7 else '',
        'id_code_qualifier': elements[8] if len(elements) > 8 else '',
        'id_code_qualifier_desc': get_id_code_qualifier_description(elements[8]) if len(elements) > 8 else '',
        'id_code': elements[9] if len(elements) > 9 else '',
        'entity_relationship': elements[10] if len(elements) > 10 else '',  
        'entity_relationship_desc': get_entity_relationship_description(elements[10]) if len(elements) > 10 else '',
        'entity_id_code_secondary': elements[11] if len(elements) > 11 else '',  
        'name_last_secondary': elements[12] if len(elements) > 12 else ''  
    }

def parse_n3(elements):
    return {
        'address_line_1': elements[1] if len(elements) > 1 else '',
        'address_line_2': elements[2] if len(elements) > 2 else ''
    }


def parse_n4(elements):
    """
    Parse N4 (Geographic Location) segment.

    Fix Issue 3: N4 only has 7 elements in HIPAA 5010, not 8.
    Removed non-existent N408 'postal_code_formatted' field.
    """
    return {
        'city': elements[1] if len(elements) > 1 else '',                    # N401 - City Name
        'state': elements[2] if len(elements) > 2 else '',                   # N402 - State/Province Code
        'postal_code': elements[3] if len(elements) > 3 else '',             # N403 - Postal Code
        'country_code': elements[4] if len(elements) > 4 else '',            # N404 - Country Code
        'country_desc': get_country_code_description(elements[4]) if len(elements) > 4 else '',
        'location_qualifier': elements[5] if len(elements) > 5 else '',      # N405 - Location Qualifier
        'location_qualifier_desc': get_location_qualifier_description(elements[5]) if len(elements) > 5 else '',
        'location_id': elements[6] if len(elements) > 6 else '',             # N406 - Location Identifier
        'country_subdivision': elements[7] if len(elements) > 7 else ''      # N407 - Country Subdivision Code
        # N408 does not exist in HIPAA 5010 specification
    }


def parse_per(elements):
    communication_pairs = []

    for i in range(3, len(elements), 2):
        if i + 1 < len(elements):
            communication_pairs.append({
                'qualifier': elements[i],
                'number': elements[i + 1]
            })

    return {
        'contact_function_code': elements[1] if len(elements) > 1 else '',
        'name': elements[2] if len(elements) > 2 else '',
        'communication': communication_pairs
    }


def parse_amt(elements):
    return {
        'amount_qualifier': elements[1] if len(elements) > 1 else '',  
        'amount_qualifier_desc': get_amount_qualifier_description(elements[1]) if len(elements) > 1 else '',
        'monetary_amount': elements[2] if len(elements) > 2 else '',  
        'credit_debit_flag': elements[3] if len(elements) > 3 else ''  
    }


def parse_qty(elements):
    return {
        'quantity_qualifier': elements[1] if len(elements) > 1 else '',  
        'quantity_qualifier_desc': get_quantity_qualifier_description(elements[1]) if len(elements) > 1 else '',
        'quantity': elements[2] if len(elements) > 2 else '',  
        'unit_of_measure': elements[3] if len(elements) > 3 else '',  
        'free_form_info': elements[4] if len(elements) > 4 else ''  
    }


def parse_moa(elements):
    """Parse MOA (Medicare Outpatient Adjudication) segment.
    
    Contains Medicare-specific outpatient claim adjudication information
    including reimbursement rates and remark codes.
    """
    return {
        'reimbursement_rate': elements[1] if len(elements) > 1 else '',  
        'claim_hcpcs_payable_amount': elements[2] if len(elements) > 2 else '',  
        'claim_payment_remark_code_1': elements[3] if len(elements) > 3 else '',  
        'claim_payment_remark_code_2': elements[4] if len(elements) > 4 else '',  
        'claim_payment_remark_code_3': elements[5] if len(elements) > 5 else '',  
        'claim_payment_remark_code_4': elements[6] if len(elements) > 6 else '',  
        'claim_payment_remark_code_5': elements[7] if len(elements) > 7 else '',  
        'esrd_payment_amount': elements[8] if len(elements) > 8 else '',  
        'nonpayable_professional_component': elements[9] if len(elements) > 9 else ''  
    }


def parse_lq(elements):
    return {
        'code_list_qualifier': elements[1] if len(elements) > 1 else '',
        'industry_code': elements[2] if len(elements) > 2 else ''
    }


def parse_cur(elements):
    """Parse CUR (Currency) segment"""
    # CUR segment is for foreign currency claims - not used for US professional claims
    # Keeping function for compatibility but returning empty dict
    return {}
    # Original currency fields commented out:
    # return {
    #     'entity_identifier_code': elements[1] if len(elements) > 1 else '',
    #     'currency_code': elements[2] if len(elements) > 2 else '',
    #     'exchange_rate': elements[3] if len(elements) > 3 else '',
    #     'entity_identifier_code_2': elements[4] if len(elements) > 4 else '',
    #     'currency_code_2': elements[5] if len(elements) > 5 else '',
    #     'currency_market_exchange_code': elements[6] if len(elements) > 6 else '',
    #     'date_time_qualifier': elements[7] if len(elements) > 7 else '',
    #     'date': elements[8] if len(elements) > 8 else ''
    # }


def parse_rdm(elements):
    """Parse RDM (Remittance Delivery Method) segment"""
    return {
        'report_transmission_code': elements[1] if len(elements) > 1 else '',
        'name': elements[2] if len(elements) > 2 else '',
        'communication_number': elements[3] if len(elements) > 3 else '',
        'reference_identifier': elements[4] if len(elements) > 4 else '',
        'reference_identifier_2': elements[5] if len(elements) > 5 else '',
        'communication_number_2': elements[6] if len(elements) > 6 else '',
        'contact_function_code': elements[7] if len(elements) > 7 else ''
    }


def parse_n2(elements):
    """Parse N2 (Additional Name Information) segment"""
    return {
        'name_line_1': elements[1] if len(elements) > 1 else '',
        'name_line_2': elements[2] if len(elements) > 2 else ''
    }


def parse_isa(elements):
    """Parse ISA (Interchange Control Header) segment"""
    return {
        'authorization_information_qualifier': elements[1] if len(elements) > 1 else '',
        'authorization_information': elements[2] if len(elements) > 2 else '',
        'security_information_qualifier': elements[3] if len(elements) > 3 else '',
        'security_information': elements[4] if len(elements) > 4 else '',
        'interchange_id_qualifier_sender': elements[5] if len(elements) > 5 else '',
        'interchange_sender_id': elements[6] if len(elements) > 6 else '',
        'interchange_id_qualifier_receiver': elements[7] if len(elements) > 7 else '',
        'interchange_receiver_id': elements[8] if len(elements) > 8 else '',
        'interchange_date': elements[9] if len(elements) > 9 else '',
        'interchange_time': elements[10] if len(elements) > 10 else '',
        'repetition_separator': elements[11] if len(elements) > 11 else '',
        'interchange_control_version_number': elements[12] if len(elements) > 12 else '',
        'interchange_control_number': elements[13] if len(elements) > 13 else '',
        'acknowledgment_requested': elements[14] if len(elements) > 14 else '',
        'interchange_usage_indicator': elements[15] if len(elements) > 15 else '',
        'component_element_separator': elements[16] if len(elements) > 16 else ''
    }


def parse_gs(elements):
    """Parse GS (Functional Group Header) segment"""
    return {
        'functional_identifier_code': elements[1] if len(elements) > 1 else '',
        'application_sender_code': elements[2] if len(elements) > 2 else '',
        'application_receiver_code': elements[3] if len(elements) > 3 else '',
        'date': elements[4] if len(elements) > 4 else '',
        'time': elements[5] if len(elements) > 5 else '',
        'group_control_number': elements[6] if len(elements) > 6 else '',
        'responsible_agency_code': elements[7] if len(elements) > 7 else '',
        'version_release_industry_id': elements[8] if len(elements) > 8 else ''
    }


def parse_lx(elements):
    return {
        'header_number': elements[1] if len(elements) > 1 else ''
    }


def parse_ts3(elements):
    # TS3 segment is primarily for institutional provider summaries - not used for professional claims
    # Keeping function for compatibility but returning empty dict
    return {}
    # Original TS3 fields commented out:
    # return {
    #     'provider_identifier': elements[1] if len(elements) > 1 else '',
    #     'facility_type_code': elements[2] if len(elements) > 2 else '',
    #     'fiscal_period_end_date': elements[3] if len(elements) > 3 else '',
    #     'total_claim_count': elements[4] if len(elements) > 4 else '',
    #     'total_claim_charge_amount': elements[5] if len(elements) > 5 else '',
    #     'total_covered_charge_amount': elements[6] if len(elements) > 6 else '',
    #     'total_noncovered_charge_amount': elements[7] if len(elements) > 7 else '',
    #     'total_denied_charge_amount': elements[8] if len(elements) > 8 else '',
    #     'total_provider_payment_amount': elements[9] if len(elements) > 9 else '',
    #     'total_interest_amount': elements[10] if len(elements) > 10 else '',
    #     'total_contractual_adjustment_amount': elements[11] if len(elements) > 11 else '',
    #     'total_gramm_rudman_reduction_amount': elements[12] if len(elements) > 12 else '',
    #     'total_msp_payer_amount': elements[13] if len(elements) > 13 else '',
    #     'total_blood_deductible_amount': elements[14] if len(elements) > 14 else '',
    #     'total_noncovered_days_count': elements[15] if len(elements) > 15 else '',
    #     'total_coinsurance_days_count': elements[16] if len(elements) > 16 else '',
    #     'total_conditional_pay_amount': elements[17] if len(elements) > 17 else '',
    #     'total_psp_fsi_claim_count': elements[18] if len(elements) > 18 else '',
    #     'total_pps_capital_amount': elements[19] if len(elements) > 19 else '',
    #     'pps_capital_fsp_drg_amount': elements[20] if len(elements) > 20 else '',
    #     'total_pps_capital_hsp_drg_amount': elements[21] if len(elements) > 21 else '',
    #     'total_pps_dsh_drg_amount': elements[22] if len(elements) > 22 else '',
    #     'total_pip_claim_count': elements[23] if len(elements) > 23 else '',
    #     'total_pip_adjustment_amount': elements[24] if len(elements) > 24 else ''
    # }


def parse_ts2(elements):
    # TS2 segment is for institutional Medicare Part A - not used for professional claims
    # Keeping function for compatibility but returning empty dict
    return {}
    # Original institutional-only fields commented out:
    # return {
    #     'total_drg_amount': elements[1] if len(elements) > 1 else '',
    #     'total_federal_specific_amount': elements[2] if len(elements) > 2 else '',  
    #     'total_hospital_specific_amount': elements[3] if len(elements) > 3 else '',  
    #     'total_disproportionate_share_amount': elements[4] if len(elements) > 4 else '', 
    #     'total_capital_amount': elements[5] if len(elements) > 5 else '',  
    #     'total_indirect_medical_education_amount': elements[6] if len(elements) > 6 else '',  
    #     'total_outlier_day_count': elements[7] if len(elements) > 7 else '',  
    #     'total_day_outlier_amount': elements[8] if len(elements) > 8 else '',  
    #     'total_cost_outlier_amount': elements[9] if len(elements) > 9 else '',  
    #     'average_drg_length_of_stay': elements[10] if len(elements) > 10 else '',  
    #     'total_discharge_count': elements[11] if len(elements) > 11 else '',  
    #     'total_cost_report_day_count': elements[12] if len(elements) > 12 else '',  
    #     'total_covered_day_count': elements[13] if len(elements) > 13 else '',  
    #     'total_noncovered_day_count': elements[14] if len(elements) > 14 else '',  
    #     'total_msp_pass_through_amount': elements[15] if len(elements) > 15 else '',  
    #     'average_drg_weight': elements[16] if len(elements) > 16 else '',  
    #     'total_pps_standard_amount': elements[17] if len(elements) > 17 else '',  
    #     'total_pps_capital_fsp_amount': elements[18] if len(elements) > 18 else '',  
    #     'total_pps_capital_hsp_amount': elements[19] if len(elements) > 19 else ''  
    # }


def parse_mia(elements):
    """Parse MIA (Medicare Inpatient Adjudication) segment.
    
    Contains Medicare-specific inpatient claim adjudication information
    including covered days, DRG amounts, and remark codes.
    """
    return {
        'covered_days_or_visits_count': elements[1] if len(elements) > 1 else '',
        'pps_operating_outlier_amount': elements[2] if len(elements) > 2 else '',
        'lifetime_psychiatric_days_count': elements[3] if len(elements) > 3 else '',
        'claim_drg_amount': elements[4] if len(elements) > 4 else '',
        'claim_payment_remark_code': elements[5] if len(elements) > 5 else '',
        'claim_disproportionate_share_amount': elements[6] if len(elements) > 6 else '',
        'claim_msp_pass_through_amount': elements[7] if len(elements) > 7 else '',
        'claim_pps_capital_amount': elements[8] if len(elements) > 8 else '',
        'pps_capital_fsp_drg_amount': elements[9] if len(elements) > 9 else '',
        'pps_capital_hsp_drg_amount': elements[10] if len(elements) > 10 else '',
        'pps_capital_dsh_drg_amount': elements[11] if len(elements) > 11 else '',
        'old_capital_amount': elements[12] if len(elements) > 12 else '',
        'pps_capital_ime_amount': elements[13] if len(elements) > 13 else '',
        'pps_operating_hospital_specific_drg_amount': elements[14] if len(elements) > 14 else '',
        'cost_report_day_count': elements[15] if len(elements) > 15 else '',
        'pps_operating_federal_specific_drg_amount': elements[16] if len(elements) > 16 else '',
        'claim_pps_capital_outlier_amount': elements[17] if len(elements) > 17 else '',
        'claim_indirect_teaching_amount': elements[18] if len(elements) > 18 else '',
        'nonpayable_professional_component_amount': elements[19] if len(elements) > 19 else '',
        'claim_payment_remark_code_2': elements[20] if len(elements) > 20 else '',
        'claim_payment_remark_code_3': elements[21] if len(elements) > 21 else '',
        'claim_payment_remark_code_4': elements[22] if len(elements) > 22 else '',
        'claim_payment_remark_code_5': elements[23] if len(elements) > 23 else '',
        'pps_capital_exception_amount': elements[24] if len(elements) > 24 else ''
    }


def get_claim_filing_indicator_description(code):
    return dictionary.get_claim_filing_indicator_description(code)


def get_claim_status_description(code):
    return dictionary.get_claim_status_description(code)


def get_facility_type_description(code):
    """Get description for Facility Type Code (CLP*08)."""
    return dictionary.get_facility_type_description(code)


def get_discharge_status_description(code):
    """Get description for Patient Discharge Status (CLP*10)."""
    return dictionary.get_discharge_status_description(code)


def get_place_of_service_description(code):
    """Get description for Place of Service code."""
    return dictionary.get_facility_type_description(code)  # Using facility_type since it's the same POS codes


def get_claim_frequency_description(code):
    return dictionary.get_claim_frequency_description(code)


def get_trace_type_description(code):
    return dictionary.get_trace_type_description(code)


# Foreign currency - not used for US professional claims
# def get_currency_code_description(code):
#     return dictionary.get_currency_code_description(code)


def get_date_qualifier_description(code, payer_key=None):
    """Get date qualifier description with payer override support."""
    if payer_key:
        payer_desc = colloquial.get_payer_date_qualifier_description(payer_key, code)
        if payer_desc:
            return payer_desc
    return dictionary.get_date_qualifier_description(code)


def get_ambulance_code_description(code):

    return dictionary.get_ambulance_code_description(code)


def is_transportation_code(code):
    """Check if a procedure code is an ambulance/transportation code that has a description.
    
    Includes:
        - A0* - Ambulance HCPCS codes
        - T2* - Non-emergency transportation codes  
        - S0207, S0208, S0209, S0215 - Wheelchair/mileage codes
    """
    if not code:
        return False
    return (code.startswith('A0') or 
            code.startswith('T2') or 
            code in ('S0207', 'S0208', 'S0209', 'S0215'))


def get_ambulance_modifier_description(modifier):
    return dictionary.get_ambulance_modifier_description(modifier)


def get_ambulance_service_level_name(code):
    return dictionary.get_ambulance_service_level_name(code)


def get_payment_method_description(code):
    return dictionary.get_payment_method_description(code)


def get_payment_format_description(code):
    return dictionary.get_payment_format_description(code)


def get_service_qualifier_description(code):
    return dictionary.get_service_qualifier_description(code)


def get_contact_function_description(code):
    return dictionary.get_contact_function_description(code)


def get_remark_code_description(code, payer_key=None):
    """Get RARC code description with payer override support."""
    if payer_key:
        payer_desc = colloquial.get_payer_rarc_description(payer_key, code)
        if payer_desc:
            return payer_desc
    return dictionary.get_remark_code_description(code)


def get_carc_classifications():
    return dictionary.get_carc_classifications()


def normalize_carc_code(code):
    """Normalize CARC code by stripping leading zeros if the result is valid.
    This handles payer-specific variations like Medi-Cal's leading zero codes.
    """
    return colloquial.normalize_carc_code(code)


def get_carc_description(code, payer_key=None):
    """Get CARC description with payer override support."""
    if not code:
        return ''
    # Try payer override first
    if payer_key:
        payer_desc = colloquial.get_payer_carc_description(payer_key, code)
        if payer_desc:
            return payer_desc
    # Fall back to standard lookup
    classifications = get_carc_classifications()
    # Normalize the code first (handles leading zeros from some agencies)
    normalized = normalize_carc_code(code)
    if normalized in classifications:
        return classifications[normalized].get('description', f'Unknown CARC: {code}')
    return f'Unknown CARC: {code}'


def get_dfi_id_number_qualifier_description(code):
    return dictionary.get_dfi_id_number_qualifier_description(code)


def get_account_number_qualifier_description(code):
    return dictionary.get_account_number_qualifier_description(code)


def get_bpr_transaction_handling_description(code):
    return dictionary.get_bpr_transaction_handling_description(code)


def get_communication_number_qualifier_description(code):
    return dictionary.get_communication_number_qualifier_description(code)

def get_cas_group_code_description(code, segment_context=''):
    desc = dictionary.get_cas_group_code_description(code)
    return desc


def get_entity_identifier_description(code, payer_key=None):
    """Get entity identifier description with payer override support."""
    # Check for payer-specific override first
    if payer_key:
        payer_desc = colloquial.get_payer_entity_description(payer_key, code)
        if payer_desc:
            return payer_desc
    # Fall back to standard dictionary
    return dictionary.get_entity_identifier_description(code)


def get_reference_qualifier_description(code, payer_key=None):
    """Get reference qualifier description with payer override support."""
    # Check for payer-specific override first
    if payer_key:
        payer_desc = colloquial.get_payer_reference_qualifier_description(payer_key, code)
        if payer_desc:
            return payer_desc
    # Fall back to standard dictionary
    return dictionary.get_reference_qualifier_description(code)


def get_entity_type_qualifier_description(code):
    return dictionary.get_entity_type_qualifier_description(code)


def get_id_code_qualifier_description(code):
    return dictionary.get_id_code_qualifier_description(code)


def get_plb_adjustment_code_description(code, payer_key=None):
    """Get PLB adjustment code description with payer override support."""
    # Check for payer-specific override first
    if payer_key:
        payer_desc = colloquial.get_payer_plb_description(payer_key, code)
        if payer_desc:
            return payer_desc
    # Fall back to standard dictionary
    return dictionary.get_plb_adjustment_code_description(code)

def get_amount_qualifier_description(code):
    return dictionary.get_amount_qualifier_description(code)


def get_payer_parsing_rule(payer_key, rule_name, default=None):
    """Get a payer-specific parsing rule value.
    
    Args:
        payer_key: Payer identifier key (e.g., "MEDI_CAL", "EMEDNY")
        rule_name: Name of the parsing rule to retrieve
        default: Default value if rule not found
        
    Returns:
        The rule value if found, otherwise the default
    """
    if payer_key:
        rules = colloquial.get_parsing_rules(payer_key)
        return rules.get(rule_name, default)
    return default


def get_quantity_qualifier_description(code):
    return dictionary.get_quantity_qualifier_description(code)


def get_unit_of_measure_description(code):
    return dictionary.get_unit_of_measure_description(code)


def get_code_list_qualifier_description(code):
    return dictionary.get_code_list_qualifier_description(code)


def get_location_qualifier_description(code):
    return dictionary.get_location_qualifier_description(code)


def get_credit_debit_indicator_description(code):
    return dictionary.get_credit_debit_indicator_description(code)


def get_business_function_code_description(code):
    return dictionary.get_business_function_code_description(code)


def get_yes_no_condition_description(code):
    return dictionary.get_yes_no_condition_description(code)


def get_payment_typology_description(code):
    return dictionary.get_payment_typology_description(code)


def get_country_code_description(code):
    return dictionary.get_country_code_description(code)


def get_entity_relationship_description(code):
    return dictionary.get_entity_relationship_description(code)


def extract_subscriber_info(claim):
    member_id = ''
    subscriber_name = ''
    patient_name = ''
    ssn = ''

    if '_nm1' in claim:
        for nm1 in claim['_nm1']:
            if nm1.get('entity_id_code') == 'IL':
                name_parts = [
                    nm1.get('last_name', ''),
                    nm1.get('first_name', ''),
                    nm1.get('middle_name', '')
                ]
                subscriber_name = ', '.join(filter(None, name_parts))

            if nm1.get('entity_id_code') == 'QC':
                name_parts = [
                    nm1.get('last_name', ''),
                    nm1.get('first_name', ''),
                    nm1.get('middle_name', '')
                ]
                patient_name = ', '.join(filter(None, name_parts))
                if not member_id and nm1.get('id_code'):
                    member_id = nm1.get('id_code', '')

    if '_ref' in claim:
        for ref in claim['_ref']:
            if ref.get('qualifier') == '1W':
                member_id = ref.get('ref_value', '')
            elif ref.get('qualifier') == 'SY':
                ssn = ref.get('ref_value', '')

    return {
        'MemberID': member_id,
        'SubscriberName': subscriber_name,
        'PatientName': patient_name,
        'SSN': ssn
    }


def extract_payer_info(claim):
    payer_id = ''
    payer_name = ''

    if '_nm1' in claim:
        for nm1 in claim['_nm1']:
            if nm1.get('entity_id_code') == 'PR':
                payer_name = nm1.get('last_name', '')
                payer_id = nm1.get('id_code', '')
                break
    if '_ref' in claim:
        for ref in claim['_ref']:
            if ref.get('qualifier') == '2U':
                payer_id = ref.get('ref_value', '')
                break

    return {
        'PayerName': payer_name,
        'PayerID': payer_id
    }


def extract_secondary_payer_info(claim):
    """Extract secondary payer information from NM1*TT segment"""
    secondary_payer_name = ''
    secondary_payer_id_qualifier = ''
    secondary_payer_id = ''

    if '_nm1' in claim:
        for nm1 in claim['_nm1']:
            if nm1.get('entity_id_code') == 'TT':
                secondary_payer_name = nm1.get('last_name', '')
                secondary_payer_id_qualifier = nm1.get('id_qualifier', '')
                secondary_payer_id = nm1.get('id_code', '')
                break

    return {
        'SecondaryPayerName': secondary_payer_name,
        'SecondaryPayerIDQualifier': secondary_payer_id_qualifier,
        'SecondaryPayerID': secondary_payer_id
    }


# DEAD CODE - extract_provider_info is never called; provider info is parsed in convert_segments_to_rows main loop
# def extract_provider_info(segments, element_delimiter='|'):
#     provider_info = {
#         'ProviderName': '',
#         'ProviderEntityIDCode': '',  # N101 - Entity Identifier Code (PE)
#         'ProviderIDQualifier': '',   # N103 - ID Code Qualifier
#         'ProviderIDCode': '',        # N104 - ID Code
#         'ProviderAddress': '',
#         'ProviderCity': '',
#         'ProviderState': '',
#         'ProviderZip': '',
#         'ProviderTIN': '',
#         'ProviderSecondaryID': '',
#         'ProviderTaxID': ''
#     }
#
#     for idx, seg in enumerate(segments):
#         elements = seg.split(element_delimiter)
#
#         if elements[0] == 'N1' and len(elements) > 1 and elements[1] == 'PE':
#             provider_info['ProviderEntityIDCode'] = elements[1]  # N101 = "PE"
#             if len(elements) > 2:
#                 provider_info['ProviderName'] = elements[2]
#             if len(elements) > 3:
#                 provider_info['ProviderIDQualifier'] = elements[3]  # N103
#             if len(elements) > 4:
#                 provider_info['ProviderIDCode'] = elements[4]  # N104
#                 provider_info['ProviderTIN'] = elements[4]  # Keep for backward compatibility
#             if len(elements) > 5:
#                 provider_info['EntityRelationship'] = elements[5]
#                 provider_info['EntityRelationshipDesc'] = get_entity_relationship_description(elements[5])
#             if len(elements) > 6:
#                 provider_info['EntityIDSecondary'] = elements[6]
#
#             offset = 1
#             while idx + offset < len(segments):
#                 next_seg = segments[idx + offset]
#                 next_elements = next_seg.split(element_delimiter)
#
#                 if next_elements[0] == 'N2':
#                     # Additional Name Information
#                     provider_info['ProviderNameLine2'] = next_elements[1] if len(next_elements) > 1 else ''
#                     provider_info['ProviderNameLine3'] = next_elements[2] if len(next_elements) > 2 else ''
#                 elif next_elements[0] == 'N3':
#                     provider_info['ProviderAddress'] = next_elements[1] if len(next_elements) > 1 else ''
#                 elif next_elements[0] == 'N4':
#                     provider_info['ProviderCity'] = next_elements[1] if len(next_elements) > 1 else ''
#                     provider_info['ProviderState'] = next_elements[2] if len(next_elements) > 2 else ''
#                     provider_info['ProviderZip'] = next_elements[3] if len(next_elements) > 3 else ''
#                 elif next_elements[0] == 'REF' and len(next_elements) > 2:
#                     qualifier = next_elements[1]
#                     value = next_elements[2]
#                     if qualifier == 'PQ':
#                         provider_info['ProviderSecondaryID'] = value
#                     elif qualifier == 'TJ':
#                         provider_info['ProviderTaxID'] = value
#                 elif next_elements[0] in ['LX', 'CLP']:
#                     break
#
#                 offset += 1
#                 if offset > 10:
#                     break
#
#             break
#
#     return provider_info


def extract_ref_values(ref_list):
    ref_values = {
        'prior_auth': '',
        'claim_number': '',
        'provider_control': '',
        'original_ref': '',
        'referral_number': '',
        'medical_record': '',
        'member_id': '',
        'ssn': '',
        'line_item_control': '',
        'group_number': '',
        'plan_name': '',
        'facility_type': '',
        'authorization_number': '',  
        'plan_type': '',  
        'payer_additional_id': '',
        # Additional REF qualifiers
        'state_medical_assistance_number': '',  # 0B
        'blue_cross_provider_number': '',       # 1A
        'blue_shield_provider_number': '',      # 1B
        'medicare_provider_number': '',         # 1C
        'medicaid_provider_number': '',         # 1D
        'provider_upin_number': '',             # 1G
        'champus_identification_number': '',    # 1H
        # 'facility_id_number': '',               # 1J - Institutional facility ID
        'facility_id_number': '',  # Keep for compatibility but empty for professional claims
        'payers_claim_number': '',              # 1K
        'employee_identification_number': '',    # 28 or A6
        'payer_identification_number': '',      # 2U (already exists)
        'insurance_policy_number': '',          # IG
        'payee_identification': '',             # PQ
        'case_number': '',                      # 9F (was referral_number)
        'hpid': '',                             # HPI
        # Repricing Reference Numbers
        'repriced_claim_ref_number': '',        # 9A - Repriced Claim Reference Number
        'repriced_line_item_ref_number': '',    # 9C - Repriced Line Item Reference Number
        'ambulatory_payment_classification': '',# APC - Ambulatory Payment Classification
        'naic_code': ''                         # NF - NAIC Code
    }

    for ref in ref_list:
        qual = ref.get('qualifier', '')
        val = ref.get('ref_value', '')

        if qual == 'G1':
            ref_values['prior_auth'] = val
        elif qual == 'D9':
            ref_values['claim_number'] = val
        elif qual == '6R':
            ref_values['provider_control'] = val
        elif qual == 'F8':
            ref_values['original_ref'] = val
        elif qual == '9F':
            ref_values['referral_number'] = val
        elif qual == 'EA':
            ref_values['medical_record'] = val
        elif qual == '1W':
            ref_values['member_id'] = val
        elif qual == 'SY':
            ref_values['ssn'] = val
        elif qual == '1L':
            ref_values['group_number'] = val
        elif qual == '6P':
            # 6P is also "Group Number" - use if 1L not already set
            if not ref_values.get('group_number'):
                ref_values['group_number'] = val  
        elif qual == 'CE':
            ref_values['plan_name'] = val
        elif qual == 'LU':
            ref_values['facility_type'] = val
        elif qual == '2U':
            ref_values['payer_additional_id'] = val
        elif qual == 'BB':
            ref_values['authorization_number'] = val
        elif qual == '0B':
            ref_values['state_medical_assistance_number'] = val
        elif qual == '1A':
            ref_values['blue_cross_provider_number'] = val
        elif qual == '1B':
            ref_values['blue_shield_provider_number'] = val
        elif qual == '1C':
            ref_values['medicare_provider_number'] = val
        elif qual == '1D':
            ref_values['medicaid_provider_number'] = val
        elif qual == '1G':
            ref_values['provider_upin_number'] = val
        elif qual == '1H':
            ref_values['champus_identification_number'] = val
        elif qual == '1J':
            # Institutional facility ID - not used for professional claims
            # ref_values['facility_id_number'] = val
            pass
        elif qual == '1K':
            ref_values['payers_claim_number'] = val
        elif qual in ['28', 'A6']:
            ref_values['employee_identification_number'] = val
        elif qual == 'IG':
            ref_values['insurance_policy_number'] = val
        elif qual == 'PQ':
            ref_values['payee_identification'] = val
        elif qual == '9A':
            ref_values['repriced_claim_ref_number'] = val
        elif qual == '9C':
            ref_values['repriced_line_item_ref_number'] = val
        elif qual == 'APC':
            ref_values['ambulatory_payment_classification'] = val
        elif qual == 'NF':
            ref_values['naic_code'] = val
        elif qual == 'HPI':
            ref_values['hpid'] = val

    for ref in ref_list:
        qual = ref.get('qualifier', '')
        val = ref.get('ref_value', '')
        if qual == 'G1' and val in ['PPO', 'HMO', 'EPO', 'POS']:
            ref_values['plan_type'] = val

    return ref_values


def extract_dtm_values(dtm_list):
    """
    Extract date values from DTM segments.
    If multiple DTM segments with same qualifier exist, prefer the FIRST one
    (it's closer to the claim/service in hierarchical order).
    
    FIXED: Properly separate claim-level and service-level dates per X12 spec:
    - DTM*232/233 are CLAIM-level service dates (Loop 2100)
    - DTM*150/151/472 are SERVICE-level dates (Loop 2110)
    
    Args:
        dtm_list: List of DTM segments
    """
    dtm_values = {
        'service_start': '',  # For CLAIM: DTM*232, For SERVICE: DTM*150/472
        'service_end': '',    # For CLAIM: DTM*233, For SERVICE: DTM*151
        'statement_start': '',
        'statement_end': '',
        'received_date': '',
        'expiration_date': '',
        # Additional DTM qualifiers
        'process_date': '',           # 009
        # Institutional-only dates - not used for professional claims
        # 'discharge_date': '',         # 096
        # 'admission_date': ''          # 435
        'discharge_date': '',  # Keep for compatibility but empty for professional claims
        'statement_from_date': '',    # 434
        'admission_date': ''  # Keep for compatibility but empty for professional claims
    }
    dtm_050_count = 0
    dtm_050_segments = []
    for dtm in dtm_list:
        qual = dtm.get('date_qualifier', '')
        val = dtm.get('date_value', '')
        
        # CLAIM-LEVEL DATES (Loop 2100)
        if qual == '232':  # Claim Statement Period Start
            if not dtm_values['service_start']:
                dtm_values['service_start'] = val    
        elif qual == '233':  # Claim Statement Period End
            if not dtm_values['service_end']:
                dtm_values['service_end'] = val
                
        # SERVICE-LEVEL DATES (Loop 2110) - Should only appear in service context
        # but we keep them here for backward compatibility
        elif qual == '150':  # Service Period Start
            if not dtm_values['service_start']:  
                dtm_values['service_start'] = val  
        elif qual == '151':  # Service Period End
            if not dtm_values['service_end']:
                dtm_values['service_end'] = val
        elif qual == '472':  # Service Date
            if not dtm_values['service_start']:
                dtm_values['service_start'] = val
                
        # OTHER CLAIM-LEVEL DATES
        elif qual == '050':
            dtm_050_count += 1
            dtm_050_segments.append(f"DTM*050*{val}")
            if not dtm_values['received_date']:  
                dtm_values['received_date'] = val
        elif qual == '036':
            if not dtm_values['expiration_date']:
                dtm_values['expiration_date'] = val
        elif qual == '009':
            if not dtm_values['process_date']:
                dtm_values['process_date'] = val
        elif qual == '096':
            # Institutional discharge date - not used for professional claims
            # if not dtm_values['discharge_date']:
            #     dtm_values['discharge_date'] = val
            pass
        elif qual == '434':
            if not dtm_values['statement_start']:
                dtm_values['statement_start'] = val
            if not dtm_values['statement_from_date']:
                dtm_values['statement_from_date'] = val
        elif qual == '435':
            if not dtm_values['statement_end']:
                dtm_values['statement_end'] = val
            # Institutional admission date - not used for professional claims
            # if not dtm_values['admission_date']:
            #     dtm_values['admission_date'] = val
    return dtm_values

def identify_payer(segments, element_delimiter):
    payer_info = {
        'payer_id': '',
        'payer_name': '',
        'trn03': '',  # Originating Company ID for colloquial payer identification
        'isa06': ''   # Interchange Sender ID for batch senders (e.g., eMedNY)
    }
    for seg in segments:
        elements = seg.split(element_delimiter)
        
        # Extract ISA06 (Interchange Sender ID) for batch sender identification
        if elements[0] == 'ISA' and len(elements) > 6:
            payer_info['isa06'] = elements[6].strip()
        
        elif elements[0] == 'REF' and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2] if len(elements) > 2 else ''

            if qualifier == '2U':
                payer_info['payer_id'] = value

        elif elements[0] == 'TRN' and len(elements) > 3:
            # Extract TRN03 (Originating Company Identifier) for colloquial payer identification
            payer_info['trn03'] = elements[3]

        elif elements[0] == 'N1' and len(elements) > 1 and elements[1] == 'PR':
            if len(elements) > 2:
                payer_info['payer_name'] = elements[2]

            if len(elements) > 4:
                id_qualifier = elements[3]
                id_code = elements[4]

                if id_qualifier in ['PI', 'XV', 'FI']:
                    if not payer_info['payer_id']:
                        payer_info['payer_id'] = id_code

        elif elements[0] == 'NM1' and len(elements) > 1 and elements[1] == 'PR':
            if len(elements) > 3:  # Changed from > 2 to get NM103
                payer_info['payer_name'] = elements[3]  # NM103 - Name Last or Organization Name
            if len(elements) > 9:
                if not payer_info['payer_id']:
                    payer_info['payer_id'] = elements[9]
    
    # Identify colloquial payer based on TRN03, ISA06, or payer name
    payer_info['colloquial_payer_key'] = colloquial.identify_payer(
        trn03=payer_info.get('trn03'),
        payer_name=payer_info.get('payer_name'),
        isa06=payer_info.get('isa06')
    )
    
    return payer_info


def get_payer_summary(payer_info):
    """Format payer information for logging"""
    parts = []
    if payer_info.get('payer_name'):
        parts.append(payer_info['payer_name'])
    if payer_info.get('payer_id'):
        parts.append(f"ID: {payer_info['payer_id']}")
    return ' | '.join(parts) if parts else 'Unknown Payer'


def parse_plb(elements, component_delimiter=':'):
    """
    Parse PLB (Provider Level Adjustment) segment per X12 spec section 1.10.2.1.3.
    
    Per X12 spec: PLB positive amounts DECREASE payment, negative amounts INCREASE payment.
    PLB adjustments are applied at the transaction level (not claim or service level).
    
    Returns dict with provider_identifier, fiscal_period_date, and list of adjustments.
    Each adjustment has: reason_code, reference_id, amount, is_refund_ack (initially False)
    """
    plb = {
        'provider_identifier': elements[1] if len(elements) > 1 else '',
        'fiscal_period_date': elements[2] if len(elements) > 2 else '',
        'adjustments': []
    }

    # PLB can have up to 6 adjustment pairs (elements 3-14)
    # Per X12 spec: PLB03-04, PLB05-06, PLB07-08, PLB09-10, PLB11-12, PLB13-14
    for i in range(3, min(len(elements), 15), 2):
        if i < len(elements) and elements[i]:
            # Parse composite adjustment identifier (C042)
            # C042-01: Reason code, C042-02: Reference ID
            adj_parts = elements[i].split(component_delimiter)
            adjustment = {
                'reason_code': adj_parts[0] if len(adj_parts) > 0 else '',
                'reference_id': adj_parts[1] if len(adj_parts) > 1 else '',
                'amount': elements[i+1] if i+1 < len(elements) else '0',
                'is_refund_ack': False  # Will be set by detect_refund_acknowledgments()
            }
            plb['adjustments'].append(adjustment)

    return plb


def detect_refund_acknowledgments(plb_list):
    """
    Detect WO/72 refund acknowledgment pairs across all PLB segments.
    Per healthcare guidance: WO (Overpayment Recovery) + 72 (Authorized Return)
    that sum to ~$0 indicate refund acknowledgments.

    Per X12 RFI #2704: WO and 72 can be in same or different PLB segments.
    """
    # Collect all WO and 72 adjustments
    wo_adjustments = []
    adj_72_adjustments = []

    for plb in plb_list:
        for adj in plb.get('adjustments', []):
            reason_code = adj.get('reason_code', '')
            if reason_code == 'WO':
                wo_adjustments.append(adj)
            elif reason_code == '72':
                adj_72_adjustments.append(adj)

    # Match WO and 72 pairs that offset (sum to ~$0)
    for wo_adj in wo_adjustments:
        try:
            wo_amount = float(wo_adj.get('amount', 0))
            for adj_72 in adj_72_adjustments:
                adj_72_amount = float(adj_72.get('amount', 0))
                # Check if amounts offset (WO positive, 72 negative, sum ~= 0)
                if abs(wo_amount + adj_72_amount) < 0.01:
                    wo_adj['is_refund_ack'] = True
                    adj_72['is_refund_ack'] = True
                    break  # Match found for this WO
        except (ValueError, TypeError):
            pass


def detect_and_classify_835_type(segments, element_delimiter):
    """
    Detects whether 835 is standard remittance or 835S pended claims report.
    835S (Pended Claims Report) - eMedNY specific:
    - Same structure as 835 but different transaction code
    - Used for claims pended for additional review
    - Common in NY Medicaid
    Returns:
        dict with is_pended_report boolean and supporting metadata
    """
    result = {
        'is_pended_report': False,
        'has_st_header': False,
        'transaction_code': '',
        'transaction_control_number': '',
        'implementation_convention_ref': '',
        'pended_indicators': []
    }
    for seg in segments:
        elements = seg.split(element_delimiter)
        if elements[0] == 'ST' and len(elements) > 1:
            transaction_code = elements[1]  
            result['transaction_code'] = transaction_code
            result['has_st_header'] = True
            if len(elements) > 2:
                result['transaction_control_number'] = elements[2]  
            if len(elements) > 3:
                result['implementation_convention_ref'] = elements[3]  
            if transaction_code == '835':
                for check_seg in segments:
                    check_elements = check_seg.split(element_delimiter)
                    if check_elements[0] == 'CLP' and len(check_elements) > 2:
                        status = check_elements[2]
                        if status in ['5', '13', '14', '15', '16', '17', '18']:  
                            result['pended_indicators'].append(f"CLP status {status}")
                if len(result['pended_indicators']) > 0:
                    result['is_pended_report'] = True
            break
    return result


def convert_segments_to_rows(segments, element_delimiter, file_name, component_delimiter=None):
    """Convert parsed segments into structured rows for CSV export with comprehensive fields
    Args:
        segments: List of EDI segments
        element_delimiter: Element delimiter character
        file_name: Name of the file being processed
        component_delimiter: Component delimiter character (default None, will extract from ISA16)
    """
    rows = []
    logger = None  
    
    # Track claim occurrences - same claim number can appear multiple times
    # (reversal, correction, original payment, etc.)
    claim_occurrence_tracker = {}
    auto_claim_counter = 0
    
    # Initialize field tracker for validation
    field_tracker = EDIFieldTracker()
    
    # Parse ISA and GS segments first - they're envelope level data
    isa_data = {}
    gs_data = {}
    for seg in segments:
        elements = seg.split(element_delimiter)
        if elements[0] == 'ISA':
            isa_data = parse_isa(elements)
            # Extract component delimiter from ISA16 if not provided
            if component_delimiter is None:
                component_delimiter = isa_data.get('component_element_separator', ':')
                if not component_delimiter:
                    component_delimiter = ':'  # Default fallback
        elif elements[0] == 'GS':
            gs_data = parse_gs(elements)
            # Stop after finding GS as we've got the envelope data
            break
    
    # Ensure component_delimiter is set (fallback to ':' if not extracted from ISA)
    if component_delimiter is None:
        component_delimiter = ':'
    
    payer_info = identify_payer(segments, element_delimiter)
    # Extract payer key for colloquial overrides (Medi-Cal, eMedNY, etc.)
    payer_key = payer_info.get('colloquial_payer_key')
    file_metadata = detect_and_classify_835_type(segments, element_delimiter)
    is_pended_report = file_metadata['is_pended_report']
    transaction_control_number = file_metadata.get('transaction_control_number', '')
    implementation_convention_ref = file_metadata.get('implementation_convention_ref', '')
    file_naming_metadata = {}

    # Fix Issue 6: Separate header payer (N1*PR Loop 1000A) from claim payer (NM1*PR Loop 2100)
    header_payer_name = ''  # N1*PR - Payer issuing this 835 remittance
    header_payer_id = ''
    payer_address = ''
    payer_address2 = ''  # N302
    payer_city = ''
    payer_state = ''
    payer_zip = ''
    payer_country_code = ''  # N404
    payer_location_qualifier = ''  # N405
    payer_location_id = ''  # N406
    payer_country_subdivision = ''  # N407
    payment_method_code = ''
    payment_method = ''
    payment_format_code = ''
    payment_format = ''
    check_number = ''
    check_date = ''
    check_amount = ''
    trace_number = ''
    trace_type = ''
    originating_company_id = ''
    originating_company_id_trn = ''
    reference_id_secondary = ''
    receiver_id = ''
    check_eft_effective_date = ''
    plb_adjustments = []
    plb_refund_acks_detected = False  # Flag to ensure detection runs once per transaction
    transaction_handling = ''
    credit_debit_flag = ''
    payer_dfi_qualifier = ''
    payer_dfi_id = ''
    payer_account_qualifier = ''
    payer_account_number = ''
    originating_company_supplemental = ''
    payee_dfi_qualifier = ''
    payee_dfi_id = ''
    payee_account_qualifier = ''
    payee_account_number = ''
    business_function_code = ''
    dfi_id_qualifier_3 = ''
    dfi_id_3 = ''
    account_qualifier_3 = ''
    account_number_3 = ''
    contact_bl_function = ''
    contact_bl_name = ''
    contact_bl_comm1_qualifier = ''
    contact_bl_comm1_number = ''
    contact_bl_comm2_qualifier = ''
    contact_bl_comm2_number = ''
    contact_cx_function = ''
    contact_cx_name = ''
    contact_cx_comm1_qualifier = ''
    contact_cx_comm1_number = ''
    contact_cx_comm2_qualifier = ''
    contact_cx_comm2_number = ''
    contact_ic_function = ''
    contact_ic_name = ''
    contact_ic_comm1_qualifier = ''
    contact_ic_comm1_number = ''
    contact_ic_comm2_qualifier = ''
    contact_ic_comm2_number = ''

    payer_entity_id_code = ''  # N101 - Entity Identifier Code (PR)
    payer_id_qualifier = ''
    payer_id_code = ''
    payer_entity_relationship = ''
    payer_entity_relationship_desc = ''
    payer_entity_id_secondary = ''
    payer_additional_id = ''
    business_function_desc = ''
    current_transaction_st_number = ''
    current_transaction_bpr_data = {}
    current_transaction_dtm_data = {}
    transactions = []
    
    # CUR segment data (header level)
    cur_data = {}
    
    # RDM and N2 segment data for L1000A (Payer) and L1000B (Payee)
    payer_rdm_data = {}
    payer_n2_data = {}
    payee_rdm_data = {}
    payee_n2_data = {}
    
    # Track current loop context
    current_loop = None  # Will be 'L1000A' for payer, 'L1000B' for payee  
    def init_transaction():
        return {
            'st_control_number': '',
            'implementation_convention_ref': '',
            'bpr_payment_method': '',
            'bpr_payment_amount': '',
            'bpr_credit_debit_flag': '',
            'bpr_payment_format': '',
            'bpr_check_number': '',
            'bpr_effective_date': '',  
            'bpr_transaction_handling': '',
            'bpr_dfi_qualifiers': {},
            'dtm_production_date': '',  
            'dtm_check_date': '',
            'trace_number': '',
            'trace_type': '',
            'originating_company_id_trn': '',
            'receiver_id': '',
            'payer_name': '',
            'payer_id': '',
            'payer_address_info': {},
            'provider_info': {},
            'contact_bl_info': {},
            'contact_cx_info': {},
            'payer_additional_id': '',
            'claims': []
        }
    current_transaction = init_transaction()
    current_lx_header = None
    current_ts3 = None
    current_ts2 = None

    # Fix: Provider Scope - Removed file-level extraction
    # provider_info will be extracted per-transaction in the ST segment
    provider_info = {}  # Initialize empty, will be populated per transaction

    payer_id = payer_info.get('payer_id', '')
    payer_name_extracted = payer_info.get('payer_name', '')

    current_claim = None
    for idx, seg in enumerate(segments):
        elements = seg.split(element_delimiter)

        if elements[0] == 'CLP':
            break
        if elements[0] == 'GS' and len(elements) > 8:
            gs_implementation_convention = elements[8]  
            if not implementation_convention_ref:  
                implementation_convention_ref = gs_implementation_convention


        # N1*PR handling moved to main loop (line 1694) to avoid being reset at ST segment

        # Note: NM1*PR in header section (before claims) is non-standard
        # Standard 835 uses N1*PR in Loop 1000A, not NM1*PR
        # If found here, treat as legacy/alternate header payer format
        if elements[0] == 'NM1' and len(elements) > 1 and elements[1] == 'PR' and not current_claim:
            if len(elements) > 3 and not header_payer_name:
                header_payer_name = elements[3]  # NM103 - Use if N1*PR not present  


        if elements[0] == 'TS3':
            current_ts3 = parse_ts3(elements)

        elif elements[0] == 'TS2':
            current_ts2 = parse_ts2(elements)
        if elements[0] == 'PLB':
            plb_entry = safe_parse_segment(
                seg, element_delimiter,
                lambda els: parse_plb(els, component_delimiter),
                {}
            )
            if plb_entry:
                plb_adjustments.append(plb_entry)

    current_service = None
    current_claim_services = []
    svc_encountered_for_claim = False  # Track if ANY SVC seen for current claim
    # Institutional-only patient condition/discharge status - not used for professional claims
    # patient_condition_code = ''
    # Institutional-only DRG variables - not used for professional claims
    # drg_code = ''
    # drg_weight = ''
    # discharge_fraction = ''

    for seg_idx, seg in enumerate(segments):
        elements = seg.split(element_delimiter)
        seg_id = elements[0] if elements else ''
        if seg_id == 'ST' and len(elements) > 1:
            transaction_type = elements[1]
            if transaction_type == '835':
                if current_transaction is not None and current_transaction.get('claims'):
                    transactions.append(current_transaction.copy())
                current_transaction = init_transaction()
                if len(elements) > 2:
                    current_transaction['st_control_number'] = elements[2]
                else:
                    current_transaction['st_control_number'] = ''
                transaction_control_number = elements[2] if len(elements) > 2 else ''
                current_claim = None
                current_service = None
                svc_encountered_for_claim = False  # Reset for new transaction
                # Fix Issue 8: Reset PLB list for each transaction to prevent cross-transaction mixing
                plb_adjustments = []  # Each ST/SE transaction has its own PLB segments
                plb_refund_acks_detected = False  # Reset for new transaction
                # Fix: PLB Data Scope - Buffer rows until PLB segments are parsed
                transaction_rows_buffer = []  # Buffer rows for this transaction
                header_payer_name = ''  # Reset payer for new transaction
                # Fix: Provider Scope - Reset provider for each transaction
                provider_info = {}  # Each ST/SE transaction has its own provider
                payer_address = ''
                payer_address2 = ''
                payer_city = ''
                payer_state = ''
                payer_zip = ''
                payer_country_code = ''
                payer_location_qualifier = ''
                payer_location_id = ''
                payer_country_subdivision = ''
                payer_entity_id_code = ''  # N101 - Entity Identifier Code (PR)
                payer_id_qualifier = ''
                payer_id_code = ''
                payer_entity_relationship = ''
                payer_entity_relationship_desc = ''
                payer_entity_id_secondary = ''

        elif seg_id == 'BPR':
            current_transaction['bpr_transaction_handling'] = elements[1] if len(elements) > 1 else ''
            current_transaction['bpr_payment_amount'] = elements[2] if len(elements) > 2 else ''
            current_transaction['bpr_credit_debit_flag'] = elements[3] if len(elements) > 3 else ''
            current_transaction['bpr_payment_method'] = elements[4] if len(elements) > 4 else ''
            current_transaction['bpr_payment_format'] = elements[5] if len(elements) > 5 else ''
            current_transaction['bpr_effective_date'] = elements[16] if len(elements) > 16 else ''  
            if len(elements) > 1:
                transaction_handling = get_bpr_transaction_handling_description(elements[1])
            if len(elements) > 2:
                check_amount = elements[2]
            if len(elements) > 3:
                credit_debit_flag = elements[3]  
            if len(elements) > 4:
                payment_method_code = elements[4]  
                payment_method = get_payment_method_description(elements[4])  
            if len(elements) > 5:
                payment_format_code = elements[5]  
                payment_format = get_payment_format_description(elements[5])  
            if len(elements) > 6:
                payer_dfi_qualifier = elements[6]  # Store raw value, not description
            if len(elements) > 7:
                payer_dfi_id = elements[7]  
            if len(elements) > 8:
                payer_account_qualifier = elements[8]  # Store raw value, not description  
            if len(elements) > 9:
                payer_account_number = elements[9]  
            if len(elements) > 10:
                originating_company_id = elements[10]
            if len(elements) > 11:
                originating_company_supplemental = elements[11]  
            if len(elements) > 12:
                payee_dfi_qualifier = elements[12]  # Store raw value, not description
            if len(elements) > 13:
                payee_dfi_id = elements[13]
            if len(elements) > 14:
                payee_account_qualifier = elements[14]  # Store raw value, not description
            if len(elements) > 15:
                payee_account_number = elements[15]
            if len(elements) > 16:
                check_eft_effective_date = elements[16]
            if len(elements) > 17:
                business_function_code = elements[17]  
                business_function_desc = get_business_function_code_description(elements[17])
            if len(elements) > 18:
                dfi_id_qualifier_3 = elements[18]  # Store raw value, not description
            if len(elements) > 19:
                dfi_id_3 = elements[19]  
            if len(elements) > 20:
                account_qualifier_3 = elements[20]  # Store raw value, not description
            if len(elements) > 21:
                account_number_3 = elements[21]

        elif seg_id == 'CUR':
            # Parse CUR segment at header level
            cur_data = parse_cur(elements)
            
        elif seg_id == 'TRN':
            current_transaction['trace_number'] = elements[2] if len(elements) > 2 else ''
            current_transaction['trace_type'] = elements[1] if len(elements) > 1 else ''
            current_transaction['originating_company_id_trn'] = elements[3] if len(elements) > 3 else ''
            if len(elements) > 1:
                trace_type = elements[1]  
            if len(elements) > 2:
                trace_number = elements[2]  
            if len(elements) > 3:
                originating_company_id_trn = elements[3]  
            if len(elements) > 4:
                reference_id_secondary = elements[4]  

        elif seg_id == 'CUR' and not current_claim:
            # Currency segment (for international claims)
            current_transaction['currency'] = parse_cur(elements)
            
        elif seg_id == 'RDM' and not current_claim:
            # Remittance Delivery Method
            current_transaction['remittance_delivery'] = parse_rdm(elements)
        
        elif seg_id == 'REF' and len(elements) > 2 and not current_claim and current_loop != 'L1000B':
            # Handle header-level REF segments (not in provider loop)
            qualifier = elements[1]
            if qualifier == 'EV':
                current_transaction['receiver_id'] = elements[2]
                receiver_id = elements[2]
            elif qualifier == '2U':
                current_transaction['payer_additional_id'] = elements[2]
                payer_additional_id = elements[2]  

        elif seg_id == 'PER' and not current_claim:
            contact_function_code = elements[1] if len(elements) > 1 else ''
            if contact_function_code == 'BL':
                current_transaction['contact_bl_info'] = {
                    'function': elements[1] if len(elements) > 1 else '',
                    'name': elements[2] if len(elements) > 2 else '',
                    'comm1_qualifier': elements[3] if len(elements) > 3 else '',
                    'comm1_number': elements[4] if len(elements) > 4 else '',
                    'comm2_qualifier': elements[5] if len(elements) > 5 else '',
                    'comm2_number': elements[6] if len(elements) > 6 else '',
                }
                contact_bl_function = current_transaction['contact_bl_info']['function']
                contact_bl_name = current_transaction['contact_bl_info']['name']
                contact_bl_comm1_qualifier = current_transaction['contact_bl_info']['comm1_qualifier']
                contact_bl_comm1_number = current_transaction['contact_bl_info']['comm1_number']
                contact_bl_comm2_qualifier = current_transaction['contact_bl_info']['comm2_qualifier']
                contact_bl_comm2_number = current_transaction['contact_bl_info']['comm2_number']
            elif contact_function_code == 'CX':
                current_transaction['contact_cx_info'] = {
                    'function': elements[1] if len(elements) > 1 else '',
                    'name': elements[2] if len(elements) > 2 else '',
                    'comm1_qualifier': elements[3] if len(elements) > 3 else '',
                    'comm1_number': elements[4] if len(elements) > 4 else '',
                    'comm2_qualifier': elements[5] if len(elements) > 5 else '',
                    'comm2_number': elements[6] if len(elements) > 6 else '',
                }
                contact_cx_function = current_transaction['contact_cx_info']['function']
                contact_cx_name = current_transaction['contact_cx_info']['name']
                contact_cx_comm1_qualifier = current_transaction['contact_cx_info']['comm1_qualifier']
                contact_cx_comm1_number = current_transaction['contact_cx_info']['comm1_number']
                contact_cx_comm2_qualifier = current_transaction['contact_cx_info']['comm2_qualifier']
                contact_cx_comm2_number = current_transaction['contact_cx_info']['comm2_number']
            elif contact_function_code == 'IC':
                current_transaction['contact_ic_info'] = {
                    'function': elements[1] if len(elements) > 1 else '',
                    'name': elements[2] if len(elements) > 2 else '',
                    'comm1_qualifier': elements[3] if len(elements) > 3 else '',
                    'comm1_number': elements[4] if len(elements) > 4 else '',
                    'comm2_qualifier': elements[5] if len(elements) > 5 else '',
                    'comm2_number': elements[6] if len(elements) > 6 else '',
                }
                contact_ic_function = current_transaction['contact_ic_info']['function']
                contact_ic_name = current_transaction['contact_ic_info']['name']
                contact_ic_comm1_qualifier = current_transaction['contact_ic_info']['comm1_qualifier']
                contact_ic_comm1_number = current_transaction['contact_ic_info']['comm1_number']
                contact_ic_comm2_qualifier = current_transaction['contact_ic_info']['comm2_qualifier']
                contact_ic_comm2_number = current_transaction['contact_ic_info']['comm2_number']

        elif seg_id == 'DTM':
            dtm_qual = elements[1] if len(elements) > 1 else ''
            dtm_date = elements[2] if len(elements) > 2 else ''
            if not current_claim and not current_service:
                if dtm_qual == '405':
                    current_transaction['dtm_production_date'] = dtm_date
                    check_date = dtm_date
            elif current_claim and not current_service:
                dtm_data = parse_dtm(elements, payer_key=payer_key)
                current_claim['_dtm'].append(dtm_data)
            elif current_service and current_claim:
                current_service['_dtm'].append(parse_dtm(elements, payer_key=payer_key))

        elif seg_id == 'LX':
            current_lx_header = parse_lx(elements)

        elif seg_id == 'CLP':
            # Reset loop context - entering claim loop (L2100)
            current_loop = None
            # PLB refund detection moved to SE segment (after PLBs are parsed)

            if current_claim:
                # FIX: Process any pending service FIRST before checking if claim is service-less
                if current_service:
                    current_claim_services.append(current_service)
                    row = create_output_row(current_claim, current_service,
                                           header_payer_name, payer_address, payer_address2, payer_city, payer_state, payer_zip,
                                           payer_country_code, payer_location_qualifier, payer_location_id, payer_country_subdivision,
                                           provider_info, payment_method_code, payment_format_code,
                                           check_number, check_date, check_amount, trace_number, trace_type,
                                           plb_adjustments, is_pended_report,
                                           originating_company_id_trn, reference_id_secondary, receiver_id,
                                           transaction_handling, credit_debit_flag, payer_dfi_qualifier, payer_dfi_id,
                                           payer_account_qualifier, payer_account_number, originating_company_id,
                                           originating_company_supplemental, payee_dfi_qualifier, payee_dfi_id,
                                           payee_account_qualifier, payee_account_number, check_eft_effective_date,
                                           business_function_code, business_function_desc, dfi_id_qualifier_3, dfi_id_3,
                                           account_qualifier_3, account_number_3, transaction_control_number,
                                           implementation_convention_ref, payer_entity_id_code, payer_id_qualifier, payer_id_code,
                                           payer_entity_relationship, payer_entity_relationship_desc, payer_entity_id_secondary,
                                           payer_additional_id, contact_bl_function, contact_bl_name, contact_bl_comm1_qualifier, contact_bl_comm1_number,
                                           contact_bl_comm2_qualifier, contact_bl_comm2_number,
                                           contact_cx_function, contact_cx_name, contact_cx_comm1_qualifier, contact_cx_comm1_number,
                                           contact_cx_comm2_qualifier, contact_cx_comm2_number,
                                           contact_ic_function, contact_ic_name, contact_ic_comm1_qualifier, contact_ic_comm1_number,
                                           contact_ic_comm2_qualifier, contact_ic_comm2_number, file_name, current_transaction,
                                           isa_data, gs_data, cur_data, payer_rdm_data, payer_n2_data,
                                           payee_rdm_data, payee_n2_data, payer_key=payer_key)
                    transaction_rows_buffer.append(row)
                    current_service = None  # Clear after processing
                
                # NOW check if previous claim has no services (after processing pending service)
                if not svc_encountered_for_claim:
                    # This is a truly service-less claim (claim-level denial, capitation, etc.)
                    row = create_output_row(current_claim, None,
                                           header_payer_name, payer_address, payer_address2, payer_city, payer_state, payer_zip,
                                           payer_country_code, payer_location_qualifier, payer_location_id, payer_country_subdivision,
                                           provider_info, payment_method_code, payment_format_code,
                                           check_number, check_date, check_amount, trace_number, trace_type,
                                           plb_adjustments, is_pended_report,
                                           originating_company_id_trn, reference_id_secondary, receiver_id,
                                           transaction_handling, credit_debit_flag, payer_dfi_qualifier, payer_dfi_id,
                                           payer_account_qualifier, payer_account_number, originating_company_id,
                                           originating_company_supplemental, payee_dfi_qualifier, payee_dfi_id,
                                           payee_account_qualifier, payee_account_number, check_eft_effective_date,
                                           business_function_code, business_function_desc, dfi_id_qualifier_3, dfi_id_3,
                                           account_qualifier_3, account_number_3, transaction_control_number,
                                           implementation_convention_ref, payer_entity_id_code, payer_id_qualifier, payer_id_code,
                                           payer_entity_relationship, payer_entity_relationship_desc, payer_entity_id_secondary,
                                           payer_additional_id, contact_bl_function, contact_bl_name, contact_bl_comm1_qualifier, contact_bl_comm1_number,
                                           contact_bl_comm2_qualifier, contact_bl_comm2_number,
                                           contact_cx_function, contact_cx_name, contact_cx_comm1_qualifier, contact_cx_comm1_number,
                                           contact_cx_comm2_qualifier, contact_cx_comm2_number,
                                           contact_ic_function, contact_ic_name, contact_ic_comm1_qualifier, contact_ic_comm1_number,
                                           contact_ic_comm2_qualifier, contact_ic_comm2_number, file_name, current_transaction,
                                           isa_data, gs_data, cur_data, payer_rdm_data, payer_n2_data,
                                           payee_rdm_data, payee_n2_data, payer_key=payer_key)
                    transaction_rows_buffer.append(row)
                
                # Add completed claim to transaction
                current_transaction['claims'].append(current_claim.copy())

            current_service = None
            current_claim_services = []
            svc_encountered_for_claim = False  # Reset for new claim

            # Extract claim number - if empty, use a descriptive identifier
            claim_number = elements[1] if len(elements) > 1 else ''
            if not claim_number:
                auto_claim_counter += 1
                claim_number = f'EMPTY_CLAIM_{auto_claim_counter}'
            
            # Track claim occurrence (same claim can appear multiple times: reversal, correction, etc.)
            if claim_number not in claim_occurrence_tracker:
                claim_occurrence_tracker[claim_number] = 0
            claim_occurrence_tracker[claim_number] += 1
            claim_occurrence = claim_occurrence_tracker[claim_number]
            
            current_claim = {
                'claim_number': claim_number,
                'claim_occurrence': claim_occurrence,
                'claim_status': elements[2] if len(elements) > 2 else '',
                'claim_status_desc': get_claim_status_description(elements[2]) if len(elements) > 2 else '',
                'total_charged': elements[3] if len(elements) > 3 else '',
                'total_paid': elements[4] if len(elements) > 4 else '',
                'patient_responsibility': elements[5] if len(elements) > 5 else '',
                'filing_indicator': elements[6] if len(elements) > 6 else '',
                'filing_indicator_desc': get_claim_filing_indicator_description(elements[6]) if len(elements) > 6 else '',
                'payer_claim_control': elements[7] if len(elements) > 7 else '',
                # Facility Type Code (CLP*08) - used by institutional and some professional claims
                'facility_type_code': elements[8] if len(elements) > 8 else '',
                'claim_frequency_code': elements[9] if len(elements) > 9 else '',
                'claim_frequency_desc': get_claim_frequency_description(elements[9]) if len(elements) > 9 else '',
                # Patient condition/discharge status (CLP*10)
                'patient_condition_code': elements[10] if len(elements) > 10 else '',
                'patient_condition_desc': get_discharge_status_description(elements[10]) if len(elements) > 10 else '',
                # DRG fields (CLP*11-13)
                'drg_code': elements[11] if len(elements) > 11 else '',
                'drg_weight': elements[12] if len(elements) > 12 else '',
                'discharge_fraction': elements[13] if len(elements) > 13 else '',
                'yes_no_condition': elements[14] if len(elements) > 14 else '',  
                'yes_no_condition_desc': get_yes_no_condition_description(elements[14]) if len(elements) > 14 else '',
                # Foreign currency exchange rate - not used for US professional claims
                # 'exchange_rate': elements[15] if len(elements) > 15 else '',  
                'exchange_rate': '',
                'payment_typology': elements[16] if len(elements) > 16 else '',
                'payment_typology_desc': get_payment_typology_description(elements[16]) if len(elements) > 16 else '',
                '_cas': [],
                '_ref': [],
                '_dtm': [],
                '_nm1': [],
                '_lq': [],
                '_amt': [],
                '_qty': [],
                '_moa': [],
                '_mia': [],  
                '_per': [],  
                '_service_provider': {},  
                'lx_header': current_lx_header,
                'ts3': current_ts3,
                'ts2': current_ts2,
                '_transaction_st_number': current_transaction['st_control_number'],
                '_transaction_bpr_effective_date': current_transaction['bpr_effective_date'],
                '_transaction_dtm_production_date': current_transaction['dtm_production_date'],
                '_transaction_trace_number': current_transaction['trace_number'],
                '_transaction_payer_additional_id': current_transaction['payer_additional_id'],
                'source_file': file_name,  # Track which file this claim came from
            }

        elif seg_id == 'NM1' and current_claim:
            nm1_data = parse_nm1(elements, payer_key=payer_key)
            entity_code = nm1_data.get('entity_id_code', '')

            # Fix Issue 6: Capture claim-specific payer (NM1*PR in Loop 2100)
            # This represents the payer responsible for THIS claim (may differ from header payer)
            if entity_code == 'PR':
                current_claim['claim_payer_name'] = nm1_data.get('last_name', '')  # NM103
                current_claim['claim_payer_id'] = nm1_data.get('id_code', '')    # NM109
                current_claim['claim_payer_id_qualifier'] = nm1_data.get('id_code_qualifier', '')  # NM108

            # Capture Ambulance Pickup Location (PW)
            if entity_code == 'PW':
                current_claim['_ambulance_pickup'] = nm1_data
                # Look ahead for N3/N4 segments (address information)
                try:
                    # Use seg_idx from enumerate() - NOT segments.index(seg) which returns first occurrence
                    # Check next few segments for N3 and N4
                    for i in range(seg_idx + 1, min(seg_idx + 4, len(segments))):
                        next_seg = segments[i].split(element_delimiter)
                        next_seg_id = next_seg[0] if next_seg else ''
                        
                        if next_seg_id == 'N3':
                            current_claim['_ambulance_pickup_address'] = parse_n3(next_seg)
                        elif next_seg_id == 'N4':
                            n4_data = parse_n4(next_seg)
                            current_claim['_ambulance_pickup_city'] = n4_data.get('city', '')
                            current_claim['_ambulance_pickup_state'] = n4_data.get('state', '')
                            current_claim['_ambulance_pickup_zip'] = n4_data.get('postal_code', '')
                        elif next_seg_id in ['NM1', 'CLP', 'SE']:
                            break  # Stop if we hit another major segment
                except (ValueError, IndexError):
                    pass
                    
            # Capture Ambulance Drop-off Location (45)
            elif entity_code == '45':
                current_claim['_ambulance_dropoff'] = nm1_data
                # Look ahead for N3/N4 segments (address information)
                try:
                    # Use seg_idx from enumerate() - NOT segments.index(seg) which returns first occurrence
                    # Check next few segments for N3 and N4
                    for i in range(seg_idx + 1, min(seg_idx + 4, len(segments))):
                        next_seg = segments[i].split(element_delimiter)
                        next_seg_id = next_seg[0] if next_seg else ''
                        
                        if next_seg_id == 'N3':
                            current_claim['_ambulance_dropoff_address'] = parse_n3(next_seg)
                        elif next_seg_id == 'N4':
                            n4_data = parse_n4(next_seg)
                            current_claim['_ambulance_dropoff_city'] = n4_data.get('city', '')
                            current_claim['_ambulance_dropoff_state'] = n4_data.get('state', '')
                            current_claim['_ambulance_dropoff_zip'] = n4_data.get('postal_code', '')
                        elif next_seg_id in ['NM1', 'CLP', 'SE']:
                            break  # Stop if we hit another major segment
                except (ValueError, IndexError):
                    pass
                    
            elif entity_code == '82':
                current_claim['_service_provider'] = {
                    'entity_code': nm1_data.get('entity_id_code', ''),
                    'name': nm1_data.get('last_name', ''),
                    'id_qualifier': nm1_data.get('id_code_qualifier', ''),
                    'id_code': nm1_data.get('id_code', ''),
                    'entity_relationship': nm1_data.get('entity_relationship', ''),
                    'entity_relationship_desc': nm1_data.get('entity_relationship_desc', ''),
                    'entity_id_secondary': nm1_data.get('entity_id_code_secondary', '')
                }
            current_claim['_nm1'].append(nm1_data)

        elif seg_id == 'REF' and current_claim and not current_service:
            current_claim['_ref'].append(parse_ref(elements, payer_key=payer_key))

        elif seg_id == 'CAS' and current_claim and not current_service:
            claim_status = current_claim.get('claim_status')
            cas_data = safe_parse_segment(seg, element_delimiter,
                                         lambda els: parse_cas(els, 'CLAIM', claim_status, payer_key=payer_key),
                                         [])
            if cas_data:
                current_claim['_cas'].extend(cas_data)

        elif seg_id == 'AMT' and current_claim and not current_service:
            current_claim['_amt'].append(parse_amt(elements))

        elif seg_id == 'QTY' and current_claim and not current_service:
            current_claim['_qty'].append(parse_qty(elements))
        
        # Remove duplicate - already handled earlier
        
        elif seg_id == 'N1' and len(elements) > 1 and elements[1] == 'PR' and not current_claim:
            current_loop = 'L1000A'  # We're in the payer loop
            payer_entity_id_code = elements[1]  # N101 = "PR"
            if len(elements) > 2:
                header_payer_name = elements[2]  # Use header_payer_name for output
            if len(elements) > 3:
                payer_id_qualifier = elements[3]  
            if len(elements) > 4:
                payer_id_code = elements[4]  
            if len(elements) > 5:
                payer_entity_relationship = elements[5]  
                payer_entity_relationship_desc = get_entity_relationship_description(elements[5])
            if len(elements) > 6:
                payer_entity_id_secondary = elements[6]  
            # Use seg_idx from enumerate() - NOT segments.index(seg) which returns first occurrence
            if seg_idx + 1 < len(segments):
                next_seg = segments[seg_idx + 1]
                next_elements = next_seg.split(element_delimiter)
                if next_elements[0] == 'N3':
                    payer_address = next_elements[1] if len(next_elements) > 1 else ''
                    payer_address2 = next_elements[2] if len(next_elements) > 2 else ''
            if seg_idx + 2 < len(segments):
                next_seg2 = segments[seg_idx + 2]
                next_elements2 = next_seg2.split(element_delimiter)
                if next_elements2[0] == 'N4':
                    payer_city = next_elements2[1] if len(next_elements2) > 1 else ''
                    payer_state = next_elements2[2] if len(next_elements2) > 2 else ''
                    payer_zip = next_elements2[3] if len(next_elements2) > 3 else ''
                    payer_country_code = next_elements2[4] if len(next_elements2) > 4 else ''
                    payer_location_qualifier = next_elements2[5] if len(next_elements2) > 5 else ''
                    payer_location_id = next_elements2[6] if len(next_elements2) > 6 else ''
                    payer_country_subdivision = next_elements2[7] if len(next_elements2) > 7 else ''

        elif seg_id == 'RDM':
            # Parse RDM based on current loop context
            if current_loop == 'L1000A':
                payer_rdm_data = parse_rdm(elements)
            elif current_loop == 'L1000B':
                payee_rdm_data = parse_rdm(elements)
        
        elif seg_id == 'N2':
            # Parse N2 based on current loop context
            if current_loop == 'L1000A':
                payer_n2_data = parse_n2(elements)
            elif current_loop == 'L1000B':
                payee_n2_data = parse_n2(elements)
                # Fix: Provider Scope - Add N2 data to provider_info
                provider_info['ProviderNameLine2'] = elements[1] if len(elements) > 1 else ''
                provider_info['ProviderNameLine3'] = elements[2] if len(elements) > 2 else ''

        elif seg_id == 'N3' and current_loop == 'L1000B':
            # Fix: Provider Scope - Extract provider address
            provider_info['ProviderAddress'] = elements[1] if len(elements) > 1 else ''
            provider_info['ProviderAddress2'] = elements[2] if len(elements) > 2 else ''

        elif seg_id == 'N4' and current_loop == 'L1000B':
            # Fix: Provider Scope - Extract provider city/state/zip + additional N4 fields
            provider_info['ProviderCity'] = elements[1] if len(elements) > 1 else ''
            provider_info['ProviderState'] = elements[2] if len(elements) > 2 else ''
            provider_info['ProviderZip'] = elements[3] if len(elements) > 3 else ''
            provider_info['ProviderCountryCode'] = elements[4] if len(elements) > 4 else ''
            provider_info['ProviderLocationQualifier'] = elements[5] if len(elements) > 5 else ''
            provider_info['ProviderLocationID'] = elements[6] if len(elements) > 6 else ''
            provider_info['ProviderCountrySubdivision'] = elements[7] if len(elements) > 7 else ''

        elif seg_id == 'REF' and current_loop == 'L1000B' and not current_claim:
            # Fix: Provider Scope - Extract provider reference identifiers
            if len(elements) > 1:
                qualifier = elements[1]
                value = elements[2] if len(elements) > 2 else ''
                if qualifier == 'PQ':
                    provider_info['ProviderSecondaryID'] = value
                elif qualifier == 'TJ':
                    provider_info['ProviderTaxID'] = value

        elif seg_id == 'MOA' and current_claim:
            current_claim['_moa'].append(parse_moa(elements))

        elif seg_id == 'MIA' and current_claim:
            current_claim['_mia'].append(parse_mia(elements))

        elif seg_id == 'PER' and current_claim and not current_service:
            current_claim['_per'].append(parse_per(elements))

        elif seg_id == 'LQ' and current_claim and not current_service:
            current_claim['_lq'].append(parse_lq(elements))

        elif seg_id == 'N1' and len(elements) > 1 and elements[1] == 'PE' and not current_claim:
            # Fix: Provider Scope - Extract provider info per transaction (Loop 1000B)
            current_loop = 'L1000B'  # We're in the payee loop
            provider_info['ProviderEntityIDCode'] = elements[1]  # N101 = "PE"
            if len(elements) > 2:
                provider_info['ProviderName'] = elements[2]  # N102
            if len(elements) > 3:
                provider_info['ProviderIDQualifier'] = elements[3]  # N103
            if len(elements) > 4:
                provider_info['ProviderIDCode'] = elements[4]  # N104
                provider_info['ProviderTIN'] = elements[4]  # Backward compatibility
            if len(elements) > 5:
                provider_info['EntityRelationship'] = elements[5]
                provider_info['EntityRelationshipDesc'] = get_entity_relationship_description(elements[5])
            if len(elements) > 6:
                provider_info['EntityIDSecondary'] = elements[6]
        
        elif seg_id == 'N1' and current_claim and not current_service:
            if len(elements) > 1:
                current_claim['_service_provider'] = {
                    'entity_code': elements[1] if len(elements) > 1 else '',
                    'name': elements[2] if len(elements) > 2 else '',
                    'id_qualifier': elements[3] if len(elements) > 3 else '',
                    'id_code': elements[4] if len(elements) > 4 else '',
                    'entity_relationship': elements[5] if len(elements) > 5 else '',
                    'entity_relationship_desc': get_entity_relationship_description(elements[5]) if len(elements) > 5 else '',
                    'entity_id_secondary': elements[6] if len(elements) > 6 else ''
                }

        elif seg_id == 'SVC' and current_claim:
            svc_encountered_for_claim = True  # Mark that this claim has service detail
            if current_service:
                current_claim_services.append(current_service)
                row = create_output_row(current_claim, current_service,
                                       header_payer_name, payer_address, payer_address2, payer_city, payer_state, payer_zip,
                                       payer_country_code, payer_location_qualifier, payer_location_id, payer_country_subdivision,
                                       provider_info, payment_method_code, payment_format_code,
                                       check_number, check_date, check_amount, trace_number, trace_type,
                                       plb_adjustments, is_pended_report,
                                       originating_company_id_trn, reference_id_secondary, receiver_id,
                                       transaction_handling, credit_debit_flag, payer_dfi_qualifier, payer_dfi_id,
                                       payer_account_qualifier, payer_account_number, originating_company_id,
                                       originating_company_supplemental, payee_dfi_qualifier, payee_dfi_id,
                                       payee_account_qualifier, payee_account_number, check_eft_effective_date,
                                       business_function_code, business_function_desc, dfi_id_qualifier_3, dfi_id_3,
                                       account_qualifier_3, account_number_3, transaction_control_number,
                                       implementation_convention_ref, payer_entity_id_code, payer_id_qualifier, payer_id_code,
                                       payer_entity_relationship, payer_entity_relationship_desc, payer_entity_id_secondary,
                                       payer_additional_id, contact_bl_function, contact_bl_name, contact_bl_comm1_qualifier, contact_bl_comm1_number,
                                       contact_bl_comm2_qualifier, contact_bl_comm2_number,
                                       contact_cx_function, contact_cx_name, contact_cx_comm1_qualifier, contact_cx_comm1_number,
                                       contact_cx_comm2_qualifier, contact_cx_comm2_number,
                                       contact_ic_function, contact_ic_name, contact_ic_comm1_qualifier, contact_ic_comm1_number,
                                       contact_ic_comm2_qualifier, contact_ic_comm2_number, file_name, current_transaction,
                                       isa_data, gs_data, cur_data, payer_rdm_data, payer_n2_data,
                                       payee_rdm_data, payee_n2_data, payer_key=payer_key)
                transaction_rows_buffer.append(row)  # Fix: PLB Scope - Buffer rows

            # Fix Issue 1: Correct SVC01 composite parsing
            # SVC01 format: Qualifier:Code:Mod1:Mod2:Mod3:Mod4
            # Per X12 spec, both qualifier and code are mandatory in composite
            proc_code_composite = elements[1] if len(elements) > 1 else ''

            if component_delimiter in proc_code_composite:
                # Properly formatted composite: "Qualifier:Code:Mod1:Mod2..."
                composite_parts = proc_code_composite.split(component_delimiter)
                qualifier = composite_parts[0] if len(composite_parts) > 0 else 'HC'
                proc_code = composite_parts[1] if len(composite_parts) > 1 else ''
                modifier1 = composite_parts[2] if len(composite_parts) > 2 else ''
                modifier2 = composite_parts[3] if len(composite_parts) > 3 else ''
                modifier3 = composite_parts[4] if len(composite_parts) > 4 else ''
                modifier4 = composite_parts[5] if len(composite_parts) > 5 else ''
            else:
                # No delimiter - assume entire value is procedure code with default qualifier
                # This handles legacy/malformed data where qualifier is implicit
                qualifier = 'HC'  # Healthcare Common Procedure Coding System (default)
                proc_code = proc_code_composite  # Entire value is the code
                modifier1 = modifier2 = modifier3 = modifier4 = ''
            modifiers_list = [m for m in [modifier1, modifier2, modifier3, modifier4] if m]
            current_service = {
                'qualifier': qualifier,
                'procedure_code': proc_code,
                'modifiers': modifiers_list,
                'modifier1': modifier1,
                'modifier2': modifier2,
                'modifier3': modifier3,
                'modifier4': modifier4,
                'line_charged': elements[2] if len(elements) > 2 else '',
                'line_paid': elements[3] if len(elements) > 3 else '',
                # SVC04 - Not used (Revenue Code for institutional, rarely populated for professional)
                'units': elements[5] if len(elements) > 5 else '',  # SVC05 - Paid Service Unit Count
                'original_procedure': elements[6] if len(elements) > 6 else '',
                'original_units': elements[7] if len(elements) > 7 else '',  # SVC07 - Original Units of Service  
                '_cas': [],
                '_ref': [],
                '_dtm': [],
                '_lq': [],
                '_amt': [],
                '_qty': []
            }

        elif seg_id == 'CAS' and current_service and current_claim:
            claim_status = current_claim.get('claim_status')
            cas_data = safe_parse_segment(seg, element_delimiter,
                                         lambda els: parse_cas(els, 'SERVICE', claim_status, payer_key=payer_key),
                                         [])
            if cas_data:
                current_service['_cas'].extend(cas_data)

        elif seg_id == 'REF' and current_service and current_claim:
            current_service['_ref'].append(parse_ref(elements, payer_key=payer_key))

        elif seg_id == 'LQ' and current_service and current_claim:
            current_service['_lq'].append(parse_lq(elements))

        elif seg_id == 'AMT' and current_claim:
            if current_service:
                current_service['_amt'].append(parse_amt(elements))
            else:
                current_claim['_amt'].append(parse_amt(elements))

        elif seg_id == 'QTY' and current_claim:
            if current_service:
                current_service['_qty'].append(parse_qty(elements))
            else:
                current_claim['_qty'].append(parse_qty(elements))
            
        elif seg_id == 'TS3':
            # Provider Summary Information
            if current_lx_header:
                current_lx_header['ts3'] = parse_ts3(elements)
                if current_claim:
                    current_claim['ts3'] = parse_ts3(elements)
            
        elif seg_id == 'TS2':
            # Provider Supplemental Summary
            if current_lx_header:
                current_lx_header['ts2'] = parse_ts2(elements)
                if current_claim:
                    current_claim['ts2'] = parse_ts2(elements)
                    
        elif seg_id == 'PLB':
            # Provider Level Adjustment
            plb = parse_plb(elements, component_delimiter)
            plb_adjustments.append(plb)

        elif seg_id == 'SE':
            # Run PLB refund detection now that all PLBs have been parsed
            if plb_adjustments and not plb_refund_acks_detected:
                detect_refund_acknowledgments(plb_adjustments)
                plb_refund_acks_detected = True

            if current_service and current_claim:
                current_claim_services.append(current_service)
                row = create_output_row(current_claim, current_service,
                                       header_payer_name, payer_address, payer_address2, payer_city, payer_state, payer_zip,
                                       payer_country_code, payer_location_qualifier, payer_location_id, payer_country_subdivision,
                                       provider_info, payment_method_code, payment_format_code,
                                       check_number, check_date, check_amount, trace_number, trace_type,
                                       plb_adjustments, is_pended_report,
                                       originating_company_id_trn, reference_id_secondary, receiver_id,
                                       transaction_handling, credit_debit_flag, payer_dfi_qualifier, payer_dfi_id,
                                       payer_account_qualifier, payer_account_number, originating_company_id,
                                       originating_company_supplemental, payee_dfi_qualifier, payee_dfi_id,
                                       payee_account_qualifier, payee_account_number, check_eft_effective_date,
                                       business_function_code, business_function_desc, dfi_id_qualifier_3, dfi_id_3,
                                       account_qualifier_3, account_number_3, transaction_control_number,
                                       implementation_convention_ref, payer_entity_id_code, payer_id_qualifier, payer_id_code,
                                       payer_entity_relationship, payer_entity_relationship_desc, payer_entity_id_secondary,
                                       payer_additional_id, contact_bl_function, contact_bl_name, contact_bl_comm1_qualifier, contact_bl_comm1_number,
                                       contact_bl_comm2_qualifier, contact_bl_comm2_number,
                                       contact_cx_function, contact_cx_name, contact_cx_comm1_qualifier, contact_cx_comm1_number,
                                       contact_cx_comm2_qualifier, contact_cx_comm2_number,
                                       contact_ic_function, contact_ic_name, contact_ic_comm1_qualifier, contact_ic_comm1_number,
                                       contact_ic_comm2_qualifier, contact_ic_comm2_number, file_name, current_transaction,
                                       isa_data, gs_data, cur_data, payer_rdm_data, payer_n2_data,
                                       payee_rdm_data, payee_n2_data, payer_key=payer_key)
                transaction_rows_buffer.append(row)  # Fix: PLB Scope - Buffer rows
                current_service = None

            # FIXED: Check if services list was empty BEFORE we cleared it
            # If claim had no services at all, output one claim header row
            if current_claim and not svc_encountered_for_claim:
                row = create_output_row(current_claim, None,
                                       header_payer_name, payer_address, payer_address2, payer_city, payer_state, payer_zip,
                                       payer_country_code, payer_location_qualifier, payer_location_id, payer_country_subdivision,
                                       provider_info, payment_method_code, payment_format_code,
                                       check_number, check_date, check_amount, trace_number, trace_type,
                                       plb_adjustments, is_pended_report,
                                       originating_company_id_trn, reference_id_secondary, receiver_id,
                                       transaction_handling, credit_debit_flag, payer_dfi_qualifier, payer_dfi_id,
                                       payer_account_qualifier, payer_account_number, originating_company_id,
                                       originating_company_supplemental, payee_dfi_qualifier, payee_dfi_id,
                                       payee_account_qualifier, payee_account_number, check_eft_effective_date,
                                       business_function_code, business_function_desc, dfi_id_qualifier_3, dfi_id_3,
                                       account_qualifier_3, account_number_3, transaction_control_number,
                                       implementation_convention_ref, payer_entity_id_code, payer_id_qualifier, payer_id_code,
                                       payer_entity_relationship, payer_entity_relationship_desc, payer_entity_id_secondary,
                                       payer_additional_id, contact_bl_function, contact_bl_name, contact_bl_comm1_qualifier, contact_bl_comm1_number,
                                       contact_bl_comm2_qualifier, contact_bl_comm2_number,
                                       contact_cx_function, contact_cx_name, contact_cx_comm1_qualifier, contact_cx_comm1_number,
                                       contact_cx_comm2_qualifier, contact_cx_comm2_number,
                                       contact_ic_function, contact_ic_name, contact_ic_comm1_qualifier, contact_ic_comm1_number,
                                       contact_ic_comm2_qualifier, contact_ic_comm2_number, file_name, current_transaction,
                                       isa_data, gs_data, cur_data, payer_rdm_data, payer_n2_data,
                                       payee_rdm_data, payee_n2_data, payer_key=payer_key)
                transaction_rows_buffer.append(row)  # Fix: PLB Scope - Buffer rows

            # Fix: PLB Data Scope - Back-fill PLB data into buffered rows
            # Now that all PLB segments have been parsed, update all rows with PLB data
            # When rows were created, plb_adjustments was empty. Now it's populated.
            flat_plb_adjustments = flatten_plb_adjustments(plb_adjustments)
            plb_total = calculate_plb_total(plb_adjustments)
            plb_details = format_plb_details(plb_adjustments, payer_key=payer_key)

            for buffered_row in transaction_rows_buffer:
                # Update PLB summary fields
                buffered_row['PLB_TotalAdjustments_PLB'] = plb_total
                buffered_row['PLB_Details_PLB'] = plb_details

                # Update individual PLB adjustment fields (up to 6 adjustments)
                for i in range(6):
                    adj_num = i + 1
                    if i < len(flat_plb_adjustments):
                        adj = flat_plb_adjustments[i]
                        buffered_row[f'PLB_Adj{adj_num}_ReasonCode_PLB'] = adj.get('reason_code', '')
                        buffered_row[f'PLB_Adj{adj_num}_RefID_PLB'] = adj.get('reference_id', '')
                        buffered_row[f'PLB_Adj{adj_num}_Amount_PLB'] = adj.get('amount', '')
                    else:
                        # Clear fields if no adjustment at this position
                        buffered_row[f'PLB_Adj{adj_num}_ReasonCode_PLB'] = ''
                        buffered_row[f'PLB_Adj{adj_num}_RefID_PLB'] = ''
                        buffered_row[f'PLB_Adj{adj_num}_Amount_PLB'] = ''

            # Add all buffered rows to main output
            rows.extend(transaction_rows_buffer)
            transaction_rows_buffer = []  # Clear buffer for next transaction

            if current_claim:
                current_transaction['claims'].append(current_claim.copy())
                current_claim = None

            # Reset for next transaction
            current_claim_services = []

    if current_service and current_claim:
        current_claim_services.append(current_service)
        row = create_output_row(current_claim, current_service,
                               header_payer_name, payer_address, payer_address2, payer_city, payer_state, payer_zip,
                               payer_country_code, payer_location_qualifier, payer_location_id, payer_country_subdivision,
                               provider_info, payment_method_code, payment_format_code,
                               check_number, check_date, check_amount, trace_number, trace_type,
                               plb_adjustments, is_pended_report,
                               originating_company_id_trn, reference_id_secondary, receiver_id,
                               transaction_handling, credit_debit_flag, payer_dfi_qualifier, payer_dfi_id,
                               payer_account_qualifier, payer_account_number, originating_company_id,
                               originating_company_supplemental, payee_dfi_qualifier, payee_dfi_id,
                               payee_account_qualifier, payee_account_number, check_eft_effective_date,
                               business_function_code, business_function_desc, dfi_id_qualifier_3, dfi_id_3,
                               account_qualifier_3, account_number_3, transaction_control_number,
                               implementation_convention_ref, payer_entity_id_code, payer_id_qualifier, payer_id_code,
                               payer_entity_relationship, payer_entity_relationship_desc, payer_entity_id_secondary,
                               payer_additional_id, contact_bl_function, contact_bl_name, contact_bl_comm1_qualifier, contact_bl_comm1_number,
                               contact_bl_comm2_qualifier, contact_bl_comm2_number,
                               contact_cx_function, contact_cx_name, contact_cx_comm1_qualifier, contact_cx_comm1_number,
                               contact_cx_comm2_qualifier, contact_cx_comm2_number,
                               contact_ic_function, contact_ic_name, contact_ic_comm1_qualifier, contact_ic_comm1_number,
                               contact_ic_comm2_qualifier, contact_ic_comm2_number, file_name, None,
                               isa_data, gs_data, cur_data, payer_rdm_data, payer_n2_data,
                               payee_rdm_data, payee_n2_data, payer_key=payer_key)
        rows.append(row)  # This is after loop, goes directly to rows (not buffer)

    # Fix: PLB Scope - Flush any remaining buffered rows from last transaction
    if 'transaction_rows_buffer' in locals() and transaction_rows_buffer:
        # Back-fill PLB data for final transaction
        flat_plb_adjustments = flatten_plb_adjustments(plb_adjustments)
        plb_total = calculate_plb_total(plb_adjustments)
        plb_details = format_plb_details(plb_adjustments, payer_key=payer_key)
        for buffered_row in transaction_rows_buffer:
            buffered_row['PLB_TotalAdjustments_PLB'] = plb_total
            buffered_row['PLB_Details_PLB'] = plb_details
            for i in range(6):
                adj_num = i + 1
                if i < len(flat_plb_adjustments):
                    adj = flat_plb_adjustments[i]
                    buffered_row[f'PLB_Adj{adj_num}_ReasonCode_PLB'] = adj.get('reason_code', '')
                    buffered_row[f'PLB_Adj{adj_num}_RefID_PLB'] = adj.get('reference_id', '')
                    buffered_row[f'PLB_Adj{adj_num}_Amount_PLB'] = adj.get('amount', '')
                else:
                    buffered_row[f'PLB_Adj{adj_num}_ReasonCode_PLB'] = ''
                    buffered_row[f'PLB_Adj{adj_num}_RefID_PLB'] = ''
                    buffered_row[f'PLB_Adj{adj_num}_Amount_PLB'] = ''
        rows.extend(transaction_rows_buffer)

    if current_transaction and current_transaction['claims']:
        transactions.append(current_transaction.copy())
    if current_claim:
        current_transaction['claims'].append(current_claim.copy())
        if current_transaction and current_transaction['claims']:
            transactions.append(current_transaction.copy())

    return rows

def calculate_plb_total(plb_list):
    """
    Calculate total PLB adjustments, excluding refund acknowledgments.
    Per guidance: WO/72 pairs should be ignored during posting as they net to $0.

    Args:
        plb_list: List of PLB dicts, each with 'adjustments' array
    """
    total = Decimal('0')
    for plb in plb_list:
        for adj in plb.get('adjustments', []):
            # Skip refund acknowledgments
            if adj.get('is_refund_ack', False):
                continue
            try:
                total += Decimal(str(adj.get('amount', 0)))
            except (ValueError, TypeError, ArithmeticError):
                pass
    return str(total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def format_plb_details(plb_list, payer_key=None):
    """
    Format PLB adjustment details with complete information for audit trail.
    Includes provider ID, date, reason codes with descriptions, and refund markers.

    Args:
        plb_list: List of PLB dicts, each with 'adjustments' array
        payer_key: Optional payer key for payer-specific PLB descriptions
    """
    details = []
    for plb in plb_list:
        provider = plb.get('provider_identifier', '')
        date = plb.get('fiscal_period_date', '')

        for adj in plb.get('adjustments', []):
            reason_code = adj.get('reason_code', '')
            ref = adj.get('reference_id', '')
            amt = adj.get('amount', '')
            is_refund_ack = adj.get('is_refund_ack', False)

            if reason_code and amt:
                description = get_plb_adjustment_code_description(reason_code, payer_key=payer_key)

                # Format: "ProviderID Date: Code (Description)-Ref $Amount [REFUND ACK]"
                detail_str = f"{provider} {date}: {reason_code}"
                if description:
                    detail_str += f" ({description})"
                if ref:
                    detail_str += f"-{ref}"
                detail_str += f" ${amt}"

                # Mark refund acknowledgments
                if is_refund_ack:
                    detail_str += " [REFUND ACK]"

                details.append(detail_str)
    return '; '.join(details) if details else ''


def flatten_plb_adjustments(plb_list):
    """
    Flatten all PLB adjustments from multiple PLB segments into a single list.
    Each PLB segment can have up to 6 adjustments.

    Args:
        plb_list: List of PLB dicts, each with 'adjustments' array

    Returns:
        List of individual adjustment dicts from all PLB segments
    """
    flattened = []
    for plb in plb_list:
        for adj in plb.get('adjustments', []):
            flattened.append(adj)
    return flattened


def extract_amt_value(amt_list, qualifier):
    for amt in amt_list:
        if amt.get('amount_qualifier') == qualifier:
            return amt.get('monetary_amount', '')
    return ''


def extract_amt_values(amt_list):
    """Extract all amount values from AMT segments"""
    amt_values = {
        'allowed_amount': '',  
        'interest_amount': '',  
        'coverage_amount': '',  
        'tax_amount': '',  
        'patient_amount_paid': '',
        # Additional AMT qualifiers
        'discount_amount': '',               # D8
        'per_day_limit_amount': '',          # DY
        'deduction_amount': '',              # KH
        'net_billed_amount': '',             # NL
        'total_claim_before_taxes': '',      # T2
        'federal_mandate_amount_1': '',      # ZK
        'federal_mandate_amount_2': '',      # ZL
        'federal_mandate_amount_3': '',      # ZM
        'federal_mandate_amount_4': '',      # ZN
        'federal_mandate_amount_5': ''       # ZO
    }
    for amt in amt_list:
        qual = amt.get('amount_qualifier', '')
        val = amt.get('monetary_amount', '')
        if qual == 'B6':
            amt_values['allowed_amount'] = val
        elif qual == 'I':
            amt_values['interest_amount'] = val
        elif qual == 'AU':
            amt_values['coverage_amount'] = val
        elif qual == 'T':
            amt_values['tax_amount'] = val
        elif qual == 'F5':
            amt_values['patient_amount_paid'] = val
        elif qual == 'D8':
            amt_values['discount_amount'] = val
        elif qual == 'DY':
            amt_values['per_day_limit_amount'] = val
        elif qual == 'KH':
            amt_values['deduction_amount'] = val
        elif qual == 'NL':
            amt_values['net_billed_amount'] = val
        elif qual == 'T2':
            amt_values['total_claim_before_taxes'] = val
        elif qual == 'ZK':
            amt_values['federal_mandate_amount_1'] = val
        elif qual == 'ZL':
            amt_values['federal_mandate_amount_2'] = val
        elif qual == 'ZM':
            amt_values['federal_mandate_amount_3'] = val
        elif qual == 'ZN':
            amt_values['federal_mandate_amount_4'] = val
        elif qual == 'ZO':
            amt_values['federal_mandate_amount_5'] = val
    return amt_values


def extract_qty_value(qty_list, qualifier):
    for qty in qty_list:
        if qty.get('quantity_qualifier') == qualifier:
            return qty.get('quantity', '')
    return ''


def extract_ref_value(ref_list, qualifier):
    """Extract specific REF value by qualifier"""
    for ref in ref_list:
        if ref.get('qualifier') == qualifier:
            return ref.get('ref_value', '')
    return ''


def extract_lq_codes_by_qualifier(lq_list, code_list_qualifier):
    """
    Extract LQ industry codes filtered by code list qualifier.
    
    Args:
        lq_list: List of parsed LQ segments
        code_list_qualifier: The qualifier to filter by (e.g., 'HE' for Healthcare Remark Codes)
    
    Returns:
        List of industry codes matching the qualifier
    """
    codes = []
    for lq in lq_list:
        if lq.get('code_list_qualifier') == code_list_qualifier:
            code = lq.get('industry_code', '')
            if code:
                codes.append(code)
    return codes


def extract_dtm_value(dtm_list, qualifier):
    """Extract specific DTM date by qualifier"""
    for dtm in dtm_list:
        if dtm.get('date_qualifier') == qualifier:
            return dtm.get('date_value', '')
    return ''


def create_output_row(claim, service, header_payer_name, payer_address, payer_address2, payer_city, payer_state, payer_zip,
                     payer_country_code, payer_location_qualifier, payer_location_id, payer_country_subdivision,
                     provider_info, payment_method_code, payment_format_code, check_number, check_date, check_amount,
                     trace_number, trace_type, plb_adjustments, is_pended_report,
                     originating_company_id_trn, reference_id_secondary, receiver_id,
                     transaction_handling, credit_debit_flag, payer_dfi_qualifier, payer_dfi_id,
                     payer_account_qualifier, payer_account_number, originating_company_id,
                     originating_company_supplemental, payee_dfi_qualifier, payee_dfi_id,
                     payee_account_qualifier, payee_account_number, check_eft_effective_date,
                     business_function_code, business_function_desc, dfi_id_qualifier_3, dfi_id_3,
                     account_qualifier_3, account_number_3, transaction_control_number,
                     implementation_convention_ref, payer_entity_id_code, payer_id_qualifier, payer_id_code,
                     payer_entity_relationship, payer_entity_relationship_desc, payer_entity_id_secondary,
                     payer_additional_id, contact_bl_function, contact_bl_name, contact_bl_comm1_qualifier, contact_bl_comm1_number,
                     contact_bl_comm2_qualifier, contact_bl_comm2_number,
                     contact_cx_function, contact_cx_name, contact_cx_comm1_qualifier, contact_cx_comm1_number,
                     contact_cx_comm2_qualifier, contact_cx_comm2_number,
                     contact_ic_function, contact_ic_name, contact_ic_comm1_qualifier, contact_ic_comm1_number,
                     contact_ic_comm2_qualifier, contact_ic_comm2_number, file_name, current_transaction=None,
                     isa_data=None, gs_data=None, cur_data=None, payer_rdm_data=None, payer_n2_data=None,
                     payee_rdm_data=None, payee_n2_data=None, payer_key=None):

    # Flatten PLB adjustments for CSV output
    # PLB segments contain 'adjustments' arrays, but CSV expects flat list
    flat_plb_adjustments = flatten_plb_adjustments(plb_adjustments)

    # Handle None values for optional parameters
    if isa_data is None:
        isa_data = {}
    if gs_data is None:
        gs_data = {}
    if cur_data is None:
        cur_data = {}
    if payer_rdm_data is None:
        payer_rdm_data = {}
    if payer_n2_data is None:
        payer_n2_data = {}
    if payee_rdm_data is None:
        payee_rdm_data = {}
    if payee_n2_data is None:
        payee_n2_data = {}

    if '_transaction_bpr_effective_date' in claim:
        check_eft_effective_date = claim['_transaction_bpr_effective_date']
    if '_transaction_dtm_production_date' in claim:
        check_date = claim['_transaction_dtm_production_date']
    if '_transaction_trace_number' in claim:
        trace_number = claim['_transaction_trace_number']
    if '_transaction_payer_additional_id' in claim:
        payer_additional_id = claim['_transaction_payer_additional_id']

    subscriber_info = extract_subscriber_info(claim)
    payer_info = extract_payer_info(claim)
    secondary_payer_info = extract_secondary_payer_info(claim)
    claim_ref_values = extract_ref_values(claim.get('_ref', []))
    claim_dtm_values = extract_dtm_values(claim.get('_dtm', []))
    claim_amt_values = extract_amt_values(claim.get('_amt', []))
    
    service_ref_values = {}
    if service:
        service_ref_values = extract_ref_values(service.get('_ref', []))

    service_start = ''
    service_end = ''
    if service and '_dtm' in service:
        for dtm in service['_dtm']:
            if dtm['date_qualifier'] == '150':
                service_start = dtm['date_value']
            elif dtm['date_qualifier'] == '151':
                service_end = dtm['date_value']
            elif dtm['date_qualifier'] == '472':  # Service date
                if not service_start:  # Use 472 as service start if not already set
                    service_start = dtm['date_value']

    if not service_start and '_dtm' in claim:
        for dtm in claim['_dtm']:
            if dtm['date_qualifier'] == '232':
                service_start = dtm['date_value']

    rendering_provider = ''
    if '_nm1' in claim:
        for nm1 in claim['_nm1']:
            if nm1.get('entity_id_code') == '82':
                name_parts = [
                    nm1.get('last_name', ''),
                    nm1.get('first_name', '')
                ]
                rendering_provider = ', '.join(filter(None, name_parts))
                break

    corrected_insured_name = ''
    corrected_insured_suffix = ''
    corrected_insured_id_qualifier = ''
    corrected_insured_id = ''

    if '_nm1' in claim:
        for nm1 in claim['_nm1']:
            if nm1.get('entity_id_code') == '74':
                name_parts = []
                if nm1.get('last_name'):
                    name_parts.append(nm1.get('last_name', ''))
                if nm1.get('first_name'):
                    name_parts.append(nm1.get('first_name', ''))
                if nm1.get('middle_name'):
                    name_parts.append(nm1.get('middle_name', ''))
                corrected_insured_name = ', '.join(filter(None, name_parts))
                corrected_insured_suffix = nm1.get('suffix', '')  # Fix: Use correct field name from parser
                corrected_insured_id_qualifier = nm1.get('id_code_qualifier', '')
                corrected_insured_id = nm1.get('id_code', '')
                break

    claim_adjustments = {
        'Contractual': 0.0,
        'Copay': 0.0,
        'Coinsurance': 0.0,
        'Deductible': 0.0,
        'Denied': 0.0,
        'OtherAdjustments': 0.0,
        'Sequestration': 0.0,
        'COB': 0.0,
        'HCRA': 0.0,
        'QMB': 0.0,
        'PR_NonCovered': 0.0,
        'OtherPatientResp': 0.0,
        'AuditFlag': ''
    }

    service_adjustments = {
        'Contractual': 0.0,
        'Copay': 0.0,
        'Coinsurance': 0.0,
        'Deductible': 0.0,
        'Denied': 0.0,
        'OtherAdjustments': 0.0,
        'Sequestration': 0.0,
        'COB': 0.0,
        'HCRA': 0.0,
        'QMB': 0.0,
        'PR_NonCovered': 0.0,
        'OtherPatientResp': 0.0,
        'AuditFlag': ''
    }

    claim_adj_list = []
    claim_audit_flags = []
    if '_cas' in claim:
        for adj in claim['_cas']:
            categories = categorize_adjustment(adj['group_code'], adj['reason_code'], adj['amount'])
            for cat, val in categories.items():
                if cat == 'AuditFlag':
                    if val:  # Non-empty audit flag
                        claim_audit_flags.append(val)
                else:
                    claim_adjustments[cat] += val

            claim_adj_list.append(f"{adj['group_code']}-{adj['reason_code']}: ${adj['amount']}")
    claim_adjustments['AuditFlag'] = '; '.join(claim_audit_flags)

    service_adj_list = []
    service_audit_flags = []
    if service and '_cas' in service:
        for adj in service['_cas']:
            categories = categorize_adjustment(adj['group_code'], adj['reason_code'], adj['amount'])
            for cat, val in categories.items():
                if cat == 'AuditFlag':
                    if val:  # Non-empty audit flag
                        service_audit_flags.append(val)
                else:
                    service_adjustments[cat] += val

            service_adj_list.append(f"{adj['group_code']}-{adj['reason_code']}: ${adj['amount']}")
    service_adjustments['AuditFlag'] = '; '.join(service_audit_flags)

    claim_remark_codes = []
    claim_remark_desc = []
    if '_lq' in claim:
        for lq in claim['_lq']:
            code = lq.get('industry_code', '')
            if code:
                claim_remark_codes.append(code)
                claim_remark_desc.append(get_remark_code_description(code, payer_key=payer_key))

    service_remark_codes = []
    service_remark_desc = []
    if service and '_lq' in service:
        for lq in service['_lq']:
            code = lq.get('industry_code', '')
            if code:
                service_remark_codes.append(code)
                service_remark_desc.append(get_remark_code_description(code, payer_key=payer_key))

    service_qualifier_desc = ''
    if service:
        service_qualifier_desc = get_service_qualifier_description(service.get('qualifier', ''))

    row = {
        # ISA Envelope fields
        'ENV_AuthorizationQualifier_Envelope_ISA': isa_data.get('authorization_information_qualifier', ''),
        'ENV_AuthorizationInfo_Envelope_ISA': isa_data.get('authorization_information', ''),
        'ENV_SecurityQualifier_Envelope_ISA': isa_data.get('security_information_qualifier', ''),
        'ENV_SecurityInfo_Envelope_ISA': isa_data.get('security_information', ''),
        'ENV_SenderIDQualifier_Envelope_ISA': isa_data.get('interchange_id_qualifier_sender', ''),
        'ENV_SenderID_Envelope_ISA': isa_data.get('interchange_sender_id', ''),
        'ENV_ReceiverIDQualifier_Envelope_ISA': isa_data.get('interchange_id_qualifier_receiver', ''),
        'ENV_ReceiverID_Envelope_ISA': isa_data.get('interchange_receiver_id', ''),
        'ENV_InterchangeDate_Envelope_ISA': isa_data.get('interchange_date', ''),
        'ENV_InterchangeTime_Envelope_ISA': isa_data.get('interchange_time', ''),
        'ENV_RepetitionSeparator_Envelope_ISA': isa_data.get('repetition_separator', ''),
        'ENV_VersionNumber_Envelope_ISA': isa_data.get('interchange_control_version_number', ''),
        'ENV_InterchangeControlNumber_Envelope_ISA': isa_data.get('interchange_control_number', ''),
        'ENV_AcknowledgmentRequested_Envelope_ISA': isa_data.get('acknowledgment_requested', ''),
        'ENV_UsageIndicator_Envelope_ISA': isa_data.get('interchange_usage_indicator', ''),
        'ENV_ComponentSeparator_Envelope_ISA': isa_data.get('component_element_separator', ''),
        
        # GS Envelope fields
        'ENV_FunctionalIDCode_Envelope_GS': gs_data.get('functional_identifier_code', ''),
        'ENV_ApplicationSenderCode_Envelope_GS': gs_data.get('application_sender_code', ''),
        'ENV_ApplicationReceiverCode_Envelope_GS': gs_data.get('application_receiver_code', ''),
        'ENV_Date_Envelope_GS': gs_data.get('date', ''),
        'ENV_Time_Envelope_GS': gs_data.get('time', ''),
        'ENV_GroupControlNumber_Envelope_GS': gs_data.get('group_control_number', ''),
        'ENV_ResponsibleAgencyCode_Envelope_GS': gs_data.get('responsible_agency_code', ''),
        'ENV_VersionReleaseID_Envelope_GS': gs_data.get('version_release_industry_id', ''),
        
        # CUR Header fields - Foreign currency fields not used for US professional claims
        # 'CHK_EntityIDCode_Header_CUR': cur_data.get('entity_identifier_code', ''),
        # 'CHK_EntityIDCodeDesc_Header_CUR': get_entity_identifier_description(cur_data.get('entity_identifier_code', '')),
        # 'CHK_CurrencyCode_Header_CUR': cur_data.get('currency_code', ''),
        # 'CHK_CurrencyCodeDesc_Header_CUR': get_currency_code_description(cur_data.get('currency_code', '')),
        # 'CHK_ExchangeRate_Header_CUR': cur_data.get('exchange_rate', ''),
        # 'CHK_EntityIDCode2_Header_CUR': cur_data.get('entity_identifier_code_2', ''),
        # 'CHK_EntityIDCode2Desc_Header_CUR': get_entity_identifier_description(cur_data.get('entity_identifier_code_2', '')),
        # 'CHK_CurrencyCode2_Header_CUR': cur_data.get('currency_code_2', ''),
        # 'CHK_CurrencyCode2Desc_Header_CUR': get_currency_code_description(cur_data.get('currency_code_2', '')),
        # 'CHK_MarketExchangeCode_Header_CUR': cur_data.get('currency_market_exchange_code', ''),
        # 'CHK_DateTimeQualifier_Header_CUR': cur_data.get('date_time_qualifier', ''),
        # 'CHK_DateTimeQualifierDesc_Header_CUR': get_date_qualifier_description(cur_data.get('date_time_qualifier', '')),
        # 'CHK_Date_Header_CUR': cur_data.get('date', ''),
        
        'CHK_Number_Header_BPR': check_number,
        'CHK_PaymentDate_Header_BPR': check_eft_effective_date,
        'CHK_ProductionDate_Header_DTM405': check_date,  # DTM*405 - Production Date
        'CHK_PaymentAmount_Header_BPR': check_amount,
        'CHK_PaymentMethod_Header_BPR': payment_method_code,
        'CHK_PaymentMethodDesc_Header_BPR': get_payment_method_description(payment_method_code) if payment_method_code else '',
        'CHK_Format_Header_BPR': payment_format_code,
        'CHK_FormatDesc_Header_BPR': get_payment_format_description(payment_format_code) if payment_format_code else '',
        'CHK_TraceNumber_Header_TRN': trace_number,
        'CHK_TraceType_Header_TRN': trace_type,
        'CHK_TraceTypeDesc_Header_TRN': get_trace_type_description(trace_type) if trace_type else '',
        'CHK_OriginatingCompanyID_TRN_Header_TRN': originating_company_id_trn,
        'CHK_ReferenceIDSecondary_Header_TRN': reference_id_secondary,
        'CHK_ReceiverID_Header_REF': receiver_id,
        'File_TransactionType_Header_ST': '835S' if is_pended_report else '835',
        # 'File_IsPendedReport_Header_ST': 'Yes' if is_pended_report else 'No',  # Redundant - covered by TransactionType
        'Filename_File': file_name,
        'CHK_TransactionHandling_Header_BPR': transaction_handling,
        'CHK_PayerDFI_Qualifier_Header_BPR': payer_dfi_qualifier,
        'CHK_PayerDFI_QualifierDesc_Header_BPR': get_dfi_id_number_qualifier_description(payer_dfi_qualifier),
        'CHK_PayerDFI_ID_Header_BPR': payer_dfi_id,
        'CHK_PayerAccountQualifier_Header_BPR': payer_account_qualifier,
        'CHK_PayerAccountQualifierDesc_Header_BPR': get_account_number_qualifier_description(payer_account_qualifier),
        'CHK_PayerAccountNumber_Header_BPR': payer_account_number,
        'CHK_PayeeDFI_Qualifier_Header_BPR': payee_dfi_qualifier,
        'CHK_PayeeDFI_QualifierDesc_Header_BPR': get_dfi_id_number_qualifier_description(payee_dfi_qualifier),
        'CHK_PayeeDFI_ID_Header_BPR': payee_dfi_id,
        'CHK_PayeeAccountQualifier_Header_BPR': payee_account_qualifier,
        'CHK_PayeeAccountQualifierDesc_Header_BPR': get_account_number_qualifier_description(payee_account_qualifier),
        'CHK_PayeeAccountNumber_Header_BPR': payee_account_number,
        'CHK_OriginatingCompanyID_Header_BPR': originating_company_id,
        'CHK_OriginatingCompanySupplemental_Header_BPR': originating_company_supplemental,
        'CHK_EffectiveDate_Header_BPR': check_eft_effective_date,
        'CHK_CreditDebitFlag_Header_BPR': credit_debit_flag,
        'CHK_CreditDebitFlagDesc_Header_BPR': get_credit_debit_indicator_description(credit_debit_flag),
        'CHK_BusinessFunctionCode_Header_BPR': business_function_code,
        'CHK_BusinessFunctionDesc_Header_BPR': business_function_desc,
        'CHK_DFI_Qualifier_3_Header_BPR': dfi_id_qualifier_3,
        'CHK_DFI_Qualifier_3_Desc_Header_BPR': get_dfi_id_number_qualifier_description(dfi_id_qualifier_3),
        'CHK_DFI_ID_3_Header_BPR': dfi_id_3,
        'CHK_AccountQualifier_3_Header_BPR': account_qualifier_3,
        'CHK_AccountQualifier_3_Desc_Header_BPR': get_account_number_qualifier_description(account_qualifier_3),
        'CHK_AccountNumber_3_Header_BPR': account_number_3,
        'File_TransactionControlNumber_Header_ST': transaction_control_number,
        'File_ImplementationConventionRef_Header_ST': implementation_convention_ref,
        # Source_File_Name REMOVED - duplicate of Filename_File
        
        # ISA/GS fields - REMOVED (duplicates of ENV_* columns above)
        # Keep ENV_*_Envelope_ISA and ENV_*_Envelope_GS for consistency
        
        # PER segments are in L1000A (Payer Loop) per X12 spec
        'Contact_BL_Function_L1000A_PER': contact_bl_function,
        'Contact_BL_FunctionDesc_L1000A_PER': get_contact_function_description(contact_bl_function),
        'Contact_BL_Name_L1000A_PER': contact_bl_name,
        'Contact_BL_Phone_Qualifier_L1000A_PER': contact_bl_comm1_qualifier,
        'Contact_BL_Phone_QualifierDesc_L1000A_PER': get_communication_number_qualifier_description(contact_bl_comm1_qualifier),
        'Contact_BL_Phone_L1000A_PER': contact_bl_comm1_number,
        'Contact_BL_Comm2_Qualifier_L1000A_PER': contact_bl_comm2_qualifier,
        'Contact_BL_Comm2_QualifierDesc_L1000A_PER': get_communication_number_qualifier_description(contact_bl_comm2_qualifier),
        'Contact_BL_Comm2_Number_L1000A_PER': contact_bl_comm2_number,
        'Contact_CX_Function_L1000A_PER': contact_cx_function,
        'Contact_CX_FunctionDesc_L1000A_PER': get_contact_function_description(contact_cx_function),
        'Contact_CX_Name_L1000A_PER': contact_cx_name,
        'Contact_CX_Phone_Qualifier_L1000A_PER': contact_cx_comm1_qualifier,
        'Contact_CX_Phone_QualifierDesc_L1000A_PER': get_communication_number_qualifier_description(contact_cx_comm1_qualifier),
        'Contact_CX_Phone_L1000A_PER': contact_cx_comm1_number,
        'Contact_CX_Comm2_Qualifier_L1000A_PER': contact_cx_comm2_qualifier,
        'Contact_CX_Comm2_QualifierDesc_L1000A_PER': get_communication_number_qualifier_description(contact_cx_comm2_qualifier),
        'Contact_CX_Comm2_Number_L1000A_PER': contact_cx_comm2_number,
        'Contact_IC_Function_L1000A_PER': contact_ic_function,
        'Contact_IC_FunctionDesc_L1000A_PER': get_contact_function_description(contact_ic_function),
        'Contact_IC_Name_L1000A_PER': contact_ic_name,
        'Contact_IC_Phone_Qualifier_L1000A_PER': contact_ic_comm1_qualifier,
        'Contact_IC_Phone_QualifierDesc_L1000A_PER': get_communication_number_qualifier_description(contact_ic_comm1_qualifier),
        'Contact_IC_Phone_L1000A_PER': contact_ic_comm1_number,
        'Contact_IC_Comm2_Qualifier_L1000A_PER': contact_ic_comm2_qualifier,
        'Contact_IC_Comm2_QualifierDesc_L1000A_PER': get_communication_number_qualifier_description(contact_ic_comm2_qualifier),
        'Contact_IC_Comm2_Number_L1000A_PER': contact_ic_comm2_number,

        # Fix Issue 6: Header payer from N1*PR (Loop 1000A) - payer issuing 835
        'Payer_EntityIDCode_L1000A_N1': payer_entity_id_code,
        'Payer_Name_L1000A_N1': header_payer_name,  # Header/check-level payer
        'Payer_Address_L1000A_N3': payer_address,
        'Payer_Address2_L1000A_N3': payer_address2,
        'Payer_City_L1000A_N4': payer_city,
        'Payer_State_L1000A_N4': payer_state,
        'Payer_Zip_L1000A_N4': payer_zip,
        'Payer_CountryCode_L1000A_N4': payer_country_code,
        'Payer_LocationQualifier_L1000A_N4': payer_location_qualifier,
        'Payer_LocationID_L1000A_N4': payer_location_id,
        'Payer_CountrySubdivision_L1000A_N4': payer_country_subdivision,

        # Fix Issue 6: Claim-specific payer from NM1*PR (Loop 2100) - may differ from header
        'CLM_PayerName_L2100_NM1PR': claim.get('claim_payer_name', '') if claim else '',
        'CLM_PayerID_L2100_NM1PR': claim.get('claim_payer_id', '') if claim else '',
        'CLM_PayerIDQualifier_L2100_NM1PR': claim.get('claim_payer_id_qualifier', '') if claim else '',

        # Effective payer (use claim payer if present, otherwise header payer)
        'Effective_PayerName': claim.get('claim_payer_name', header_payer_name) if claim else header_payer_name,
        'CHK_PayerID_L1000A_REF': payer_info.get('PayerID', ''),
        'Payer_IDQualifier_L1000A_REF': payer_id_qualifier,
        'Payer_IDQualifierDesc_L1000A_REF': get_id_code_qualifier_description(payer_id_qualifier),
        'Payer_IDCode_L1000A_REF': payer_id_code,
        'Payer_EntityRelationship_L1000A_REF': payer_entity_relationship,
        'Payer_EntityRelationshipDesc_L1000A_REF': payer_entity_relationship_desc,
        'Payer_EntityIDSecondary_L1000A_REF': payer_entity_id_secondary,
        'SecondaryPayer_Name_L1000A_N1': secondary_payer_info.get('SecondaryPayerName', ''),
        'SecondaryPayer_IDQualifier_L1000A_REF': secondary_payer_info.get('SecondaryPayerIDQualifier', ''),
        'SecondaryPayer_IDQualifierDesc_L1000A_REF': get_id_code_qualifier_description(secondary_payer_info.get('SecondaryPayerIDQualifier', '')),
        'SecondaryPayer_ID_L1000A_REF': secondary_payer_info.get('SecondaryPayerID', ''),
        'Payer_AdditionalID_L1000A_REF': payer_additional_id,
        
        # Payer RDM fields (L1000A)
        'Payer_ReportTransmissionCode_L1000A_RDM': payer_rdm_data.get('report_transmission_code', ''),
        'Payer_ContactName_L1000A_RDM': payer_rdm_data.get('name', ''),
        'Payer_CommunicationNumber_L1000A_RDM': payer_rdm_data.get('communication_number', ''),
        'Payer_ReferenceID_L1000A_RDM': payer_rdm_data.get('reference_identifier', ''),
        'Payer_ReferenceID2_L1000A_RDM': payer_rdm_data.get('reference_identifier_2', ''),
        'Payer_CommunicationNumber2_L1000A_RDM': payer_rdm_data.get('communication_number_2', ''),
        'Payer_ContactFunctionCode_L1000A_RDM': payer_rdm_data.get('contact_function_code', ''),
        'Payer_ContactFunctionCodeDesc_L1000A_RDM': get_contact_function_description(payer_rdm_data.get('contact_function_code', '')),
        
        # Payer N2 fields (L1000A)
        'Payer_AdditionalName1_L1000A_N2': payer_n2_data.get('name_line_1', ''),
        'Payer_AdditionalName2_L1000A_N2': payer_n2_data.get('name_line_2', ''),

        'Provider_EntityIDCode_L1000B_N1': provider_info.get('ProviderEntityIDCode', ''),
        'Provider_Name_L1000B_N1': provider_info.get('ProviderName', ''),
        'Provider_IDQualifier_L1000B_N1': provider_info.get('ProviderIDQualifier', ''),
        'Provider_IDCode_L1000B_N1': provider_info.get('ProviderIDCode', ''),
        'Provider_Address_L1000B_N3': provider_info.get('ProviderAddress', ''),
        'Provider_Address2_L1000B_N3': provider_info.get('ProviderAddress2', ''),
        'Provider_City_L1000B_N4': provider_info.get('ProviderCity', ''),
        'Provider_State_L1000B_N4': provider_info.get('ProviderState', ''),
        'Provider_Zip_L1000B_N4': provider_info.get('ProviderZip', ''),
        'Provider_CountryCode_L1000B_N4': provider_info.get('ProviderCountryCode', ''),
        'Provider_LocationQualifier_L1000B_N4': provider_info.get('ProviderLocationQualifier', ''),
        'Provider_LocationID_L1000B_N4': provider_info.get('ProviderLocationID', ''),
        'Provider_CountrySubdivision_L1000B_N4': provider_info.get('ProviderCountrySubdivision', ''),
        'Provider_TIN_L1000B_REF': provider_info.get('ProviderTIN', ''),
        'Provider_SecondaryID_L1000B_REF': provider_info.get('ProviderSecondaryID', ''),
        'Provider_TaxID_L1000B_REF': provider_info.get('ProviderTaxID', ''),
        'Provider_EntityRelationship_L1000B_REF': provider_info.get('EntityRelationship', ''),
        'Provider_EntityRelationshipDesc_L1000B_REF': provider_info.get('EntityRelationshipDesc', ''),
        'Provider_EntityIDSecondary_L1000B_REF': provider_info.get('EntityIDSecondary', ''),
        
        # Payee RDM fields (L1000B)
        'Payee_ReportTransmissionCode_L1000B_RDM': payee_rdm_data.get('report_transmission_code', ''),
        'Payee_ContactName_L1000B_RDM': payee_rdm_data.get('name', ''),
        'Payee_CommunicationNumber_L1000B_RDM': payee_rdm_data.get('communication_number', ''),
        'Payee_ReferenceID_L1000B_RDM': payee_rdm_data.get('reference_identifier', ''),
        'Payee_ReferenceID2_L1000B_RDM': payee_rdm_data.get('reference_identifier_2', ''),
        'Payee_CommunicationNumber2_L1000B_RDM': payee_rdm_data.get('communication_number_2', ''),
        'Payee_ContactFunctionCode_L1000B_RDM': payee_rdm_data.get('contact_function_code', ''),
        'Payee_ContactFunctionCodeDesc_L1000B_RDM': get_contact_function_description(payee_rdm_data.get('contact_function_code', '')),
        
        # Payee N2 fields (L1000B)
        'Payee_AdditionalName1_L1000B_N2': payee_n2_data.get('name_line_1', ''),
        'Payee_AdditionalName2_L1000B_N2': payee_n2_data.get('name_line_2', ''),

        'CLM_PatientControlNumber_L2100_CLP': claim['claim_number'],
        'RUN': format_run_number(claim['claim_number']),
        'CLM_Occurrence_L2100_CLP': claim.get('claim_occurrence', 1),
        'CLM_Status_L2100_CLP': claim['claim_status'],
        'CLM_StatusDescr_L2100_CLP': claim['claim_status_desc'],
        'CLM_FilingIndicator_L2100_CLP': claim['filing_indicator'],
        'CLM_FilingIndicatorDesc_L2100_CLP': claim['filing_indicator_desc'],
        'CLM_PayerControlNumber_L2100_CLP': claim['payer_claim_control'],
        # Institutional facility type codes - not used for professional claims
        'CLM_FacilityTypeCode_L2100_CLP': claim.get('facility_type_code', ''),
        'CLM_FacilityTypeDesc_L2100_CLP': get_facility_type_description(claim.get('facility_type_code', '')),
        'CLM_FrequencyCode_L2100_CLP': claim.get('claim_frequency_code', ''),
        'CLM_FrequencyCodeDesc_L2100_CLP': claim.get('claim_frequency_desc', ''),
        'CLM_ChargeAmount_L2100_CLP': claim['total_charged'],
        'CLM_PaymentAmount_L2100_CLP': claim['total_paid'],
        'CLM_PatientResponsibility_L2100_CLP': claim['patient_responsibility'],
        'CLM_IsReversal_L2100_CLP': 'Yes' if claim.get('claim_status', '') in ['22', '4'] and float(claim.get('total_charged', '0') or '0') < 0 else 'No',
        'CLM_ReversalIndicator_L2100_CLP': claim.get('claim_status', '') if claim.get('claim_status', '') in ['22', '4'] else '',
        # Patient condition/discharge status (CLP*10)
        'CLM_PatientConditionCode_L2100_CLP': claim.get('patient_condition_code', ''),
        'CLM_PatientConditionDesc_L2100_CLP': claim.get('patient_condition_desc', ''),
        # DRG fields (CLP*11-13)
        'CLM_DRGCode_L2100_CLP': claim.get('drg_code', ''),
        'CLM_DRGWeight_L2100_CLP': claim.get('drg_weight', ''),
        'CLM_DischargeFraction_L2100_CLP': claim.get('discharge_fraction', ''),
        'CLM_YesNoCondition_L2100_CLP': claim.get('yes_no_condition', ''),
        'CLM_YesNoConditionDesc_L2100_CLP': claim.get('yes_no_condition_desc', ''),
        # Foreign currency exchange rate - not used for US professional claims
        # 'CLM_ExchangeRate_L2100_CLP': claim.get('exchange_rate', ''),
        'CLM_PaymentTypology_L2100_CLP': claim.get('payment_typology', ''),
        'CLM_PaymentTypologyDesc_L2100_CLP': claim.get('payment_typology_desc', ''),

        'CLM_Contractual_L2100_CAS': f"{claim_adjustments['Contractual']:.2f}",
        'CLM_Copay_L2100_CAS': f"{claim_adjustments['Copay']:.2f}",
        'CLM_Coinsurance_L2100_CAS': f"{claim_adjustments['Coinsurance']:.2f}",
        'CLM_Deductible_L2100_CAS': f"{claim_adjustments['Deductible']:.2f}",
        'CLM_Denied_L2100_CAS': f"{claim_adjustments['Denied']:.2f}",
        'CLM_OtherAdjustments_L2100_CAS': f"{claim_adjustments['OtherAdjustments']:.2f}",
        'CLM_Sequestration_L2100_CAS': f"{claim_adjustments['Sequestration']:.2f}",
        'CLM_COB_L2100_CAS': f"{claim_adjustments['COB']:.2f}",
        'CLM_HCRA_L2100_CAS': f"{claim_adjustments['HCRA']:.2f}",
        'CLM_QMB_L2100_CAS': f"{claim_adjustments['QMB']:.2f}",
        'CLM_AuditFlag_L2100_CAS': claim_adjustments['AuditFlag'],
        'CLM_Adjustments_L2100_CAS': '; '.join(claim_adj_list),
        
        # Claim-level CAS occurrence fields
        'CLM_CAS1_Group_L2100_CAS': claim.get('_cas', [{}])[0].get('group_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 0 else '',
        'CLM_CAS1_Reason_L2100_CAS': claim.get('_cas', [{}])[0].get('reason_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 0 else '',
        'CLM_CAS1_Amount_L2100_CAS': claim.get('_cas', [{}])[0].get('amount', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 0 else '',
        'CLM_CAS1_Quantity_L2100_CAS': claim.get('_cas', [{}])[0].get('quantity', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 0 else '',
        'CLM_CAS1_ReasonDesc_L2100_CAS': get_carc_description(claim.get('_cas', [{}])[0].get('reason_code', ''), payer_key=payer_key) if claim.get('_cas') and len(claim.get('_cas', [])) > 0 else '',
        
        'CLM_CAS2_Group_L2100_CAS': claim.get('_cas', [{}])[1].get('group_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 1 else '',
        'CLM_CAS2_Reason_L2100_CAS': claim.get('_cas', [{}])[1].get('reason_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 1 else '',
        'CLM_CAS2_Amount_L2100_CAS': claim.get('_cas', [{}])[1].get('amount', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 1 else '',
        'CLM_CAS2_Quantity_L2100_CAS': claim.get('_cas', [{}])[1].get('quantity', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 1 else '',
        'CLM_CAS2_ReasonDesc_L2100_CAS': get_carc_description(claim.get('_cas', [{}])[1].get('reason_code', ''), payer_key=payer_key) if claim.get('_cas') and len(claim.get('_cas', [])) > 1 else '',
        
        'CLM_CAS3_Group_L2100_CAS': claim.get('_cas', [{}])[2].get('group_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 2 else '',
        'CLM_CAS3_Reason_L2100_CAS': claim.get('_cas', [{}])[2].get('reason_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 2 else '',
        'CLM_CAS3_Amount_L2100_CAS': claim.get('_cas', [{}])[2].get('amount', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 2 else '',
        'CLM_CAS3_Quantity_L2100_CAS': claim.get('_cas', [{}])[2].get('quantity', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 2 else '',
        'CLM_CAS3_ReasonDesc_L2100_CAS': get_carc_description(claim.get('_cas', [{}])[2].get('reason_code', ''), payer_key=payer_key) if claim.get('_cas') and len(claim.get('_cas', [])) > 2 else '',
        
        'CLM_CAS4_Group_L2100_CAS': claim.get('_cas', [{}])[3].get('group_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 3 else '',
        'CLM_CAS4_Reason_L2100_CAS': claim.get('_cas', [{}])[3].get('reason_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 3 else '',
        'CLM_CAS4_Amount_L2100_CAS': claim.get('_cas', [{}])[3].get('amount', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 3 else '',
        'CLM_CAS4_Quantity_L2100_CAS': claim.get('_cas', [{}])[3].get('quantity', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 3 else '',
        'CLM_CAS4_ReasonDesc_L2100_CAS': get_carc_description(claim.get('_cas', [{}])[3].get('reason_code', ''), payer_key=payer_key) if claim.get('_cas') and len(claim.get('_cas', [])) > 3 else '',
        
        'CLM_CAS5_Group_L2100_CAS': claim.get('_cas', [{}])[4].get('group_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 4 else '',
        'CLM_CAS5_Reason_L2100_CAS': claim.get('_cas', [{}])[4].get('reason_code', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 4 else '',
        'CLM_CAS5_Amount_L2100_CAS': claim.get('_cas', [{}])[4].get('amount', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 4 else '',
        'CLM_CAS5_Quantity_L2100_CAS': claim.get('_cas', [{}])[4].get('quantity', '') if claim.get('_cas') and len(claim.get('_cas', [])) > 4 else '',
        'CLM_CAS5_ReasonDesc_L2100_CAS': get_carc_description(claim.get('_cas', [{}])[4].get('reason_code', ''), payer_key=payer_key) if claim.get('_cas') and len(claim.get('_cas', [])) > 4 else '',

        'CLM_InterestAmount_L2100_AMT': claim_amt_values.get('interest_amount', ''),
        'CLM_CoverageAmount_L2100_AMT': claim_amt_values.get('coverage_amount', ''),
        'CLM_TaxAmount_L2100_AMT': claim_amt_values.get('tax_amount', ''),
        'CLM_PatientAmountPaid_L2100_AMT': claim_amt_values.get('patient_amount_paid', ''),
        
        # Additional AMT fields
        'CLM_DiscountAmount_L2100_AMT': claim_amt_values.get('discount_amount', ''),
        'CLM_PerDayLimitAmount_L2100_AMT': claim_amt_values.get('per_day_limit_amount', ''),
        'CLM_DeductionAmount_L2100_AMT': claim_amt_values.get('deduction_amount', ''),
        'CLM_NetBilledAmount_L2100_AMT': claim_amt_values.get('net_billed_amount', ''),
        'CLM_TotalClaimBeforeTaxes_L2100_AMT': claim_amt_values.get('total_claim_before_taxes', ''),
        'CLM_FederalMandateAmount1_L2100_AMT': claim_amt_values.get('federal_mandate_amount_1', ''),
        'CLM_FederalMandateAmount2_L2100_AMT': claim_amt_values.get('federal_mandate_amount_2', ''),
        'CLM_FederalMandateAmount3_L2100_AMT': claim_amt_values.get('federal_mandate_amount_3', ''),
        'CLM_FederalMandateAmount4_L2100_AMT': claim_amt_values.get('federal_mandate_amount_4', ''),
        'CLM_FederalMandateAmount5_L2100_AMT': claim_amt_values.get('federal_mandate_amount_5', ''),

        'CLM_MemberID_L2100_NM1': subscriber_info['MemberID'],
        'CLM_SubscriberName_L2100_NM1': subscriber_info['SubscriberName'],
        'CLM_PatientName_L2100_NM1': subscriber_info['PatientName'],
        'CLM_SSN_L2100_NM1': subscriber_info['SSN'],
        'CLM_CorrectedInsuredName_L2100_NM1': corrected_insured_name,
        'CLM_CorrectedInsuredSuffix_L2100_NM1': corrected_insured_suffix,
        'CLM_CorrectedInsuredIDQualifier_L2100_NM1': corrected_insured_id_qualifier,
        'CLM_CorrectedInsuredIDQualifierDesc_L2100_NM1': get_id_code_qualifier_description(corrected_insured_id_qualifier),
        'CLM_CorrectedInsuredID_L2100_NM1': corrected_insured_id,

        'CLM_PriorAuth_L2100_REF': claim_ref_values.get('prior_auth', ''),
        'CLM_ClaimNumber_L2100_REF': claim_ref_values.get('claim_number', ''),
        'CLM_ProviderControl_L2100_REF': claim_ref_values.get('provider_control', ''),
        'CLM_OriginalRef_L2100_REF': claim_ref_values.get('original_ref', ''),
        'CLM_ReferralNumber_L2100_REF': claim_ref_values.get('referral_number', ''),
        'CLM_MedicalRecord_L2100_REF': claim_ref_values.get('medical_record', ''),
        'CLM_GroupNumber_L2100_REF': claim_ref_values.get('group_number', ''),
        'CLM_PlanName_L2100_REF': claim_ref_values.get('plan_name', ''),
        'CLM_AuthorizationNumber_L2100_REF': claim_ref_values.get('authorization_number', ''),
        'CLM_PlanType_L2100_REF': claim_ref_values.get('plan_type', ''),
        
        # Additional REF fields  
        'CLM_StateMedicalAssistanceNumber_L2100_REF': claim_ref_values.get('state_medical_assistance_number', ''),
        'CLM_BlueCrossProviderNumber_L2100_REF': claim_ref_values.get('blue_cross_provider_number', ''),
        'CLM_BlueShieldProviderNumber_L2100_REF': claim_ref_values.get('blue_shield_provider_number', ''),
        'CLM_MedicareProviderNumber_L2100_REF': claim_ref_values.get('medicare_provider_number', ''),
        'CLM_MedicaidProviderNumber_L2100_REF': claim_ref_values.get('medicaid_provider_number', ''),
        'CLM_ProviderUPINNumber_L2100_REF': claim_ref_values.get('provider_upin_number', ''),
        'CLM_CHAMPUSIdentificationNumber_L2100_REF': claim_ref_values.get('champus_identification_number', ''),
        # Institutional-only facility ID - not used for professional claims
        # 'CLM_FacilityIDNumber_L2100_REF': claim_ref_values.get('facility_id_number', ''),
        'CLM_PayersClaimNumber_L2100_REF': claim_ref_values.get('payers_claim_number', ''),
        'CLM_EmployeeIdentificationNumber_L2100_REF': claim_ref_values.get('employee_identification_number', ''),
        'CLM_InsurancePolicyNumber_L2100_REF': claim_ref_values.get('insurance_policy_number', ''),
        'CLM_PayeeIdentification_L2100_REF': claim_ref_values.get('payee_identification', ''),
        'CLM_RepricedClaimRefNumber_L2100_REF': claim_ref_values.get('repriced_claim_ref_number', ''),
        'CLM_RepricedLineItemRefNumber_L2100_REF': claim_ref_values.get('repriced_line_item_ref_number', ''),
        'CLM_AmbulatoryPaymentClassification_L2100_REF': claim_ref_values.get('ambulatory_payment_classification', ''),
        'CLM_NAICCode_L2100_REF': claim_ref_values.get('naic_code', ''),
        'CLM_HPID_L2100_REF': claim_ref_values.get('hpid', ''),

        'CLM_ServiceStartDate_L2100_DTM': claim_dtm_values.get('service_start', ''),
        'CLM_ServiceEndDate_L2100_DTM': claim_dtm_values.get('service_end', ''),
        'CLM_StatementStartDate_L2100_DTM': claim_dtm_values.get('statement_start', ''),
        'CLM_StatementEndDate_L2100_DTM': claim_dtm_values.get('statement_end', ''),
        'CLM_ReceivedDate_L2100_DTM': claim_dtm_values.get('received_date', ''),
        
        # Additional DTM fields
        'CLM_ProcessDate_L2100_DTM': claim_dtm_values.get('process_date', ''),
        # Institutional-only dates - not used for professional claims
        # 'CLM_DischargeDate_L2100_DTM': claim_dtm_values.get('discharge_date', ''),
        # 'CLM_AdmissionDate_L2100_DTM': claim_dtm_values.get('admission_date', ''),
        'CLM_StatementFromDate_L2100_DTM': claim_dtm_values.get('statement_from_date', ''),

        # DTM time fields (comma-separated list if multiple)
        'CLM_DTM_Times_L2100_DTM': ', '.join([d.get('time', '') for d in claim.get('_dtm', []) if d.get('time', '')]) if claim else '',
        'CLM_DTM_TimeCodes_L2100_DTM': ', '.join([d.get('time_code', '') for d in claim.get('_dtm', []) if d.get('time_code', '')]) if claim else '',
        'CLM_DTM_PeriodFormats_L2100_DTM': ', '.join([d.get('date_time_period_format', '') for d in claim.get('_dtm', []) if d.get('date_time_period_format', '')]) if claim else '',
        'CLM_DTM_Periods_L2100_DTM': ', '.join([d.get('date_time_period', '') for d in claim.get('_dtm', []) if d.get('date_time_period', '')]) if claim else '',
        'CLM_ExpirationDate_L2100_DTM': claim_dtm_values.get('expiration_date', ''),
        
        # QTY values - Institutional-only day counts
        # 'CLM_CoveredActualDays_L2100_QTY': covered_actual_days,
        # 'CLM_LifetimeReserveDays_L2100_QTY': lifetime_reserve_days,
        # 'CLM_CoInsuranceDays_L2100_QTY': coinsurance_days,

        'CLM_RemarkCodes_L2100_LQ': ', '.join(claim_remark_codes),
        'CLM_RemarkDescriptions_L2100_LQ': '; '.join(claim_remark_desc),
        # LQ*HE - Healthcare Remark Codes (RARC codes)
        'CLM_HealthcareRemarkCodes_L2100_LQ': ', '.join(extract_lq_codes_by_qualifier(claim.get('_lq', []), 'HE')),
        'CLM_HealthcareRemarkDescriptions_L2100_LQ': '; '.join([get_remark_code_description(c, payer_key=payer_key) for c in extract_lq_codes_by_qualifier(claim.get('_lq', []), 'HE')]),
        # QTY*CA - Covered Actual Days/Units
        'CLM_CoveredActual_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'CA'),
        'CLM_ContactFunction_L2100_PER': claim.get('_per', [{}])[0].get('contact_function_code', '') if claim.get('_per') else '',
        'CLM_ContactFunctionDesc_L2100_PER': get_contact_function_description(claim.get('_per', [{}])[0].get('contact_function_code', '')) if claim.get('_per') else '',
        'CLM_ContactName_L2100_PER': claim.get('_per', [{}])[0].get('name', '') if claim.get('_per') else '',
        'CLM_ContactPhone_Qualifier_L2100_PER': claim.get('_per', [{}])[0].get('communication', [{}])[0].get('qualifier', '') if claim.get('_per') and claim.get('_per', [{}])[0].get('communication') else '',
        'CLM_ContactPhone_QualifierDesc_L2100_PER': get_communication_number_qualifier_description(claim.get('_per', [{}])[0].get('communication', [{}])[0].get('qualifier', '')) if claim.get('_per') and claim.get('_per', [{}])[0].get('communication') else '',
        'CLM_ContactPhone_L2100_PER': claim.get('_per', [{}])[0].get('communication', [{}])[0].get('number', '') if claim.get('_per') and claim.get('_per', [{}])[0].get('communication') else '',
        'ServiceProvider_EntityCode_L2100_NM1': claim.get('_service_provider', {}).get('entity_code', ''),
        'ServiceProvider_EntityCodeDesc_L2100_NM1': get_entity_identifier_description(claim.get('_service_provider', {}).get('entity_code', ''), payer_key=payer_key),
        'ServiceProvider_Name_L2100_NM1': claim.get('_service_provider', {}).get('name', ''),
        'ServiceProvider_IDQualifier_L2100_NM1': claim.get('_service_provider', {}).get('id_qualifier', ''),
        'ServiceProvider_IDQualifierDesc_L2100_NM1': get_id_code_qualifier_description(claim.get('_service_provider', {}).get('id_qualifier', '')),
        'ServiceProvider_IDCode_L2100_NM1': claim.get('_service_provider', {}).get('id_code', ''),
        'ServiceProvider_EntityRelationship_L2100_NM1': claim.get('_service_provider', {}).get('entity_relationship', ''),
        'ServiceProvider_EntityRelationshipDesc_L2100_NM1': get_entity_relationship_description(claim.get('_service_provider', {}).get('entity_relationship', '')),
        'ServiceProvider_EntityIDSecondary_L2100_NM1': claim.get('_service_provider', {}).get('entity_id_secondary', ''),

        'SVC_ProcedureCode_L2110_SVC': service.get('procedure_code', '') if service else '',
        'SVC_CodeDescription_L2110_SVC': get_ambulance_code_description(service.get('procedure_code', '')) if service and is_transportation_code(service.get('procedure_code', '')) else '',
        'SVC_ServiceLevel_L2110_SVC': get_ambulance_service_level_name(service.get('procedure_code', '')) if service and is_transportation_code(service.get('procedure_code', '')) else '',
        'SVC_Modifier1_L2110_SVC': service.get('modifier1', '') if service else '',
        'SVC_Modifier2_L2110_SVC': service.get('modifier2', '') if service else '',
        'SVC_Modifier3_L2110_SVC': service.get('modifier3', '') if service else '',
        'SVC_Modifier4_L2110_SVC': service.get('modifier4', '') if service else '',
        'SVC_Modifiers_L2110_SVC': ', '.join(service.get('modifiers', [])) if service else '',  
        'SVC_ModifierDescriptions_L2110_SVC': '; '.join([get_ambulance_modifier_description(m) for m in service.get('modifiers', [])]) if service else '',
        'SVC_ChargeAmount_L2110_SVC': service.get('line_charged', '') if service else '',
        'SVC_PaymentAmount_L2110_SVC': service.get('line_paid', '') if service else '',
        'SEQ': '',  # Populated with sequential row number at CSV write time
        'SVC_Units_L2110_SVC': service.get('units', '') if service else '',
        'SVC_Qualifier_L2110_SVC': service.get('qualifier', '') if service else '',
        'SVC_QualifierDesc_L2110_SVC': service_qualifier_desc,
        'SVC_OriginalProcedure_L2110_SVC': service.get('original_procedure', '') if service else '',
        'SVC_OriginalUnits_L2110_SVC': service.get('original_units', '') if service else '',

        'SVC_Contractual_L2110_CAS': f"{service_adjustments['Contractual']:.2f}" if service else '',
        'SVC_Copay_L2110_CAS': f"{service_adjustments['Copay']:.2f}" if service else '',
        'SVC_Coinsurance_L2110_CAS': f"{service_adjustments['Coinsurance']:.2f}" if service else '',
        'SVC_Deductible_L2110_CAS': f"{service_adjustments['Deductible']:.2f}" if service else '',
        'SVC_Denied_L2110_CAS': f"{service_adjustments['Denied']:.2f}" if service else '',
        'SVC_OtherAdjustments_L2110_CAS': f"{service_adjustments['OtherAdjustments']:.2f}" if service else '',
        'SVC_Sequestration_L2110_CAS': f"{service_adjustments['Sequestration']:.2f}" if service else '',
        'SVC_COB_L2110_CAS': f"{service_adjustments['COB']:.2f}" if service else '',
        'SVC_HCRA_L2110_CAS': f"{service_adjustments['HCRA']:.2f}" if service else '',
        'SVC_QMB_L2110_CAS': f"{service_adjustments['QMB']:.2f}" if service else '',
        'SVC_AuditFlag_L2110_CAS': service_adjustments['AuditFlag'] if service else '',
        'SVC_Adjustments_L2110_CAS': '; '.join(service_adj_list) if service else '',
        # SVC_AllAdjustments_L2110_CAS REMOVED - duplicate of SVC_Adjustments_L2110_CAS
        'SVC_AdjustmentCount_L2110_CAS': len(service['_cas']) if service and '_cas' in service else 0,
        'SVC_CAS1_Group_L2110_CAS': service['_cas'][0]['group_code'] if service and '_cas' in service and len(service['_cas']) > 0 else '',
        'SVC_CAS1_GroupDesc_L2110_CAS': get_cas_group_code_description(service['_cas'][0]['group_code']) if service and '_cas' in service and len(service['_cas']) > 0 else '',
        'SVC_CAS1_Reason_L2110_CAS': service['_cas'][0]['reason_code'] if service and '_cas' in service and len(service['_cas']) > 0 else '',
        'SVC_CAS1_Amount_L2110_CAS': service['_cas'][0]['amount'] if service and '_cas' in service and len(service['_cas']) > 0 else '',
        'SVC_CAS1_ReasonDesc_L2110_CAS': get_carc_description(service['_cas'][0]['reason_code'], payer_key=payer_key) if service and '_cas' in service and len(service['_cas']) > 0 else '',
        'SVC_CAS2_Group_L2110_CAS': service['_cas'][1]['group_code'] if service and '_cas' in service and len(service['_cas']) > 1 else '',
        'SVC_CAS2_GroupDesc_L2110_CAS': get_cas_group_code_description(service['_cas'][1]['group_code']) if service and '_cas' in service and len(service['_cas']) > 1 else '',
        'SVC_CAS2_Reason_L2110_CAS': service['_cas'][1]['reason_code'] if service and '_cas' in service and len(service['_cas']) > 1 else '',
        'SVC_CAS2_Amount_L2110_CAS': service['_cas'][1]['amount'] if service and '_cas' in service and len(service['_cas']) > 1 else '',
        'SVC_CAS2_ReasonDesc_L2110_CAS': get_carc_description(service['_cas'][1]['reason_code'], payer_key=payer_key) if service and '_cas' in service and len(service['_cas']) > 1 else '',
        'SVC_CAS3_Group_L2110_CAS': service['_cas'][2]['group_code'] if service and '_cas' in service and len(service['_cas']) > 2 else '',
        'SVC_CAS3_GroupDesc_L2110_CAS': get_cas_group_code_description(service['_cas'][2]['group_code']) if service and '_cas' in service and len(service['_cas']) > 2 else '',
        'SVC_CAS3_Reason_L2110_CAS': service['_cas'][2]['reason_code'] if service and '_cas' in service and len(service['_cas']) > 2 else '',
        'SVC_CAS3_Amount_L2110_CAS': service['_cas'][2]['amount'] if service and '_cas' in service and len(service['_cas']) > 2 else '',
        'SVC_CAS3_ReasonDesc_L2110_CAS': get_carc_description(service['_cas'][2]['reason_code'], payer_key=payer_key) if service and '_cas' in service and len(service['_cas']) > 2 else '',
        'SVC_CAS4_Group_L2110_CAS': service['_cas'][3]['group_code'] if service and '_cas' in service and len(service['_cas']) > 3 else '',
        'SVC_CAS4_GroupDesc_L2110_CAS': get_cas_group_code_description(service['_cas'][3]['group_code']) if service and '_cas' in service and len(service['_cas']) > 3 else '',
        'SVC_CAS4_Reason_L2110_CAS': service['_cas'][3]['reason_code'] if service and '_cas' in service and len(service['_cas']) > 3 else '',
        'SVC_CAS4_Amount_L2110_CAS': service['_cas'][3]['amount'] if service and '_cas' in service and len(service['_cas']) > 3 else '',
        'SVC_CAS4_ReasonDesc_L2110_CAS': get_carc_description(service['_cas'][3]['reason_code'], payer_key=payer_key) if service and '_cas' in service and len(service['_cas']) > 3 else '',
        'SVC_CAS5_Group_L2110_CAS': service['_cas'][4]['group_code'] if service and '_cas' in service and len(service['_cas']) > 4 else '',
        'SVC_CAS5_GroupDesc_L2110_CAS': get_cas_group_code_description(service['_cas'][4]['group_code']) if service and '_cas' in service and len(service['_cas']) > 4 else '',
        'SVC_CAS5_Reason_L2110_CAS': service['_cas'][4]['reason_code'] if service and '_cas' in service and len(service['_cas']) > 4 else '',
        'SVC_CAS5_Amount_L2110_CAS': service['_cas'][4]['amount'] if service and '_cas' in service and len(service['_cas']) > 4 else '',
        'SVC_CAS5_ReasonDesc_L2110_CAS': get_carc_description(service['_cas'][4]['reason_code'], payer_key=payer_key) if service and '_cas' in service and len(service['_cas']) > 4 else '',

        'SVC_PriorAuth_L2110_REF': service_ref_values.get('prior_auth', '') if service else '',
        'SVC_ProviderControl_L2110_REF': service_ref_values.get('provider_control', '') if service else '',
        'SVC_LineItemControl_L2110_REF': service_ref_values.get('line_item_control', '') if service else '',
        # Repricing Reference Numbers
        'SVC_RepricedClaimRefNumber_L2110_REF': service_ref_values.get('repriced_claim_ref_number', '') if service else '',
        'SVC_RepricedLineItemRefNumber_L2110_REF': service_ref_values.get('repriced_line_item_ref_number', '') if service else '',
        'SVC_AmbulatoryPaymentClassification_L2110_REF': service_ref_values.get('ambulatory_payment_classification', '') if service else '',
        'SVC_NAICCode_L2110_REF': service_ref_values.get('naic_code', '') if service else '',
        # Institutional facility type codes - not used for professional claims
        # 'SVC_FacilityTypeCode_L2110_REF': service_ref_values.get('facility_type', '') if service else '',
        # 'SVC_FacilityTypeDesc_L2110_REF': get_facility_type_description(service_ref_values.get('facility_type', '')) if service else '',

        'SVC_ServiceStartDate_L2110_DTM': service_start,
        'SVC_ServiceEndDate_L2110_DTM': service_end,

        # SVC DTM time fields (comma-separated list if multiple)
        'SVC_DTM_Times_L2110_DTM': ', '.join([d.get('time', '') for d in service.get('_dtm', []) if d.get('time', '')]) if service else '',
        'SVC_DTM_TimeCodes_L2110_DTM': ', '.join([d.get('time_code', '') for d in service.get('_dtm', []) if d.get('time_code', '')]) if service else '',
        'SVC_DTM_PeriodFormats_L2110_DTM': ', '.join([d.get('date_time_period_format', '') for d in service.get('_dtm', []) if d.get('date_time_period_format', '')]) if service else '',
        'SVC_DTM_Periods_L2110_DTM': ', '.join([d.get('date_time_period', '') for d in service.get('_dtm', []) if d.get('date_time_period', '')]) if service else '',

        'SVC_RemarkCodes_L2110_LQ': ', '.join(service_remark_codes) if service else '',
        'SVC_RemarkDescriptions_L2110_LQ': '; '.join(service_remark_desc) if service else '',
        # LQ*HE - Healthcare Remark Codes (RARC codes)
        'SVC_HealthcareRemarkCodes_L2110_LQ': ', '.join(extract_lq_codes_by_qualifier(service.get('_lq', []), 'HE')) if service else '',
        'SVC_HealthcareRemarkDescriptions_L2110_LQ': '; '.join([get_remark_code_description(c, payer_key=payer_key) for c in extract_lq_codes_by_qualifier(service.get('_lq', []), 'HE')]) if service else '',
        'SVC_AllowedAmount_L2110_AMT': extract_amt_value(service.get('_amt', []), 'B6') if service else '',
        'SVC_TaxAmount_L2110_AMT': extract_amt_value(service.get('_amt', []), 'T') if service else '',
        'SVC_AmbulancePatientCount_L2110_QTY': extract_qty_value(service.get('_qty', []), 'PT') if service else '',
        # QTY*CA - Covered Actual Days/Units
        'SVC_CoveredActual_L2110_QTY': extract_qty_value(service.get('_qty', []), 'CA') if service else '',
        # 'SVC_ObstetricAnesthesiaUnits_L2110_QTY': extract_qty_value(service.get('_qty', []), 'FL') if service else '',  # Not applicable for EMS

        'SVC_RenderingProvider_L2110_NM1': rendering_provider,

        'PLB_TotalAdjustments_PLB': calculate_plb_total(plb_adjustments),
        'PLB_Details_PLB': format_plb_details(plb_adjustments, payer_key=payer_key),
        'PLB_ProviderID_PLB': plb_adjustments[0].get('provider_identifier', '') if plb_adjustments else '',
        'PLB_FiscalPeriodDate_PLB': plb_adjustments[0].get('fiscal_period_date', '') if plb_adjustments else '',
        
        # PLB occurrence fields with composite sub-elements
        # Note: flat_plb_adjustments contains individual adjustments from all PLB segments
        'PLB_Adj1_ReasonCode_PLB': flat_plb_adjustments[0].get('reason_code', '') if len(flat_plb_adjustments) > 0 else '',
        'PLB_Adj1_RefQualifier_PLB': '',  # This would need to be parsed if qualifier is sent separately
        'PLB_Adj1_RefID_PLB': flat_plb_adjustments[0].get('reference_id', '') if len(flat_plb_adjustments) > 0 else '',
        'PLB_Adj1_Amount_PLB': flat_plb_adjustments[0].get('amount', '') if len(flat_plb_adjustments) > 0 else '',

        'PLB_Adj2_ReasonCode_PLB': flat_plb_adjustments[1].get('reason_code', '') if len(flat_plb_adjustments) > 1 else '',
        'PLB_Adj2_RefQualifier_PLB': '',  # This would need to be parsed if qualifier is sent separately
        'PLB_Adj2_RefID_PLB': flat_plb_adjustments[1].get('reference_id', '') if len(flat_plb_adjustments) > 1 else '',
        'PLB_Adj2_Amount_PLB': flat_plb_adjustments[1].get('amount', '') if len(flat_plb_adjustments) > 1 else '',

        'PLB_Adj3_ReasonCode_PLB': flat_plb_adjustments[2].get('reason_code', '') if len(flat_plb_adjustments) > 2 else '',
        'PLB_Adj3_RefQualifier_PLB': '',  # This would need to be parsed if qualifier is sent separately
        'PLB_Adj3_RefID_PLB': flat_plb_adjustments[2].get('reference_id', '') if len(flat_plb_adjustments) > 2 else '',
        'PLB_Adj3_Amount_PLB': flat_plb_adjustments[2].get('amount', '') if len(flat_plb_adjustments) > 2 else '',

        'PLB_Adj4_ReasonCode_PLB': flat_plb_adjustments[3].get('reason_code', '') if len(flat_plb_adjustments) > 3 else '',
        'PLB_Adj4_RefQualifier_PLB': '',  # This would need to be parsed if qualifier is sent separately
        'PLB_Adj4_RefID_PLB': flat_plb_adjustments[3].get('reference_id', '') if len(flat_plb_adjustments) > 3 else '',
        'PLB_Adj4_Amount_PLB': flat_plb_adjustments[3].get('amount', '') if len(flat_plb_adjustments) > 3 else '',

        'PLB_Adj5_ReasonCode_PLB': flat_plb_adjustments[4].get('reason_code', '') if len(flat_plb_adjustments) > 4 else '',
        'PLB_Adj5_RefQualifier_PLB': '',  # This would need to be parsed if qualifier is sent separately
        'PLB_Adj5_RefID_PLB': flat_plb_adjustments[4].get('reference_id', '') if len(flat_plb_adjustments) > 4 else '',
        'PLB_Adj5_Amount_PLB': flat_plb_adjustments[4].get('amount', '') if len(flat_plb_adjustments) > 4 else '',

        'PLB_Adj6_ReasonCode_PLB': flat_plb_adjustments[5].get('reason_code', '') if len(flat_plb_adjustments) > 5 else '',
        'PLB_Adj6_RefQualifier_PLB': '',  # This would need to be parsed if qualifier is sent separately
        'PLB_Adj6_RefID_PLB': flat_plb_adjustments[5].get('reference_id', '') if len(flat_plb_adjustments) > 5 else '',
        'PLB_Adj6_Amount_PLB': flat_plb_adjustments[5].get('amount', '') if len(flat_plb_adjustments) > 5 else '',

        'LX_Number_L2000_LX': claim.get('lx_header', {}).get('header_number', '') if claim.get('lx_header') else '',
        # TS3 segment is primarily for institutional provider summaries - not used for professional claims
        # 'TS3_ProviderID_L2000_TS3': claim.get('ts3', {}).get('provider_identifier', '') if claim.get('ts3') else '',
        # 'TS3_FacilityTypeCode_L2000_TS3': claim.get('ts3', {}).get('facility_type_code', '') if claim.get('ts3') else '',
        # 'TS3_FiscalPeriodEndDate_L2000_TS3': claim.get('ts3', {}).get('fiscal_period_end_date', '') if claim.get('ts3') else '',
        # 'TS3_TotalClaimCount_L2000_TS3': claim.get('ts3', {}).get('total_claim_count', '') if claim.get('ts3') else '',
        # 'TS3_TotalClaimChargeAmount_L2000_TS3': claim.get('ts3', {}).get('total_claim_charge_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalCoveredChargeAmount_L2000_TS3': claim.get('ts3', {}).get('total_covered_charge_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalNoncoveredChargeAmount_L2000_TS3': claim.get('ts3', {}).get('total_noncovered_charge_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalDeniedChargeAmount_L2000_TS3': claim.get('ts3', {}).get('total_denied_charge_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalProviderPaymentAmount_L2000_TS3': claim.get('ts3', {}).get('total_provider_payment_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalInterestAmount_L2000_TS3': claim.get('ts3', {}).get('total_interest_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalContractualAdjustmentAmount_L2000_TS3': claim.get('ts3', {}).get('total_contractual_adjustment_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalGrammRudmanReductionAmount_L2000_TS3': claim.get('ts3', {}).get('total_gramm_rudman_reduction_amount', '') if claim.get('ts3') else '',
        # Medicare Part A only TS3 fields - not used for professional claims
        # 'TS3_TotalMSPPayerAmount_L2000_TS3': claim.get('ts3', {}).get('total_msp_payer_amount', '') if claim.get('ts3') else '',  # TS313
        # 'TS3_TotalBloodDeductibleAmount_L2000_TS3': claim.get('ts3', {}).get('total_blood_deductible_amount', '') if claim.get('ts3') else '',  # TS314
        # 'TS3_TotalNoncoveredDaysCount_L2000_TS3': claim.get('ts3', {}).get('total_noncovered_days_count', '') if claim.get('ts3') else '',  # Days are institutional
        # 'TS3_TotalCoinsuranceDaysCount_L2000_TS3': claim.get('ts3', {}).get('total_coinsurance_days_count', '') if claim.get('ts3') else '',  # Days are institutional
        # 'TS3_TotalConditionalPayAmount_L2000_TS3': claim.get('ts3', {}).get('total_conditional_pay_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalPSPFSIClaimCount_L2000_TS3': claim.get('ts3', {}).get('total_psp_fsi_claim_count', '') if claim.get('ts3') else '',  # TS318
        # Institutional-only TS3 DRG-related fields - not used for professional claims
        # 'TS3_TotalPPSCapitalAmount_L2000_TS3': claim.get('ts3', {}).get('total_pps_capital_amount', '') if claim.get('ts3') else '',
        # 'TS3_PPSCapitalFSPDRGAmount_L2000_TS3': claim.get('ts3', {}).get('pps_capital_fsp_drg_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalPPSCapitalHSPDRGAmount_L2000_TS3': claim.get('ts3', {}).get('total_pps_capital_hsp_drg_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalPPSDSHDRGAmount_L2000_TS3': claim.get('ts3', {}).get('total_pps_dsh_drg_amount', '') if claim.get('ts3') else '',
        # 'TS3_TotalPIPClaimCount_L2000_TS3': claim.get('ts3', {}).get('total_pip_claim_count', '') if claim.get('ts3') else '',  # TS323
        # 'TS3_TotalPIPAdjustmentAmount_L2000_TS3': claim.get('ts3', {}).get('total_pip_adjustment_amount', '') if claim.get('ts3') else '',  # TS324
        # Institutional-only TS2 fields - not used for professional claims
        # 'TS2_TotalDRGAmount_L2000_TS2': claim.get('ts2', {}).get('total_drg_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalFederalSpecificAmount_L2000_TS2': claim.get('ts2', {}).get('total_federal_specific_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalHospitalSpecificAmount_L2000_TS2': claim.get('ts2', {}).get('total_hospital_specific_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalDisproportionateShareAmount_L2000_TS2': claim.get('ts2', {}).get('total_disproportionate_share_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalCapitalAmount_L2000_TS2': claim.get('ts2', {}).get('total_capital_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalIndirectMedicalEducationAmount_L2000_TS2': claim.get('ts2', {}).get('total_indirect_medical_education_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalOutlierDayCount_L2000_TS2': claim.get('ts2', {}).get('total_outlier_day_count', '') if claim.get('ts2') else '',
        # 'TS2_TotalDayOutlierAmount_L2000_TS2': claim.get('ts2', {}).get('total_day_outlier_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalCostOutlierAmount_L2000_TS2': claim.get('ts2', {}).get('total_cost_outlier_amount', '') if claim.get('ts2') else '',
        # 'TS2_AverageDRGLengthOfStay_L2000_TS2': claim.get('ts2', {}).get('average_drg_length_of_stay', '') if claim.get('ts2') else '',
        # 'TS2_TotalDischargeCount_L2000_TS2': claim.get('ts2', {}).get('total_discharge_count', '') if claim.get('ts2') else '',
        # 'TS2_TotalCostReportDayCount_L2000_TS2': claim.get('ts2', {}).get('total_cost_report_day_count', '') if claim.get('ts2') else '',
        # 'TS2_TotalCoveredDayCount_L2000_TS2': claim.get('ts2', {}).get('total_covered_day_count', '') if claim.get('ts2') else '',
        # 'TS2_TotalNoncoveredDayCount_L2000_TS2': claim.get('ts2', {}).get('total_noncovered_day_count', '') if claim.get('ts2') else '',
        # 'TS2_TotalMSPPassThroughAmount_L2000_TS2': claim.get('ts2', {}).get('total_msp_pass_through_amount', '') if claim.get('ts2') else '',
        # 'TS2_AverageDRGWeight_L2000_TS2': claim.get('ts2', {}).get('average_drg_weight', '') if claim.get('ts2') else '',
        # 'TS2_TotalPPSStandardAmount_L2000_TS2': claim.get('ts2', {}).get('total_pps_standard_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalPPSCapitalFSPAmount_L2000_TS2': claim.get('ts2', {}).get('total_pps_capital_fsp_amount', '') if claim.get('ts2') else '',
        # 'TS2_TotalPPSCapitalHSPAmount_L2000_TS2': claim.get('ts2', {}).get('total_pps_capital_hsp_amount', '') if claim.get('ts2') else '',
        # MOA fields (Medicare Outpatient Adjudication)
        'MOA_ReimbursementRate_L2100_MOA': claim.get('_moa', [{}])[0].get('reimbursement_rate', '') if claim.get('_moa') else '',  
        'MOA_ClaimHCPCSPayableAmount_L2100_MOA': claim.get('_moa', [{}])[0].get('claim_hcpcs_payable_amount', '') if claim.get('_moa') else '',  
        'MOA_ClaimPaymentRemarkCode1_L2100_MOA': claim.get('_moa', [{}])[0].get('claim_payment_remark_code_1', '') if claim.get('_moa') else '',  
        'MOA_ClaimPaymentRemarkCode2_L2100_MOA': claim.get('_moa', [{}])[0].get('claim_payment_remark_code_2', '') if claim.get('_moa') else '',  
        'MOA_ClaimPaymentRemarkCode3_L2100_MOA': claim.get('_moa', [{}])[0].get('claim_payment_remark_code_3', '') if claim.get('_moa') else '',  
        'MOA_ClaimPaymentRemarkCode4_L2100_MOA': claim.get('_moa', [{}])[0].get('claim_payment_remark_code_4', '') if claim.get('_moa') else '',  
        'MOA_ClaimPaymentRemarkCode5_L2100_MOA': claim.get('_moa', [{}])[0].get('claim_payment_remark_code_5', '') if claim.get('_moa') else '',
        'MOA_ClaimPaymentRemarkDesc1_L2100_MOA': get_remark_code_description(claim.get('_moa', [{}])[0].get('claim_payment_remark_code_1', ''), payer_key=payer_key) if claim.get('_moa') and claim.get('_moa', [{}])[0].get('claim_payment_remark_code_1') else '',
        'MOA_ClaimPaymentRemarkDesc2_L2100_MOA': get_remark_code_description(claim.get('_moa', [{}])[0].get('claim_payment_remark_code_2', ''), payer_key=payer_key) if claim.get('_moa') and claim.get('_moa', [{}])[0].get('claim_payment_remark_code_2') else '',
        'MOA_ClaimPaymentRemarkDesc3_L2100_MOA': get_remark_code_description(claim.get('_moa', [{}])[0].get('claim_payment_remark_code_3', ''), payer_key=payer_key) if claim.get('_moa') and claim.get('_moa', [{}])[0].get('claim_payment_remark_code_3') else '',
        'MOA_ClaimPaymentRemarkDesc4_L2100_MOA': get_remark_code_description(claim.get('_moa', [{}])[0].get('claim_payment_remark_code_4', ''), payer_key=payer_key) if claim.get('_moa') and claim.get('_moa', [{}])[0].get('claim_payment_remark_code_4') else '',
        'MOA_ClaimPaymentRemarkDesc5_L2100_MOA': get_remark_code_description(claim.get('_moa', [{}])[0].get('claim_payment_remark_code_5', ''), payer_key=payer_key) if claim.get('_moa') and claim.get('_moa', [{}])[0].get('claim_payment_remark_code_5') else '',
        'MOA_ESRDPaymentAmount_L2100_MOA': claim.get('_moa', [{}])[0].get('esrd_payment_amount', '') if claim.get('_moa') else '',  
        'MOA_NonpayableProfessionalComponent_L2100_MOA': claim.get('_moa', [{}])[0].get('nonpayable_professional_component', '') if claim.get('_moa') else '',  
        # MIA fields (Medicare Inpatient Adjudication)
        'MIA_CoveredDaysOrVisitsCount_L2100_MIA': claim.get('_mia', [{}])[0].get('covered_days_or_visits_count', '') if claim.get('_mia') else '',
        'MIA_PPSOperatingOutlierAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_operating_outlier_amount', '') if claim.get('_mia') else '',
        'MIA_LifetimePsychiatricDaysCount_L2100_MIA': claim.get('_mia', [{}])[0].get('lifetime_psychiatric_days_count', '') if claim.get('_mia') else '',
        'MIA_ClaimDRGAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_drg_amount', '') if claim.get('_mia') else '',
        'MIA_ClaimPaymentRemarkCode_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_payment_remark_code', '') if claim.get('_mia') else '',
        'MIA_ClaimPaymentRemarkDesc_L2100_MIA': get_remark_code_description(claim.get('_mia', [{}])[0].get('claim_payment_remark_code', ''), payer_key=payer_key) if claim.get('_mia') and claim.get('_mia', [{}])[0].get('claim_payment_remark_code') else '',
        'MIA_ClaimDisproportionateShareAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_disproportionate_share_amount', '') if claim.get('_mia') else '',
        'MIA_ClaimMSPPassThroughAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_msp_pass_through_amount', '') if claim.get('_mia') else '',
        'MIA_ClaimPPSCapitalAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_pps_capital_amount', '') if claim.get('_mia') else '',
        'MIA_PPSCapitalFSPDRGAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_capital_fsp_drg_amount', '') if claim.get('_mia') else '',
        'MIA_PPSCapitalHSPDRGAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_capital_hsp_drg_amount', '') if claim.get('_mia') else '',
        'MIA_PPSCapitalDSHDRGAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_capital_dsh_drg_amount', '') if claim.get('_mia') else '',
        'MIA_OldCapitalAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('old_capital_amount', '') if claim.get('_mia') else '',
        'MIA_PPSCapitalIMEAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_capital_ime_amount', '') if claim.get('_mia') else '',
        'MIA_PPSOperatingHospitalSpecificDRGAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_operating_hospital_specific_drg_amount', '') if claim.get('_mia') else '',
        'MIA_CostReportDayCount_L2100_MIA': claim.get('_mia', [{}])[0].get('cost_report_day_count', '') if claim.get('_mia') else '',
        'MIA_PPSOperatingFederalSpecificDRGAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_operating_federal_specific_drg_amount', '') if claim.get('_mia') else '',
        'MIA_ClaimPPSCapitalOutlierAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_pps_capital_outlier_amount', '') if claim.get('_mia') else '',
        'MIA_ClaimIndirectTeachingAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_indirect_teaching_amount', '') if claim.get('_mia') else '',
        'MIA_NonpayableProfessionalComponentAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('nonpayable_professional_component_amount', '') if claim.get('_mia') else '',
        'MIA_ClaimPaymentRemarkCode2_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_payment_remark_code_2', '') if claim.get('_mia') else '',
        'MIA_ClaimPaymentRemarkCode3_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_payment_remark_code_3', '') if claim.get('_mia') else '',
        'MIA_ClaimPaymentRemarkCode4_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_payment_remark_code_4', '') if claim.get('_mia') else '',
        'MIA_ClaimPaymentRemarkCode5_L2100_MIA': claim.get('_mia', [{}])[0].get('claim_payment_remark_code_5', '') if claim.get('_mia') else '',
        'MIA_ClaimPaymentRemarkDesc2_L2100_MIA': get_remark_code_description(claim.get('_mia', [{}])[0].get('claim_payment_remark_code_2', ''), payer_key=payer_key) if claim.get('_mia') and claim.get('_mia', [{}])[0].get('claim_payment_remark_code_2') else '',
        'MIA_ClaimPaymentRemarkDesc3_L2100_MIA': get_remark_code_description(claim.get('_mia', [{}])[0].get('claim_payment_remark_code_3', ''), payer_key=payer_key) if claim.get('_mia') and claim.get('_mia', [{}])[0].get('claim_payment_remark_code_3') else '',
        'MIA_ClaimPaymentRemarkDesc4_L2100_MIA': get_remark_code_description(claim.get('_mia', [{}])[0].get('claim_payment_remark_code_4', ''), payer_key=payer_key) if claim.get('_mia') and claim.get('_mia', [{}])[0].get('claim_payment_remark_code_4') else '',
        'MIA_ClaimPaymentRemarkDesc5_L2100_MIA': get_remark_code_description(claim.get('_mia', [{}])[0].get('claim_payment_remark_code_5', ''), payer_key=payer_key) if claim.get('_mia') and claim.get('_mia', [{}])[0].get('claim_payment_remark_code_5') else '',
        'MIA_PPSCapitalExceptionAmount_L2100_MIA': claim.get('_mia', [{}])[0].get('pps_capital_exception_amount', '') if claim.get('_mia') else '',
        
        # Ambulance Location Data
        'AMB_PickupName_L2100_NM1': claim.get('_ambulance_pickup', {}).get('last_name', '') if claim.get('_ambulance_pickup') else '',
        'AMB_PickupAddress_L2100_N3': claim.get('_ambulance_pickup_address', {}).get('address_line_1', '') if claim.get('_ambulance_pickup_address') else '',
        'AMB_PickupCity_L2100_N4': claim.get('_ambulance_pickup_city', ''),
        'AMB_PickupState_L2100_N4': claim.get('_ambulance_pickup_state', ''),
        'AMB_PickupZip_L2100_N4': claim.get('_ambulance_pickup_zip', ''),
        'AMB_DropoffName_L2100_NM1': claim.get('_ambulance_dropoff', {}).get('last_name', '') if claim.get('_ambulance_dropoff') else '',
        'AMB_DropoffAddress_L2100_N3': claim.get('_ambulance_dropoff_address', {}).get('address_line_1', '') if claim.get('_ambulance_dropoff_address') else '',
        'AMB_DropoffCity_L2100_N4': claim.get('_ambulance_dropoff_city', ''),
        'AMB_DropoffState_L2100_N4': claim.get('_ambulance_dropoff_state', ''),
        'AMB_DropoffZip_L2100_N4': claim.get('_ambulance_dropoff_zip', ''),
        
        # Calculated fields (derived from CAS segments, not directly parsed)
        # Patient responsibility categories not captured by standard Deductible/Coinsurance/Copay
        'Patient_NonCovered': f"{service_adjustments['PR_NonCovered']:.2f}" if service else '',
        'Patient_OtherResp': f"{service_adjustments['OtherPatientResp']:.2f}" if service else '',
        
        # Allowed Amount = What the payer's fee schedule says the service is WORTH
        # This is the maximum collectible amount (Payer Payment + Patient Responsibility)
        # Two calculation methods that should equal each other if data is valid:
        #   Method 1: Billed Charge - Non-Collectible Write-offs (CO/OA adjustments)
        #   Method 2: Payer Payment + Patient Responsibility (PR adjustments)
        # QMB is a federally mandated write-off - provider cannot collect from patient
        'Allowed_Amount': f"{(float(service.get('line_charged', 0) or 0) - service_adjustments['Contractual'] - service_adjustments['COB'] - service_adjustments['Sequestration'] - service_adjustments['HCRA'] - service_adjustments['OtherAdjustments'] - service_adjustments['Denied'] - service_adjustments['QMB']):.2f}" if service else '',
        'Allowed_Verification': f"{(float(service.get('line_paid', 0) or 0) + service_adjustments['Deductible'] + service_adjustments['Coinsurance'] + service_adjustments['Copay'] + service_adjustments['PR_NonCovered'] + service_adjustments['OtherPatientResp']):.2f}" if service else '',
        
        # Currency Information (CUR) - Foreign currency fields not used for US professional claims
        # 'CUR_EntityCode_Header_CUR': current_transaction.get('currency', {}).get("entity_identifier_code", "") if current_transaction else "",
        # 'CUR_CurrencyCode_Header_CUR': current_transaction.get('currency', {}).get("currency_code", "") if current_transaction else "",
        # 'CUR_ExchangeRate_Header_CUR': current_transaction.get('currency', {}).get("exchange_rate", "") if current_transaction else "",
        # 'CUR_EntityCode2_Header_CUR': current_transaction.get('currency', {}).get("entity_identifier_code_2", "") if current_transaction else "",
        # 'CUR_CurrencyCode2_Header_CUR': current_transaction.get('currency', {}).get("currency_code_2", "") if current_transaction else "",
        # 'CUR_MarketExchangeCode_Header_CUR': current_transaction.get('currency', {}).get("currency_market_exchange_code", "") if current_transaction else "",
        # 'CUR_DateQualifier_Header_CUR': current_transaction.get('currency', {}).get("date_time_qualifier", "") if current_transaction else "",
        # 'CUR_Date_Header_CUR': current_transaction.get('currency', {}).get("date", "") if current_transaction else "",

        # Remittance Delivery Method (RDM)
        'RDM_TransmissionCode_Header_RDM': current_transaction.get('remittance_delivery', {}).get("report_transmission_code", "") if current_transaction else "",
        'RDM_Name_Header_RDM': current_transaction.get('remittance_delivery', {}).get("name", "") if current_transaction else "",
        'RDM_CommunicationNumber_Header_RDM': current_transaction.get('remittance_delivery', {}).get("communication_number", "") if current_transaction else "",
        'RDM_ReferenceID_Header_RDM': current_transaction.get('remittance_delivery', {}).get("reference_identifier", "") if current_transaction else "",
        'RDM_ReferenceID2_Header_RDM': current_transaction.get('remittance_delivery', {}).get("reference_identifier_2", "") if current_transaction else "",
        'RDM_CommunicationNumber2_Header_RDM': current_transaction.get('remittance_delivery', {}).get("communication_number_2", "") if current_transaction else "",
        'RDM_ContactFunction_Header_RDM': current_transaction.get('remittance_delivery', {}).get("contact_function_code", "") if current_transaction else "",

        # Institutional-only Extended MIA fields - not used for professional claims
        # 'MIA_NonpayableProfessionalComponent_L2100_MIA': claim.get('mia', {}).get('nonpayable_professional_component_amount', '') if claim.get('mia') else '',

        # Institutional-only Extended MOA field - not used for professional claims
        # 'MOA_NonpayableProfessionalComponent_L2100_MOA': claim.get('moa', {}).get('nonpayable_professional_component', '') if claim.get('moa') else '',

        # NOTE: AMT fields (DY, KH, NL, T2, ZK-ZO) are already extracted above as CLM_* fields
        # See: CLM_PerDayLimitAmount, CLM_DeductionAmount, CLM_NetBilledAmount, etc.

        # Additional QTY fields
        # Institutional-only QTY fields - not used for professional claims
        # 'QTY_noncovered_estimated_days_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'NE'),  # Days are institutional
        # 'QTY_not_replaced_blood_units_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'NR'),  # Blood units are institutional
        # 'QTY_outlier_days_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'OU'),  # Outlier days are institutional
        'QTY_prescriptions_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'PS'),
        'QTY_visits_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'VS'),
        'QTY_federal_mandate_category_1_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'ZK'),
        'QTY_federal_mandate_category_2_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'ZL'),
        'QTY_federal_mandate_category_3_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'ZM'),
        'QTY_federal_mandate_category_4_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'ZN'),
        'QTY_federal_mandate_category_5_L2100_QTY': extract_qty_value(claim.get('_qty', []), 'ZO'),

        # Additional REF fields (unique - not duplicates of claim_ref_values)
        'CLM_PayerIdentificationNumber_L2100_REF': extract_ref_value(claim.get('_ref', []), '2U'),
        'CLM_PredeterminationNumber_L2100_REF': extract_ref_value(claim.get('_ref', []), 'G3'),
        'CLM_AgencyClaimNumber_L2100_REF': extract_ref_value(claim.get('_ref', []), 'Y8'),
        'CLM_AdjustmentIdentifier_L2100_REF': extract_ref_value(claim.get('_ref', []), '9C'),
        'CLM_QualifiedProductsList_L2100_REF': extract_ref_value(claim.get('_ref', []), 'LX'),
        'CLM_HierarchicalParentId_L2100_REF': extract_ref_value(claim.get('_ref', []), 'F4'),
        'CLM_PolicyNumber_L2100_REF': extract_ref_value(claim.get('_ref', []), '0K'),

        # Additional DTM field (unique - statement_to_date not in claim_dtm_values)
        'CLM_StatementToDate_L2100_DTM': extract_dtm_value(claim.get('_dtm', []), '435'),
        
        # Service-level AMT fields
        'SVC_AMT_per_day_limit_L2110_AMT': extract_amt_value(service.get('_amt', []), 'DY') if service else '',
        'SVC_AMT_deduction_L2110_AMT': extract_amt_value(service.get('_amt', []), 'KH') if service else '',
        'SVC_AMT_net_billed_L2110_AMT': extract_amt_value(service.get('_amt', []), 'NL') if service else '',
    }
    
    # Add Fair Health rate columns
    hcpcs_code = service.get('procedure_code', '') if service else ''
    
    # Mileage codes (A0425 is ground mileage, based on 15 miles in Fair Health)
    MILEAGE_CODES = ['A0425', 'A0435', 'A0436']  # Ground, fixed wing, rotary wing mileage
    FH_MILEAGE_BASE_UNITS = 15  # Fair Health mileage rates are based on 15 miles
    BASE_RATE_CODES = {'A0426', 'A0427', 'A0428', 'A0429', 'A0433', 'A0434'}
    
    is_mileage_code = hcpcs_code.upper() in MILEAGE_CODES if hcpcs_code else False
    
    # Helper to filter out undefined/N/A values - returns empty string for non-numeric values
    def clean_rate(rate):
        if rate is None:
            return ''
        rate_str = str(rate).strip()
        rate_lower = rate_str.lower()
        # Filter out non-numeric placeholder values
        if rate_lower in ('n/a', 'na', 'undefined', 'null', 'none', '', 'error'):
            return ''
        # Try to ensure it's a valid number
        try:
            float(rate_str.replace(',', '').replace('$', ''))
            return rate_str
        except (ValueError, TypeError):
            return ''
    
    def parse_positive_decimal(value):
        """Return Decimal for positive numeric strings, else None."""
        if value is None:
            return None
        value_str = str(value).strip()
        if not value_str:
            return None
        try:
            dec_value = Decimal(value_str)
        except (InvalidOperation, ValueError, TypeError):
            return None
        return dec_value if dec_value > 0 else None

    def format_units(value):
        if not value or value <= 0:
            return ''
        normalized = value.normalize()
        text = format(normalized, 'f')
        if '.' in text:
            text = text.rstrip('0').rstrip('.')
        return text or '0'

    # Get pickup ZIP from Trips.csv using RUN
    processor = _get_processor()
    run_number = format_run_number(claim['claim_number'])
    pickup_zip = processor.trips_by_run.get(run_number)
    
    # Add Fair Health columns based on matched ZIP
    row['FH_PickupZIP'] = pickup_zip or ''
    
    paid_units_decimal = parse_positive_decimal(service.get('units')) if service else None
    original_units_decimal = parse_positive_decimal(service.get('original_units')) if service else None
    effective_units_decimal = paid_units_decimal or original_units_decimal
    service_units_decimal = effective_units_decimal if effective_units_decimal is not None else Decimal('0')
    
    display_units_decimal = service_units_decimal
    if not display_units_decimal or display_units_decimal <= 0:
        if is_mileage_code:
            display_units_decimal = Decimal(str(FH_MILEAGE_BASE_UNITS))
        elif hcpcs_code.upper() in BASE_RATE_CODES:
            display_units_decimal = Decimal('1')
    row['FH_EffectiveUnits'] = format_units(display_units_decimal)
    
    # Calculate EDI mileage unit price (charge per mile) with fallback units - using Decimal for precision
    edi_mileage_unit_price = ''
    if is_mileage_code and service and effective_units_decimal:
        try:
            charge = Decimal(str(service.get('line_charged', 0) or 0))
            edi_mileage_unit_price = str(
                (charge / effective_units_decimal).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            )
        except (ValueError, TypeError, ArithmeticError, InvalidOperation):
            pass
    row['EDI_MileageUnitPrice'] = edi_mileage_unit_price
    
    if pickup_zip and processor.fair_health_rates:
        # Use O(1) direct lookup instead of building full ZIP dict
        rate_tuple = get_fair_health_rate_for_zip(pickup_zip, hcpcs_code, service_start)
        if rate_tuple:
            oon_rate = clean_rate(rate_tuple[0])
            in_rate = clean_rate(rate_tuple[1])
            row['FH_OutOfNetwork'] = oon_rate
            row['FH_InNetwork'] = in_rate
            
            # Calculate Fair Health unit prices and totals for mileage codes - using Decimal for precision
            if is_mileage_code:
                # Unit prices (rate / 15 miles base)
                oon_unit_price = ''
                in_unit_price = ''
                base_units = Decimal(str(FH_MILEAGE_BASE_UNITS))
                try:
                    if oon_rate:
                        oon_unit_price = str((Decimal(str(oon_rate)) / base_units).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                except (ValueError, TypeError, ArithmeticError):
                    pass
                try:
                    if in_rate:
                        in_unit_price = str((Decimal(str(in_rate)) / base_units).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                except (ValueError, TypeError, ArithmeticError):
                    pass
                
                row['FH_OON_UnitPrice'] = oon_unit_price
                row['FH_IN_UnitPrice'] = in_unit_price
                
                # Mileage totals (unit price × service units)
                oon_miles = ''
                in_miles = ''
                if oon_unit_price and service_units_decimal > 0:
                    try:
                        oon_miles = str((Decimal(str(oon_unit_price)) * service_units_decimal).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                    except (ValueError, TypeError, ArithmeticError, InvalidOperation):
                        pass
                if in_unit_price and service_units_decimal > 0:
                    try:
                        in_miles = str((Decimal(str(in_unit_price)) * service_units_decimal).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
                    except (ValueError, TypeError, ArithmeticError, InvalidOperation):
                        pass
                
                row['FH_OON_Miles'] = oon_miles
                row['FH_IN_Miles'] = in_miles
                
                # Final amounts (use mileage calculation when units are known, otherwise fall back to base rate)
                if service_units_decimal > 0:
                    row['FH_OON_Final'] = oon_miles
                    row['FH_IN_Final'] = in_miles
                else:
                    row['FH_OON_Final'] = oon_rate
                    row['FH_IN_Final'] = in_rate
            else:
                # Non-mileage codes: no unit prices or mileage
                row['FH_OON_UnitPrice'] = ''
                row['FH_IN_UnitPrice'] = ''
                row['FH_OON_Miles'] = ''
                row['FH_IN_Miles'] = ''
                # Final amounts use base rate directly for non-mileage codes
                row['FH_OON_Final'] = oon_rate
                row['FH_IN_Final'] = in_rate
        else:
            # ZIP not found in Fair Health rates
            row['FH_OutOfNetwork'] = ''
            row['FH_InNetwork'] = ''
            row['FH_OON_UnitPrice'] = ''
            row['FH_IN_UnitPrice'] = ''
            row['FH_OON_Miles'] = ''
            row['FH_IN_Miles'] = ''
            row['FH_OON_Final'] = ''
            row['FH_IN_Final'] = ''
    else:
        # No match in Trips.csv or no Fair Health rates loaded - leave empty
        row['FH_OutOfNetwork'] = ''
        row['FH_InNetwork'] = ''
        row['FH_OON_UnitPrice'] = ''
        row['FH_IN_UnitPrice'] = ''
        row['FH_OON_Miles'] = ''
        row['FH_IN_Miles'] = ''
        row['FH_OON_Final'] = ''
        row['FH_IN_Final'] = ''
    
    return row


def process_folder(folder_path, enable_redaction=False, status_callback=None):
    """Process all 835 files in a folder and generate consolidated CSV
    
    Args:
        folder_path: Path to folder containing 835 files
        enable_redaction: Whether to enable redaction mode
        status_callback: Optional callback function for GUI status updates
    """
    # Ensure logging is configured (for GUI or direct calls)
    # Use simple format (no timestamps) for cleaner GUI output
    if not logger.handlers:
        configure_logging(level=logging.INFO, simple_format=True)
    
    import os
    from pathlib import Path
    import shutil
    
    # Get configuration
    config = get_config()
    
    folder = Path(folder_path)
    if not folder.exists():
        logger.error("Folder does not exist: %s", folder_path)
        return None
    testing_folder = None
    if enable_redaction:
        folder_name = folder.name  
        testing_folder = folder.parent / f"{folder_name}_testing"
        if testing_folder.exists():
            try:
                shutil.rmtree(testing_folder)
            except (PermissionError, OSError) as e:
                if hasattr(e, 'winerror') and e.winerror == 32:
                    logger.warning("Cannot delete existing testing folder (in use by another process)")
                    logger.info("Creating a new folder with timestamp instead...")
                    # Create a unique folder name with timestamp
                    from datetime import datetime
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    testing_folder = folder.parent / f"{folder_name}_testing_{timestamp}"
                else:
                    raise
        testing_folder.mkdir(exist_ok=True)
        logger.info("Created testing folder: %s", testing_folder)
    # Case-insensitive file discovery for cross-platform compatibility (Linux has case-sensitive FS)
    valid_extensions = {'.835', '.txt', '.edi', '.x12', '.processed'}
    candidate_files = []
    try:
        for f in folder.iterdir():
            if f.is_file():
                ext = f.suffix.lower()
                if ext in valid_extensions:
                    candidate_files.append(str(f))
    except OSError as e:
        logger.error("Error reading folder %s: %s", folder_path, e)
        return None
    if not candidate_files:
        logger.warning("No 835 files found in %s", folder_path)
        return None
    
    # Filter to only valid EDI files (must start with ISA header)
    files = []
    skipped_files = []
    for candidate in candidate_files:
        try:
            with open(candidate, 'r', encoding='utf-8', errors='ignore') as f:
                header = f.read(3)
            if header == 'ISA':
                files.append(candidate)
            else:
                skipped_files.append((candidate, f"Does not start with ISA (found: {repr(header)})"))
        except Exception as e:
            skipped_files.append((candidate, f"Could not read: {e}"))
    
    if skipped_files:
        logger.info("Skipped %d non-EDI file(s):", len(skipped_files))
        for path, reason in skipped_files:
            logger.debug("  - %s: %s", os.path.basename(path), reason)
    
    if not files:
        logger.warning("No valid EDI files found in %s", folder_path)
        return None
    logger.info("Found %d valid EDI file(s) to process", len(files))
    
    # Load Fair Health rates (if enabled)
    if config.get('enable_fair_health_rates', True):
        logger.info("Loading Fair Health rates...")
        if not load_fair_health_rates():
            logger.info("Fair Health rate columns will be empty")
    else:
        logger.info("Fair Health rates disabled in config")
    
    # Load Trips.csv for ZIP lookup (if enabled)
    if config.get('enable_trips_lookup', True):
        logger.info("Loading Trips.csv for ZIP lookup...")
        if not load_trips_csv():
            logger.info("ZIP lookup from Trips.csv will be unavailable")
    else:
        logger.info("Trips.csv lookup disabled in config")
    
    all_rows = []
    # MEMORY OPTIMIZATION: Store only segments+delimiter for validation, not full content
    validation_data = []  # Minimal data needed for validation (segments + delimiter only)
    # Track payer keys for validation overrides (indexed by file position in validation_data)
    payer_keys_map = {}

    # Track EDI element presence across all files to find unmapped elements
    element_tracker = EDIElementPresenceTracker()

    for idx, file_path in enumerate(files, 1):
        file_name = os.path.basename(file_path)
        logger.info("Processing: %s", file_name)
        if status_callback:
            status_callback(f"Processing file {idx}/{len(files)}: {file_name}")
        try:
            parsed_data = parse_835_file(file_path)
            if parsed_data:
                # Store only what's needed for validation (not the full 'content' string)
                validation_data.append({
                    'file': str(file_path),
                    'segments': parsed_data['segments'],
                    'delimiter': parsed_data['element_delimiter']
                })
                
                # Get payer name for element tracking
                payer_info = identify_payer(parsed_data['segments'], parsed_data['element_delimiter'])
                payer_name = payer_info.get('payer_name', 'Unknown')
                # Track payer key for validation overrides (file index in validation_data)
                colloquial_payer_key = payer_info.get('colloquial_payer_key')
                if colloquial_payer_key:
                    payer_keys_map[len(validation_data) - 1] = colloquial_payer_key
                
                # Track element presence for this file
                element_tracker.new_file(payer_name)
                for seg in parsed_data['segments']:
                    elements = seg.split(parsed_data['element_delimiter'])
                    element_tracker.track_segment(seg, elements, parsed_data['element_delimiter'])
                
                # Determine the file path to use in the CSV
                csv_file_path = str(file_path)
                
                if enable_redaction and testing_folder:
                    try:
                        redacted_content = redact_835_file(parsed_data['content'], parsed_data['element_delimiter'])
                        redacted_file_path = testing_folder / os.path.basename(file_path)
                        with open(redacted_file_path, 'w', encoding='utf-8') as f:
                            f.write(redacted_content)
                        logger.debug("  - Saved redacted EDI: %s", redacted_file_path.name)
                        # Use redacted file path for CSV
                        csv_file_path = str(redacted_file_path)
                    except Exception as redact_error:
                        logger.warning("  - Redaction failed: %s", str(redact_error))
                
                try:
                    rows = convert_segments_to_rows(
                        parsed_data['segments'],
                        parsed_data['element_delimiter'],
                        csv_file_path,
                        parsed_data['component_delimiter']
                    )
                    all_rows.extend(rows)
                    logger.info("  - Extracted %d service lines", len(rows))
                except Exception as convert_error:
                    logger.error("  - ERROR in convert_segments_to_rows: %s", str(convert_error))
                    logger.error("  - Error type: %s", type(convert_error).__name__)
                    logger.error("  - Full traceback:")
                    for line in traceback.format_exception(type(convert_error), convert_error, convert_error.__traceback__):
                        logger.error("    %s", line.rstrip())
                    raise
        except Exception as e:
            logger.error("  - ERROR: %s", str(e))
            continue
    if not all_rows:
        logger.warning("No data extracted from any files")
        return None
    # Get output file name from config (config already loaded at start of function)
    if enable_redaction and testing_folder:
        output_file = testing_folder / config.output_csv_name
    else:
        output_file = folder / config.output_csv_name
    import csv
    
    # ============================================================
    # PERFORMANCE OPTIMIZATION: Normalize once, detect columns in single pass
    # ============================================================
    
    # Update status for preprocessing
    if status_callback:
        status_callback(f"Normalizing {len(all_rows):,} rows...")
    logger.info("Preprocessing %d rows (normalize, detect columns)...", len(all_rows))

    # MEMORY OPTIMIZATION: Normalize in-place instead of creating new list
    # This reduces peak memory usage by ~50% (no duplicate row storage)
    populated_columns_set = set()
    current_claim_key = None
    claim_seq = 0

    for idx, row in enumerate(all_rows, 1):
        # Build unique key from claim number + occurrence
        claim_num = row.get('CLM_PatientControlNumber_L2100_CLP', '')
        occurrence = row.get('CLM_Occurrence_L2100_CLP', 1)
        claim_key = f"{claim_num}|{occurrence}"

        # Reset SEQ for each new claim occurrence
        if claim_key != current_claim_key:
            current_claim_key = claim_key
            claim_seq = 1
        else:
            claim_seq += 1

        # SEQ format: occurrence-service (e.g., "1-1", "1-2", "2-1")
        row['SEQ'] = f"{occurrence}-{claim_seq}"

        # Normalize all values IN-PLACE: uppercase, strip whitespace, format dates/currency
        normalized = normalize_csv_row(row)
        if enable_redaction:
            normalized = redact_csv_row(normalized)

        # Replace row contents with normalized values (in-place update)
        row.clear()
        row.update(normalized)

        # Track populated columns (single pass - O(n) instead of O(n*m))
        for field, value in row.items():
            if value is not None and str(value).strip():
                populated_columns_set.add(field)

        # Progress feedback every 10000 rows
        if idx % 10000 == 0:
            if status_callback:
                status_callback(f"Normalizing: {idx:,}/{len(all_rows):,} rows...")
            logger.debug("    Processed %d rows...", idx)

    # Reuse all_rows as normalized_rows (saves memory)
    normalized_rows = all_rows
    logger.info("  Normalization complete. Found %d populated columns.", len(populated_columns_set))
    
    # ============================================================
    # Write Standard CSV (using pre-normalized rows)
    # ============================================================
    
    if status_callback:
        status_callback("Writing standard CSV...")
    logger.info("Saving consolidated output to: %s", output_file)
    logger.info("  Writing %d service lines...", len(normalized_rows))
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        if normalized_rows:
            # Get display-friendly column names for CSV header
            display_fieldnames = [DISPLAY_COLUMN_NAMES.get(k, k) for k in normalized_rows[0].keys()]
            writer = csv.DictWriter(f, fieldnames=display_fieldnames)
            writer.writeheader()
            
            # Write pre-normalized rows (no re-processing needed)
            for idx, row in enumerate(normalized_rows, 1):
                display_row = rename_columns_for_display(row)
                writer.writerow(display_row)
                
                # Progress feedback every 10000 rows
                if idx % 10000 == 0:
                    if status_callback:
                        status_callback(f"Writing CSV: {idx:,}/{len(normalized_rows):,} rows...")
                    logger.debug("    Written %d rows...", idx)
    
    logger.info("Consolidated output saved: %s", output_file)
    logger.info("  Total service lines: %d", len(normalized_rows))
    logger.info("  Total files: %d", len(files))
    
    # ============================================================
    # Write Compact CSV (reusing pre-normalized rows and column detection)
    # ============================================================
    
    if normalized_rows and config.get('enable_compact_csv', True):
        all_fieldnames = list(normalized_rows[0].keys())
        
        # Filter out ENV and contact columns from compact version
        compact_excluded_patterns = ['ENV', 'contact', 'Contact']
        populated_columns = [
            col for col in all_fieldnames 
            if col in populated_columns_set and 
               not any(pattern.lower() in col.lower() for pattern in compact_excluded_patterns)
        ]
        
        # Only create compact file if we actually removed some columns
        removed_count = len(all_fieldnames) - len(populated_columns)
        if removed_count > 0:
            if enable_redaction and testing_folder:
                compact_file = testing_folder / config.output_csv_compact_name
            else:
                compact_file = folder / config.output_csv_compact_name
            
            logger.info("Generating compact CSV (removed %d empty columns)...", removed_count)
            if status_callback:
                status_callback(f"Writing compact CSV ({len(populated_columns)} columns)...")
            
            # Get display-friendly column names for compact CSV
            display_populated_columns = [DISPLAY_COLUMN_NAMES.get(col, col) for col in populated_columns]
            
            with open(compact_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=display_populated_columns, extrasaction='ignore')
                writer.writeheader()
                
                # Write pre-normalized rows (no re-processing needed!)
                for idx, row in enumerate(normalized_rows, 1):
                    display_row = rename_columns_for_display(row)
                    writer.writerow(display_row)
                    
                    # Progress feedback every 10000 rows
                    if idx % 10000 == 0:
                        if status_callback:
                            status_callback(f"Writing compact CSV: {idx:,}/{len(normalized_rows):,} rows...")
                        logger.debug("    Written %d rows...", idx)
            
            logger.info("Compact output saved: %s", compact_file)
            logger.info("  Total service lines: %d", len(normalized_rows))
            logger.info("  Columns: %d of %d (%d empty columns removed)", len(populated_columns), len(all_fieldnames), removed_count)
        else:
            logger.info("  All %d columns have data - no compact version needed", len(all_fieldnames))
    elif not config.get('enable_compact_csv', True):
        logger.info("Compact CSV generation disabled in config")
    
    # Run zero-fail validation
    logger.info("Running Zero-Fail Validation...")
    if status_callback:
        status_callback("Running Zero-Fail Validation...")
    try:
        # Use pre-built validation_data (already contains file, segments, delimiter)
        # This avoids rebuilding the structure from all_parsed_data

        # Run validation
        if validation_data and all_rows:
            # Determine validation output path
            if enable_redaction and testing_folder:
                validation_output = testing_folder / config.validation_report_txt_name
                validation_html = testing_folder / config.validation_report_html_name
            else:
                validation_output = folder / config.validation_report_txt_name
                validation_html = folder / config.validation_report_html_name

            # Run validation and generate reports
            validation_result = validate_835_output(
                validation_data,
                all_rows,
                element_delimiter=validation_data[-1]['delimiter'] if validation_data else '*',
                output_file=str(validation_output),
                output_format='text',
                verbose=True,  # Show validation progress
                status_callback=status_callback,
                payer_keys=payer_keys_map  # Pass payer keys for validation overrides
            )

            # Also generate HTML report
            if status_callback:
                status_callback("Generating HTML report...")
            generate_validation_report(
                validation_result,
                output_format='html',
                output_file=str(validation_html),
                redact=enable_redaction
            )

            if status_callback:
                status_callback("Analyzing results...")

            # Display summary
            summary = validation_result.get('summary', {})
            status = summary.get('validation_status', 'UNKNOWN')
            errors = summary.get('error_count', 0)
            warnings = summary.get('warning_count', 0)

            logger.info("=" * 80)
            logger.info("VALIDATION SUMMARY REPORT")
            logger.info("=" * 80)
            logger.info("Overall Status: %s", status)
            logger.info("  Total Segments Processed: %d", summary.get('total_segments', 0))
            logger.info("  Total Fields Validated: %d", summary.get('fields_validated', 0))
            logger.info("  Calculations Checked: %d", summary.get('calculations_checked', 0))
            logger.info("  Errors Found: %d", errors)
            logger.info("  Warnings: %d", warnings)
            logger.info("=" * 80)
            logger.info("DETAILED REPORTS")
            logger.info("=" * 80)
            logger.info("  Text Report: %s", validation_output)
            logger.info("  HTML Report: %s", validation_html)
            try:
                html_uri = Path(validation_html).resolve().as_uri()
                logger.info("  Open HTML Report: %s", html_uri)
                if status_callback:
                    status_callback(f"HTML report ready: {html_uri}")
            except Exception:
                if status_callback:
                    status_callback(f"HTML report saved to: {validation_html}")

            # Show payer data quality issues
            payer_quality_issues = validation_result.get('payer_data_quality_issues', {})
            if payer_quality_issues:
                logger.info("=" * 80)
                logger.info("PAYER DATA QUALITY ISSUES")
                logger.info("=" * 80)

                # Group issues by type across all payers
                issue_types = defaultdict(lambda: defaultdict(int))
                for payer_key, issues in payer_quality_issues.items():
                    for issue_type, count in issues.items():
                        issue_types[issue_type][payer_key] += count
                
                # Display each type of issue
                for issue_type in sorted(issue_types.keys()):
                    payers = issue_types[issue_type]
                    total_count = sum(payers.values())
                    logger.info("%s: %d total", issue_type, total_count)
                    logger.info("-" * 40)
                    
                    # Sort payers by count for this issue type
                    payer_list = sorted(payers.items(), key=lambda x: x[1], reverse=True)
                    
                    # Show all payers for this issue
                    for payer_key, count in payer_list:
                        payer_parts = payer_key.split('|')
                        payer_name = payer_parts[0]
                        payer_state = payer_parts[1] if len(payer_parts) > 1 else 'N/A'
                        logger.info("  - %s (%s): %d", payer_name, payer_state, count)
            else:
                # Fallback to old format if new structure isn't available
                payers_missing_units = validation_result.get('payers_missing_mileage_units', {})
                if payers_missing_units:
                    logger.info("Payer Data Quality Issues (Missing Mileage Units):")
                    payer_list = sorted(payers_missing_units.items(), key=lambda x: x[1], reverse=True)
                    for payer, count in payer_list:
                        payer_parts = payer.split('|')
                        payer_name = payer_parts[0]
                        payer_state = payer_parts[1] if len(payer_parts) > 1 else 'N/A'
                        logger.info("  - %s (%s): %d service line(s)", payer_name, payer_state, count)
            
            # Show errors grouped by Provider State → Provider → Payer → Type (with limited details)
            if errors > 0:
                all_errors = []
                for error_list in validation_result.get('errors_by_type', {}).values():
                    all_errors.extend(error_list)
                
                # Extract provider info from CSV rows
                provider_name = "Unknown Provider"
                provider_state = "Unknown State"
                if all_rows:
                    provider_name = all_rows[0].get('Provider_Name_L1000B_N1', 'Unknown Provider')
                    provider_state = all_rows[0].get('Provider_State_L1000B_N4', 'Unknown State')
                
                # Group errors: State (Provider) → Provider → Payer → Error Type
                state_provider_payer_type_errors = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
                
                for error in all_errors:
                    payer_info = error.get('payer_info', {})
                    payer_name = payer_info.get('name', 'Unknown Payer')
                    error_type = error.get('type', 'UNKNOWN')
                    
                    state_provider_payer_type_errors[provider_state][provider_name][payer_name][error_type].append(error)
                
                logger.info("=" * 80)
                logger.info("ERROR ANALYSIS - GROUPED BY PROVIDER STATE -> PROVIDER -> PAYER -> ERROR TYPE")
                logger.info("(Showing sample of 10 errors per type - full list in report files)")
                logger.info("=" * 80)
                
                # Calculate totals for sorting
                state_totals = {}
                for state, providers in state_provider_payer_type_errors.items():
                    total = sum(sum(sum(len(errs) for errs in types.values()) for types in payers.values()) for payers in providers.values())
                    state_totals[state] = total
                
                # Sort states by error count (highest first)
                for state in sorted(state_totals.keys(), key=lambda s: state_totals[s], reverse=True):
                    providers = state_provider_payer_type_errors[state]
                    logger.info("PROVIDER STATE: %s", state)
                    logger.info("  Total Errors: %d", state_totals[state])
                    
                    # Calculate provider totals for this state
                    provider_totals = {}
                    for provider, payers in providers.items():
                        total = sum(sum(len(errs) for errs in types.values()) for types in payers.values())
                        provider_totals[provider] = total
                    
                    # Sort providers by error count (highest first)
                    for provider in sorted(provider_totals.keys(), key=lambda p: provider_totals[p], reverse=True):
                        payers = providers[provider]
                        logger.info("  PROVIDER: %s", provider)
                        logger.info("    Total Errors: %d", provider_totals[provider])
                        
                        # Calculate payer totals
                        payer_totals = {}
                        for payer, types in payers.items():
                            payer_totals[payer] = sum(len(errs) for errs in types.values())
                        
                        # Sort payers by error count (highest first)
                        for payer in sorted(payer_totals.keys(), key=lambda p: payer_totals[p], reverse=True):
                            types = payers[payer]
                            logger.info("    PAYER: %s", payer)
                            logger.info("      Errors: %d", payer_totals[payer])
                            
                            # Sort error types by count (highest first)
                            for error_type in sorted(types.keys(), key=lambda t: len(types[t]), reverse=True):
                                type_errors = types[error_type]
                                logger.info("      %s: %d error(s)", error_type, len(type_errors))
                                
                                # Show only first 10 examples for console
                                # For COMPOSITE_PARSE, show claim summary instead of individual services
                                if error_type == 'COMPOSITE_PARSE' and len(type_errors) > 5:
                                    # Group errors by claim
                                    claims_affected = {}
                                    for error in type_errors:
                                        location = error.get('location') or ''
                                        if location and 'Claim' in location:
                                            try:
                                                claim_part = location.split('Claim')[1].split(',')[0].strip()
                                                if claim_part not in claims_affected:
                                                    claims_affected[claim_part] = 0
                                                claims_affected[claim_part] += 1
                                            except (ValueError, IndexError):
                                                pass
                                    
                                    logger.info("        Summary: %d claims with composite errors", len(claims_affected))
                                    for i, (claim_id, service_count) in enumerate(list(claims_affected.items())[:10], 1):
                                        logger.info("        %d. Claim %s: %d service(s)", i, claim_id, service_count)
                                    if len(claims_affected) > 10:
                                        logger.info("        ... and %d more claims", len(claims_affected) - 10)
                                else:
                                    # Show first 10 errors for other types
                                    for i, error in enumerate(type_errors[:10], 1):
                                        msg = error.get('message', 'Unknown')
                                        location = error.get('location') or ''
                                        # Keep location short for console
                                        if location and 'Claim' in location:
                                            try:
                                                # Use same extraction logic as COMPOSITE_PARSE - split by comma first, then strip whitespace
                                                claim_id = location.split('Claim')[1].split(',')[0].strip()
                                                logger.info("        %d. Claim %s: %s", i, claim_id, msg)
                                            except (ValueError, IndexError):
                                                logger.info("        %d. %s", i, msg)
                                        else:
                                            logger.info("        %d. %s", i, msg)
                                    
                                    if len(type_errors) > 10:
                                        logger.info("        ... and %d more errors", len(type_errors) - 10)
            
            # Show warnings summary (limited details)
            if warnings > 0:
                warnings_list = validation_result.get('warnings', [])
                logger.info("-" * 80)
                logger.info("WARNINGS (%d found)", warnings)
                logger.info("(Showing sample of 10 per payer - full list in report files)")
                logger.info("-" * 80)
                
                # Group warnings by payer for better organization
                warnings_by_payer = defaultdict(list)
                for warning in warnings_list:
                    payer_info = warning.get('payer_info', {})
                    payer_key = f"{payer_info.get('name', 'Unknown')} ({payer_info.get('state', 'N/A')})"
                    warnings_by_payer[payer_key].append(warning)
                
                # Display grouped warnings with limited details
                for payer_key in sorted(warnings_by_payer.keys()):
                    payer_warnings = warnings_by_payer[payer_key]
                    logger.info("%s: %d warning(s)", payer_key, len(payer_warnings))
                    
                    # Group by warning message type
                    msg_types = defaultdict(list)
                    for warning in payer_warnings:
                        msg = warning.get('message', 'Unknown')
                        msg_types[msg].append(warning)
                    
                    for msg, instances in msg_types.items():
                        logger.info("  %s: %d instance(s)", msg, len(instances))
                        
                        # Extract claim IDs for first 10
                        claim_ids = []
                        for warning in instances[:10]:
                            location = warning.get('location') or ''
                            if location and 'Claim ID:' in location:
                                claim_id = location.split('Claim ID:')[1].split('|')[0].strip()
                                claim_ids.append(claim_id)
                        
                        if claim_ids:
                            logger.info("    Sample claims: %s", ', '.join(claim_ids))
                        if len(instances) > 10:
                            logger.info("    ... and %d more", len(instances) - 10)
            
            logger.info("=" * 80)
            logger.info("DETAILED REPORTS GENERATED:")
            logger.info("  Text Report: %s", validation_output)
            logger.info("  HTML Report: %s", validation_html)
            logger.info("=" * 80)
            
            # Notify GUI that validation is complete
            if status_callback:
                status_callback("Validation complete! Processing finished.")
                    
    except Exception as e:
        logger.warning("Validation failed with error: %s", str(e))
        logger.warning("  Error type: %s", type(e).__name__)
        logger.warning("  Full traceback:")
        for line in traceback.format_tb(e.__traceback__):
            logger.warning("    %s", line.rstrip())
        logger.warning("  CSV output was still generated successfully")
    
    # Report unmapped EDI elements (at end of output)
    unmapped_elements = element_tracker.get_unmapped_elements()
    unmapped_qualifiers = element_tracker.get_unmapped_qualifiers()
    
    if unmapped_elements or unmapped_qualifiers:
        logger.info("=" * 100)
        logger.info("EDI ELEMENT COVERAGE ANALYSIS")
        logger.info("These EDI fields have data but are not currently extracted to CSV:")
        logger.info("=" * 100)
        
        # Report position-based unmapped elements
        if unmapped_elements:
            logger.info("--- POSITION-BASED SEGMENTS ---")
            shown = 0
            for key, info in unmapped_elements.items():
                # Only show elements present in at least 1% of files or at least 5 files
                if info['files_with_data'] >= 5 or info['pct'] >= 1.0:
                    logger.info("  %s: %d files (%s%%) - %d total occurrences", key, info['files_with_data'], info['pct'], info['total_occurrences'])
                    logger.info("       Description: %s", info['description'])
                    if info['payers']:
                        payer_list = ', '.join(f"{p[0]} ({p[1]:,})" for p in info['payers'][:5])
                        logger.info("       Top Payers: %s", payer_list)
                        if len(info['payers']) > 5:
                            logger.info("                   ... and %d more payers", len(info['payers']) - 5)
                    shown += 1
            if shown == 0:
                logger.info("  (No significant unmapped elements - all rare occurrences)")
            elif shown < len(unmapped_elements):
                logger.info("  ... plus %d rare elements (< 5 files or < 1%%)", len(unmapped_elements) - shown)
        
        # Report qualifier-based unmapped fields
        if unmapped_qualifiers:
            logger.info("--- QUALIFIER-BASED SEGMENTS (REF/DTM/AMT/QTY) ---")
            shown = 0
            for key, info in unmapped_qualifiers.items():
                # Only show qualifiers present in at least 1% of files or at least 5 files
                if info['files_with_data'] >= 5 or info['pct'] >= 1.0:
                    logger.info("  %s: %d files (%s%%) - %d total occurrences", key, info['files_with_data'], info['pct'], info['total_occurrences'])
                    logger.info("       Segment: %s, Qualifier: %s", info['segment'], info['qualifier'])
                    if info['payers']:
                        payer_list = ', '.join(f"{p[0]} ({p[1]:,})" for p in info['payers'][:5])
                        logger.info("       Top Payers: %s", payer_list)
                        if len(info['payers']) > 5:
                            logger.info("                   ... and %d more payers", len(info['payers']) - 5)
                    shown += 1
            if shown == 0:
                logger.info("  (No significant unmapped qualifiers - all rare occurrences)")
            elif shown < len(unmapped_qualifiers):
                logger.info("  ... plus %d rare qualifiers (< 5 files or < 1%%)", len(unmapped_qualifiers) - shown)
        
        logger.info("=" * 100)
    
    if enable_redaction and testing_folder:
        logger.info("Redacted files (EDI + CSV) saved to: %s", testing_folder)
        logger.info("   Original files remain unchanged in: %s", folder)
    
    # Final status update before returning
    if status_callback:
        status_callback("All processing complete!")
    
    return str(output_file)


def main(verbose=False):
    """
    Main entry point - launches GUI.
    
    Args:
        verbose: If True, sets logging to DEBUG level for detailed output.
    """
    # Configure logging (INFO by default, DEBUG if verbose)
    level = logging.DEBUG if verbose else logging.INFO
    configure_logging(level=level)
    
    import gui
    gui.main()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='835 EDI Parser')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose (DEBUG) logging')
    args = parser.parse_args()
    main(verbose=args.verbose)
