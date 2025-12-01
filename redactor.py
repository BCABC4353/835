import re
from datetime import datetime, date

# ============================================================
# PRE-COMPILED REGEX PATTERNS (compiled once at import time)
# ============================================================
_YYYYMMDD_PATTERN = re.compile(r'^\d{8}$')
_YYMMDD_PATTERN = re.compile(r'^\d{6}$')
_ISO_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}')
_WHITESPACE_PATTERN = re.compile(r'\s+')
_CONTROL_CHAR_PATTERN = re.compile(r'[\x00-\x1f\x7f-\x9f]')

# ============================================================
# PRE-BUILT FIELD TYPE SETS (O(1) lookup instead of O(n) iteration)
# ============================================================
_CURRENCY_PATTERNS = frozenset([
    'AMOUNT', 'PAYMENT', 'CHARGE', 'PAID', 'PRICE',
    'CONTRACTUAL', 'COPAY', 'COINSURANCE', 'DEDUCTIBLE',
    'DENIED', 'SEQUESTRATION', 'COB', 'HCRA', 'QMB',
    'OTHERADJUSTMENTS', 'NONCOVERED', 'OTHERRESP',
    'ALLOWED', 'INTEREST', 'COVERAGE', 'DISCOUNT',
    'FH_OUTOFNETWORK', 'FH_INNETWORK', 'FH_OON', 'FH_IN',
    '_FINAL', '_MILES'
])

_CURRENCY_EXCLUDE_PATTERNS = frozenset(['UNITS', 'UNIT_COUNT', 'OCCURRENCE'])

_DATE_FIELD_PATTERNS = frozenset([
    'INTERCHANGEDATE', 'DATE_ENVELOPE_GS', 'PAYMENTDATE', 'EFFECTIVEDATE'
])

# Cache for field type lookups (built lazily)
_field_type_cache = {}


def redact_string(text):
    if not text:
        return text
    
    result = []
    for char in text:
        if char.isalpha():
            result.append('X')
        elif char.isdigit():
            result.append('1')
        else:
            result.append(char)
    
    return ''.join(result)


def redact_835_segment(segment, element_delimiter):
    elements = segment.split(element_delimiter)
    
    if not elements:
        return segment
    
    seg_id = elements[0]
    
    if seg_id == 'NM1' and len(elements) > 3:
        entity_code = elements[1] if len(elements) > 1 else ''
        
        if entity_code in ['IL', 'QC']:
            for idx in range(3, min(8, len(elements))):
                if elements[idx]:
                    elements[idx] = redact_string(elements[idx])
            
            if len(elements) > 9 and elements[9]:
                elements[9] = redact_string(elements[9])
    
    elif seg_id == 'REF' and len(elements) > 2:
        qualifier = elements[1]
        if qualifier in ['SY', '1W']:
            elements[2] = redact_string(elements[2])

    return element_delimiter.join(elements)


def redact_835_file(content, element_delimiter):

    segment_terminator = content[105] if len(content) > 105 else '~'
    
    segments = content.split(segment_terminator)
    
    redacted_segments = []
    for segment in segments:
        if segment.strip():
            redacted_segment = redact_835_segment(segment.strip(), element_delimiter)
            redacted_segments.append(redacted_segment)
    
    return segment_terminator.join(redacted_segments) + segment_terminator


def redact_csv_row(row):

    name_fields = [
        'CLM_SubscriberName_L2100_NM1', 'CLM_PatientName_L2100_NM1'
        # Contact_Name removed - these are EDI/technical contacts, not patient PII
    ]

    address_fields = []
    
    id_fields = [
        'CLM_MemberID_L2100_NM1', 'CLM_SSN_L2100_NM1'
    ]
    
    redacted_row = row.copy()
    
    for field in name_fields:
        if field in redacted_row and redacted_row[field]:
            redacted_row[field] = redact_string(redacted_row[field])
    
    for field in address_fields:
        if field in redacted_row and redacted_row[field]:
            redacted_row[field] = redact_string(redacted_row[field])
    
    for field in id_fields:
        if field in redacted_row and redacted_row[field]:
            redacted_row[field] = redact_string(redacted_row[field])
    
    return redacted_row


def is_currency_field(field_name):
    """
    Check if a field name indicates it should be formatted as currency.
    Uses cached lookup for performance.
    """
    if not field_name:
        return False
    
    # Check cache first
    if field_name in _field_type_cache:
        return _field_type_cache[field_name] == 'currency'
    
    field_upper = field_name.upper()
    
    # Check exclusions first
    for exclude in _CURRENCY_EXCLUDE_PATTERNS:
        if exclude in field_upper:
            _field_type_cache[field_name] = 'not_currency'
            return False
    
    # Check currency patterns
    for pattern in _CURRENCY_PATTERNS:
        if pattern in field_upper:
            _field_type_cache[field_name] = 'currency'
            return True
    
    _field_type_cache[field_name] = 'not_currency'
    return False


def format_currency(value):
    """
    Format a numeric value as currency with $ sign and commas.
    Handles negative values and preserves decimal places.
    
    Examples:
        1234.56 -> "$1,234.56"
        -500 -> "-$500.00"
        0 -> "$0.00"
        "" -> ""
    """
    if value is None or value == '':
        return ''
    
    try:
        # Convert to float
        if isinstance(value, str):
            # Remove existing $ and commas if present
            cleaned = value.replace('$', '').replace(',', '').strip()
            if not cleaned:
                return ''
            num = float(cleaned)
        else:
            num = float(value)
        
        # Handle zero
        if num == 0:
            return '$0.00'
        
        # Format with commas and 2 decimal places
        if num < 0:
            return f'-${abs(num):,.2f}'
        else:
            return f'${num:,.2f}'
    except (ValueError, TypeError):
        # Not a number, return as-is
        return str(value) if value else ''


def format_date(value):
    """
    Format a date value to MM/DD/YYYY format with leading zeros.
    
    Handles:
    - datetime objects
    - date objects  
    - YYYYMMDD format (EDI standard)
    - YYMMDD format (EDI ISA)
    - YYYY-MM-DD format (ISO)
    - MM/DD/YYYY format
    - MM-DD-YYYY format
    - Already formatted MM/DD/YY
    
    Returns formatted string or None if not a date.
    """
    if value is None:
        return None
    
    # Handle datetime/date objects
    if isinstance(value, datetime):
        return value.strftime('%m/%d/%Y')
    if isinstance(value, date):
        return value.strftime('%m/%d/%Y')
    
    # Convert to string
    text = str(value).strip()
    
    if not text:
        return None
    
    # Try various date formats
    date_formats = [
        ('%Y%m%d', 8),           # YYYYMMDD (EDI DTM format)
        ('%y%m%d', 6),           # YYMMDD (EDI ISA format)
        ('%Y-%m-%d', 10),        # YYYY-MM-DD (ISO)
        ('%m/%d/%Y', 10),        # MM/DD/YYYY
        ('%m-%d-%Y', 10),        # MM-DD-YYYY
        ('%m/%d/%y', 8),         # MM/DD/YY (already correct format)
        ('%m-%d-%y', 8),         # MM-DD-YY
        ('%Y/%m/%d', 10),        # YYYY/MM/DD
        ('%d/%m/%Y', 10),        # DD/MM/YYYY (less common)
        ('%Y-%m-%d %H:%M:%S', None),  # ISO with time
        ('%Y-%m-%dT%H:%M:%S', None),  # ISO with T separator
    ]
    
    for fmt, expected_len in date_formats:
        try:
            # For fixed-length formats, check length first
            if expected_len and len(text) != expected_len:
                continue
            parsed = datetime.strptime(text[:expected_len] if expected_len else text.split('.')[0], fmt)
            return parsed.strftime('%m/%d/%Y')
        except (ValueError, TypeError):
            continue
    
    return None


def is_date_field(field_name):
    """
    Check if a field name indicates it contains a date value.
    Uses cached lookup for performance.
    """
    if not field_name:
        return False
    
    # Check cache first
    cache_key = f"date_{field_name}"
    if cache_key in _field_type_cache:
        return _field_type_cache[cache_key]
    
    field_upper = field_name.upper()
    
    # Exclude time-only columns
    if 'TIME' in field_upper and 'DATE' not in field_upper:
        _field_type_cache[cache_key] = False
        return False
    
    # DTM segment fields are ALWAYS dates
    if '_DTM' in field_upper:
        _field_type_cache[cache_key] = True
        return True
    
    # Check known date fields
    result = any(known in field_upper for known in _DATE_FIELD_PATTERNS)
    _field_type_cache[cache_key] = result
    return result


def normalize_value(value, field_name=None):
    """
    Normalize a single value: uppercase, strip whitespace, clean stray characters.
    Formats dates to MM/DD/YY format and currency to $X,XXX.XX format.
    
    - Dates: formatted to MM/DD/YY with leading zeros (checked FIRST by field name)
    - Currency: formatted with $ sign and commas (e.g., $1,234.56)
    - Strings: uppercase, strip leading/trailing whitespace, collapse internal whitespace
    - Numbers: formatted as currency if field indicates money
    - None/empty: left unchanged
    """
    if value is None:
        return value
    
    # Handle datetime/date objects directly
    if isinstance(value, (datetime, date)):
        return value.strftime('%m/%d/%y')
    
    # Check for date fields FIRST by field name - prevents "PaymentDate" from being 
    # formatted as currency due to containing "Payment"
    if field_name and is_date_field(field_name):
        if isinstance(value, str):
            text = value.strip()
            if text:
                formatted_date = format_date(text)
                if formatted_date:
                    return formatted_date
    
    # Check if this is a currency field - format numeric types
    if field_name and is_currency_field(field_name):
        if isinstance(value, (int, float)):
            return format_currency(value)
        # Also try to format string values that look like numbers
        if isinstance(value, str):
            text = value.strip()
            if text:
                formatted = format_currency(text)
                if formatted:
                    return formatted
    
    # Leave numeric types unchanged if not currency
    if isinstance(value, (int, float)):
        return value
    
    # Convert to string and process
    text = str(value)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    # If empty after stripping, return empty string
    if not text:
        return ''
    
    # Uppercase
    text = text.upper()
    
    # Collapse multiple spaces/tabs/newlines into single space (pre-compiled)
    text = _WHITESPACE_PATTERN.sub(' ', text)
    
    # Remove non-printable characters (pre-compiled)
    text = _CONTROL_CHAR_PATTERN.sub('', text)
    
    return text


def normalize_csv_row(row):
    """
    Normalize all values in a CSV row: uppercase, strip whitespace, clean stray characters.
    Formats date fields to MM/DD/YY format.
    
    Processes all string fields while preserving numeric values.
    """
    normalized_row = {}
    
    for field, value in row.items():
        normalized_row[field] = normalize_value(value, field_name=field)
    
    return normalized_row