"""
Enhanced Deductible Analysis Report Generator with Patient Collection Data
Joins 835 data with Fair Health ZIP CSV (patient payments) to analyze deductible collection rates.
"""

import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional


class FlexibleRow(dict):
    """
    Dict subclass that handles both space and underscore column names.

    This allows code written for CSV data (column names with spaces) to work
    seamlessly with database data (column names with underscores).

    Uses per-instance key caching to avoid repeated string operations and lookups.

    Example:
        row['CALCULATED DEDUCTIBLE'] works whether the actual key is
        'CALCULATED DEDUCTIBLE' or 'CALCULATED_DEDUCTIBLE'
    """

    __slots__ = ("_key_cache",)  # Minimize memory overhead

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cache maps requested_key -> actual_key (or None if not found)
        object.__setattr__(self, "_key_cache", {})

    def _resolve_key(self, key):
        """Resolve a requested key to its actual key in the dict, with caching."""
        cache = object.__getattribute__(self, "_key_cache")

        if key in cache:
            return cache[key]

        # Try exact match first
        if dict.__contains__(self, key):
            cache[key] = key
            return key

        # Try underscore version (CSV key → DB key)
        underscore_key = key.replace(" ", "_")
        if dict.__contains__(self, underscore_key):
            cache[key] = underscore_key
            return underscore_key

        # Try space version (DB key → CSV key)
        space_key = key.replace("_", " ")
        if dict.__contains__(self, space_key):
            cache[key] = space_key
            return space_key

        # Key not found - cache this result too
        cache[key] = None
        return None

    def __getitem__(self, key):
        actual_key = self._resolve_key(key)
        if actual_key is not None:
            return dict.__getitem__(self, actual_key)
        raise KeyError(key)

    def get(self, key, default=None):
        actual_key = self._resolve_key(key)
        if actual_key is not None:
            return dict.__getitem__(self, actual_key)
        return default

    def __contains__(self, key):
        return self._resolve_key(key) is not None


def load_data_from_database(db_path: Optional[str] = None) -> List[FlexibleRow]:
    """
    Load 835 transaction data from SQLite database.

    Returns data wrapped in FlexibleRow for seamless integration with
    existing report generation logic that expects CSV column names.

    Args:
        db_path: Path to database file. If None, uses default AppData location.

    Returns:
        List of FlexibleRow dictionaries matching CSV column access patterns
    """
    from database import get_database

    # Only load the columns actually needed for deductible reports (~20 vs 500+)
    # This is ~25x faster than SELECT *
    REQUIRED_COLUMNS = [
        # Core identifiers
        "Filename_File",
        "RUN",
        "CLM_PatientControlNumber_L2100_CLP",
        # Company/Provider
        "COMPANY",
        # Payer info
        "PAYOR_PAID",
        "Effective_PayerName",
        # Patient responsibility columns (calculated)
        "CALCULATED_DEDUCTIBLE",
        "CALCULATED_COINSURANCE",
        "CALCULATED_COPAY",
        "CALCULATED_PATIENT_NON_COVERED",
        "CALCULATED_PATIENT_OTHER",
        # Service details
        "SERVICE_DATE",
        "DATE_OF_SERVICE",
        "HCPCS",
        "SERVICE_CHARGE",
        "SERVICE_PAYMENT",
        # Patient info
        "MEMBER_ID",
        "NAME",
        # Secondary payer tracking
        "IS_PRIMARY",
        "SecondaryPayer_Name_L1000A_N1",
    ]

    print("Loading data from database...")
    db = get_database(db_path)

    # Get total count for progress
    total_count = db.get_transaction_count()
    print(f"  Found {total_count:,} transactions to load...")

    # Stream results with progress feedback
    flexible_rows = []
    last_progress = 0

    for row, row_num, total in db.query_transactions_streaming(columns=REQUIRED_COLUMNS):
        # Filter out None, empty strings, and whitespace-only values
        filtered = {k: v for k, v in row.items() if v is not None and str(v).strip() != ""}
        flexible_rows.append(FlexibleRow(filtered))

        # Progress feedback every 5%
        progress = (row_num * 100) // total if total > 0 else 100
        if progress >= last_progress + 5:
            print(f"  Loading: {row_num:,}/{total:,} ({progress}%)...")
            last_progress = progress

    print(f"  Loaded {len(flexible_rows):,} transactions from database")

    return flexible_rows


def parse_currency(value):
    """Convert currency string to float."""
    if not value or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace("$", "").replace(",", "").strip()
    if not cleaned:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date(value):
    """Parse date string to datetime object."""
    if not value or value == "":
        return None
    formats = ["%m/%d/%Y", "%m/%d/%y", "%Y%m%d", "%Y-%m-%d"]
    for fmt in formats:
        try:
            return datetime.strptime(str(value).strip(), fmt)
        except ValueError:
            continue
    return None


def get_month_name(month_num):
    """Get month name from number."""
    months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    if 1 <= month_num <= 12:
        return months[month_num - 1]
    return f"Unknown ({month_num})"


def format_dos_range(min_date, max_date):
    """Format a DOS range string from min and max dates."""
    if min_date and max_date:
        return f"{min_date.strftime('%m/%d/%Y')} - {max_date.strftime('%m/%d/%Y')}"
    elif min_date:
        return min_date.strftime("%m/%d/%Y")
    elif max_date:
        return max_date.strftime("%m/%d/%Y")
    return "No dates"


def get_aggregate_dos_range(all_company_data, year):
    """Get the aggregate DOS range for a specific year across all companies."""
    min_date = None
    max_date = None
    for data in all_company_data.values():
        if year in data.dos_min_by_year:
            if min_date is None or data.dos_min_by_year[year] < min_date:
                min_date = data.dos_min_by_year[year]
        if year in data.dos_max_by_year:
            if max_date is None or data.dos_max_by_year[year] > max_date:
                max_date = data.dos_max_by_year[year]
    return format_dos_range(min_date, max_date)


def get_company_dos_range(data, year):
    """Get the DOS range for a specific year for a single company."""
    min_date = data.dos_min_by_year.get(year)
    max_date = data.dos_max_by_year.get(year)
    return format_dos_range(min_date, max_date)


def is_medicare_part_b(payer_name):
    """Check if payer is Medicare Part B."""
    payer_upper = payer_name.upper()
    return (
        ("MEDICARE" in payer_upper and ("PART B" in payer_upper or "PARTB" in payer_upper))
        or ("PART B" in payer_upper)
        or ("MAC J" in payer_upper)
        or ("WPS GHA" in payer_upper and "PART B" in payer_upper)
        or ("PALMETTO GBA" in payer_upper)
        or ("NOVITAS" in payer_upper)
        or ("FIRST COAST" in payer_upper)
        or ("CGS ADMINISTRATORS" in payer_upper)
        or ("NGS" in payer_upper and "MEDICARE" in payer_upper)
    )


# CARC descriptions
CARC_DESCRIPTIONS = {
    # Primary PR codes (Patient Responsibility)
    "1": "Deductible Amount",
    "2": "Coinsurance Amount",
    "3": "Co-payment Amount",
    "4": "Procedure code inconsistent with modifier",
    "16": "Claim/service lacks information needed for adjudication",
    "18": "Exact duplicate claim/service",
    "19": "Claim denied; this is a work-related injury/illness",
    "20": "Claim denied; this injury/illness is covered by another payer",
    "21": "Claim denied; this injury/illness is covered by liability carrier",
    "22": "Payment adjusted; care may be covered by another payer",
    "23": "Payment adjusted; charges covered under capitation agreement",
    "24": "Payment for charges adjusted; charges covered by savings offset",
    "26": "Expenses incurred prior to coverage",
    "27": "Expenses incurred after coverage terminated",
    "29": "Payment adjusted; time limit for filing has expired",
    "31": "Claim denied as patient cannot be identified as our insured",
    "32": "Our records indicate the patient is not an eligible dependent",
    "33": "Claim denied; insured has no dependent coverage",
    "34": "Claim denied; insured has no coverage for newborns",
    "35": "Benefit maximum for this time period or occurrence has been reached",
    "39": "Services denied at the time authorization/pre-cert was requested",
    "40": "Charges do not meet qualifications for emergent/urgent care",
    "45": "Charges exceed contracted/maximum allowable amount",
    "49": "Non-covered because this is a routine/preventive exam",
    "50": "Non-covered service; not deemed a medical necessity",
    "51": "Non-covered; pre-existing condition",
    "53": "Non-covered; services by an immediate relative or household member",
    "54": "Multiple physicians/ambulances not covered for this service",
    "55": "Procedure/treatment not deemed experimental/investigational",
    "56": "Procedure/treatment has not been deemed proven to be effective",
    "58": "Treatment was deemed by the payer to have been rendered in inappropriate setting",
    "59": "Charges are processed based on multiple/other coverage rules",
    "66": "Blood deductible",
    "89": "Services not provided/authorized by designated provider",
    "96": "Non-covered charges",
    "100": "Payment made to patient/insured/guarantor",
    "101": "Predetermination: anticipated payment upon completion of services",
    "102": "Major medical adjustment",
    "103": "Provider promotional discount",
    "104": "Managed care withholding",
    "107": "Related/prior authorized claim/service not paid or adjusted",
    "108": "Rent/purchase guidelines were not met",
    "109": "Claim/service not covered by this payer per contract",
    "110": "Billing date precedes service date",
    "111": "Not covered unless the provider accepts assignment",
    "116": "Claim lacks patient information per contract",
    "117": "Patient/Insured health identification number and name do not match",
    "119": "Benefit maximum for this time period has been reached",
    "128": "Newborn's services are covered in mother's allowance",
    "129": "Prior processing information appears incorrect",
    "130": "Claim submission fee",
    "131": "Claim specific negotiated discount",
    "132": "Prearranged demonstration project adjustment",
    "133": "Premium payment withholding",
    "134": "Technical fees removed from charges",
    "135": "Interim bill",
    "136": "Failure to follow prior authorization guidelines",
    "137": "Regulatory surcharges/assessments/recovery fees",
    "138": "Appeal procedures not followed or time limits not met",
    "139": "Contracted funding agreement",
    "140": "Patient/insured health ID and name do not match",
    "141": "Claim adjustment for IRS withholding",
    "142": "Monthly Medicaid patient liability amount",
    "143": "Portion of payment held in suspense",
    "144": "Incentive adjustment",
    "146": "Diagnosis inconsistent with procedure",
    "147": "Provider contracted/negotiated rate expired",
    "148": "Information from another provider was not provided",
    "149": "Lifetime benefit maximum has been reached",
    "150": "Payment adjusted; payer deems service not reasonable/necessary",
    "151": "Payment denied/reduced; service authorization not obtained",
    "152": "Payer believes the patient was not an eligible member",
    "153": "Payer deems information submitted does not support medical necessity",
    "154": "Entity/patient eligibility/other source pay/info inconsistent",
    "155": "Patient refused the service/procedure",
    "157": "Services provided in full as contracted",
    "158": "Service/procedure provided as result of act of war",
    "159": "Service/procedure provided due to terrorism",
    "160": "Injury/illness is result of activity that is a benefit exclusion",
    "161": "Service/procedure related to provider self-referral",
    "162": "State mandated requirements not met",
    "163": "Attachment/other documentation referenced but not received",
    "164": "Attachment/other documentation referenced is illegible",
    "166": "These services cannot be paid; payer/payee cannot identify",
    "167": "Diagnosis is not covered",
    "169": "Alternate benefit has been provided",
    "170": "Payment adjusted for litigation",
    "171": "Payment denied; required cert/license missing",
    "172": "Payment adjusted; no cert from primary care physician",
    "173": "Service/equipment/drug is not covered under the benefit plan",
    "174": "Service was submitted for the wrong date",
    "175": "Prescription drug is not covered",
    "176": "Service was ordered/referred by a non-network provider",
    "177": "Patient has not met the deductible for this benefit plan",
    "178": "Services have been processed at the out-of-network level",
    "179": "Services performed by a non-network provider",
    "180": "Service requires predetermination",
    "181": "Procedure code inconsistent with place of service",
    "182": "Claim/service lacks prior authorization",
    "183": "Administrative cost",
    "184": "Medical Record Request Charge",
    "185": "Administrative billing error",
    "186": "Level of care adjustment",
    "187": "Consumer Spending Account payments",
    "188": "Service was submitted as part of a separate claim",
    "189": "This is not a covered service/benefit under patient plan",
    "190": "Payment is included in allowance for another service",
    "191": "Rendering provider is not eligible to perform this procedure",
    "192": "Non-standard adjustment code from paper remittance advice",
    "193": "Original payment decision maintained after review",
    "194": "Anesthesia performed by surgeon",
    "195": "Refund issued to an erroneous priority payer for this claim",
    "196": "Provider must accept the previously paid amount",
    "197": "Precertification/notification/authorization/pre-treatment exceeded",
    "198": "Claim/service was denied based on prior determination",
    "199": "Revenue code and procedure code do not match",
    "200": "Expenses incurred during lapse of coverage",
    "201": "Patient is responsible for any overpaid amounts",
    "202": "Non-covered days/room charge adjustment",
    "203": "Discontinued or reduced service",
    "204": "Service/equipment/drug is not covered under benefit plan",
    "205": "Pharmacy discount",
    "206": "National Drug Code (NDC) not eligible for payment",
    "207": "Claim/service adjusted based on state regulation/waiver",
    "208": "Claim/service adjusted for National Coverage Determination",
    "209": "Claim/service adjusted for Local Coverage Determination",
    "210": "Payment adjusted per clinical criteria",
    "211": "Claim payment based on pricing under demonstration project",
    "212": "Administrative cost recovery amount",
    "213": "Non-covered; days/room charge adjustment based on patient status",
    "214": "Payment adjusted per patient/family request",
    "215": "Payment adjusted; federal/state/local requirement not met",
    "216": "Plan does not cover this service for this type of injury",
    "217": "Claim denied; not submitted within patient plan's timely filing",
    "218": "Payment not allowed; misrouted claim",
    "219": "Payment denied based on this provider's certification",
    "220": "Covered under Workman's Compensation",
    "221": "Payment denied; prior cert/auth was revoked",
    "222": "Exceeds the contracted cap for fee schedule/max allowable",
    "223": "Adjustment code for use by Dental",
    "224": "Patient ID/subscriber ID does not match",
    "225": "Invoice not received within time period allowed",
    "226": "Information requested is not available at this time",
    "227": "Information requested has already been provided",
    "228": "Denied as not prescribed by a network physician",
    "229": "Claim denied; patient is enrolled in Hospice",
    "230": "Adjustment to prior overpayment determination",
    "231": "Mutually exclusive procedures cannot be done same day",
    "232": "Adjustment for services/charges related to COVID-19",
    "233": "Service rendered to member released from plan",
    "234": "This procedure is not paid unless performed with another service",
    "235": "Sales tax adjustment",
    "236": "Plan procedures not followed",
    "237": "Claim denied; not deemed preventive or screening per benefit plan",
    "238": "Claim spans ineligible/eligible periods",
    "239": "Charges adjusted based on Stop Loss provisions",
    "240": "Provider performed services after leaving the plan",
    "241": "Low income subsidy amount",
    "242": "Services not provided or authorized by designated provider",
    "243": "Services not authorized by network/primary care provider",
    "244": "Payment reduced; this service was verified but not authorized",
    "245": "Provider performance bonus",
    "246": "Service not covered when patient is not at home",
    "247": "Deductible/cost sharing for Covered D drugs",
    "248": "Plan adjustment for non-network benefit",
    "249": "Diagnosis does not match patient gender",
    "250": "Claim submitted with an invalid number of supplemental records",
    "251": "Service was performed by unlicensed provider",
    "252": "Adjusted for earlier service within time frame",
    "253": "Sequestration adjustment",
    "254": "Claim spans multiple patients",
    "255": "Rebate adjustment",
    "256": "Service not payable per provider contractual agreement",
    "257": "Patient not covered by benefit plan for member's employer",
    "258": "Claim denied due to provider ineligibility",
    "259": "Charges beyond time period in first service date",
    "260": "Claim/service denied/reduced based on MUE adjudication",
    "261": "Claim denied/reduced based on Maximum Units guidelines",
    "262": "Disputed claim",
    "263": "Corrected payment adjusted for partial period",
    "264": "Total claim reduced for performance quality reporting",
    "265": "Service not approved by payer's dental consultant",
    "266": "Service denied by prior authorization",
    "267": "Adjusted on behalf of not-for-profit health plan",
    "268": "Service cost exceeds network allowance for same service",
    "269": "Payment denied; change physician/institution arrangement",
    "270": "Claim/service not payable per covered entity",
    "271": "Coverage is not renewable or expired",
    "272": "Ambulance certification not received",
    "273": "Coverage/benefit not in effect on service date",
    "274": "Service denied; does not meet criteria for payment",
    "275": "Claim/service adjusted based on prior authorization",
    "276": "Service not covered unless performed by rendering provider",
    "277": "Patient is not enrolled in drug plan",
    "278": "Service not covered by this benefit plan",
    "279": "Service requires pre-authorization",
    "280": "Claim denied; penalty assessed for failure to comply",
    "281": "Reduction for network deficiency",
    "282": "Payment denied based on status of patient claim",
    "283": "Diagnosis does not support level of service",
    "284": "Premium contribution/adjustment",
    "285": "Payment denied; appeal decision was upheld",
    "286": "Adjustment related to appeal resolution",
    "287": "Claim denied for processing error on previous payment",
    "288": "Prior contractual reduction",
    "289": "Claim duplicate of prior processed claim",
    "290": "Claim adjusted for IRS backup withholding per taxpayer",
    "A0": "Patient refund amount",
    "A1": "Claim denied charges",
    "A5": "Medicare Claim PPS Capital Cost Outlier Amount",
    "A6": "Prior hospitalization or 30 day transfer requirement not met",
    "A7": "Presumptive Payment Adjustment",
    "A8": "Ungroupable DRG",
    "B1": "Non-covered visits",
    "B4": "Late filing penalty",
    "B5": "Coverage/program guidelines not met",
    "B7": "Provider not certified for this procedure/service",
    "B8": "Alternative services were available",
    "B9": "Patient is enrolled in a Hospice",
    "B10": "Allowed amount has been reduced",
    "B11": "Claim/service denied because diagnosis inconsistent",
    "B12": "Services not documented in patient's medical records",
    "B13": "Previously paid; payment for this claim/service included",
    "B14": "Procedure only covered for certain diagnoses/conditions",
    "B15": "This service/procedure requires authorization",
    "B16": "New patient qualifications were not met",
    "B20": "Procedure/service was partially/fully furnished by another",
    "B22": "Payment adjusted for medical necessity",
    "B23": "Procedure billed is not authorized per your Clinical Lab",
    "D23": "Payment denied; this is a duplicate of a service considered",
    "N130": "Consult the Provider Remittance Advice/Payment information",
    "N381": "Alert: Additional information is included in another part",
    "N432": "Alert: Adjustment based on law (not covered)",
    "N519": "Services not considered due to missing/invalid diagnosis codes",
    "N522": "Adjusted based on applicable Demonstration project",
    "N527": "Payment reflects allowable under demonstration project",
    "N533": "Service/supply not covered for specified diagnosis/procedure",
    "N538": "Actual payment based on HCPCS unit conversion factor",
    "N657": "Adjusted for Medicare Multiple Procedure Payment Reduction",
    "W1": "Workers compensation state fee schedule amount",
}


def load_trip_credits(trip_path):
    """Load and aggregate patient payments by RUN from Fair Health ZIP CSV.

    Supports both new format (FAIR HEALTH ZIP[RUN], FAIR HEALTH ZIP[PT PAID])
    and legacy format (RUN, AMOUNT columns).
    """
    print(f"Loading Patient Payments from: {trip_path}")

    run_payments = defaultdict(lambda: {"total": 0.0, "payment_count": 0})

    with open(trip_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Support both new format (FAIR HEALTH ZIP[...]) and legacy format
            run = row.get("FAIR HEALTH ZIP[RUN]", "").strip()
            amount_str = row.get("FAIR HEALTH ZIP[PT PAID]", "")

            # Fallback to legacy column names if new format not found
            if not run:
                run = row.get("RUN", "").strip()
            if not amount_str:
                amount_str = row.get("AMOUNT", "")

            amount = float(amount_str or 0)

            if run and amount:
                run_payments[run]["total"] += amount
                run_payments[run]["payment_count"] += 1

    print(f"  Loaded {len(run_payments):,} unique RUNs with payments")
    print(f"  Total Patient Payments: ${sum(r['total'] for r in run_payments.values()):,.2f}")

    return run_payments


class CompanyData:
    """Stores data for a single company."""

    def __init__(self, company_id, company_name):
        self.company_id = company_id
        self.company_name = company_name
        self.total_records = 0
        self.calculated_deductible_total = 0.0
        self.pr1_cas_total = 0.0
        self.patient_collected_total = 0.0
        self.patient_collected_for_pr = 0.0  # Collections on claims with any PR
        self.claims_with_deductible = 0
        self.claims_with_collection = 0
        self.all_claims = set()  # Track all unique claim numbers
        self.claims_with_ded_set = set()  # Track unique claims with deductible
        self.claims_with_pr_set = set()  # Track unique claims with any PR
        # Total Patient Responsibility components (ALL claims)
        self.total_coinsurance = 0.0
        self.total_copay = 0.0
        self.total_noncovered = 0.0
        self.total_other_pr = 0.0
        # PR components specifically from DEDUCTIBLE lines only
        self.ded_lines_total_pr = 0.0  # Total PR on lines with deductible
        self.ded_lines_other_pr = 0.0  # Other PR (non-deductible) on lines with deductible
        self.ded_lines_coinsurance = 0.0
        self.ded_lines_copay = 0.0
        self.ded_lines_noncovered = 0.0
        self.ded_lines_other = 0.0
        self.payer_deductibles = defaultdict(
            lambda: {
                "runs": set(),
                "total": 0.0,
                "total_pr": 0.0,
                "collected": 0.0,
                "secondary_collected": 0.0,
                "claims": set(),
            }
        )
        # Changed to use (year, month) tuple keys for year-month breakdown
        self.medicare_by_year_month = defaultdict(
            lambda: {"runs": set(), "amount": 0.0, "total_pr": 0.0, "collected": 0.0, "secondary_collected": 0.0}
        )
        self.other_by_year_month = defaultdict(
            lambda: {"runs": set(), "amount": 0.0, "total_pr": 0.0, "collected": 0.0, "secondary_collected": 0.0}
        )
        # Track RUN -> month mapping for secondary allocation
        self.run_to_month = {}  # RUN -> {'month': int, 'is_medicare': bool, 'payer': str, 'year': int}
        self.medicare_amounts = []
        self.medicare_members = defaultdict(lambda: {"total": 0.0, "total_pr": 0.0, "collected": 0.0, "claims": 0})
        self.medicare_runs_collected = set()
        self.top_payer_name = None
        self.top_payer_by_month = defaultdict(lambda: {"runs": set(), "amount": 0.0, "total_pr": 0.0, "collected": 0.0})
        self.top_payer_members = defaultdict(lambda: {"total": 0.0, "total_pr": 0.0, "collected": 0.0, "claims": 0})
        self.top_payer_amounts = []
        self.top_payer_lines = []  # Will store dict with ded, total_pr, collected
        self.pr_adjustments = defaultdict(float)
        self.pr_adjustments_with_ded = defaultdict(float)  # PR adjustments on claims WITH deductible
        self.pr_adjustments_by_year = defaultdict(lambda: defaultdict(float))  # year -> code -> amount
        self.pr_adjustments_with_ded_by_year = defaultdict(lambda: defaultdict(float))  # year -> code -> amount
        # Claim counts for PR adjustments
        self.pr_adjustments_claims = defaultdict(set)  # code -> set of claim numbers
        self.pr_adjustments_claims_with_ded = defaultdict(set)  # code -> set of claim numbers (with deductible)
        self.pr_adjustments_claims_by_year = defaultdict(lambda: defaultdict(set))  # year -> code -> set of claims
        self.pr_adjustments_claims_with_ded_by_year = defaultdict(
            lambda: defaultdict(set)
        )  # year -> code -> set of claims
        # PR adjustments by payer type (medicare vs other)
        self.pr_adjustments_medicare = defaultdict(float)  # code -> amount
        self.pr_adjustments_other = defaultdict(float)  # code -> amount
        self.pr_adjustments_claims_medicare = defaultdict(set)  # code -> set of claims
        self.pr_adjustments_claims_other = defaultdict(set)  # code -> set of claims
        self.pr_adjustments_medicare_by_year = defaultdict(lambda: defaultdict(float))  # year -> code -> amount
        self.pr_adjustments_other_by_year = defaultdict(lambda: defaultdict(float))  # year -> code -> amount
        self.pr_adjustments_claims_medicare_by_year = defaultdict(lambda: defaultdict(set))  # year -> code -> claims
        self.pr_adjustments_claims_other_by_year = defaultdict(lambda: defaultdict(set))  # year -> code -> claims
        # Deductible claims by year for Layer 2
        self.ded_claims_by_year = defaultdict(set)  # year -> set of claims with deductible
        self.collection_by_amount_range = defaultdict(lambda: {"deductible": 0.0, "collected": 0.0, "count": 0})
        self.rows = []
        self.ded_runs_collected = set()
        self.top_payer_runs_collected = set()

        # === SECONDARY PAYER RECOVERY TRACKING ===
        # Track ALL primary claims (regardless of deductible) - matched to secondary by RUN
        self.all_forwarded_claims = {}  # RUN -> {total_pr, primary_payer, secondary_payer_name}
        # Track primary claims where deductible was applied - matched to secondary by RUN
        self.forwarded_claims = {}  # RUN -> {deductible, total_pr, primary_payer, secondary_payer_name}
        # Track secondary payer payments (any claim processed as secondary)
        self.secondary_claims = {}  # RUN -> {payment, payer_name}

        # Aggregated secondary recovery metrics - ALL CLAIMS (populated after reconciliation)
        self.all_secondary_recovery_total = 0.0  # Total secondary payments on ALL claims
        self.all_claims_forwarded_count = 0  # Count of ALL primary claims tracked
        self.all_claims_secondary_paid_count = 0  # Count where secondary paid (all claims)
        self.all_pr_forwarded_total = 0.0  # Total PR on ALL primary claims

        # Aggregated secondary recovery metrics - DEDUCTIBLE CLAIMS ONLY (populated after reconciliation)
        self.secondary_recovery_total = 0.0  # Total secondary payments on deductible claims
        self.claims_forwarded_count = 0  # Count of deductible claims with primary record
        self.claims_secondary_paid_count = 0  # Count where secondary made payment
        self.deductible_forwarded_total = 0.0  # Total deductible on claims with secondary
        # By secondary payer type
        self.secondary_payer_recovery = defaultdict(
            lambda: {"claims": 0, "primary_deductible": 0.0, "primary_total_pr": 0.0, "secondary_payment": 0.0}
        )

        # === LOSS TRACKING (deductible on zero-collection claims) ===
        # Track RUN -> deductible info for LOSS calculation
        self.run_ded_info = {}  # RUN -> {'deductible': float, 'payer': str, 'month': int, 'is_medicare': bool}
        # Track RUN -> payer -> info for accurate per-payer LOSS attribution
        self.run_payer_info = defaultdict(
            lambda: defaultdict(lambda: {"deductible": 0.0, "month": None, "is_medicare": False, "year": None})
        )
        # LOSS totals (populated after reconciliation)
        self.loss_total = 0.0
        self.loss_medicare = 0.0
        self.loss_other = 0.0
        self.loss_by_payer = defaultdict(float)
        # Changed to use (year, month) tuple keys
        self.loss_by_year_month_medicare = defaultdict(float)
        self.loss_by_year_month_other = defaultdict(float)
        self.loss_runs = set()  # RUNs with zero collection

        # === PATIENT TRACKING ===
        # Track patients most affected by deductible
        self.patient_deductibles = defaultdict(
            lambda: {
                "name": "",
                "runs": set(),
                "deductible": 0.0,
                "total_pr": 0.0,
                "collected": 0.0,
                "secondary": 0.0,
                "loss": 0.0,
            }
        )

        # === YEAR-BASED TRACKING (2024, 2025) ===
        # DOS range tracking per year
        self.dos_min_by_year = {}  # year -> min date
        self.dos_max_by_year = {}  # year -> max date

        # Helper to create year metrics dict
        def make_year_metrics():
            return {
                "total_pr": 0.0,
                "deductible": 0.0,
                "coinsurance": 0.0,
                "copay": 0.0,
                "noncovered": 0.0,
                "other_pr": 0.0,
                "patient_pmt": 0.0,
                "secondary_pmt": 0.0,
                "loss": 0.0,
                "runs": set(),
                "claims": set(),
                "claims_with_pr": set(),
                "claims_with_ded": set(),
                "ded_runs_collected": set(),
            }

        # Layer 1 metrics by year (ALL claims)
        self.all_claims_by_year = defaultdict(set)
        self.claims_with_pr_by_year = defaultdict(set)
        self.claims_with_ded_by_year = defaultdict(set)
        self.total_pr_by_year = defaultdict(float)  # deductible component
        self.coinsurance_by_year = defaultdict(float)
        self.copay_by_year = defaultdict(float)
        self.noncovered_by_year = defaultdict(float)
        self.other_pr_by_year = defaultdict(float)
        self.patient_collected_for_pr_by_year = defaultdict(float)
        self.all_secondary_by_year = defaultdict(float)

        # Layer 1 metrics by year AND payer type (ALL claims, Medicare vs Other)
        def make_layer1_metrics():
            return {
                "claims": set(),
                "pr_claims": set(),
                "deductible": 0.0,
                "coinsurance": 0.0,
                "copay": 0.0,
                "noncovered": 0.0,
                "other_pr": 0.0,
                "patient_pmt": 0.0,
                "secondary_pmt": 0.0,
            }

        self.layer1_medicare_by_year = defaultdict(make_layer1_metrics)
        self.layer1_other_by_year = defaultdict(make_layer1_metrics)

        # Layer 2 metrics by year (DEDUCTIBLE claims only)
        self.ded_lines_total_pr_by_year = defaultdict(float)
        self.ded_lines_deductible_by_year = defaultdict(float)
        self.ded_lines_coinsurance_by_year = defaultdict(float)
        self.ded_lines_copay_by_year = defaultdict(float)
        self.ded_lines_noncovered_by_year = defaultdict(float)
        self.ded_lines_other_by_year = defaultdict(float)
        self.patient_collected_by_year = defaultdict(float)
        self.secondary_recovery_by_year = defaultdict(float)
        self.ded_runs_collected_by_year = defaultdict(set)

        # Medicare vs Other by year
        self.medicare_by_year = defaultdict(make_year_metrics)
        self.other_by_year = defaultdict(make_year_metrics)

        # Payer data by year: payer_name -> year -> metrics
        self.payer_by_year = defaultdict(lambda: defaultdict(make_year_metrics))

        # LOSS by year
        self.loss_by_year = defaultdict(float)
        self.loss_medicare_by_year = defaultdict(float)
        self.loss_other_by_year = defaultdict(float)
        self.loss_by_payer_year = defaultdict(lambda: defaultdict(float))

        # Patient data by year: member_id -> year -> metrics
        self.patient_by_year = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "name": "",
                    "runs": set(),
                    "deductible": 0.0,
                    "total_pr": 0.0,
                    "collected": 0.0,
                    "secondary": 0.0,
                    "loss": 0.0,
                }
            )
        )

        # Patient data by year and payer type: member_id -> year -> metrics
        self.patient_by_year_medicare = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "name": "",
                    "runs": set(),
                    "deductible": 0.0,
                    "total_pr": 0.0,
                    "collected": 0.0,
                    "secondary": 0.0,
                    "loss": 0.0,
                }
            )
        )
        self.patient_by_year_other = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "name": "",
                    "runs": set(),
                    "deductible": 0.0,
                    "total_pr": 0.0,
                    "collected": 0.0,
                    "secondary": 0.0,
                    "loss": 0.0,
                }
            )
        )

        # Track RUN -> year mapping
        self.run_to_year = {}  # RUN -> year


def generate_company_report(
    data, run_payments, output_path, source_description="835_consolidated_output.csv + Trip_Credits.csv"
):
    """Generate an enhanced deductible analysis report for a single company."""

    # Find top commercial payer
    sorted_payers = sorted(data.payer_deductibles.items(), key=lambda x: x[1]["total"], reverse=True)

    for payer_name, payer_data in sorted_payers:
        if not is_medicare_part_b(payer_name):
            data.top_payer_name = payer_name
            break

    if not data.top_payer_name and sorted_payers:
        data.top_payer_name = sorted_payers[0][0]

    # Second pass for top payer detailed analysis
    if data.top_payer_name:
        for row in data.rows:
            calc_ded = parse_currency(row.get("CALCULATED DEDUCTIBLE", 0))
            payer_name = row.get("PAYOR PAID", "") or row.get("Effective_PayerName", "") or "UNKNOWN"
            payer_name = payer_name.strip().upper() if payer_name else "UNKNOWN"

            if payer_name == data.top_payer_name and calc_ded != 0:
                service_date_str = row.get("SERVICE DATE", "") or row.get("DATE OF SERVICE", "")
                service_date = parse_date(service_date_str)
                member_id = row.get("MEMBER ID", "") or "UNKNOWN"
                claim_num = row.get("CLM_PatientControlNumber_L2100_CLP", "")
                run_num = row.get("RUN", "")
                hcpcs = row.get("HCPCS", "")
                charge = parse_currency(row.get("SERVICE CHARGE", 0))
                payment = parse_currency(row.get("SERVICE PAYMENT", 0))

                # Get all PR components for this line
                calc_coins = parse_currency(row.get("CALCULATED COINSURANCE", 0))
                calc_copay = parse_currency(row.get("CALCULATED COPAY", 0))
                calc_noncov = parse_currency(row.get("CALCULATED PATIENT NON COVERED", 0))
                calc_other = parse_currency(row.get("CALCULATED PATIENT OTHER", 0))
                total_pr = calc_ded + calc_coins + calc_copay + calc_noncov + calc_other

                # Get patient collection for this RUN
                collected = run_payments.get(run_num, {}).get("total", 0.0)

                top_payer_run_collection_not_counted = run_num and run_num not in data.top_payer_runs_collected

                if service_date:
                    month = service_date.month
                    if run_num:
                        data.top_payer_by_month[month]["runs"].add(run_num)
                    data.top_payer_by_month[month]["amount"] += calc_ded
                    data.top_payer_by_month[month]["total_pr"] += total_pr
                    if top_payer_run_collection_not_counted:
                        data.top_payer_by_month[month]["collected"] += collected

                data.top_payer_members[member_id]["total"] += calc_ded
                data.top_payer_members[member_id]["total_pr"] += total_pr
                if top_payer_run_collection_not_counted:
                    data.top_payer_members[member_id]["collected"] += collected
                    data.top_payer_runs_collected.add(run_num)
                data.top_payer_members[member_id]["claims"] += 1
                data.top_payer_amounts.append(calc_ded)
                data.top_payer_lines.append(
                    {
                        "amount": calc_ded,
                        "total_pr": total_pr,
                        "collected": collected if top_payer_run_collection_not_counted else 0,
                        "claim": claim_num,
                        "hcpcs": hcpcs,
                        "charge": charge,
                        "payment": payment,
                    }
                )

    # Calculate collection rate (stored for potential future use)
    _collection_rate = (
        (data.patient_collected_total / data.calculated_deductible_total * 100)
        if data.calculated_deductible_total > 0
        else 0
    )

    # Dynamically determine years with data (fixes math error when data spans multiple years)
    # FIXED: Only include 2024 and 2025 in report years
    all_data_years = sorted(data.claims_with_ded_by_year.keys()) if data.claims_with_ded_by_year else []
    report_years = [yr for yr in all_data_years if yr in [2024, 2025]]
    if not report_years:
        report_years = [2024, 2025]  # Fallback to defaults

    # FIXED: Calculate year-filtered totals so TOTAL = sum(year columns)
    # Layer 1 totals (ALL claims)
    filtered_deductible_total = sum(data.total_pr_by_year.get(yr, 0) for yr in report_years)
    filtered_coinsurance = sum(data.coinsurance_by_year.get(yr, 0) for yr in report_years)
    filtered_copay = sum(data.copay_by_year.get(yr, 0) for yr in report_years)
    filtered_noncovered = sum(data.noncovered_by_year.get(yr, 0) for yr in report_years)
    filtered_other_pr = sum(data.other_pr_by_year.get(yr, 0) for yr in report_years)
    filtered_patient_collected_for_pr = sum(data.patient_collected_for_pr_by_year.get(yr, 0) for yr in report_years)
    _filtered_all_secondary = sum(data.all_secondary_by_year.get(yr, 0) for yr in report_years)
    filtered_total_claims = sum(len(data.all_claims_by_year.get(yr, set())) for yr in report_years)
    filtered_claims_with_pr = sum(len(data.claims_with_pr_by_year.get(yr, set())) for yr in report_years)
    filtered_claims_with_ded = sum(len(data.claims_with_ded_by_year.get(yr, set())) for yr in report_years)

    # Layer 2 totals (DEDUCTIBLE claims only)
    filtered_ded_claims = sum(len(data.claims_with_ded_by_year.get(yr, set())) for yr in report_years)
    filtered_ded_deductible = sum(data.ded_lines_deductible_by_year.get(yr, 0) for yr in report_years)
    filtered_ded_coinsurance = sum(data.ded_lines_coinsurance_by_year.get(yr, 0) for yr in report_years)
    filtered_ded_copay = sum(data.ded_lines_copay_by_year.get(yr, 0) for yr in report_years)
    filtered_ded_noncovered = sum(data.ded_lines_noncovered_by_year.get(yr, 0) for yr in report_years)
    filtered_ded_other = sum(data.ded_lines_other_by_year.get(yr, 0) for yr in report_years)
    filtered_ded_total_pr = sum(data.ded_lines_total_pr_by_year.get(yr, 0) for yr in report_years)
    filtered_patient_collected = sum(data.patient_collected_by_year.get(yr, 0) for yr in report_years)
    filtered_secondary_recovery = sum(data.secondary_recovery_by_year.get(yr, 0) for yr in report_years)
    _filtered_loss = sum(data.loss_by_year.get(yr, 0) for yr in report_years)

    # Generate report
    report = []
    report.append("=" * 100)
    report.append("DEDUCTIBLE DEEP DIVE ANALYSIS REPORT - WITH PATIENT COLLECTION DATA")
    report.append("=" * 100)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Company: {data.company_name}")
    report.append(f"Company ID: {data.company_id}")
    report.append(f"Source: {source_description}")
    report.append(f"Total Records: {data.total_records:,}")
    report.append("=" * 100)
    report.append("")

    # Section 1: Patient Responsibility & Collection Summary
    report.append("SECTION 1: PATIENT RESPONSIBILITY & COLLECTION SUMMARY")
    report.append("-" * 100)
    report.append("")

    # Calculate total patient responsibility (ALL claims) - FIXED: Use year-filtered totals
    total_pr = (
        filtered_deductible_total + filtered_coinsurance + filtered_copay + filtered_noncovered + filtered_other_pr
    )
    total_pr_collection_rate = (filtered_patient_collected_for_pr / total_pr * 100) if total_pr > 0 else 0

    # Claim volume metrics - FIXED: Use year-filtered counts
    total_claims = filtered_total_claims
    claims_with_ded = filtered_claims_with_ded
    claims_with_pr = filtered_claims_with_pr
    ded_lines = data.claims_with_deductible
    pct_claims_with_ded = (claims_with_ded / total_claims * 100) if total_claims > 0 else 0
    pct_claims_with_pr = (claims_with_pr / total_claims * 100) if total_claims > 0 else 0

    # Deductible lines specific calculations - FIXED: Use year-filtered totals
    ded_lines_pr_rate = (filtered_patient_collected / filtered_ded_total_pr * 100) if filtered_ded_total_pr > 0 else 0

    # --- LAYER 1: OVERALL CONTEXT ---
    report.append("=" * 75)
    report.append("LAYER 1: OVERALL CONTEXT (All Claims)")
    report.append("=" * 75)
    report.append("")
    report.append("CLAIM VOLUME:")
    report.append(f"{'Total Unique Claims':<50} {total_claims:>20,}")
    report.append(f"{'Claims with Any Patient Responsibility':<50} {claims_with_pr:>20,} ({pct_claims_with_pr:.1f}%)")
    report.append(f"{'Claims with Deductible Specifically':<50} {claims_with_ded:>20,} ({pct_claims_with_ded:.1f}%)")
    report.append("")

    report.append("TOTAL PATIENT RESPONSIBILITY (ALL CLAIMS):")
    report.append(f"{'Component':<50} {'Amount':>20}")
    report.append("-" * 75)
    report.append(f"{'Deductible (PR-1)':<50} ${filtered_deductible_total:>18,.2f}")
    report.append(f"{'Coinsurance (PR-2)':<50} ${filtered_coinsurance:>18,.2f}")
    report.append(f"{'Copay (PR-3)':<50} ${filtered_copay:>18,.2f}")
    report.append(f"{'Non-Covered (Patient Resp)':<50} ${filtered_noncovered:>18,.2f}")
    report.append(f"{'Other Patient Responsibility':<50} ${filtered_other_pr:>18,.2f}")
    report.append("-" * 75)
    report.append(f"{'TOTAL PATIENT RESPONSIBILITY':<50} ${total_pr:>18,.2f}")
    report.append("")

    report.append("OVERALL COLLECTION (ALL PR CLAIMS):")
    report.append(f"{'Patient Payments Collected':<50} ${filtered_patient_collected_for_pr:>18,.2f}")
    report.append(f"{'Uncollected Patient Responsibility':<50} ${total_pr - filtered_patient_collected_for_pr:>18,.2f}")
    report.append(f"{'COLLECTION RATE vs TOTAL PR':<50} {total_pr_collection_rate:>18.1f}%")
    report.append("")

    # === LAYER 1 YEAR BREAKDOWN ===
    for yr in report_years:
        dos_range = get_company_dos_range(data, yr)
        yr_claims = len(data.all_claims_by_year.get(yr, set()))
        yr_claims_with_pr = len(data.claims_with_pr_by_year.get(yr, set()))
        yr_claims_with_ded = len(data.claims_with_ded_by_year.get(yr, set()))
        yr_total_pr = data.total_pr_by_year.get(yr, 0)

        if yr_claims == 0 and yr_total_pr == 0:
            continue

        yr_pct_pr = (yr_claims_with_pr / yr_claims * 100) if yr_claims > 0 else 0
        yr_pct_ded = (yr_claims_with_ded / yr_claims * 100) if yr_claims > 0 else 0

        report.append(f"--- {yr} DOS ({dos_range}) ---")
        report.append(f"{'Total Unique Claims':<50} {yr_claims:>20,}")
        report.append(f"{'Claims with Any Patient Responsibility':<50} {yr_claims_with_pr:>20,} ({yr_pct_pr:.1f}%)")
        report.append(f"{'Claims with Deductible Specifically':<50} {yr_claims_with_ded:>20,} ({yr_pct_ded:.1f}%)")
        report.append(f"{'Total Patient Responsibility':<50} ${yr_total_pr:>18,.2f}")
        report.append("")

    report.append("")

    # --- LAYER 2: DEDUCTIBLE CLAIMS FOCUS ---
    report.append("=" * 130)
    report.append("LAYER 2: DEDUCTIBLE CLAIMS DEEP DIVE")
    report.append("=" * 130)
    report.append("")

    # Get year data dynamically
    yr_data = {}
    for yr in report_years:
        yr_data[yr] = {
            "ded": data.ded_lines_deductible_by_year.get(yr, 0),
            "coins": data.ded_lines_coinsurance_by_year.get(yr, 0),
            "copay": data.ded_lines_copay_by_year.get(yr, 0),
            "noncov": data.ded_lines_noncovered_by_year.get(yr, 0),
            "other": data.ded_lines_other_by_year.get(yr, 0),
            "total": data.ded_lines_total_pr_by_year.get(yr, 0),
            "patient": data.patient_collected_by_year.get(yr, 0),
            "secondary": data.secondary_recovery_by_year.get(yr, 0),
            "claims": len(data.claims_with_ded_by_year.get(yr, set())),
        }
        yr_data[yr]["coll"] = yr_data[yr]["patient"] + yr_data[yr]["secondary"]
        yr_data[yr]["uncoll"] = yr_data[yr]["total"] - yr_data[yr]["coll"]
        yr_data[yr]["rate"] = (yr_data[yr]["coll"] / yr_data[yr]["total"] * 100) if yr_data[yr]["total"] > 0 else 0

    # FIXED: Use year-filtered totals so TOTAL = sum(year columns)
    total_ded_claims = filtered_ded_claims
    total_collected_on_ded_lines = filtered_patient_collected + filtered_secondary_recovery
    uncollected_pr = filtered_ded_total_pr - total_collected_on_ded_lines
    total_collected_rate = (
        (total_collected_on_ded_lines / filtered_ded_total_pr * 100) if filtered_ded_total_pr > 0 else 0
    )

    # Build dynamic header with year columns
    year_headers = " ".join(f"{str(yr):>18}" for yr in report_years)
    report.append("PATIENT RESPONSIBILITY ON DEDUCTIBLE CLAIMS:")
    report.append(f"{'Component':<35} {year_headers} {'TOTAL':>18}")
    report.append("-" * 130)

    # Claims row
    claims_values = " ".join(f'{yr_data[yr]["claims"]:>18,}' for yr in report_years)
    report.append(f"{'Claims with Deductible':<35} {claims_values} {total_ded_claims:>18,}")

    # Deductible row - FIXED: Use year-filtered total
    ded_values = " ".join(f'${yr_data[yr]["ded"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Deductible (PR-1)':<35} {ded_values} ${filtered_ded_deductible:>16,.2f}")

    # Coinsurance row - FIXED: Use year-filtered total
    coins_values = " ".join(f'${yr_data[yr]["coins"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Coinsurance (PR-2)':<35} {coins_values} ${filtered_ded_coinsurance:>16,.2f}")

    # Copay row - FIXED: Use year-filtered total
    copay_values = " ".join(f'${yr_data[yr]["copay"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Copay (PR-3)':<35} {copay_values} ${filtered_ded_copay:>16,.2f}")

    # Non-covered row - FIXED: Use year-filtered total
    noncov_values = " ".join(f'${yr_data[yr]["noncov"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Non-Covered (Patient Resp)':<35} {noncov_values} ${filtered_ded_noncovered:>16,.2f}")

    # Other row - FIXED: Use year-filtered total
    other_values = " ".join(f'${yr_data[yr]["other"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Other Patient Responsibility':<35} {other_values} ${filtered_ded_other:>16,.2f}")

    report.append("-" * 130)

    # Total PR row - FIXED: Use year-filtered total
    total_values = " ".join(f'${yr_data[yr]["total"]:>16,.2f}' for yr in report_years)
    report.append(f"{'TOTAL PR ON DEDUCTIBLE LINES':<35} {total_values} ${filtered_ded_total_pr:>16,.2f}")
    report.append("")

    # Collection table with year columns
    report.append("COLLECTION ON DEDUCTIBLE LINES:")
    report.append(f"{'Metric':<35} {year_headers} {'TOTAL':>18}")
    report.append("-" * 130)

    # Patient payments row - FIXED: Use year-filtered total
    patient_values = " ".join(f'${yr_data[yr]["patient"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Patient Payments':<35} {patient_values} ${filtered_patient_collected:>16,.2f}")

    # Secondary payments row - FIXED: Use year-filtered total
    secondary_values = " ".join(f'${yr_data[yr]["secondary"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Secondary Payer Payments':<35} {secondary_values} ${filtered_secondary_recovery:>16,.2f}")

    report.append("-" * 130)

    # Total collected row
    coll_values = " ".join(f'${yr_data[yr]["coll"]:>16,.2f}' for yr in report_years)
    report.append(f"{'TOTAL COLLECTED':<35} {coll_values} ${total_collected_on_ded_lines:>16,.2f}")

    # Uncollected row
    uncoll_values = " ".join(f'${yr_data[yr]["uncoll"]:>16,.2f}' for yr in report_years)
    report.append(f"{'UNCOLLECTED':<35} {uncoll_values} ${uncollected_pr:>16,.2f}")

    report.append("-" * 130)

    # Collection rate row
    rate_values = " ".join(f'{yr_data[yr]["rate"]:>17.1f}%' for yr in report_years)
    report.append(f"{'COLLECTION RATE':<35} {rate_values} {total_collected_rate:>17.1f}%")
    report.append("")
    report.append("")

    # Section 1A: Patient Responsibility Adjustment Breakdown - Year columns side-by-side with claim counts
    if data.pr_adjustments:
        report.append("=" * 210)
        report.append("SECTION 1A: PATIENT RESPONSIBILITY (PR) ADJUSTMENT BREAKDOWN")
        report.append("-" * 210)
        report.append("")

        # Build dynamic header based on report_years
        header_parts = [f"{'Code':<8}", f"{'Description':<26}"]
        for yr in report_years:
            header_parts.append(f"{yr} Clms".rjust(9))
            header_parts.append(f"{yr} Amount".rjust(14))
        header_parts.extend(
            [f"{'Total Clms':>10}", f"{'Total Amount':>14}", f"{'w/Ded Clms':>10}", f"{'w/Ded Amount':>14}"]
        )
        report.append(" ".join(header_parts))
        report.append("-" * 210)

        # Sort by total amount descending
        sorted_pr = sorted(data.pr_adjustments.items(), key=lambda x: abs(x[1]), reverse=True)

        # Dynamic totals per year
        year_totals_claims = {yr: 0 for yr in report_years}
        year_totals_amount = {yr: 0.0 for yr in report_years}
        total_claims = 0
        total_all = 0.0
        total_ded_claims = 0
        total_ded = 0.0

        for code, amount in sorted_pr:
            reason_num = code.replace("PR-", "")
            desc = CARC_DESCRIPTIONS.get(reason_num, "Other Patient Responsibility")[:26]

            row_parts = [f"{code:<8}", f"{desc:<26}"]
            for yr in report_years:
                yr_pr = data.pr_adjustments_by_year.get(yr, {})
                yr_claims = data.pr_adjustments_claims_by_year.get(yr, {})
                amt = yr_pr.get(code, 0.0)
                clms = len(yr_claims.get(code, set()))
                row_parts.append(f"{clms:>9,}")
                row_parts.append(f"${amt:>12,.2f}")
                year_totals_claims[yr] += clms
                year_totals_amount[yr] += amt

            # FIXED: Calculate year-filtered totals so TOTAL = sum(year columns)
            claims_total = sum(
                len(data.pr_adjustments_claims_by_year.get(yr, {}).get(code, set())) for yr in report_years
            )
            amount_filtered = sum(data.pr_adjustments_by_year.get(yr, {}).get(code, 0.0) for yr in report_years)
            claims_ded = sum(
                len(data.pr_adjustments_claims_with_ded_by_year.get(yr, {}).get(code, set())) for yr in report_years
            )
            ded_total = sum(data.pr_adjustments_with_ded_by_year.get(yr, {}).get(code, 0.0) for yr in report_years)
            row_parts.extend(
                [f"{claims_total:>10,}", f"${amount_filtered:>12,.2f}", f"{claims_ded:>10,}", f"${ded_total:>12,.2f}"]
            )
            report.append(" ".join(row_parts))

            total_claims += claims_total
            total_all += amount_filtered
            total_ded_claims += claims_ded
            total_ded += ded_total

        report.append("-" * 210)
        total_parts = [f"{'TOTAL':<8}", f"{'':<26}"]
        for yr in report_years:
            total_parts.append(f"{year_totals_claims[yr]:>9,}")
            total_parts.append(f"${year_totals_amount[yr]:>12,.2f}")
        total_parts.extend(
            [f"{total_claims:>10,}", f"${total_all:>12,.2f}", f"{total_ded_claims:>10,}", f"${total_ded:>12,.2f}"]
        )
        report.append(" ".join(total_parts))
        report.append("")
        report.append("")

    # Section 3: Payer Analysis - Split by Medicare and Other
    report.append("=" * 135)
    report.append("SECTION 3: PAYER ANALYSIS (Deductible Claims)")
    report.append("-" * 135)
    report.append("")

    # Split payers into Medicare and Other
    medicare_payers = []
    other_payers = []
    for payer_name, payer_data in sorted_payers:
        if payer_data["total"] > 0:
            if is_medicare_part_b(payer_name):
                medicare_payers.append((payer_name, payer_data))
            else:
                other_payers.append((payer_name, payer_data))

    # Section 3A: Traditional Medicare Payers
    report.append("SECTION 3A: TRADITIONAL MEDICARE PAYERS")
    report.append("-" * 145)
    report.append(
        f"{'Payer Name':<35} {'Claims':>8} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
    )
    report.append("-" * 145)

    medicare_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
    for payer_name, payer_data in medicare_payers:
        total_pr = payer_data.get("total_pr", 0)
        patient_pmt = payer_data["collected"]
        secondary_pmt = payer_data.get("secondary_collected", 0)
        loss = data.loss_by_payer.get(payer_name, 0)
        claims = len(payer_data.get("claims", set()))
        total_coll = patient_pmt + secondary_pmt
        uncollected = total_pr - total_coll
        rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

        medicare_totals["total_pr"] += total_pr
        medicare_totals["patient_pmt"] += patient_pmt
        medicare_totals["secondary_pmt"] += secondary_pmt
        medicare_totals["loss"] += loss
        medicare_totals["claims"] += claims

        report.append(
            f"{payer_name[:35]:<35} {claims:>8,} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
        )

    # Medicare subtotal
    med_total_coll = medicare_totals["patient_pmt"] + medicare_totals["secondary_pmt"]
    med_uncoll = medicare_totals["total_pr"] - med_total_coll
    med_rate = (med_total_coll / medicare_totals["total_pr"] * 100) if medicare_totals["total_pr"] > 0 else 0
    report.append("-" * 145)
    report.append(
        f"{'MEDICARE SUBTOTAL':<35} {medicare_totals['claims']:>8,} ${medicare_totals['total_pr']:>10,.0f} ${medicare_totals['secondary_pmt']:>9,.0f} ${medicare_totals['patient_pmt']:>10,.0f} ${med_total_coll:>9,.0f} ${med_uncoll:>10,.0f} {med_rate:>5.1f}% ${medicare_totals['loss']:>10,.0f}"
    )
    report.append("")

    # === SECTION 3A YEAR BREAKDOWN ===
    for yr in report_years:
        dos_range = get_company_dos_range(data, yr)
        yr_metrics = data.medicare_by_year.get(yr, {})
        yr_total_pr = yr_metrics.get("total_pr", 0)

        if yr_total_pr == 0:
            continue

        yr_patient = yr_metrics.get("patient_pmt", 0)  # FIXED: was 'collected'
        yr_secondary = yr_metrics.get("secondary_pmt", 0)  # FIXED: was 'secondary_collected'
        yr_loss = data.loss_medicare_by_year.get(yr, 0)
        yr_total_coll = yr_patient + yr_secondary
        yr_uncoll = yr_total_pr - yr_total_coll
        yr_rate = (yr_total_coll / yr_total_pr * 100) if yr_total_pr > 0 else 0

        report.append(f"--- {yr} DOS ({dos_range}) ---")
        report.append(
            f"{'MEDICARE':<35} ${yr_total_pr:>10,.0f} ${yr_secondary:>9,.0f} ${yr_patient:>10,.0f} ${yr_total_coll:>9,.0f} ${yr_uncoll:>10,.0f} {yr_rate:>5.1f}% ${yr_loss:>10,.0f}"
        )
        report.append("")

    report.append("")

    # Section 3B: All Other Payers
    report.append("SECTION 3B: ALL OTHER PAYERS")
    report.append("-" * 145)
    report.append(
        f"{'Payer Name':<35} {'Claims':>8} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
    )
    report.append("-" * 145)

    other_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
    for payer_name, payer_data in other_payers[:20]:  # Top 20
        total_pr = payer_data.get("total_pr", 0)
        patient_pmt = payer_data["collected"]
        secondary_pmt = payer_data.get("secondary_collected", 0)
        loss = data.loss_by_payer.get(payer_name, 0)
        claims = len(payer_data.get("claims", set()))
        total_coll = patient_pmt + secondary_pmt
        uncollected = total_pr - total_coll
        rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

        other_totals["total_pr"] += total_pr
        other_totals["patient_pmt"] += patient_pmt
        other_totals["secondary_pmt"] += secondary_pmt
        other_totals["loss"] += loss
        other_totals["claims"] += claims

        report.append(
            f"{payer_name[:35]:<35} {claims:>8,} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
        )

    # Other payers subtotal
    other_total_coll = other_totals["patient_pmt"] + other_totals["secondary_pmt"]
    other_uncoll = other_totals["total_pr"] - other_total_coll
    other_rate = (other_total_coll / other_totals["total_pr"] * 100) if other_totals["total_pr"] > 0 else 0
    report.append("-" * 145)
    report.append(
        f"{'OTHER PAYERS SUBTOTAL (shown)':<35} {other_totals['claims']:>8,} ${other_totals['total_pr']:>10,.0f} ${other_totals['secondary_pmt']:>9,.0f} ${other_totals['patient_pmt']:>10,.0f} ${other_total_coll:>9,.0f} ${other_uncoll:>10,.0f} {other_rate:>5.1f}% ${other_totals['loss']:>10,.0f}"
    )
    report.append("")

    # === SECTION 3B YEAR BREAKDOWN ===
    for yr in report_years:
        dos_range = get_company_dos_range(data, yr)
        yr_metrics = data.other_by_year.get(yr, {})
        yr_total_pr = yr_metrics.get("total_pr", 0)

        if yr_total_pr == 0:
            continue

        yr_patient = yr_metrics.get("patient_pmt", 0)  # FIXED: was 'collected'
        yr_secondary = yr_metrics.get("secondary_pmt", 0)  # FIXED: was 'secondary_collected'
        yr_loss = data.loss_other_by_year.get(yr, 0)
        yr_total_coll = yr_patient + yr_secondary
        yr_uncoll = yr_total_pr - yr_total_coll
        yr_rate = (yr_total_coll / yr_total_pr * 100) if yr_total_pr > 0 else 0

        report.append(f"--- {yr} DOS ({dos_range}) ---")
        report.append(
            f"{'OTHER PAYERS':<35} ${yr_total_pr:>10,.0f} ${yr_secondary:>9,.0f} ${yr_patient:>10,.0f} ${yr_total_coll:>9,.0f} ${yr_uncoll:>10,.0f} {yr_rate:>5.1f}% ${yr_loss:>10,.0f}"
        )
        report.append("")

    report.append("")

    # Section 3C: Monthly Analysis - Year-Month breakdown with subtotals
    report.append("=" * 135)
    report.append("SECTION 3C: MONTHLY ANALYSIS (Deductible Claims)")
    report.append("-" * 135)
    report.append("")

    # Medicare Monthly Table - Full year-month breakdown
    report.append("MEDICARE PART B - BY MONTH OF SERVICE")
    report.append("-" * 135)
    report.append(
        f"{'Month':<16} {'Claims':>8} {'Total PR':>14} {'Secondary':>12} {'Patient Pmt':>14} {'Total Coll':>13} {'Uncollected':>14} {'Rate':>8} {'LOSS':>14}"
    )
    report.append("-" * 135)

    medicare_grand_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}

    for yr in report_years:
        medicare_year_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
        has_year_data = False

        for month in range(1, 13):
            year_month = (yr, month)
            if year_month in data.medicare_by_year_month:
                has_year_data = True
                mdata = data.medicare_by_year_month[year_month]
                total_pr = mdata["total_pr"]
                patient_pmt = mdata["collected"]
                secondary_pmt = mdata["secondary_collected"]
                loss = data.loss_by_year_month_medicare.get(year_month, 0)
                claims = len(mdata.get("runs", set()))
                total_coll = patient_pmt + secondary_pmt
                uncollected = total_pr - total_coll
                rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

                medicare_year_totals["total_pr"] += total_pr
                medicare_year_totals["patient_pmt"] += patient_pmt
                medicare_year_totals["secondary_pmt"] += secondary_pmt
                medicare_year_totals["loss"] += loss
                medicare_year_totals["claims"] += claims

                month_label = f"{get_month_name(month)} {yr}"
                report.append(
                    f"{month_label:<16} {claims:>8,} ${total_pr:>12,.0f} ${secondary_pmt:>10,.0f} ${patient_pmt:>12,.0f} ${total_coll:>11,.0f} ${uncollected:>12,.0f} {rate:>6.1f}% ${loss:>12,.0f}"
                )

        # Year subtotal
        if has_year_data:
            yr_total_coll = medicare_year_totals["patient_pmt"] + medicare_year_totals["secondary_pmt"]
            yr_uncoll = medicare_year_totals["total_pr"] - yr_total_coll
            yr_rate = (
                (yr_total_coll / medicare_year_totals["total_pr"] * 100) if medicare_year_totals["total_pr"] > 0 else 0
            )
            report.append("-" * 135)
            report.append(
                f"{yr} SUBTOTAL      {medicare_year_totals['claims']:>8,} ${medicare_year_totals['total_pr']:>12,.0f} ${medicare_year_totals['secondary_pmt']:>10,.0f} ${medicare_year_totals['patient_pmt']:>12,.0f} ${yr_total_coll:>11,.0f} ${yr_uncoll:>12,.0f} {yr_rate:>6.1f}% ${medicare_year_totals['loss']:>12,.0f}"
            )
            report.append("")

            # Add to grand totals
            medicare_grand_totals["total_pr"] += medicare_year_totals["total_pr"]
            medicare_grand_totals["patient_pmt"] += medicare_year_totals["patient_pmt"]
            medicare_grand_totals["secondary_pmt"] += medicare_year_totals["secondary_pmt"]
            medicare_grand_totals["loss"] += medicare_year_totals["loss"]
            medicare_grand_totals["claims"] += medicare_year_totals["claims"]

    # Grand total
    if medicare_grand_totals["total_pr"] > 0:
        grand_total_coll = medicare_grand_totals["patient_pmt"] + medicare_grand_totals["secondary_pmt"]
        grand_uncoll = medicare_grand_totals["total_pr"] - grand_total_coll
        grand_rate = (
            (grand_total_coll / medicare_grand_totals["total_pr"] * 100) if medicare_grand_totals["total_pr"] > 0 else 0
        )
        report.append("=" * 135)
        report.append(
            f"{'GRAND TOTAL':<16} {medicare_grand_totals['claims']:>8,} ${medicare_grand_totals['total_pr']:>12,.0f} ${medicare_grand_totals['secondary_pmt']:>10,.0f} ${medicare_grand_totals['patient_pmt']:>12,.0f} ${grand_total_coll:>11,.0f} ${grand_uncoll:>12,.0f} {grand_rate:>6.1f}% ${medicare_grand_totals['loss']:>12,.0f}"
        )
    report.append("")
    report.append("")

    # Medicare Members Table
    sorted_medicare_members = sorted(data.medicare_members.items(), key=lambda x: x[1]["total"], reverse=True)
    if sorted_medicare_members:
        report.append("MEDICARE PART B - TOP 15 MEMBERS BY DEDUCTIBLE")
        report.append("-" * 90)
        report.append(
            f"{'Member ID':<20} {'Claims':>8} {'Deductible':>12} {'Total PR':>12} {'Collected':>12} {'Rate':>8}"
        )
        report.append("-" * 90)

        medicare_member_totals = {"claims": 0, "ded": 0, "pr": 0, "collected": 0}
        for member_id, mdata in sorted_medicare_members[:15]:
            # Mask member ID for privacy
            if len(member_id) > 5:
                masked_id = member_id[:3] + "*" * (len(member_id) - 5) + member_id[-2:]
            else:
                masked_id = member_id

            claims = mdata["claims"]
            ded = mdata["total"]
            pr = mdata["total_pr"]
            collected = mdata["collected"]
            rate = (collected / pr * 100) if pr > 0 else 0

            medicare_member_totals["claims"] += claims
            medicare_member_totals["ded"] += ded
            medicare_member_totals["pr"] += pr
            medicare_member_totals["collected"] += collected

            report.append(
                f"{masked_id:<20} {claims:>8,} ${ded:>10,.0f} ${pr:>10,.0f} ${collected:>10,.0f} {rate:>6.1f}%"
            )

        medicare_member_rate = (
            (medicare_member_totals["collected"] / medicare_member_totals["pr"] * 100)
            if medicare_member_totals["pr"] > 0
            else 0
        )
        report.append("-" * 90)
        report.append(
            f"{'TOP 15 TOTAL':<20} {medicare_member_totals['claims']:>8,} ${medicare_member_totals['ded']:>10,.0f} ${medicare_member_totals['pr']:>10,.0f} ${medicare_member_totals['collected']:>10,.0f} {medicare_member_rate:>6.1f}%"
        )
        report.append("")
        report.append("")

    # Other Payers Monthly Table - Full year-month breakdown
    report.append("ALL OTHER PAYERS - BY MONTH OF SERVICE")
    report.append("-" * 135)
    report.append(
        f"{'Month':<16} {'Claims':>8} {'Total PR':>14} {'Secondary':>12} {'Patient Pmt':>14} {'Total Coll':>13} {'Uncollected':>14} {'Rate':>8} {'LOSS':>14}"
    )
    report.append("-" * 135)

    other_grand_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}

    for yr in report_years:
        other_year_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
        has_year_data = False

        for month in range(1, 13):
            year_month = (yr, month)
            if year_month in data.other_by_year_month:
                has_year_data = True
                mdata = data.other_by_year_month[year_month]
                total_pr = mdata["total_pr"]
                patient_pmt = mdata["collected"]
                secondary_pmt = mdata["secondary_collected"]
                loss = data.loss_by_year_month_other.get(year_month, 0)
                claims = len(mdata.get("runs", set()))
                total_coll = patient_pmt + secondary_pmt
                uncollected = total_pr - total_coll
                rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

                other_year_totals["total_pr"] += total_pr
                other_year_totals["patient_pmt"] += patient_pmt
                other_year_totals["secondary_pmt"] += secondary_pmt
                other_year_totals["loss"] += loss
                other_year_totals["claims"] += claims

                month_label = f"{get_month_name(month)} {yr}"
                report.append(
                    f"{month_label:<16} {claims:>8,} ${total_pr:>12,.0f} ${secondary_pmt:>10,.0f} ${patient_pmt:>12,.0f} ${total_coll:>11,.0f} ${uncollected:>12,.0f} {rate:>6.1f}% ${loss:>12,.0f}"
                )

        # Year subtotal
        if has_year_data:
            yr_total_coll = other_year_totals["patient_pmt"] + other_year_totals["secondary_pmt"]
            yr_uncoll = other_year_totals["total_pr"] - yr_total_coll
            yr_rate = (yr_total_coll / other_year_totals["total_pr"] * 100) if other_year_totals["total_pr"] > 0 else 0
            report.append("-" * 135)
            report.append(
                f"{yr} SUBTOTAL      {other_year_totals['claims']:>8,} ${other_year_totals['total_pr']:>12,.0f} ${other_year_totals['secondary_pmt']:>10,.0f} ${other_year_totals['patient_pmt']:>12,.0f} ${yr_total_coll:>11,.0f} ${yr_uncoll:>12,.0f} {yr_rate:>6.1f}% ${other_year_totals['loss']:>12,.0f}"
            )
            report.append("")

            # Add to grand totals
            other_grand_totals["total_pr"] += other_year_totals["total_pr"]
            other_grand_totals["patient_pmt"] += other_year_totals["patient_pmt"]
            other_grand_totals["secondary_pmt"] += other_year_totals["secondary_pmt"]
            other_grand_totals["loss"] += other_year_totals["loss"]
            other_grand_totals["claims"] += other_year_totals["claims"]

    # Grand total
    if other_grand_totals["total_pr"] > 0:
        grand_total_coll = other_grand_totals["patient_pmt"] + other_grand_totals["secondary_pmt"]
        grand_uncoll = other_grand_totals["total_pr"] - grand_total_coll
        grand_rate = (
            (grand_total_coll / other_grand_totals["total_pr"] * 100) if other_grand_totals["total_pr"] > 0 else 0
        )
        report.append("=" * 135)
        report.append(
            f"{'GRAND TOTAL':<16} {other_grand_totals['claims']:>8,} ${other_grand_totals['total_pr']:>12,.0f} ${other_grand_totals['secondary_pmt']:>10,.0f} ${other_grand_totals['patient_pmt']:>12,.0f} ${grand_total_coll:>11,.0f} ${grand_uncoll:>12,.0f} {grand_rate:>6.1f}% ${other_grand_totals['loss']:>12,.0f}"
        )
    report.append("")
    report.append("")

    # Section 3D: Top Commercial Payer Deep Dive
    if data.top_payer_name:
        report.append("=" * 135)
        report.append(f"SECTION 3D: TOP COMMERCIAL PAYER DEEP DIVE - {data.top_payer_name}")
        report.append("-" * 135)
        report.append("")

        # Calculate totals for this payer
        top_payer_total_ded = sum(data.top_payer_by_month[m]["amount"] for m in data.top_payer_by_month)
        top_payer_total_pr = sum(data.top_payer_by_month[m]["total_pr"] for m in data.top_payer_by_month)
        top_payer_total_collected = sum(data.top_payer_by_month[m]["collected"] for m in data.top_payer_by_month)
        top_payer_uncollected = top_payer_total_pr - top_payer_total_collected
        top_payer_rate = (top_payer_total_collected / top_payer_total_pr * 100) if top_payer_total_pr > 0 else 0

        report.append(f"{'Total Deductible Applied':<40} ${top_payer_total_ded:>15,.2f}")
        report.append(f"{'Total Patient Responsibility':<40} ${top_payer_total_pr:>15,.2f}")
        report.append(f"{'Patient Payments Collected':<40} ${top_payer_total_collected:>15,.2f}")
        report.append(f"{'Uncollected':<40} ${top_payer_uncollected:>15,.2f}")
        report.append(f"{'Collection Rate':<40} {top_payer_rate:>15.1f}%")
        report.append("")

        # Monthly breakdown
        report.append(f"{data.top_payer_name} - BY MONTH OF SERVICE")
        report.append("-" * 90)
        report.append(
            f"{'Month':<12} {'Deductible':>12} {'Total PR':>12} {'Collected':>12} {'Uncollected':>12} {'Rate':>8}"
        )
        report.append("-" * 90)

        for month in range(1, 13):
            if month in data.top_payer_by_month:
                mdata = data.top_payer_by_month[month]
                ded = mdata["amount"]
                total_pr = mdata["total_pr"]
                collected = mdata["collected"]
                uncollected = total_pr - collected
                rate = (collected / total_pr * 100) if total_pr > 0 else 0
                report.append(
                    f"{get_month_name(month):<12} ${ded:>10,.0f} ${total_pr:>10,.0f} ${collected:>10,.0f} ${uncollected:>10,.0f} {rate:>6.1f}%"
                )

        report.append("-" * 90)
        report.append(
            f"{'TOTAL':<12} ${top_payer_total_ded:>10,.0f} ${top_payer_total_pr:>10,.0f} ${top_payer_total_collected:>10,.0f} ${top_payer_uncollected:>10,.0f} {top_payer_rate:>6.1f}%"
        )
        report.append("")
        report.append("")

        # Top members for this payer
        sorted_top_payer_members = sorted(data.top_payer_members.items(), key=lambda x: x[1]["total"], reverse=True)
        if sorted_top_payer_members:
            report.append(f"{data.top_payer_name} - TOP 15 MEMBERS BY DEDUCTIBLE")
            report.append("-" * 90)
            report.append(
                f"{'Member ID':<20} {'Claims':>8} {'Deductible':>12} {'Total PR':>12} {'Collected':>12} {'Rate':>8}"
            )
            report.append("-" * 90)

            member_totals = {"claims": 0, "ded": 0, "pr": 0, "collected": 0}
            for member_id, mdata in sorted_top_payer_members[:15]:
                # Mask member ID for privacy
                if len(member_id) > 5:
                    masked_id = member_id[:3] + "*" * (len(member_id) - 5) + member_id[-2:]
                else:
                    masked_id = member_id

                claims = mdata["claims"]
                ded = mdata["total"]
                pr = mdata["total_pr"]
                collected = mdata["collected"]
                rate = (collected / pr * 100) if pr > 0 else 0

                member_totals["claims"] += claims
                member_totals["ded"] += ded
                member_totals["pr"] += pr
                member_totals["collected"] += collected

                report.append(
                    f"{masked_id:<20} {claims:>8,} ${ded:>10,.0f} ${pr:>10,.0f} ${collected:>10,.0f} {rate:>6.1f}%"
                )

            member_rate = (member_totals["collected"] / member_totals["pr"] * 100) if member_totals["pr"] > 0 else 0
            report.append("-" * 90)
            report.append(
                f"{'TOP 15 TOTAL':<20} {member_totals['claims']:>8,} ${member_totals['ded']:>10,.0f} ${member_totals['pr']:>10,.0f} ${member_totals['collected']:>10,.0f} {member_rate:>6.1f}%"
            )
            report.append("")

        report.append("")

    # Section 4: Patients Most Affected by Deductible
    report.append("=" * 135)
    report.append("SECTION 4: PATIENTS MOST AFFECTED BY DEDUCTIBLE (Top 25)")
    report.append("-" * 135)
    report.append("")

    # Sort patients by deductible (highest first)
    sorted_patients = sorted(data.patient_deductibles.items(), key=lambda x: x[1]["deductible"], reverse=True)

    report.append(
        f"{'Patient Name':<30} {'Member ID':<15} {'Claims':>7} {'Deductible':>12} {'Collected':>11} {'Secondary':>11} {'LOSS':>12}"
    )
    report.append("-" * 135)

    patient_totals = {"claims": 0, "deductible": 0, "collected": 0, "secondary": 0, "loss": 0}
    for member_id, pdata in sorted_patients[:25]:
        name = pdata["name"][:30] if pdata["name"] else "UNKNOWN"
        claims = len(pdata["runs"])
        ded = pdata["deductible"]
        coll = pdata["collected"]
        sec = pdata["secondary"]
        loss = pdata["loss"]

        patient_totals["claims"] += claims
        patient_totals["deductible"] += ded
        patient_totals["collected"] += coll
        patient_totals["secondary"] += sec
        patient_totals["loss"] += loss

        # Mask member ID for privacy (show first 3 and last 2 chars)
        if len(member_id) > 5:
            masked_id = member_id[:3] + "*" * (len(member_id) - 5) + member_id[-2:]
        else:
            masked_id = member_id

        report.append(
            f"{name:<30} {masked_id:<15} {claims:>7,} ${ded:>10,.0f} ${coll:>9,.0f} ${sec:>9,.0f} ${loss:>10,.0f}"
        )

    report.append("-" * 135)
    report.append(
        f"{'TOP 25 TOTAL':<30} {'':<15} {patient_totals['claims']:>7,} ${patient_totals['deductible']:>10,.0f} ${patient_totals['collected']:>9,.0f} ${patient_totals['secondary']:>9,.0f} ${patient_totals['loss']:>10,.0f}"
    )
    report.append("")

    # Summary stats
    total_patients = len(data.patient_deductibles)
    patients_with_loss = sum(1 for m, p in data.patient_deductibles.items() if p["loss"] > 0)
    total_patient_ded = sum(p["deductible"] for p in data.patient_deductibles.values())
    total_patient_loss = sum(p["loss"] for p in data.patient_deductibles.values())

    report.append(f"Total Unique Patients with Deductible: {total_patients:,}")
    if total_patients > 0:
        report.append(
            f"Patients with LOSS (zero collection): {patients_with_loss:,} ({patients_with_loss/total_patients*100:.1f}%)"
        )
    report.append(f"Total Deductible Across All Patients: ${total_patient_ded:,.2f}")
    report.append(f"Total LOSS Across All Patients: ${total_patient_loss:,.2f}")
    report.append("")
    report.append("")
    report.append("=" * 100)
    report.append("END OF REPORT")
    report.append("=" * 100)

    # Write report
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))

    return data.calculated_deductible_total, data.patient_collected_total


def generate_aggregate_report(
    all_company_data, run_payments, output_path, source_description="835_consolidated_output.csv + Trip_Credits.csv"
):
    """Generate an aggregate report across all companies with collection data."""

    # Dynamically determine years with data (fixes math error when data spans multiple years)
    all_years = set()
    for data in all_company_data.values():
        all_years.update(data.claims_with_ded_by_year.keys())
    # FIXED: Only include 2024 and 2025 in report years
    report_years = [yr for yr in sorted(all_years) if yr in [2024, 2025]]
    if not report_years:
        report_years = [2024, 2025]  # Fallback to defaults

    # FIXED: Calculate year-filtered totals in a single pass (performance optimization)
    # Initialize all accumulators
    filt_deductible = 0
    filt_coinsurance = 0
    filt_copay = 0
    filt_noncovered = 0
    filt_other_pr = 0
    filt_collected_for_pr = 0
    filt_all_secondary = 0
    filt_total_claims = 0
    filt_claims_with_pr = 0
    filt_claims_with_ded = 0
    filt_ded_deductible = 0
    filt_ded_coinsurance = 0
    filt_ded_copay = 0
    filt_ded_noncovered = 0
    filt_ded_other = 0
    filt_ded_total_pr = 0
    filt_patient_collected = 0
    filt_secondary_recovery = 0
    filt_loss = 0

    # Single pass over all companies and years
    for d in all_company_data.values():
        for yr in report_years:
            # Layer 1 totals (ALL claims)
            filt_deductible += d.total_pr_by_year.get(yr, 0)
            filt_coinsurance += d.coinsurance_by_year.get(yr, 0)
            filt_copay += d.copay_by_year.get(yr, 0)
            filt_noncovered += d.noncovered_by_year.get(yr, 0)
            filt_other_pr += d.other_pr_by_year.get(yr, 0)
            filt_collected_for_pr += d.patient_collected_for_pr_by_year.get(yr, 0)
            filt_all_secondary += d.all_secondary_by_year.get(yr, 0)
            filt_total_claims += len(d.all_claims_by_year.get(yr, set()))
            filt_claims_with_pr += len(d.claims_with_pr_by_year.get(yr, set()))
            filt_claims_with_ded += len(d.claims_with_ded_by_year.get(yr, set()))
            # Layer 2 totals (DEDUCTIBLE claims only)
            filt_ded_deductible += d.ded_lines_deductible_by_year.get(yr, 0)
            filt_ded_coinsurance += d.ded_lines_coinsurance_by_year.get(yr, 0)
            filt_ded_copay += d.ded_lines_copay_by_year.get(yr, 0)
            filt_ded_noncovered += d.ded_lines_noncovered_by_year.get(yr, 0)
            filt_ded_other += d.ded_lines_other_by_year.get(yr, 0)
            filt_ded_total_pr += d.ded_lines_total_pr_by_year.get(yr, 0)
            filt_patient_collected += d.patient_collected_by_year.get(yr, 0)
            filt_secondary_recovery += d.secondary_recovery_by_year.get(yr, 0)
            filt_loss += d.loss_by_year.get(yr, 0)

    filt_ded_claims = filt_claims_with_ded

    report = []
    report.append("=" * 110)
    report.append("AGGREGATE DEDUCTIBLE & COLLECTION ANALYSIS REPORT - ALL COMPANIES")
    report.append("=" * 110)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Source: {source_description}")
    report.append(f"Companies Analyzed: {len(all_company_data)}")

    total_records = sum(d.total_records for d in all_company_data.values())
    # FIXED: Use year-filtered totals
    grand_deductible = filt_deductible
    grand_collected = filt_patient_collected
    grand_rate = (grand_collected / grand_deductible * 100) if grand_deductible > 0 else 0

    report.append(f"Total Records: {total_records:,}")
    report.append("=" * 110)
    report.append("")

    # Section 1: Overall Collection Summary
    report.append("SECTION 1: PATIENT RESPONSIBILITY & COLLECTION SUMMARY")
    report.append("-" * 110)
    report.append("")

    # Calculate aggregate totals for all PR components (ALL claims) - FIXED: Use year-filtered totals
    grand_coinsurance = filt_coinsurance
    grand_copay = filt_copay
    grand_noncovered = filt_noncovered
    grand_other_pr = filt_other_pr
    grand_total_pr = grand_deductible + grand_coinsurance + grand_copay + grand_noncovered + grand_other_pr
    grand_collected_for_pr = filt_collected_for_pr
    grand_pr_rate = (grand_collected_for_pr / grand_total_pr * 100) if grand_total_pr > 0 else 0

    # Calculate aggregate claim counts - FIXED: Use year-filtered counts
    grand_total_claims = filt_total_claims
    grand_claims_with_ded = filt_claims_with_ded
    grand_claims_with_pr = filt_claims_with_pr
    grand_ded_lines = filt_ded_claims  # Approximate - service line count not tracked by year
    grand_pct_with_ded = (grand_claims_with_ded / grand_total_claims * 100) if grand_total_claims > 0 else 0
    grand_pct_with_pr = (grand_claims_with_pr / grand_total_claims * 100) if grand_total_claims > 0 else 0

    # Calculate aggregate PR from DEDUCTIBLE LINES ONLY - FIXED: Use year-filtered totals
    grand_ded_lines_total_pr = filt_ded_total_pr
    grand_ded_lines_other_pr = filt_ded_other
    grand_ded_lines_coinsurance = filt_ded_coinsurance
    grand_ded_lines_copay = filt_ded_copay
    grand_ded_lines_noncovered = filt_ded_noncovered
    grand_ded_lines_other = filt_ded_other

    # Collection rates for deductible lines - FIXED: Use year-filtered totals
    grand_ded_lines_pr_rate = (filt_patient_collected / filt_ded_total_pr * 100) if filt_ded_total_pr > 0 else 0

    # --- LAYER 1: OVERALL CONTEXT ---
    report.append("=" * 80)
    report.append("LAYER 1: OVERALL CONTEXT (All Claims)")
    report.append("=" * 80)
    report.append("")
    report.append("CLAIM VOLUME:")
    report.append(f"{'Total Unique Claims (All Companies)':<55} {grand_total_claims:>20,}")
    report.append(
        f"{'Claims with Any Patient Responsibility':<55} {grand_claims_with_pr:>20,} ({grand_pct_with_pr:.1f}%)"
    )
    report.append(
        f"{'Claims with Deductible Specifically':<55} {grand_claims_with_ded:>20,} ({grand_pct_with_ded:.1f}%)"
    )
    report.append("")

    report.append("TOTAL PATIENT RESPONSIBILITY (ALL CLAIMS):")
    report.append(f"{'Component':<55} {'Amount':>20}")
    report.append("-" * 80)
    report.append(f"{'Deductible (PR-1)':<55} ${grand_deductible:>18,.2f}")
    report.append(f"{'Coinsurance (PR-2)':<55} ${grand_coinsurance:>18,.2f}")
    report.append(f"{'Copay (PR-3)':<55} ${grand_copay:>18,.2f}")
    report.append(f"{'Non-Covered (Patient Resp)':<55} ${grand_noncovered:>18,.2f}")
    report.append(f"{'Other Patient Responsibility':<55} ${grand_other_pr:>18,.2f}")
    report.append("-" * 80)
    report.append(f"{'TOTAL PATIENT RESPONSIBILITY':<55} ${grand_total_pr:>18,.2f}")
    report.append("")

    # Calculate secondary payer totals - ALL CLAIMS (for Layer 1) - FIXED: Use year-filtered
    grand_all_forwarded_count = sum(
        d.all_claims_forwarded_count for d in all_company_data.values()
    )  # No year tracking for this
    grand_all_secondary_paid_count = sum(
        d.all_claims_secondary_paid_count for d in all_company_data.values()
    )  # No year tracking for this
    grand_all_secondary_recovery_total = filt_all_secondary

    # Calculate secondary payer totals - DEDUCTIBLE CLAIMS ONLY (for Layer 2) - FIXED: Use year-filtered
    grand_ded_forwarded_count = sum(
        d.claims_forwarded_count for d in all_company_data.values()
    )  # No year tracking for this
    grand_ded_secondary_paid_count = sum(
        d.claims_secondary_paid_count for d in all_company_data.values()
    )  # No year tracking for this
    grand_ded_secondary_recovery_total = filt_secondary_recovery
    grand_deductible_forwarded = sum(
        d.deductible_forwarded_total for d in all_company_data.values()
    )  # No year tracking for this

    # Total collected across all sources - ALL CLAIMS - FIXED: Use year-filtered
    grand_total_all_collected = grand_collected_for_pr + grand_all_secondary_recovery_total
    grand_total_all_rate = (grand_total_all_collected / grand_total_pr * 100) if grand_total_pr > 0 else 0

    report.append("OVERALL COLLECTION (ALL PR CLAIMS):")
    report.append(f"{'Metric':<55} {'Amount':>20}")
    report.append("-" * 80)
    report.append(f"{'Patient Payments':<55} ${grand_collected_for_pr:>18,.2f}")
    report.append(f"{'Secondary Payer Payments':<55} ${grand_all_secondary_recovery_total:>18,.2f}")
    report.append("-" * 80)
    report.append(f"{'TOTAL COLLECTED':<55} ${grand_total_all_collected:>18,.2f}")
    report.append(f"{'UNCOLLECTED':<55} ${grand_total_pr - grand_total_all_collected:>18,.2f}")
    report.append("-" * 80)
    report.append(f"{'COLLECTION RATE':<55} {grand_total_all_rate:>18.1f}%")
    report.append("")
    report.append("")

    # --- LAYER 2: DEDUCTIBLE CLAIMS FOCUS ---
    report.append("=" * 115)
    report.append("LAYER 2: DEDUCTIBLE CLAIMS DEEP DIVE")
    report.append("=" * 115)
    report.append("")

    # Get year data for aggregate - DYNAMIC based on actual years in data
    year_data = {}
    for yr in report_years:
        year_data[yr] = {
            "ded": sum(d.ded_lines_deductible_by_year.get(yr, 0) for d in all_company_data.values()),
            "coins": sum(d.ded_lines_coinsurance_by_year.get(yr, 0) for d in all_company_data.values()),
            "copay": sum(d.ded_lines_copay_by_year.get(yr, 0) for d in all_company_data.values()),
            "noncov": sum(d.ded_lines_noncovered_by_year.get(yr, 0) for d in all_company_data.values()),
            "other": sum(d.ded_lines_other_by_year.get(yr, 0) for d in all_company_data.values()),
            "total": sum(d.ded_lines_total_pr_by_year.get(yr, 0) for d in all_company_data.values()),
            "patient": sum(d.patient_collected_by_year.get(yr, 0) for d in all_company_data.values()),
            "secondary": sum(d.secondary_recovery_by_year.get(yr, 0) for d in all_company_data.values()),
            "claims": sum(len(d.claims_with_ded_by_year.get(yr, set())) for d in all_company_data.values()),
        }

    # Build dynamic header with all years
    year_headers = "".join(f"{yr:>18}" for yr in report_years)
    col_width = 18 * len(report_years) + 18 + 35  # years + total + component label

    report.append("PATIENT RESPONSIBILITY ON DEDUCTIBLE CLAIMS:")
    report.append(f"{'Component':<35} {year_headers} {'TOTAL':>18}")
    report.append("-" * col_width)

    # Claims with Deductible row
    claims_values = "".join(f'{year_data[yr]["claims"]:>18,}' for yr in report_years)
    report.append(f"{'Claims with Deductible':<35} {claims_values} {grand_claims_with_ded:>18,}")

    # Deductible row - FIXED: Use Layer 2 (deductible claims) total, not Layer 1
    ded_values = "".join(f'${year_data[yr]["ded"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Deductible (PR-1)':<35} {ded_values} ${filt_ded_deductible:>16,.2f}")

    # Coinsurance row
    coins_values = "".join(f'${year_data[yr]["coins"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Coinsurance (PR-2)':<35} {coins_values} ${grand_ded_lines_coinsurance:>16,.2f}")

    # Copay row
    copay_values = "".join(f'${year_data[yr]["copay"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Copay (PR-3)':<35} {copay_values} ${grand_ded_lines_copay:>16,.2f}")

    # Non-covered row
    noncov_values = "".join(f'${year_data[yr]["noncov"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Non-Covered (Patient Resp)':<35} {noncov_values} ${grand_ded_lines_noncovered:>16,.2f}")

    # Other PR row
    other_values = "".join(f'${year_data[yr]["other"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Other Patient Responsibility':<35} {other_values} ${grand_ded_lines_other:>16,.2f}")

    report.append("-" * col_width)

    # Total PR row
    total_values = "".join(f'${year_data[yr]["total"]:>16,.2f}' for yr in report_years)
    report.append(f"{'TOTAL PR ON DEDUCTIBLE LINES':<35} {total_values} ${grand_ded_lines_total_pr:>16,.2f}")
    report.append("")

    # Collection table with dynamic year columns
    for yr in report_years:
        year_data[yr]["coll"] = year_data[yr]["patient"] + year_data[yr]["secondary"]
        year_data[yr]["uncoll"] = year_data[yr]["total"] - year_data[yr]["coll"]
        year_data[yr]["rate"] = (
            (year_data[yr]["coll"] / year_data[yr]["total"] * 100) if year_data[yr]["total"] > 0 else 0
        )

    grand_total_collected_ded = grand_collected + grand_ded_secondary_recovery_total
    grand_uncollected_ded = grand_ded_lines_total_pr - grand_total_collected_ded
    grand_total_collected_ded_rate = (
        (grand_total_collected_ded / grand_ded_lines_total_pr * 100) if grand_ded_lines_total_pr > 0 else 0
    )

    report.append("COLLECTION ON DEDUCTIBLE LINES:")
    report.append(f"{'Metric':<35} {year_headers} {'TOTAL':>18}")
    report.append("-" * col_width)

    # Patient Payments row (dynamic)
    patient_values = "".join(f'${year_data[yr]["patient"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Patient Payments':<35} {patient_values} ${grand_collected:>16,.2f}")

    # Secondary Payer Payments row (dynamic)
    secondary_values = "".join(f'${year_data[yr]["secondary"]:>16,.2f}' for yr in report_years)
    report.append(f"{'Secondary Payer Payments':<35} {secondary_values} ${grand_ded_secondary_recovery_total:>16,.2f}")

    report.append("-" * col_width)

    # Total Collected row (dynamic)
    coll_values = "".join(f'${year_data[yr]["coll"]:>16,.2f}' for yr in report_years)
    report.append(f"{'TOTAL COLLECTED':<35} {coll_values} ${grand_total_collected_ded:>16,.2f}")

    # Uncollected row (dynamic)
    uncoll_values = "".join(f'${year_data[yr]["uncoll"]:>16,.2f}' for yr in report_years)
    report.append(f"{'UNCOLLECTED':<35} {uncoll_values} ${grand_uncollected_ded:>16,.2f}")

    report.append("-" * col_width)

    # Collection Rate row (dynamic)
    rate_values = "".join(f'{year_data[yr]["rate"]:>17.1f}%' for yr in report_years)
    report.append(f"{'COLLECTION RATE':<35} {rate_values} {grand_total_collected_ded_rate:>17.1f}%")
    report.append("")
    report.append("")

    # Section 1A: Patient Responsibility Adjustment Breakdown (Aggregate) - Year columns side-by-side with claim counts
    # Aggregate all PR adjustments from all companies
    aggregate_pr = defaultdict(float)
    aggregate_pr_with_ded = defaultdict(float)
    aggregate_pr_by_year = defaultdict(lambda: defaultdict(float))
    aggregate_pr_with_ded_by_year = defaultdict(lambda: defaultdict(float))
    # Aggregate claim counts
    aggregate_pr_claims = defaultdict(int)
    aggregate_pr_claims_with_ded = defaultdict(int)
    aggregate_pr_claims_by_year = defaultdict(lambda: defaultdict(int))
    for data in all_company_data.values():
        for code, amount in data.pr_adjustments.items():
            aggregate_pr[code] += amount
        for code, amount in data.pr_adjustments_with_ded.items():
            aggregate_pr_with_ded[code] += amount
        for yr, yr_data in data.pr_adjustments_by_year.items():
            for code, amount in yr_data.items():
                aggregate_pr_by_year[yr][code] += amount
        for yr, yr_data in data.pr_adjustments_with_ded_by_year.items():
            for code, amount in yr_data.items():
                aggregate_pr_with_ded_by_year[yr][code] += amount
        # Aggregate claim counts
        for code, claims_set in data.pr_adjustments_claims.items():
            aggregate_pr_claims[code] += len(claims_set)
        for code, claims_set in data.pr_adjustments_claims_with_ded.items():
            aggregate_pr_claims_with_ded[code] += len(claims_set)
        for yr, yr_data in data.pr_adjustments_claims_by_year.items():
            for code, claims_set in yr_data.items():
                aggregate_pr_claims_by_year[yr][code] += len(claims_set)

    if aggregate_pr:
        report.append("=" * 210)
        report.append("SECTION 1A: PATIENT RESPONSIBILITY (PR) ADJUSTMENT BREAKDOWN")
        report.append("-" * 210)
        report.append("")

        # Build dynamic header based on report_years
        header_parts = [f"{'Code':<8}", f"{'Description':<26}"]
        for yr in report_years:
            header_parts.append(f"{yr} Clms".rjust(9))
            header_parts.append(f"{yr} Amount".rjust(14))
        header_parts.extend(
            [f"{'Total Clms':>10}", f"{'Total Amount':>14}", f"{'w/Ded Clms':>10}", f"{'w/Ded Amount':>14}"]
        )
        report.append(" ".join(header_parts))
        report.append("-" * 210)

        # Sort by total amount descending
        sorted_pr = sorted(aggregate_pr.items(), key=lambda x: abs(x[1]), reverse=True)

        # Dynamic totals per year
        year_totals_claims = {yr: 0 for yr in report_years}
        year_totals_amount = {yr: 0.0 for yr in report_years}
        total_claims = 0
        total_all = 0.0
        total_ded_claims = 0
        total_ded = 0.0

        for code, amount in sorted_pr:
            reason_num = code.replace("PR-", "")
            desc = CARC_DESCRIPTIONS.get(reason_num, "Other Patient Responsibility")[:26]

            row_parts = [f"{code:<8}", f"{desc:<26}"]
            # FIXED: Calculate year-filtered totals for this code so TOTAL = sum(year columns)
            filt_code_amount = 0.0
            filt_code_claims = 0
            filt_code_ded_amount = 0.0
            for yr in report_years:
                yr_pr = aggregate_pr_by_year.get(yr, {})
                yr_claims = aggregate_pr_claims_by_year.get(yr, {})
                amt = yr_pr.get(code, 0.0)
                clms = yr_claims.get(code, 0)
                row_parts.append(f"{clms:>9,}")
                row_parts.append(f"${amt:>12,.2f}")
                year_totals_claims[yr] += clms
                year_totals_amount[yr] += amt
                filt_code_amount += amt
                filt_code_claims += clms
                # Also sum w/Ded amounts for this year
                yr_ded_pr = aggregate_pr_with_ded_by_year.get(yr, {})
                filt_code_ded_amount += yr_ded_pr.get(code, 0.0)

            # FIXED: Use year-filtered totals for Total columns
            claims_ded = aggregate_pr_claims_with_ded.get(code, 0)  # No year tracking for w/Ded claims per code
            row_parts.extend(
                [
                    f"{filt_code_claims:>10,}",
                    f"${filt_code_amount:>12,.2f}",
                    f"{claims_ded:>10,}",
                    f"${filt_code_ded_amount:>12,.2f}",
                ]
            )
            report.append(" ".join(row_parts))

            total_claims += filt_code_claims
            total_all += filt_code_amount
            total_ded_claims += claims_ded
            total_ded += filt_code_ded_amount

        report.append("-" * 210)
        total_parts = [f"{'TOTAL':<8}", f"{'':<26}"]
        for yr in report_years:
            total_parts.append(f"{year_totals_claims[yr]:>9,}")
            total_parts.append(f"${year_totals_amount[yr]:>12,.2f}")
        total_parts.extend(
            [f"{total_claims:>10,}", f"${total_all:>12,.2f}", f"{total_ded_claims:>10,}", f"${total_ded:>12,.2f}"]
        )
        report.append(" ".join(total_parts))
        report.append("")
        report.append("")

    # Section 2: Company Comparison (Deductible Lines Only)
    report.append("=" * 120)
    report.append("SECTION 2: COMPANY COMPARISON (Deductible Claims Analysis)")
    report.append("-" * 120)
    report.append("")
    # FIXED: Calculate Medicare vs Other totals per company from year-filtered data
    company_medicare_data = {}
    company_other_data = {}
    company_summaries = []

    for company_id, data in all_company_data.items():
        # FIXED: Sum from medicare_by_year/other_by_year for report_years only
        medicare_totals = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0}
        other_totals = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0}

        for yr in report_years:
            m_yr = data.medicare_by_year.get(yr, {})
            medicare_totals["total_pr"] += m_yr.get("total_pr", 0)
            medicare_totals["patient_pmt"] += m_yr.get("patient_pmt", 0)
            medicare_totals["secondary_pmt"] += m_yr.get("secondary_pmt", 0)

            o_yr = data.other_by_year.get(yr, {})
            other_totals["total_pr"] += o_yr.get("total_pr", 0)
            other_totals["patient_pmt"] += o_yr.get("patient_pmt", 0)
            other_totals["secondary_pmt"] += o_yr.get("secondary_pmt", 0)

        company_medicare_data[company_id] = medicare_totals
        company_other_data[company_id] = other_totals

        # FIXED: Calculate year-filtered totals for company summary
        total_claims = sum(len(data.all_claims_by_year.get(yr, set())) for yr in report_years)
        claims_with_ded = sum(len(data.claims_with_ded_by_year.get(yr, set())) for yr in report_years)
        total_pr_ded = sum(data.ded_lines_total_pr_by_year.get(yr, 0) for yr in report_years)
        patient_pmt = sum(data.patient_collected_by_year.get(yr, 0) for yr in report_years)
        secondary_pmt = sum(data.secondary_recovery_by_year.get(yr, 0) for yr in report_years)
        total_coll = patient_pmt + secondary_pmt
        rate = (total_coll / total_pr_ded * 100) if total_pr_ded > 0 else 0
        filt_ded_total = sum(data.ded_lines_deductible_by_year.get(yr, 0) for yr in report_years)
        filt_other_pr = sum(data.ded_lines_other_by_year.get(yr, 0) for yr in report_years)
        company_summaries.append(
            (
                company_id,
                data.company_name,
                data.total_records,
                filt_ded_total,
                total_coll,
                rate,
                total_claims,
                claims_with_ded,
                filt_other_pr,
                total_pr_ded,
                secondary_pmt,
            )
        )

    # Section 2A: Medicare by Company
    report.append("SECTION 2A: TRADITIONAL MEDICARE BY COMPANY")
    report.append("-" * 145)
    report.append(
        f"{'Company':<35} {'Claims':>8} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
    )
    report.append("-" * 145)

    medicare_grand = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0, "claims": 0}
    for company_id, data in sorted(
        all_company_data.items(), key=lambda x: company_medicare_data[x[0]]["total_pr"], reverse=True
    ):
        m = company_medicare_data[company_id]
        if m["total_pr"] > 0:
            # Count claims from Medicare payers
            medicare_claims = sum(
                len(payer_data["claims"])
                for payer_name, payer_data in data.payer_deductibles.items()
                if is_medicare_part_b(payer_name)
            )
            total_coll = m["patient_pmt"] + m["secondary_pmt"]
            uncollected = m["total_pr"] - total_coll
            rate = (total_coll / m["total_pr"] * 100) if m["total_pr"] > 0 else 0
            # FIXED: Use year-filtered loss
            loss = sum(data.loss_medicare_by_year.get(yr, 0) for yr in report_years)
            report.append(
                f"{data.company_name[:35]:<35} {medicare_claims:>8,} ${m['total_pr']:>10,.0f} ${m['secondary_pmt']:>9,.0f} ${m['patient_pmt']:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
            )
            medicare_grand["total_pr"] += m["total_pr"]
            medicare_grand["patient_pmt"] += m["patient_pmt"]
            medicare_grand["secondary_pmt"] += m["secondary_pmt"]
            medicare_grand["loss"] += loss
            medicare_grand["claims"] += medicare_claims

    report.append("-" * 145)
    m_total_coll = medicare_grand["patient_pmt"] + medicare_grand["secondary_pmt"]
    m_uncoll = medicare_grand["total_pr"] - m_total_coll
    m_rate = (m_total_coll / medicare_grand["total_pr"] * 100) if medicare_grand["total_pr"] > 0 else 0
    report.append(
        f"{'MEDICARE SUBTOTAL':<35} {medicare_grand['claims']:>8,} ${medicare_grand['total_pr']:>10,.0f} ${medicare_grand['secondary_pmt']:>9,.0f} ${medicare_grand['patient_pmt']:>10,.0f} ${m_total_coll:>9,.0f} ${m_uncoll:>10,.0f} {m_rate:>5.1f}% ${medicare_grand['loss']:>10,.0f}"
    )
    report.append("")

    # === YEAR BREAKDOWN FOR SECTION 2A ===
    for yr in report_years:
        # Check if any company has data for this year
        has_year_data = any(
            data.medicare_by_year.get(yr, {}).get("total_pr", 0) > 0 for data in all_company_data.values()
        )
        if not has_year_data:
            continue

        report.append(f"--- {yr} DOS ---")
        report.append("-" * 160)
        report.append(
            f"{'Company':<35} {'DOS Range':<25} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
        )
        report.append("-" * 160)

        yr_medicare_grand = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0}
        for company_id, data in sorted(
            all_company_data.items(), key=lambda x: x[1].medicare_by_year.get(yr, {}).get("total_pr", 0), reverse=True
        ):
            m_yr = data.medicare_by_year.get(yr, {})
            total_pr = m_yr.get("total_pr", 0)
            if total_pr > 0:
                company_dos_range = get_company_dos_range(data, yr)
                patient_pmt = m_yr.get("patient_pmt", 0)
                secondary_pmt = m_yr.get("secondary_pmt", 0)
                loss = data.loss_medicare_by_year.get(yr, 0)
                total_coll = patient_pmt + secondary_pmt
                uncollected = total_pr - total_coll
                rate = (total_coll / total_pr * 100) if total_pr > 0 else 0
                report.append(
                    f"{data.company_name[:35]:<35} {company_dos_range:<25} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
                )
                yr_medicare_grand["total_pr"] += total_pr
                yr_medicare_grand["patient_pmt"] += patient_pmt
                yr_medicare_grand["secondary_pmt"] += secondary_pmt
                yr_medicare_grand["loss"] += loss

        report.append("-" * 160)
        yr_m_total_coll = yr_medicare_grand["patient_pmt"] + yr_medicare_grand["secondary_pmt"]
        yr_m_uncoll = yr_medicare_grand["total_pr"] - yr_m_total_coll
        yr_m_rate = (yr_m_total_coll / yr_medicare_grand["total_pr"] * 100) if yr_medicare_grand["total_pr"] > 0 else 0
        report.append(
            f"{'MEDICARE SUBTOTAL':<35} {'':<25} ${yr_medicare_grand['total_pr']:>10,.0f} ${yr_medicare_grand['secondary_pmt']:>9,.0f} ${yr_medicare_grand['patient_pmt']:>10,.0f} ${yr_m_total_coll:>9,.0f} ${yr_m_uncoll:>10,.0f} {yr_m_rate:>5.1f}% ${yr_medicare_grand['loss']:>10,.0f}"
        )
        report.append("")

    report.append("")

    # Section 2B: Other Payers by Company
    report.append("SECTION 2B: ALL OTHER PAYERS BY COMPANY")
    report.append("-" * 145)
    report.append(
        f"{'Company':<35} {'Claims':>8} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
    )
    report.append("-" * 145)

    other_grand = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0, "claims": 0}
    for company_id, data in sorted(
        all_company_data.items(), key=lambda x: company_other_data[x[0]]["total_pr"], reverse=True
    ):
        o = company_other_data[company_id]
        if o["total_pr"] > 0:
            # Count claims from non-Medicare payers
            other_claims = sum(
                len(payer_data["claims"])
                for payer_name, payer_data in data.payer_deductibles.items()
                if not is_medicare_part_b(payer_name)
            )
            total_coll = o["patient_pmt"] + o["secondary_pmt"]
            uncollected = o["total_pr"] - total_coll
            rate = (total_coll / o["total_pr"] * 100) if o["total_pr"] > 0 else 0
            # FIXED: Use year-filtered loss
            loss = sum(data.loss_other_by_year.get(yr, 0) for yr in report_years)
            report.append(
                f"{data.company_name[:35]:<35} {other_claims:>8,} ${o['total_pr']:>10,.0f} ${o['secondary_pmt']:>9,.0f} ${o['patient_pmt']:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
            )
            other_grand["total_pr"] += o["total_pr"]
            other_grand["patient_pmt"] += o["patient_pmt"]
            other_grand["secondary_pmt"] += o["secondary_pmt"]
            other_grand["loss"] += loss
            other_grand["claims"] += other_claims

    report.append("-" * 145)
    o_total_coll = other_grand["patient_pmt"] + other_grand["secondary_pmt"]
    o_uncoll = other_grand["total_pr"] - o_total_coll
    o_rate = (o_total_coll / other_grand["total_pr"] * 100) if other_grand["total_pr"] > 0 else 0
    report.append(
        f"{'OTHER PAYERS SUBTOTAL':<35} {other_grand['claims']:>8,} ${other_grand['total_pr']:>10,.0f} ${other_grand['secondary_pmt']:>9,.0f} ${other_grand['patient_pmt']:>10,.0f} ${o_total_coll:>9,.0f} ${o_uncoll:>10,.0f} {o_rate:>5.1f}% ${other_grand['loss']:>10,.0f}"
    )
    report.append("")

    # === YEAR BREAKDOWN FOR SECTION 2B ===
    for yr in report_years:
        # Check if any company has data for this year
        has_year_data = any(data.other_by_year.get(yr, {}).get("total_pr", 0) > 0 for data in all_company_data.values())
        if not has_year_data:
            continue

        report.append(f"--- {yr} DOS ---")
        report.append("-" * 160)
        report.append(
            f"{'Company':<35} {'DOS Range':<25} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
        )
        report.append("-" * 160)

        yr_other_grand = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0}
        for company_id, data in sorted(
            all_company_data.items(), key=lambda x: x[1].other_by_year.get(yr, {}).get("total_pr", 0), reverse=True
        ):
            o_yr = data.other_by_year.get(yr, {})
            total_pr = o_yr.get("total_pr", 0)
            if total_pr > 0:
                company_dos_range = get_company_dos_range(data, yr)
                patient_pmt = o_yr.get("patient_pmt", 0)
                secondary_pmt = o_yr.get("secondary_pmt", 0)
                loss = data.loss_other_by_year.get(yr, 0)
                total_coll = patient_pmt + secondary_pmt
                uncollected = total_pr - total_coll
                rate = (total_coll / total_pr * 100) if total_pr > 0 else 0
                report.append(
                    f"{data.company_name[:35]:<35} {company_dos_range:<25} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
                )
                yr_other_grand["total_pr"] += total_pr
                yr_other_grand["patient_pmt"] += patient_pmt
                yr_other_grand["secondary_pmt"] += secondary_pmt
                yr_other_grand["loss"] += loss

        report.append("-" * 160)
        yr_o_total_coll = yr_other_grand["patient_pmt"] + yr_other_grand["secondary_pmt"]
        yr_o_uncoll = yr_other_grand["total_pr"] - yr_o_total_coll
        yr_o_rate = (yr_o_total_coll / yr_other_grand["total_pr"] * 100) if yr_other_grand["total_pr"] > 0 else 0
        report.append(
            f"{'OTHER PAYERS SUBTOTAL':<35} {'':<25} ${yr_other_grand['total_pr']:>10,.0f} ${yr_other_grand['secondary_pmt']:>9,.0f} ${yr_other_grand['patient_pmt']:>10,.0f} ${yr_o_total_coll:>9,.0f} ${yr_o_uncoll:>10,.0f} {yr_o_rate:>5.1f}% ${yr_other_grand['loss']:>10,.0f}"
        )
        report.append("")

    report.append("")

    # Combined Total
    grand_loss = sum(d.loss_total for d in all_company_data.values())
    report.append("-" * 135)
    grand_total_coll = grand_collected + grand_ded_secondary_recovery_total
    grand_uncoll = grand_ded_lines_total_pr - grand_total_coll
    grand_rate = (grand_total_coll / grand_ded_lines_total_pr * 100) if grand_ded_lines_total_pr > 0 else 0
    report.append(
        f"{'GRAND TOTAL':<35} ${grand_ded_lines_total_pr:>10,.0f} ${grand_ded_secondary_recovery_total:>9,.0f} ${grand_collected:>10,.0f} ${grand_total_coll:>9,.0f} ${grand_uncoll:>10,.0f} {grand_rate:>5.1f}% ${grand_loss:>10,.0f}"
    )
    report.append("")
    report.append("")

    # Section 3: Aggregate Payer Analysis (Deductible Lines Only)
    report.append("=" * 120)
    report.append("SECTION 3: PAYER ANALYSIS (Deductible Claims)")
    report.append("-" * 120)
    report.append("")
    aggregate_payers = defaultdict(
        lambda: {
            "runs": set(),
            "total": 0.0,
            "total_pr": 0.0,
            "collected": 0.0,
            "secondary_collected": 0.0,
            "loss": 0.0,
            "claims": 0,
        }
    )
    for data in all_company_data.values():
        for payer_name, payer_data in data.payer_deductibles.items():
            aggregate_payers[payer_name]["runs"].update(payer_data["runs"])
            aggregate_payers[payer_name]["total"] += payer_data["total"]
            aggregate_payers[payer_name]["total_pr"] += payer_data.get("total_pr", 0)
            aggregate_payers[payer_name]["collected"] += payer_data["collected"]
            aggregate_payers[payer_name]["secondary_collected"] += payer_data.get("secondary_collected", 0)
            aggregate_payers[payer_name]["loss"] += data.loss_by_payer.get(payer_name, 0)
            aggregate_payers[payer_name]["claims"] += len(payer_data.get("claims", set()))

    # Split payers into Medicare and Other
    medicare_payers = []
    other_payers = []
    for payer_name, payer_data in aggregate_payers.items():
        if payer_data["total_pr"] > 0:
            if is_medicare_part_b(payer_name):
                medicare_payers.append((payer_name, payer_data))
            else:
                other_payers.append((payer_name, payer_data))

    medicare_payers = sorted(medicare_payers, key=lambda x: x[1]["total_pr"], reverse=True)
    other_payers = sorted(other_payers, key=lambda x: x[1]["total_pr"], reverse=True)

    # Section 3A: Traditional Medicare Payers
    report.append("SECTION 3A: TRADITIONAL MEDICARE PAYERS")
    report.append("-" * 145)
    report.append(
        f"{'Payer Name':<35} {'Claims':>8} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
    )
    report.append("-" * 145)

    medicare_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
    for payer_name, payer_data in medicare_payers:
        total_pr = payer_data["total_pr"]
        patient_pmt = payer_data["collected"]
        secondary_pmt = payer_data["secondary_collected"]
        loss = payer_data["loss"]
        claims = payer_data.get("claims", 0)
        total_coll = patient_pmt + secondary_pmt
        uncollected = total_pr - total_coll
        rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

        medicare_totals["total_pr"] += total_pr
        medicare_totals["patient_pmt"] += patient_pmt
        medicare_totals["secondary_pmt"] += secondary_pmt
        medicare_totals["loss"] += loss
        medicare_totals["claims"] += claims

        report.append(
            f"{payer_name[:35]:<35} {claims:>8,} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
        )

    # Medicare subtotal
    med_total_coll = medicare_totals["patient_pmt"] + medicare_totals["secondary_pmt"]
    med_uncoll = medicare_totals["total_pr"] - med_total_coll
    med_rate = (med_total_coll / medicare_totals["total_pr"] * 100) if medicare_totals["total_pr"] > 0 else 0
    report.append("-" * 145)
    report.append(
        f"{'MEDICARE SUBTOTAL':<35} {medicare_totals['claims']:>8,} ${medicare_totals['total_pr']:>10,.0f} ${medicare_totals['secondary_pmt']:>9,.0f} ${medicare_totals['patient_pmt']:>10,.0f} ${med_total_coll:>9,.0f} ${med_uncoll:>10,.0f} {med_rate:>5.1f}% ${medicare_totals['loss']:>10,.0f}"
    )
    report.append("")

    # === YEAR BREAKDOWN FOR SECTION 3A ===
    for yr in report_years:
        dos_range = get_aggregate_dos_range(all_company_data, yr)
        # Aggregate payers by year
        yr_medicare_payers = defaultdict(
            lambda: {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0}
        )
        for data in all_company_data.values():
            for payer_name, year_data in data.payer_by_year.items():
                if is_medicare_part_b(payer_name) and yr in year_data:
                    yr_medicare_payers[payer_name]["total_pr"] += year_data[yr].get("total_pr", 0)
                    yr_medicare_payers[payer_name]["patient_pmt"] += year_data[yr].get("patient_pmt", 0)
                    yr_medicare_payers[payer_name]["secondary_pmt"] += year_data[yr].get("secondary_pmt", 0)
                    yr_medicare_payers[payer_name]["loss"] += year_data[yr].get("loss", 0)

        # Filter and sort
        yr_medicare_list = [(p, d) for p, d in yr_medicare_payers.items() if d["total_pr"] > 0]
        if not yr_medicare_list:
            continue
        yr_medicare_list = sorted(yr_medicare_list, key=lambda x: x[1]["total_pr"], reverse=True)

        report.append(f"--- {yr} DOS ({dos_range}) ---")
        report.append("-" * 135)
        report.append(
            f"{'Payer Name':<35} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
        )
        report.append("-" * 135)

        yr_med_totals = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0}
        for payer_name, pdata in yr_medicare_list:
            total_pr = pdata["total_pr"]
            patient_pmt = pdata["patient_pmt"]
            secondary_pmt = pdata["secondary_pmt"]
            loss = pdata["loss"]
            total_coll = patient_pmt + secondary_pmt
            uncollected = total_pr - total_coll
            rate = (total_coll / total_pr * 100) if total_pr > 0 else 0
            report.append(
                f"{payer_name[:35]:<35} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
            )
            yr_med_totals["total_pr"] += total_pr
            yr_med_totals["patient_pmt"] += patient_pmt
            yr_med_totals["secondary_pmt"] += secondary_pmt
            yr_med_totals["loss"] += loss

        report.append("-" * 135)
        yr_med_total_coll = yr_med_totals["patient_pmt"] + yr_med_totals["secondary_pmt"]
        yr_med_uncoll = yr_med_totals["total_pr"] - yr_med_total_coll
        yr_med_rate = (yr_med_total_coll / yr_med_totals["total_pr"] * 100) if yr_med_totals["total_pr"] > 0 else 0
        report.append(
            f"{'MEDICARE SUBTOTAL':<35} ${yr_med_totals['total_pr']:>10,.0f} ${yr_med_totals['secondary_pmt']:>9,.0f} ${yr_med_totals['patient_pmt']:>10,.0f} ${yr_med_total_coll:>9,.0f} ${yr_med_uncoll:>10,.0f} {yr_med_rate:>5.1f}% ${yr_med_totals['loss']:>10,.0f}"
        )
        report.append("")

    report.append("")

    # Section 3B: All Other Payers
    report.append("SECTION 3B: ALL OTHER PAYERS (Top 20)")
    report.append("-" * 145)
    report.append(
        f"{'Payer Name':<35} {'Claims':>8} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
    )
    report.append("-" * 145)

    other_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
    for payer_name, payer_data in other_payers[:20]:
        total_pr = payer_data["total_pr"]
        patient_pmt = payer_data["collected"]
        secondary_pmt = payer_data["secondary_collected"]
        loss = payer_data["loss"]
        claims = payer_data.get("claims", 0)
        total_coll = patient_pmt + secondary_pmt
        uncollected = total_pr - total_coll
        rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

        other_totals["total_pr"] += total_pr
        other_totals["patient_pmt"] += patient_pmt
        other_totals["secondary_pmt"] += secondary_pmt
        other_totals["loss"] += loss
        other_totals["claims"] += claims

        report.append(
            f"{payer_name[:35]:<35} {claims:>8,} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
        )

    # Other payers subtotal (for displayed payers)
    other_total_coll = other_totals["patient_pmt"] + other_totals["secondary_pmt"]
    other_uncoll = other_totals["total_pr"] - other_total_coll
    other_rate = (other_total_coll / other_totals["total_pr"] * 100) if other_totals["total_pr"] > 0 else 0
    report.append("-" * 145)
    report.append(
        f"{'OTHER PAYERS SUBTOTAL (shown)':<35} {other_totals['claims']:>8,} ${other_totals['total_pr']:>10,.0f} ${other_totals['secondary_pmt']:>9,.0f} ${other_totals['patient_pmt']:>10,.0f} ${other_total_coll:>9,.0f} ${other_uncoll:>10,.0f} {other_rate:>5.1f}% ${other_totals['loss']:>10,.0f}"
    )
    report.append("")

    # === YEAR BREAKDOWN FOR SECTION 3B ===
    for yr in report_years:
        dos_range = get_aggregate_dos_range(all_company_data, yr)
        # Aggregate payers by year
        yr_other_payers = defaultdict(lambda: {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0})
        for data in all_company_data.values():
            for payer_name, year_data in data.payer_by_year.items():
                if not is_medicare_part_b(payer_name) and yr in year_data:
                    yr_other_payers[payer_name]["total_pr"] += year_data[yr].get("total_pr", 0)
                    yr_other_payers[payer_name]["patient_pmt"] += year_data[yr].get("patient_pmt", 0)
                    yr_other_payers[payer_name]["secondary_pmt"] += year_data[yr].get("secondary_pmt", 0)
                    yr_other_payers[payer_name]["loss"] += year_data[yr].get("loss", 0)

        # Filter and sort (Top 20)
        yr_other_list = [(p, d) for p, d in yr_other_payers.items() if d["total_pr"] > 0]
        if not yr_other_list:
            continue
        yr_other_list = sorted(yr_other_list, key=lambda x: x[1]["total_pr"], reverse=True)[:20]

        report.append(f"--- {yr} DOS ({dos_range}) ---")
        report.append("-" * 135)
        report.append(
            f"{'Payer Name':<35} {'Total PR':>12} {'Secondary':>11} {'Patient Pmt':>12} {'Total Coll':>11} {'Uncollected':>12} {'Rate':>7} {'LOSS':>12}"
        )
        report.append("-" * 135)

        yr_oth_totals = {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0}
        for payer_name, pdata in yr_other_list:
            total_pr = pdata["total_pr"]
            patient_pmt = pdata["patient_pmt"]
            secondary_pmt = pdata["secondary_pmt"]
            loss = pdata["loss"]
            total_coll = patient_pmt + secondary_pmt
            uncollected = total_pr - total_coll
            rate = (total_coll / total_pr * 100) if total_pr > 0 else 0
            report.append(
                f"{payer_name[:35]:<35} ${total_pr:>10,.0f} ${secondary_pmt:>9,.0f} ${patient_pmt:>10,.0f} ${total_coll:>9,.0f} ${uncollected:>10,.0f} {rate:>5.1f}% ${loss:>10,.0f}"
            )
            yr_oth_totals["total_pr"] += total_pr
            yr_oth_totals["patient_pmt"] += patient_pmt
            yr_oth_totals["secondary_pmt"] += secondary_pmt
            yr_oth_totals["loss"] += loss

        report.append("-" * 135)
        yr_oth_total_coll = yr_oth_totals["patient_pmt"] + yr_oth_totals["secondary_pmt"]
        yr_oth_uncoll = yr_oth_totals["total_pr"] - yr_oth_total_coll
        yr_oth_rate = (yr_oth_total_coll / yr_oth_totals["total_pr"] * 100) if yr_oth_totals["total_pr"] > 0 else 0
        report.append(
            f"{'OTHER PAYERS SUBTOTAL (shown)':<35} ${yr_oth_totals['total_pr']:>10,.0f} ${yr_oth_totals['secondary_pmt']:>9,.0f} ${yr_oth_totals['patient_pmt']:>10,.0f} ${yr_oth_total_coll:>9,.0f} ${yr_oth_uncoll:>10,.0f} {yr_oth_rate:>5.1f}% ${yr_oth_totals['loss']:>10,.0f}"
        )
        report.append("")

    report.append("")

    # Section 3C: Monthly Analysis - Year-Month breakdown with subtotals
    report.append("=" * 135)
    report.append("SECTION 3C: MONTHLY ANALYSIS (Deductible Claims)")
    report.append("-" * 135)
    report.append("")

    # Aggregate monthly data across all companies using year-month keys
    medicare_year_month = defaultdict(
        lambda: {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0, "claims": 0}
    )
    other_year_month = defaultdict(
        lambda: {"total_pr": 0.0, "patient_pmt": 0.0, "secondary_pmt": 0.0, "loss": 0.0, "claims": 0}
    )

    for data in all_company_data.values():
        for year_month, mdata in data.medicare_by_year_month.items():
            medicare_year_month[year_month]["total_pr"] += mdata["total_pr"]
            medicare_year_month[year_month]["patient_pmt"] += mdata["collected"]
            medicare_year_month[year_month]["secondary_pmt"] += mdata["secondary_collected"]
            medicare_year_month[year_month]["loss"] += data.loss_by_year_month_medicare.get(year_month, 0)
            medicare_year_month[year_month]["claims"] += len(mdata.get("runs", set()))
        for year_month, mdata in data.other_by_year_month.items():
            other_year_month[year_month]["total_pr"] += mdata["total_pr"]
            other_year_month[year_month]["patient_pmt"] += mdata["collected"]
            other_year_month[year_month]["secondary_pmt"] += mdata["secondary_collected"]
            other_year_month[year_month]["loss"] += data.loss_by_year_month_other.get(year_month, 0)
            other_year_month[year_month]["claims"] += len(mdata.get("runs", set()))

    # Medicare Monthly Table - Full year-month breakdown
    report.append("MEDICARE PART B - BY MONTH OF SERVICE")
    report.append("-" * 135)
    report.append(
        f"{'Month':<16} {'Claims':>8} {'Total PR':>14} {'Secondary':>12} {'Patient Pmt':>14} {'Total Coll':>13} {'Uncollected':>14} {'Rate':>8} {'LOSS':>14}"
    )
    report.append("-" * 135)

    medicare_grand_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}

    for yr in report_years:
        medicare_year_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
        has_year_data = False

        for month in range(1, 13):
            year_month = (yr, month)
            if year_month in medicare_year_month:
                has_year_data = True
                mdata = medicare_year_month[year_month]
                total_pr = mdata["total_pr"]
                patient_pmt = mdata["patient_pmt"]
                secondary_pmt = mdata["secondary_pmt"]
                loss = mdata["loss"]
                claims = mdata["claims"]
                total_coll = patient_pmt + secondary_pmt
                uncollected = total_pr - total_coll
                rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

                medicare_year_totals["total_pr"] += total_pr
                medicare_year_totals["patient_pmt"] += patient_pmt
                medicare_year_totals["secondary_pmt"] += secondary_pmt
                medicare_year_totals["loss"] += loss
                medicare_year_totals["claims"] += claims

                month_label = f"{get_month_name(month)} {yr}"
                report.append(
                    f"{month_label:<16} {claims:>8,} ${total_pr:>12,.0f} ${secondary_pmt:>10,.0f} ${patient_pmt:>12,.0f} ${total_coll:>11,.0f} ${uncollected:>12,.0f} {rate:>6.1f}% ${loss:>12,.0f}"
                )

        # Year subtotal
        if has_year_data:
            yr_total_coll = medicare_year_totals["patient_pmt"] + medicare_year_totals["secondary_pmt"]
            yr_uncoll = medicare_year_totals["total_pr"] - yr_total_coll
            yr_rate = (
                (yr_total_coll / medicare_year_totals["total_pr"] * 100) if medicare_year_totals["total_pr"] > 0 else 0
            )
            report.append("-" * 135)
            report.append(
                f"{yr} SUBTOTAL      {medicare_year_totals['claims']:>8,} ${medicare_year_totals['total_pr']:>12,.0f} ${medicare_year_totals['secondary_pmt']:>10,.0f} ${medicare_year_totals['patient_pmt']:>12,.0f} ${yr_total_coll:>11,.0f} ${yr_uncoll:>12,.0f} {yr_rate:>6.1f}% ${medicare_year_totals['loss']:>12,.0f}"
            )
            report.append("")

            # Add to grand totals
            medicare_grand_totals["total_pr"] += medicare_year_totals["total_pr"]
            medicare_grand_totals["patient_pmt"] += medicare_year_totals["patient_pmt"]
            medicare_grand_totals["secondary_pmt"] += medicare_year_totals["secondary_pmt"]
            medicare_grand_totals["loss"] += medicare_year_totals["loss"]
            medicare_grand_totals["claims"] += medicare_year_totals["claims"]

    # Grand total
    if medicare_grand_totals["total_pr"] > 0:
        grand_total_coll = medicare_grand_totals["patient_pmt"] + medicare_grand_totals["secondary_pmt"]
        grand_uncoll = medicare_grand_totals["total_pr"] - grand_total_coll
        grand_rate = (
            (grand_total_coll / medicare_grand_totals["total_pr"] * 100) if medicare_grand_totals["total_pr"] > 0 else 0
        )
        report.append("=" * 135)
        report.append(
            f"{'GRAND TOTAL':<16} {medicare_grand_totals['claims']:>8,} ${medicare_grand_totals['total_pr']:>12,.0f} ${medicare_grand_totals['secondary_pmt']:>10,.0f} ${medicare_grand_totals['patient_pmt']:>12,.0f} ${grand_total_coll:>11,.0f} ${grand_uncoll:>12,.0f} {grand_rate:>6.1f}% ${medicare_grand_totals['loss']:>12,.0f}"
        )
    report.append("")
    report.append("")

    # Other Payers Monthly Table - Full year-month breakdown
    report.append("ALL OTHER PAYERS - BY MONTH OF SERVICE")
    report.append("-" * 135)
    report.append(
        f"{'Month':<16} {'Claims':>8} {'Total PR':>14} {'Secondary':>12} {'Patient Pmt':>14} {'Total Coll':>13} {'Uncollected':>14} {'Rate':>8} {'LOSS':>14}"
    )
    report.append("-" * 135)

    other_grand_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}

    for yr in report_years:
        other_year_totals = {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
        has_year_data = False

        for month in range(1, 13):
            year_month = (yr, month)
            if year_month in other_year_month:
                has_year_data = True
                mdata = other_year_month[year_month]
                total_pr = mdata["total_pr"]
                patient_pmt = mdata["patient_pmt"]
                secondary_pmt = mdata["secondary_pmt"]
                loss = mdata["loss"]
                claims = mdata["claims"]
                total_coll = patient_pmt + secondary_pmt
                uncollected = total_pr - total_coll
                rate = (total_coll / total_pr * 100) if total_pr > 0 else 0

                other_year_totals["total_pr"] += total_pr
                other_year_totals["patient_pmt"] += patient_pmt
                other_year_totals["secondary_pmt"] += secondary_pmt
                other_year_totals["loss"] += loss
                other_year_totals["claims"] += claims

                month_label = f"{get_month_name(month)} {yr}"
                report.append(
                    f"{month_label:<16} {claims:>8,} ${total_pr:>12,.0f} ${secondary_pmt:>10,.0f} ${patient_pmt:>12,.0f} ${total_coll:>11,.0f} ${uncollected:>12,.0f} {rate:>6.1f}% ${loss:>12,.0f}"
                )

        # Year subtotal
        if has_year_data:
            yr_total_coll = other_year_totals["patient_pmt"] + other_year_totals["secondary_pmt"]
            yr_uncoll = other_year_totals["total_pr"] - yr_total_coll
            yr_rate = (yr_total_coll / other_year_totals["total_pr"] * 100) if other_year_totals["total_pr"] > 0 else 0
            report.append("-" * 135)
            report.append(
                f"{yr} SUBTOTAL      {other_year_totals['claims']:>8,} ${other_year_totals['total_pr']:>12,.0f} ${other_year_totals['secondary_pmt']:>10,.0f} ${other_year_totals['patient_pmt']:>12,.0f} ${yr_total_coll:>11,.0f} ${yr_uncoll:>12,.0f} {yr_rate:>6.1f}% ${other_year_totals['loss']:>12,.0f}"
            )
            report.append("")

            # Add to grand totals
            other_grand_totals["total_pr"] += other_year_totals["total_pr"]
            other_grand_totals["patient_pmt"] += other_year_totals["patient_pmt"]
            other_grand_totals["secondary_pmt"] += other_year_totals["secondary_pmt"]
            other_grand_totals["loss"] += other_year_totals["loss"]
            other_grand_totals["claims"] += other_year_totals["claims"]

    # Grand total
    if other_grand_totals["total_pr"] > 0:
        grand_total_coll = other_grand_totals["patient_pmt"] + other_grand_totals["secondary_pmt"]
        grand_uncoll = other_grand_totals["total_pr"] - grand_total_coll
        grand_rate = (
            (grand_total_coll / other_grand_totals["total_pr"] * 100) if other_grand_totals["total_pr"] > 0 else 0
        )
        report.append("=" * 135)
        report.append(
            f"{'GRAND TOTAL':<16} {other_grand_totals['claims']:>8,} ${other_grand_totals['total_pr']:>12,.0f} ${other_grand_totals['secondary_pmt']:>10,.0f} ${other_grand_totals['patient_pmt']:>12,.0f} ${grand_total_coll:>11,.0f} ${grand_uncoll:>12,.0f} {grand_rate:>6.1f}% ${other_grand_totals['loss']:>12,.0f}"
        )
    report.append("")
    report.append("")

    # Medicare Part B - Top 15 Members (Aggregate across all companies)
    aggregate_medicare_members = defaultdict(lambda: {"total": 0.0, "total_pr": 0.0, "collected": 0.0, "claims": 0})
    for data in all_company_data.values():
        for member_id, mdata in data.medicare_members.items():
            aggregate_medicare_members[member_id]["total"] += mdata["total"]
            aggregate_medicare_members[member_id]["total_pr"] += mdata["total_pr"]
            aggregate_medicare_members[member_id]["collected"] += mdata["collected"]
            aggregate_medicare_members[member_id]["claims"] += mdata["claims"]

    sorted_agg_medicare_members = sorted(aggregate_medicare_members.items(), key=lambda x: x[1]["total"], reverse=True)
    if sorted_agg_medicare_members:
        report.append("MEDICARE PART B - TOP 15 MEMBERS BY DEDUCTIBLE (ALL COMPANIES)")
        report.append("-" * 90)
        report.append(
            f"{'Member ID':<20} {'Claims':>8} {'Deductible':>12} {'Total PR':>12} {'Collected':>12} {'Rate':>8}"
        )
        report.append("-" * 90)

        medicare_member_totals = {"claims": 0, "ded": 0, "pr": 0, "collected": 0}
        for member_id, mdata in sorted_agg_medicare_members[:15]:
            # Mask member ID for privacy
            if len(member_id) > 5:
                masked_id = member_id[:3] + "*" * (len(member_id) - 5) + member_id[-2:]
            else:
                masked_id = member_id

            claims = mdata["claims"]
            ded = mdata["total"]
            pr = mdata["total_pr"]
            collected = mdata["collected"]
            rate = (collected / pr * 100) if pr > 0 else 0

            medicare_member_totals["claims"] += claims
            medicare_member_totals["ded"] += ded
            medicare_member_totals["pr"] += pr
            medicare_member_totals["collected"] += collected

            report.append(
                f"{masked_id:<20} {claims:>8,} ${ded:>10,.0f} ${pr:>10,.0f} ${collected:>10,.0f} {rate:>6.1f}%"
            )

        medicare_member_rate = (
            (medicare_member_totals["collected"] / medicare_member_totals["pr"] * 100)
            if medicare_member_totals["pr"] > 0
            else 0
        )
        report.append("-" * 90)
        report.append(
            f"{'TOP 15 TOTAL':<20} {medicare_member_totals['claims']:>8,} ${medicare_member_totals['ded']:>10,.0f} ${medicare_member_totals['pr']:>10,.0f} ${medicare_member_totals['collected']:>10,.0f} {medicare_member_rate:>6.1f}%"
        )
        report.append("")
        report.append("")

    # Section 3D: Top 3 Commercial Payers Deep Dive
    report.append("=" * 135)
    report.append("SECTION 3D: TOP 3 COMMERCIAL PAYERS DEEP DIVE")
    report.append("-" * 135)
    report.append("")

    # Get top 3 commercial payers (already sorted in other_payers)
    top_3_commercial = other_payers[:3]

    for rank, (payer_name, payer_data) in enumerate(top_3_commercial, 1):
        total_pr = payer_data["total_pr"]
        patient_pmt = payer_data["collected"]
        secondary_pmt = payer_data["secondary_collected"]
        loss = payer_data["loss"]
        total_coll = patient_pmt + secondary_pmt
        uncollected = total_pr - total_coll
        rate = (total_coll / total_pr * 100) if total_pr > 0 else 0
        ded_total = payer_data["total"]

        report.append(f"#{rank}: {payer_name}")
        report.append("-" * 60)
        report.append(f"{'Total Deductible Applied':<40} ${ded_total:>15,.2f}")
        report.append(f"{'Total Patient Responsibility':<40} ${total_pr:>15,.2f}")
        report.append(f"{'Patient Payments Collected':<40} ${patient_pmt:>15,.2f}")
        report.append(f"{'Secondary Payer Payments':<40} ${secondary_pmt:>15,.2f}")
        report.append(f"{'Total Collected':<40} ${total_coll:>15,.2f}")
        report.append(f"{'Uncollected':<40} ${uncollected:>15,.2f}")
        report.append(f"{'Collection Rate':<40} {rate:>15.1f}%")
        report.append(f"{'LOSS (Zero Collection Claims)':<40} ${loss:>15,.2f}")
        report.append("")
        report.append("")

    # Section 4: Patients Most Affected by Deductible
    report.append("=" * 135)
    report.append("SECTION 4: PATIENTS MOST AFFECTED BY DEDUCTIBLE (Top 50)")
    report.append("-" * 135)
    report.append("")

    # Aggregate patient data across all companies
    all_patients = defaultdict(
        lambda: {
            "name": "",
            "runs": set(),
            "deductible": 0.0,
            "total_pr": 0.0,
            "collected": 0.0,
            "secondary": 0.0,
            "loss": 0.0,
        }
    )

    for data in all_company_data.values():
        for member_id, pdata in data.patient_deductibles.items():
            all_patients[member_id]["runs"].update(pdata["runs"])
            if not all_patients[member_id]["name"]:
                all_patients[member_id]["name"] = pdata["name"]
            all_patients[member_id]["deductible"] += pdata["deductible"]
            all_patients[member_id]["total_pr"] += pdata["total_pr"]
            all_patients[member_id]["collected"] += pdata["collected"]
            all_patients[member_id]["secondary"] += pdata["secondary"]
            all_patients[member_id]["loss"] += pdata["loss"]

    # Sort by deductible (highest first)
    sorted_patients = sorted(all_patients.items(), key=lambda x: x[1]["deductible"], reverse=True)

    report.append(
        f"{'Patient Name':<30} {'Member ID':<15} {'Claims':>7} {'Deductible':>12} {'Collected':>11} {'Secondary':>11} {'LOSS':>12}"
    )
    report.append("-" * 135)

    patient_totals = {"claims": 0, "deductible": 0, "collected": 0, "secondary": 0, "loss": 0}
    for member_id, pdata in sorted_patients[:50]:
        name = pdata["name"][:30] if pdata["name"] else "UNKNOWN"
        claims = len(pdata["runs"])
        ded = pdata["deductible"]
        coll = pdata["collected"]
        sec = pdata["secondary"]
        loss = pdata["loss"]

        patient_totals["claims"] += claims
        patient_totals["deductible"] += ded
        patient_totals["collected"] += coll
        patient_totals["secondary"] += sec
        patient_totals["loss"] += loss

        # Mask member ID for privacy (show first 3 and last 2 chars)
        if len(member_id) > 5:
            masked_id = member_id[:3] + "*" * (len(member_id) - 5) + member_id[-2:]
        else:
            masked_id = member_id

        report.append(
            f"{name:<30} {masked_id:<15} {claims:>7,} ${ded:>10,.0f} ${coll:>9,.0f} ${sec:>9,.0f} ${loss:>10,.0f}"
        )

    report.append("-" * 135)
    report.append(
        f"{'TOP 50 TOTAL':<30} {'':<15} {patient_totals['claims']:>7,} ${patient_totals['deductible']:>10,.0f} ${patient_totals['collected']:>9,.0f} ${patient_totals['secondary']:>9,.0f} ${patient_totals['loss']:>10,.0f}"
    )
    report.append("")

    # Summary stats
    total_patients = len(all_patients)
    patients_with_loss = sum(1 for m, p in all_patients.items() if p["loss"] > 0)
    total_patient_ded = sum(p["deductible"] for p in all_patients.values())
    total_patient_loss = sum(p["loss"] for p in all_patients.values())

    report.append(f"Total Unique Patients with Deductible: {total_patients:,}")
    report.append(
        f"Patients with LOSS (zero collection): {patients_with_loss:,} ({patients_with_loss/total_patients*100:.1f}%)"
    )
    report.append(f"Total Deductible Across All Patients: ${total_patient_ded:,.2f}")
    report.append(f"Total LOSS Across All Patients: ${total_patient_loss:,.2f}")
    report.append("")
    report.append("")

    report.append("=" * 110)
    report.append("END OF AGGREGATE REPORT")
    report.append("=" * 110)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))

    return grand_deductible, grand_collected


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate Deductible Collection Reports")
    parser.add_argument(
        "--csv",
        "-c",
        type=str,
        default=r"C:\Users\Brendan Cameron\Desktop\EXTRACTOR\Extracted_835s\835_consolidated_output_compact.csv",
        help="Path to 835 consolidated CSV file",
    )
    parser.add_argument(
        "--trips",
        "-t",
        type=str,
        default=r"C:\Users\Brendan Cameron\Desktop\Trip_Credits (3).csv",
        help="Path to Fair Health ZIP CSV file (contains RUN and PT PAID columns)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=r"C:\Users\Brendan Cameron\Desktop\Deductible_Reports",
        help="Output directory for reports",
    )

    args = parser.parse_args()

    csv_path = Path(args.csv)
    trip_path = Path(args.trips)
    output_dir = Path(args.output)

    output_dir.mkdir(exist_ok=True)

    print("=" * 70)
    print("DEDUCTIBLE COLLECTION REPORT GENERATOR")
    print("=" * 70)
    print()

    # Load Patient Payments from Fair Health ZIP CSV
    run_payments = load_trip_credits(trip_path)
    print()

    print(f"Reading 835 CSV file: {csv_path}")
    print()

    # First pass: collect data by company
    all_company_data = {}

    print("Pass 1: Collecting data by company with collection matching...")

    with open(csv_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)

        row_count = 0
        for row in reader:
            row_count += 1
            if row_count % 50000 == 0:
                print(f"  Processed {row_count:,} rows...")

            filename = row.get("Filename_File", "")
            parts = filename.split(".")
            if len(parts) < 2:
                continue

            company_id = parts[1]
            company_name = row.get("COMPANY", "") or "UNKNOWN"

            if company_id not in all_company_data:
                all_company_data[company_id] = CompanyData(company_id, company_name)

            data = all_company_data[company_id]
            data.total_records += 1
            data.rows.append(row)

            # Track unique claims
            claim_num = row.get("CLM_PatientControlNumber_L2100_CLP", "") or row.get("CLAIM NUMBER", "")
            if claim_num:
                data.all_claims.add(claim_num)

            calc_ded = parse_currency(row.get("CALCULATED DEDUCTIBLE", 0))
            data.calculated_deductible_total += calc_ded

            # Capture all PR components for total patient responsibility
            calc_coins = parse_currency(row.get("CALCULATED COINSURANCE", 0))
            calc_copay = parse_currency(row.get("CALCULATED COPAY", 0))
            calc_noncov = parse_currency(row.get("CALCULATED PATIENT NON COVERED", 0))
            calc_other = parse_currency(row.get("CALCULATED PATIENT OTHER", 0))

            data.total_coinsurance += calc_coins
            data.total_copay += calc_copay
            data.total_noncovered += calc_noncov
            data.total_other_pr += calc_other

            # Total patient responsibility for this service line
            total_pr = calc_ded + calc_coins + calc_copay + calc_noncov + calc_other

            run_num = row.get("RUN", "")
            collected = run_payments.get(run_num, {}).get("total", 0.0)

            # Track claims with any patient responsibility
            if total_pr > 0:
                if claim_num:
                    data.claims_with_pr_set.add(claim_num)
                data.patient_collected_for_pr += collected

            run_collection_not_counted = calc_ded != 0 and run_num and run_num not in data.ded_runs_collected

            if calc_ded != 0:
                data.claims_with_deductible += 1
                if claim_num:
                    data.claims_with_ded_set.add(claim_num)

                if run_collection_not_counted:
                    data.patient_collected_total += collected
                    data.ded_runs_collected.add(run_num)
                    if collected > 0:
                        data.claims_with_collection += 1

                # Track PR specifically from deductible lines (will NET correctly with reversals)
                data.ded_lines_total_pr += total_pr
                data.ded_lines_other_pr += total_pr - calc_ded
                data.ded_lines_coinsurance += calc_coins
                data.ded_lines_copay += calc_copay
                data.ded_lines_noncovered += calc_noncov
                data.ded_lines_other += calc_other

            payer_name = row.get("PAYOR PAID", "") or row.get("Effective_PayerName", "") or "UNKNOWN"
            payer_name = payer_name.strip().upper() if payer_name else "UNKNOWN"
            is_medicare = is_medicare_part_b(payer_name)  # Determine early for PR adjustments

            service_date_str = row.get("SERVICE DATE", "") or row.get("DATE OF SERVICE", "")
            service_date = parse_date(service_date_str)

            # === YEAR TRACKING ===
            year = None
            if service_date:
                year = service_date.year
                # Track DOS range per year
                if year not in data.dos_min_by_year or service_date < data.dos_min_by_year[year]:
                    data.dos_min_by_year[year] = service_date
                if year not in data.dos_max_by_year or service_date > data.dos_max_by_year[year]:
                    data.dos_max_by_year[year] = service_date

                # Layer 1: Track ALL claims by year
                if claim_num:
                    data.all_claims_by_year[year].add(claim_num)
                data.total_pr_by_year[year] += calc_ded
                data.coinsurance_by_year[year] += calc_coins
                data.copay_by_year[year] += calc_copay
                data.noncovered_by_year[year] += calc_noncov
                data.other_pr_by_year[year] += calc_other

                # Layer 1: Track ALL claims by year AND payer type (Medicare vs Other)
                layer1_payer = data.layer1_medicare_by_year if is_medicare else data.layer1_other_by_year
                if claim_num:
                    layer1_payer[year]["claims"].add(claim_num)
                layer1_payer[year]["deductible"] += calc_ded
                layer1_payer[year]["coinsurance"] += calc_coins
                layer1_payer[year]["copay"] += calc_copay
                layer1_payer[year]["noncovered"] += calc_noncov
                layer1_payer[year]["other_pr"] += calc_other

                # Track claims with PR by year
                if total_pr > 0:
                    if claim_num:
                        data.claims_with_pr_by_year[year].add(claim_num)
                        layer1_payer[year]["pr_claims"].add(claim_num)
                    # Match text report behavior: add collected for every row with PR
                    data.patient_collected_for_pr_by_year[year] += collected
                    layer1_payer[year]["patient_pmt"] += collected

            # Year-specific deductible tracking
            run_collection_not_counted_year = (
                calc_ded != 0 and run_num and year and run_num not in data.ded_runs_collected_by_year[year]
            )

            if calc_ded != 0 and year:
                if claim_num:
                    data.claims_with_ded_by_year[year].add(claim_num)

                if run_collection_not_counted_year:
                    data.patient_collected_by_year[year] += collected
                    data.ded_runs_collected_by_year[year].add(run_num)

                # Layer 2: Track PR on deductible lines by year
                data.ded_lines_total_pr_by_year[year] += total_pr
                data.ded_lines_deductible_by_year[year] += calc_ded
                data.ded_lines_coinsurance_by_year[year] += calc_coins
                data.ded_lines_copay_by_year[year] += calc_copay
                data.ded_lines_noncovered_by_year[year] += calc_noncov
                data.ded_lines_other_by_year[year] += calc_other

            if calc_ded != 0:
                if run_num:
                    data.payer_deductibles[payer_name]["runs"].add(run_num)
                if claim_num:
                    data.payer_deductibles[payer_name]["claims"].add(claim_num)
                data.payer_deductibles[payer_name]["total"] += calc_ded
                data.payer_deductibles[payer_name]["total_pr"] += total_pr
                if run_collection_not_counted:
                    data.payer_deductibles[payer_name]["collected"] += collected

                if service_date and year:
                    month = service_date.month
                    year_month = (year, month)  # Use tuple key for year-month tracking
                    if is_medicare:
                        if run_num:
                            data.medicare_by_year_month[year_month]["runs"].add(run_num)
                        data.medicare_by_year_month[year_month]["amount"] += calc_ded
                        data.medicare_by_year_month[year_month]["total_pr"] += total_pr
                        if run_collection_not_counted:
                            data.medicare_by_year_month[year_month]["collected"] += collected
                        # === YEAR: Medicare by year ===
                        if run_num:
                            data.medicare_by_year[year]["runs"].add(run_num)
                        data.medicare_by_year[year]["deductible"] += calc_ded
                        data.medicare_by_year[year]["total_pr"] += total_pr
                        data.medicare_by_year[year]["coinsurance"] += calc_coins
                        data.medicare_by_year[year]["copay"] += calc_copay
                        data.medicare_by_year[year]["noncovered"] += calc_noncov
                        data.medicare_by_year[year]["other_pr"] += calc_other
                        if run_collection_not_counted_year:
                            data.medicare_by_year[year]["patient_pmt"] += collected
                    else:
                        if run_num:
                            data.other_by_year_month[year_month]["runs"].add(run_num)
                        data.other_by_year_month[year_month]["amount"] += calc_ded
                        data.other_by_year_month[year_month]["total_pr"] += total_pr
                        if run_collection_not_counted:
                            data.other_by_year_month[year_month]["collected"] += collected
                        # === YEAR: Other by year ===
                        if run_num:
                            data.other_by_year[year]["runs"].add(run_num)
                        data.other_by_year[year]["deductible"] += calc_ded
                        data.other_by_year[year]["total_pr"] += total_pr
                        data.other_by_year[year]["coinsurance"] += calc_coins
                        data.other_by_year[year]["copay"] += calc_copay
                        data.other_by_year[year]["noncovered"] += calc_noncov
                        data.other_by_year[year]["other_pr"] += calc_other
                        if run_collection_not_counted_year:
                            data.other_by_year[year]["patient_pmt"] += collected

                    # Store RUN -> month mapping for secondary allocation
                    if run_num and run_num not in data.run_to_month:
                        data.run_to_month[run_num] = {"month": month, "is_medicare": is_medicare, "year": year}

                    # Track RUN deductible info for LOSS calculation
                    if run_num:
                        if run_num not in data.run_ded_info:
                            data.run_ded_info[run_num] = {
                                "deductible": 0,
                                "payer": payer_name,
                                "month": month,
                                "is_medicare": is_medicare,
                                "member_id": "",
                                "year": year,
                            }
                        data.run_ded_info[run_num]["deductible"] += calc_ded
                        # Track per-payer deductible within RUN for accurate LOSS attribution
                        data.run_payer_info[run_num][payer_name]["deductible"] += calc_ded
                        data.run_payer_info[run_num][payer_name]["month"] = month
                        data.run_payer_info[run_num][payer_name]["is_medicare"] = is_medicare
                        data.run_payer_info[run_num][payer_name]["year"] = year

                # === YEAR: Payer by year ===
                if year:
                    if run_num:
                        data.payer_by_year[payer_name][year]["runs"].add(run_num)
                    data.payer_by_year[payer_name][year]["deductible"] += calc_ded
                    data.payer_by_year[payer_name][year]["total_pr"] += total_pr
                    if run_collection_not_counted_year:
                        data.payer_by_year[payer_name][year]["patient_pmt"] += collected

                # Track patient deductibles
                member_id = str(row.get("MEMBER ID", "") or "").strip()
                patient_name = str(row.get("NAME", "") or "").strip()
                if member_id:
                    if run_num:
                        data.patient_deductibles[member_id]["runs"].add(run_num)
                        # Store member_id in run_ded_info for LOSS by patient calculation
                        if run_num in data.run_ded_info:
                            data.run_ded_info[run_num]["member_id"] = member_id
                    if not data.patient_deductibles[member_id]["name"]:
                        data.patient_deductibles[member_id]["name"] = patient_name
                    data.patient_deductibles[member_id]["deductible"] += calc_ded
                    data.patient_deductibles[member_id]["total_pr"] += total_pr
                    if run_collection_not_counted:
                        data.patient_deductibles[member_id]["collected"] += collected

                    # === YEAR: Patient by year ===
                    if year:
                        if run_num:
                            data.patient_by_year[member_id][year]["runs"].add(run_num)
                        if not data.patient_by_year[member_id][year]["name"]:
                            data.patient_by_year[member_id][year]["name"] = patient_name
                        data.patient_by_year[member_id][year]["deductible"] += calc_ded
                        data.patient_by_year[member_id][year]["total_pr"] += total_pr
                        if run_collection_not_counted_year:
                            data.patient_by_year[member_id][year]["collected"] += collected

                        # === YEAR: Patient by year by payer type ===
                        if is_medicare:
                            payer_dict = data.patient_by_year_medicare
                        else:
                            payer_dict = data.patient_by_year_other
                        if run_num:
                            payer_dict[member_id][year]["runs"].add(run_num)
                        if not payer_dict[member_id][year]["name"]:
                            payer_dict[member_id][year]["name"] = patient_name
                        payer_dict[member_id][year]["deductible"] += calc_ded
                        payer_dict[member_id][year]["total_pr"] += total_pr
                        if run_collection_not_counted_year:
                            payer_dict[member_id][year]["collected"] += collected

                if is_medicare:
                    data.medicare_amounts.append(calc_ded)
                    # Track Medicare member deductibles
                    if member_id:
                        data.medicare_members[member_id]["total"] += calc_ded
                        data.medicare_members[member_id]["total_pr"] += total_pr
                        data.medicare_members[member_id]["claims"] += 1
                        medicare_run_not_counted = run_num and run_num not in data.medicare_runs_collected
                        if medicare_run_not_counted:
                            data.medicare_members[member_id]["collected"] += collected
                            data.medicare_runs_collected.add(run_num)

            # === TRACK TOTAL_PR FOR NON-DEDUCTIBLE LINES ===
            # This ensures patient total_pr includes ALL their PR (coinsurance, copay, etc.)
            # even when there's no deductible on the line (e.g., Medicare patients after meeting deductible)
            if calc_ded == 0 and total_pr > 0:
                member_id = str(row.get("MEMBER ID", "") or "").strip()
                patient_name = str(row.get("NAME", "") or "").strip()
                if member_id:
                    # Update aggregate patient data (if patient already exists from deductible lines)
                    if not data.patient_deductibles[member_id]["name"]:
                        data.patient_deductibles[member_id]["name"] = patient_name
                    data.patient_deductibles[member_id]["total_pr"] += total_pr

                    # Year-specific tracking
                    if year:
                        if not data.patient_by_year[member_id][year]["name"]:
                            data.patient_by_year[member_id][year]["name"] = patient_name
                        data.patient_by_year[member_id][year]["total_pr"] += total_pr

                        # Payer type tracking (Medicare vs Other)
                        if is_medicare:
                            payer_dict = data.patient_by_year_medicare
                        else:
                            payer_dict = data.patient_by_year_other
                        if not payer_dict[member_id][year]["name"]:
                            payer_dict[member_id][year]["name"] = patient_name
                        payer_dict[member_id][year]["total_pr"] += total_pr

                    # Medicare member tracking (for text reports)
                    if is_medicare:
                        data.medicare_members[member_id]["total_pr"] += total_pr

            for i in range(1, 6):
                group = row.get(f"SVC_CAS{i}_Group_L2110_CAS", "")
                reason = row.get(f"SVC_CAS{i}_Reason_L2110_CAS", "")
                amount = parse_currency(row.get(f"SVC_CAS{i}_Amount_L2110_CAS", 0))

                if group == "PR":
                    pr_key = f"PR-{reason}"
                    data.pr_adjustments[pr_key] += amount
                    if claim_num:
                        data.pr_adjustments_claims[pr_key].add(claim_num)
                    if year:
                        data.pr_adjustments_by_year[year][pr_key] += amount
                        if claim_num:
                            data.pr_adjustments_claims_by_year[year][pr_key].add(claim_num)
                    # Track by payer type (medicare vs other)
                    if is_medicare:
                        data.pr_adjustments_medicare[pr_key] += amount
                        if claim_num:
                            data.pr_adjustments_claims_medicare[pr_key].add(claim_num)
                        if year:
                            data.pr_adjustments_medicare_by_year[year][pr_key] += amount
                            if claim_num:
                                data.pr_adjustments_claims_medicare_by_year[year][pr_key].add(claim_num)
                    else:
                        data.pr_adjustments_other[pr_key] += amount
                        if claim_num:
                            data.pr_adjustments_claims_other[pr_key].add(claim_num)
                        if year:
                            data.pr_adjustments_other_by_year[year][pr_key] += amount
                            if claim_num:
                                data.pr_adjustments_claims_other_by_year[year][pr_key].add(claim_num)
                    if calc_ded > 0:
                        data.pr_adjustments_with_ded[pr_key] += amount
                        if claim_num:
                            data.pr_adjustments_claims_with_ded[pr_key].add(claim_num)
                        if year:
                            data.pr_adjustments_with_ded_by_year[year][pr_key] += amount
                            if claim_num:
                                data.pr_adjustments_claims_with_ded_by_year[year][pr_key].add(claim_num)
                    if reason == "1":
                        data.pr1_cas_total += amount

            # === TRACK SECONDARY PAYER RECOVERY ===
            # Match ANY primary claim to ANY secondary claim by RUN (not just FORWARDED)
            is_primary_status = row.get("IS PRIMARY", "")
            service_payment = parse_currency(row.get("SERVICE PAYMENT", 0))

            # Track ALL PRIMARY claims (regardless of deductible or FORWARDED status)
            # Exclude: SECONDARY, TERTIARY, DENIED, REVERSAL
            is_primary_record = (
                "PRIMARY" in is_primary_status
                and "SECONDARY" not in is_primary_status
                and "TERTIARY" not in is_primary_status
                and "DENIED" not in is_primary_status
                and "REVERSAL" not in is_primary_status
            )

            if is_primary_record and run_num:
                secondary_payer_name = row.get("SecondaryPayer_Name_L1000A_N1", "") or ""
                # Store/update ALL primary claim info (aggregate if multiple lines per RUN)
                if run_num not in data.all_forwarded_claims:
                    data.all_forwarded_claims[run_num] = {
                        "total_pr": 0.0,
                        "primary_payer": payer_name,
                        "secondary_payer_name": secondary_payer_name,
                        "year": year,  # Track year for secondary reconciliation
                    }
                data.all_forwarded_claims[run_num]["total_pr"] += total_pr

                if calc_ded != 0:
                    if run_num not in data.forwarded_claims:
                        data.forwarded_claims[run_num] = {
                            "deductible": 0.0,
                            "total_pr": 0.0,
                            "primary_payer": payer_name,
                            "secondary_payer_name": secondary_payer_name,
                        }
                    data.forwarded_claims[run_num]["deductible"] += calc_ded
                    data.forwarded_claims[run_num]["total_pr"] += total_pr

            # Track SECONDARY processed claims with payments
            if "SECONDARY" in is_primary_status and run_num and service_payment > 0:
                secondary_payer = payer_name  # The payer processing as secondary
                # Store/update secondary claim payment (aggregate if multiple lines)
                if run_num not in data.secondary_claims:
                    data.secondary_claims[run_num] = {"payment": 0.0, "payer_name": secondary_payer}
                data.secondary_claims[run_num]["payment"] += service_payment

    print(f"  Total rows processed: {row_count:,}")
    print(f"  Companies found: {len(all_company_data)}")
    print()

    # === RECONCILE SECONDARY PAYER RECOVERY ===
    # Match primary claims to secondary payments by RUN (regardless of FORWARDED flag)
    print("Reconciling secondary payer recovery (matching primary to secondary by RUN)...")

    # Tracking for ALL claims
    all_total_primary = 0
    all_total_secondary_paid = 0
    all_total_secondary_payments = 0.0

    # Tracking for DEDUCTIBLE claims only
    ded_total_primary = 0
    ded_total_secondary_paid = 0
    ded_total_secondary_payments = 0.0

    for company_id, data in all_company_data.items():
        # === FIRST: Reconcile ALL primary claims ===
        for run_num, fwd_info in data.all_forwarded_claims.items():
            data.all_claims_forwarded_count += 1
            data.all_pr_forwarded_total += fwd_info["total_pr"]

            # Check if secondary payer made a payment for this RUN
            if run_num in data.secondary_claims:
                sec_info = data.secondary_claims[run_num]
                sec_payment = sec_info["payment"]

                if sec_payment > 0:
                    data.all_claims_secondary_paid_count += 1
                    data.all_secondary_recovery_total += sec_payment
                    # Track by year for dashboard filtering
                    claim_year = fwd_info.get("year")
                    if claim_year:
                        data.all_secondary_by_year[claim_year] += sec_payment
                        # Track by payer type for Layer 1 filtering
                        primary_payer = fwd_info.get("primary_payer", "")
                        if is_medicare_part_b(primary_payer):
                            data.layer1_medicare_by_year[claim_year]["secondary_pmt"] += sec_payment
                        else:
                            data.layer1_other_by_year[claim_year]["secondary_pmt"] += sec_payment

        # === SECOND: Reconcile DEDUCTIBLE-specific primary claims ===
        for run_num, fwd_info in data.forwarded_claims.items():
            data.claims_forwarded_count += 1
            data.deductible_forwarded_total += fwd_info["deductible"]

            # Check if secondary payer made a payment for this RUN
            if run_num in data.secondary_claims:
                sec_info = data.secondary_claims[run_num]
                sec_payment = sec_info["payment"]
                sec_payer = sec_info["payer_name"]

                if sec_payment > 0:
                    data.claims_secondary_paid_count += 1
                    data.secondary_recovery_total += sec_payment

                    # Track by secondary payer
                    pri_ded = fwd_info["deductible"]
                    pri_total_pr = fwd_info["total_pr"]
                    data.secondary_payer_recovery[sec_payer]["claims"] += 1
                    data.secondary_payer_recovery[sec_payer]["primary_deductible"] += pri_ded
                    data.secondary_payer_recovery[sec_payer]["primary_total_pr"] += pri_total_pr
                    data.secondary_payer_recovery[sec_payer]["secondary_payment"] += sec_payment

                    # Track secondary payment by PRIMARY payer (who applied the deductible)
                    primary_payer = fwd_info["primary_payer"]
                    data.payer_deductibles[primary_payer]["secondary_collected"] += sec_payment

                    # Allocate secondary payment by year-month using run_to_month mapping
                    # Use is_medicare_part_b(primary_payer) for classification to match Section 2A Overall
                    # (primary_payer is who applied the deductible, not the secondary payer)
                    if run_num in data.run_to_month:
                        month_info = data.run_to_month[run_num]
                        month = month_info["month"]
                        yr = month_info.get("year")
                        if yr:
                            year_month = (yr, month)
                            is_medicare = is_medicare_part_b(primary_payer)
                            if is_medicare:
                                data.medicare_by_year_month[year_month]["secondary_collected"] += sec_payment
                                # === YEAR: Secondary by year for Medicare ===
                                data.medicare_by_year[yr]["secondary_pmt"] += sec_payment
                            else:
                                data.other_by_year_month[year_month]["secondary_collected"] += sec_payment
                                # === YEAR: Secondary by year for Other ===
                                data.other_by_year[yr]["secondary_pmt"] += sec_payment
                            # === YEAR: Secondary recovery by year overall ===
                            data.secondary_recovery_by_year[yr] += sec_payment
                            # Track by payer and year
                            data.payer_by_year[primary_payer][yr]["secondary_pmt"] += sec_payment

        # Aggregate totals
        all_total_primary += data.all_claims_forwarded_count
        all_total_secondary_paid += data.all_claims_secondary_paid_count
        all_total_secondary_payments += data.all_secondary_recovery_total

        ded_total_primary += data.claims_forwarded_count
        ded_total_secondary_paid += data.claims_secondary_paid_count
        ded_total_secondary_payments += data.secondary_recovery_total

    print(
        f"  ALL CLAIMS: {all_total_primary:,} primary, {all_total_secondary_paid:,} with secondary pmt, ${all_total_secondary_payments:,.2f} total"
    )
    print(
        f"  DEDUCTIBLE CLAIMS: {ded_total_primary:,} primary, {ded_total_secondary_paid:,} with secondary pmt, ${ded_total_secondary_payments:,.2f} total"
    )
    print()

    # === CALCULATE LOSS (deductible on zero-collection claims) ===
    print("Calculating LOSS (deductible on zero-collection claims)...")
    total_loss = 0.0
    for company_id, data in all_company_data.items():
        for run_num, info in data.run_ded_info.items():
            # Check if this RUN has zero collection (patient payment is per-RUN)
            patient_pmt = run_payments.get(run_num, {}).get("total", 0)
            secondary_pmt = data.secondary_claims.get(run_num, {}).get("payment", 0)

            if patient_pmt == 0 and secondary_pmt == 0:
                # Zero collection - attribute LOSS per-payer within this RUN
                data.loss_runs.add(run_num)
                member_id = info.get("member_id", "")  # Patient is per-RUN (correct)
                run_total_loss = 0.0  # Track total LOSS for this RUN (for patient tracking)

                # Iterate through each payer's contribution within this RUN
                for payer, payer_info in data.run_payer_info[run_num].items():
                    ded_amt = payer_info["deductible"]
                    if ded_amt <= 0:
                        continue

                    month = payer_info["month"]
                    is_medicare = payer_info["is_medicare"]
                    yr = payer_info["year"]

                    data.loss_total += ded_amt
                    data.loss_by_payer[payer] += ded_amt
                    run_total_loss += ded_amt

                    if is_medicare:
                        data.loss_medicare += ded_amt
                        if yr:
                            data.loss_by_year_month_medicare[(yr, month)] += ded_amt
                    else:
                        data.loss_other += ded_amt
                        if yr:
                            data.loss_by_year_month_other[(yr, month)] += ded_amt

                    # === YEAR: LOSS by year ===
                    if yr:
                        data.loss_by_year[yr] += ded_amt
                        data.loss_by_payer_year[payer][yr] += ded_amt
                        if is_medicare:
                            data.loss_medicare_by_year[yr] += ded_amt
                            data.medicare_by_year[yr]["loss"] += ded_amt
                        else:
                            data.loss_other_by_year[yr] += ded_amt
                            data.other_by_year[yr]["loss"] += ded_amt
                        # Track payer LOSS by year
                        data.payer_by_year[payer][yr]["loss"] += ded_amt

                # Track LOSS by patient (patient gets total LOSS for the RUN)
                if member_id and member_id in data.patient_deductibles and run_total_loss > 0:
                    data.patient_deductibles[member_id]["loss"] += run_total_loss
                    # For patient year/payer-type tracking, use run-level metadata
                    yr = info.get("year")
                    is_medicare = info.get("is_medicare", False)
                    # === YEAR: Patient LOSS by year ===
                    if yr and member_id in data.patient_by_year:
                        data.patient_by_year[member_id][yr]["loss"] += run_total_loss
                    # === YEAR: Patient LOSS by year by payer type ===
                    if yr:
                        if is_medicare:
                            data.patient_by_year_medicare[member_id][yr]["loss"] += run_total_loss
                        else:
                            data.patient_by_year_other[member_id][yr]["loss"] += run_total_loss

        # Also add secondary payments to patient tracking
        for run_num, sec_info in data.secondary_claims.items():
            if run_num in data.run_ded_info:
                member_id = data.run_ded_info[run_num].get("member_id", "")
                yr = data.run_ded_info[run_num].get("year")
                is_medicare = data.run_ded_info[run_num].get("is_medicare", False)
                if member_id and member_id in data.patient_deductibles:
                    sec_pmt = sec_info.get("payment", 0)
                    data.patient_deductibles[member_id]["secondary"] += sec_pmt
                    # === YEAR: Patient secondary by year ===
                    if yr and member_id in data.patient_by_year:
                        data.patient_by_year[member_id][yr]["secondary"] += sec_pmt
                    # === YEAR: Patient secondary by year by payer type ===
                    if yr:
                        if is_medicare:
                            data.patient_by_year_medicare[member_id][yr]["secondary"] += sec_pmt
                        else:
                            data.patient_by_year_other[member_id][yr]["secondary"] += sec_pmt

        total_loss += data.loss_total

    print(f"  Total LOSS across all companies: ${total_loss:,.2f}")
    print()

    # Generate individual company reports
    print("Generating individual company reports with collection data...")
    for company_id, data in sorted(all_company_data.items()):
        output_path = (
            output_dir
            / f"Deductible_Collection_{company_id}_{data.company_name.replace(' ', '_').replace(',', '')[:25]}.txt"
        )
        total_ded, total_coll = generate_company_report(data, run_payments, output_path)
        rate = (total_coll / total_ded * 100) if total_ded > 0 else 0
        print(f"  {company_id}: ${total_ded:,.2f} deductible, ${total_coll:,.2f} collected ({rate:.1f}%)")
        data.rows = []

    print()

    # Generate aggregate report
    print("Generating aggregate report...")
    aggregate_path = output_dir / "Deductible_Collection_AGGREGATE_ALL_COMPANIES.txt"
    grand_ded, grand_coll = generate_aggregate_report(all_company_data, run_payments, aggregate_path)
    grand_rate = (grand_coll / grand_ded * 100) if grand_ded > 0 else 0
    print(f"  Aggregate: ${grand_ded:,.2f} deductible, ${grand_coll:,.2f} collected ({grand_rate:.1f}%)")

    # Generate interactive HTML dashboards (aggregate + individual companies)
    print()
    print("Generating interactive HTML dashboards...")
    generate_all_dashboards(all_company_data, output_dir)

    print()
    print("=" * 70)
    print("REPORT GENERATION COMPLETE")
    print("=" * 70)
    print(f"Output directory: {output_dir}")
    print()
    print("Generated files:")
    # Only list files generated by this report (not all files in the directory)
    for f in sorted(output_dir.glob("Deductible_Collection_*.txt")):
        print(f"  {f.name}")
    for f in sorted(output_dir.glob("Deductible_Dashboard_*.html")):
        print(f"  {f.name}")


def generate_interactive_dashboard(all_company_data, output_path, single_company_id=None):
    """Generate an interactive HTML dashboard for client presentation.

    Args:
        all_company_data: Dictionary of company_id -> CompanyData
        output_path: Path to write HTML file
        single_company_id: If set, generate dashboard for this company only
    """
    import json
    from datetime import datetime

    # Filter to single company if specified
    if single_company_id:
        if single_company_id not in all_company_data:
            return None
        company_data_subset = {single_company_id: all_company_data[single_company_id]}
        company_name = all_company_data[single_company_id].company_name
    else:
        company_data_subset = all_company_data
        company_name = None

    # Dynamically determine years from data
    all_years = set()
    for data in company_data_subset.values():
        all_years.update(data.claims_with_ded_by_year.keys())

    # FIXED: Only include 2024 and 2025 in report years (the intended years)
    # This ensures TOTAL = sum of displayed year columns
    report_years = [yr for yr in sorted(all_years) if yr in [2024, 2025]]
    if not report_years:
        report_years = [2024, 2025]  # Fallback to defaults

    # Build granular data structure that supports all filter combinations
    # Structure: company_id -> payer_type (medicare/other) -> year -> metrics

    def make_metrics():
        return {
            "total_pr": 0,
            "patient_collected": 0,
            "secondary_collected": 0,
            "claims": set(),
            "loss": 0,
            "deductible": 0,
            "coinsurance": 0,
            "copay": 0,
            "noncovered": 0,
            "other_pr": 0,
        }

    def metrics_to_dict(m):
        total_coll = m["patient_collected"] + m["secondary_collected"]
        total_pr = m["total_pr"]
        return {
            "total_pr": total_pr,
            "patient_collected": m["patient_collected"],
            "secondary_collected": m["secondary_collected"],
            "total_collected": total_coll,
            "uncollected": total_pr - total_coll,
            "claims": len(m["claims"]) if isinstance(m["claims"], set) else m["claims"],
            "loss": m["loss"],
            "rate": (total_coll / total_pr * 100) if total_pr > 0 else 0,
        }

    # Granular company data by payer type and year
    company_metrics = defaultdict(
        lambda: {
            "medicare": {yr: make_metrics() for yr in report_years},
            "other": {yr: make_metrics() for yr in report_years},
        }
    )

    # Monthly data by company, payer type
    monthly_data = defaultdict(lambda: defaultdict(lambda: {"medicare": make_metrics(), "other": make_metrics()}))

    # Payer data by company
    payer_data = defaultdict(
        lambda: defaultdict(
            lambda: {
                "total_pr": 0,
                "patient_pmt": 0,
                "secondary_pmt": 0,
                "loss": 0,
                "claims": set(),
                "by_year": {yr: make_metrics() for yr in report_years},
            }
        )
    )

    # PR adjustments by company and payer type
    pr_data = defaultdict(
        lambda: defaultdict(
            lambda: {
                "medicare": {yr: {"amount": 0, "claims": set()} for yr in report_years},
                "other": {yr: {"amount": 0, "claims": set()} for yr in report_years},
            }
        )
    )

    # Collect granular data from each company
    for company_id, data in company_data_subset.items():
        # Process payer-level data for this company
        for payer_name, payer_info in data.payer_deductibles.items():
            is_medicare = is_medicare_part_b(payer_name)
            payer_type = "medicare" if is_medicare else "other"

            # Store payer data
            payer_data[company_id][payer_name]["total_pr"] += payer_info.get("total_pr", 0)
            payer_data[company_id][payer_name]["patient_pmt"] += payer_info.get("collected", 0)
            payer_data[company_id][payer_name]["secondary_pmt"] += payer_info.get("secondary_collected", 0)
            payer_data[company_id][payer_name]["loss"] += data.loss_by_payer.get(payer_name, 0)
            payer_data[company_id][payer_name]["claims"].update(payer_info.get("claims", set()))
            payer_data[company_id][payer_name]["is_medicare"] = is_medicare

            # Add to company metrics by payer type
            # Note: This is approximate - we'd need more granular tracking in CompanyData
            # For now, use the existing year-based Medicare/Other tracking

        # Use existing Medicare/Other year breakdowns
        for yr in report_years:
            # Medicare metrics by year - use correct field names: patient_pmt, secondary_pmt
            med_pr = data.medicare_by_year.get(yr, {}).get("total_pr", 0)
            med_patient = data.medicare_by_year.get(yr, {}).get("patient_pmt", 0)
            med_secondary = data.medicare_by_year.get(yr, {}).get("secondary_pmt", 0)
            med_loss = data.loss_medicare_by_year.get(yr, 0)

            company_metrics[company_id]["medicare"][yr]["total_pr"] += med_pr
            company_metrics[company_id]["medicare"][yr]["patient_collected"] += med_patient
            company_metrics[company_id]["medicare"][yr]["secondary_collected"] += med_secondary
            company_metrics[company_id]["medicare"][yr]["loss"] += med_loss
            # PR breakdown for Medicare
            company_metrics[company_id]["medicare"][yr]["deductible"] += data.medicare_by_year.get(yr, {}).get(
                "deductible", 0
            )
            company_metrics[company_id]["medicare"][yr]["coinsurance"] += data.medicare_by_year.get(yr, {}).get(
                "coinsurance", 0
            )
            company_metrics[company_id]["medicare"][yr]["copay"] += data.medicare_by_year.get(yr, {}).get("copay", 0)
            company_metrics[company_id]["medicare"][yr]["noncovered"] += data.medicare_by_year.get(yr, {}).get(
                "noncovered", 0
            )
            company_metrics[company_id]["medicare"][yr]["other_pr"] += data.medicare_by_year.get(yr, {}).get(
                "other_pr", 0
            )

            # Other metrics by year - use correct field names: patient_pmt, secondary_pmt
            other_pr = data.other_by_year.get(yr, {}).get("total_pr", 0)
            other_patient = data.other_by_year.get(yr, {}).get("patient_pmt", 0)
            other_secondary = data.other_by_year.get(yr, {}).get("secondary_pmt", 0)
            other_loss = data.loss_other_by_year.get(yr, 0)

            company_metrics[company_id]["other"][yr]["total_pr"] += other_pr
            company_metrics[company_id]["other"][yr]["patient_collected"] += other_patient
            company_metrics[company_id]["other"][yr]["secondary_collected"] += other_secondary
            company_metrics[company_id]["other"][yr]["loss"] += other_loss
            # PR breakdown for Other
            company_metrics[company_id]["other"][yr]["deductible"] += data.other_by_year.get(yr, {}).get(
                "deductible", 0
            )
            company_metrics[company_id]["other"][yr]["coinsurance"] += data.other_by_year.get(yr, {}).get(
                "coinsurance", 0
            )
            company_metrics[company_id]["other"][yr]["copay"] += data.other_by_year.get(yr, {}).get("copay", 0)
            company_metrics[company_id]["other"][yr]["noncovered"] += data.other_by_year.get(yr, {}).get(
                "noncovered", 0
            )
            company_metrics[company_id]["other"][yr]["other_pr"] += data.other_by_year.get(yr, {}).get("other_pr", 0)

        # Claims by year and payer type (approximate split based on PR ratio)
        for yr in report_years:
            yr_claims = data.claims_with_ded_by_year.get(yr, set())
            med_pr = data.medicare_by_year.get(yr, {}).get("total_pr", 0)
            other_pr = data.other_by_year.get(yr, {}).get("total_pr", 0)
            total_pr = med_pr + other_pr
            if total_pr > 0:
                med_ratio = med_pr / total_pr
                med_claims_count = int(len(yr_claims) * med_ratio)
                company_metrics[company_id]["medicare"][yr]["claims"] = med_claims_count
                company_metrics[company_id]["other"][yr]["claims"] = len(yr_claims) - med_claims_count
            else:
                company_metrics[company_id]["medicare"][yr]["claims"] = 0
                company_metrics[company_id]["other"][yr]["claims"] = 0

        # Monthly data by payer type
        for year_month, mdata in data.medicare_by_year_month.items():
            key = f"{year_month[0]}-{year_month[1]:02d}"
            monthly_data[company_id][key]["medicare"]["total_pr"] += mdata.get("total_pr", 0)
            monthly_data[company_id][key]["medicare"]["patient_collected"] += mdata.get("collected", 0)
            monthly_data[company_id][key]["medicare"]["secondary_collected"] += mdata.get("secondary_collected", 0)
            monthly_data[company_id][key]["medicare"]["loss"] += data.loss_by_year_month_medicare.get(year_month, 0)
            monthly_data[company_id][key]["medicare"]["claims"].update(mdata.get("runs", set()))

        for year_month, mdata in data.other_by_year_month.items():
            key = f"{year_month[0]}-{year_month[1]:02d}"
            monthly_data[company_id][key]["other"]["total_pr"] += mdata.get("total_pr", 0)
            monthly_data[company_id][key]["other"]["patient_collected"] += mdata.get("collected", 0)
            monthly_data[company_id][key]["other"]["secondary_collected"] += mdata.get("secondary_collected", 0)
            monthly_data[company_id][key]["other"]["loss"] += data.loss_by_year_month_other.get(year_month, 0)
            monthly_data[company_id][key]["other"]["claims"].update(mdata.get("runs", set()))

    # Detect available years from the data dynamically
    all_years_in_data = set()
    for cid, cdata in company_data_subset.items():
        all_years_in_data.update(cdata.ded_lines_deductible_by_year.keys())
        all_years_in_data.update(cdata.patient_collected_by_year.keys())
        all_years_in_data.update(cdata.claims_with_ded_by_year.keys())
        for year_month in cdata.medicare_by_year_month.keys():
            all_years_in_data.add(year_month[0])
        for year_month in cdata.other_by_year_month.keys():
            all_years_in_data.add(year_month[0])

    # FIXED: Only show 2024 and 2025 in dashboard (the intended report years)
    # This keeps the clean 2-column layout instead of showing all historical years
    available_years = [yr for yr in sorted(all_years_in_data) if yr in [2024, 2025]]
    if not available_years:
        available_years = [2024, 2025]  # Fallback to defaults

    # Build dashboard data structure for JavaScript
    dashboard_data = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "is_single_company": single_company_id is not None,
        "company_name": company_name,
        "available_years": available_years,
        "companies": [],
        "payers": [],
        "monthly": [],
        "pr_adjustments": [],
        "overall_context": {},  # Layer 1: All claims context
        "deductible_deep_dive": {},  # Layer 2: Deductible claims breakdown
        "pr_components": [],  # PR component breakdown
        "top_patients": [],  # Top 50 patients by deductible
        "top_payers_deep_dive": [],  # Top 3 commercial payers deep dive
    }

    # === LAYER 1: OVERALL CONTEXT (All Claims) ===
    total_unique_claims = set()
    claims_with_any_pr = set()
    claims_with_ded = set()

    # PR totals across all claims
    total_deductible = 0
    total_coinsurance = 0
    total_copay = 0
    total_noncovered = 0
    total_other_pr = 0
    total_patient_pmt_all = 0
    total_secondary_all = 0

    # By year
    layer1_by_year = {
        yr: {
            "claims": set(),
            "pr_claims": set(),
            "ded_claims": set(),
            "deductible": 0,
            "coinsurance": 0,
            "copay": 0,
            "noncovered": 0,
            "other_pr": 0,
            "patient_pmt": 0,
            "secondary_pmt": 0,
            "loss": 0,
        }
        for yr in report_years
    }

    for company_id, data in company_data_subset.items():
        total_unique_claims.update(data.all_claims)
        claims_with_any_pr.update(data.claims_with_pr_set)
        claims_with_ded.update(data.claims_with_ded_set)

        # Per-year breakdown (for year columns in dashboard)
        # FIXED: Sum TOTALS from year-specific data so TOTAL = sum(year columns)
        # Previously used unconditional totals which included data from years outside report_years
        for yr in report_years:
            # Sum to totals from year-specific data (ensures TOTAL = sum of year columns)
            total_deductible += data.total_pr_by_year.get(yr, 0)
            total_coinsurance += data.coinsurance_by_year.get(yr, 0)
            total_copay += data.copay_by_year.get(yr, 0)
            total_noncovered += data.noncovered_by_year.get(yr, 0)
            total_other_pr += data.other_pr_by_year.get(yr, 0)
            total_patient_pmt_all += data.patient_collected_for_pr_by_year.get(yr, 0)
            total_secondary_all += data.all_secondary_by_year.get(yr, 0)

            layer1_by_year[yr]["claims"].update(data.all_claims_by_year.get(yr, set()))
            layer1_by_year[yr]["pr_claims"].update(data.claims_with_pr_by_year.get(yr, set()))
            layer1_by_year[yr]["ded_claims"].update(data.claims_with_ded_by_year.get(yr, set()))
            layer1_by_year[yr]["deductible"] += data.total_pr_by_year.get(yr, 0)
            layer1_by_year[yr]["coinsurance"] += data.coinsurance_by_year.get(yr, 0)
            layer1_by_year[yr]["copay"] += data.copay_by_year.get(yr, 0)
            layer1_by_year[yr]["noncovered"] += data.noncovered_by_year.get(yr, 0)
            layer1_by_year[yr]["other_pr"] += data.other_pr_by_year.get(yr, 0)
            layer1_by_year[yr]["patient_pmt"] += data.patient_collected_for_pr_by_year.get(yr, 0)
            layer1_by_year[yr]["secondary_pmt"] += data.all_secondary_by_year.get(yr, 0)
            layer1_by_year[yr]["loss"] += data.loss_by_year.get(yr, 0)

    total_pr_all = total_deductible + total_coinsurance + total_copay + total_noncovered + total_other_pr
    total_collected_all = total_patient_pmt_all + total_secondary_all

    # Calculate total loss for overall context (sum from year-specific data)
    # FIXED: Sum from year-specific data so TOTAL = sum(year columns)
    total_loss_all = sum(layer1_by_year[yr]["loss"] for yr in report_years)

    # Build Layer 1 by_company breakdown
    layer1_by_company = {}
    layer1_medicare_agg = {
        "claims": 0,
        "deductible": 0,
        "coinsurance": 0,
        "copay": 0,
        "noncovered": 0,
        "other_pr": 0,
        "patient_pmt": 0,
        "secondary_pmt": 0,
        "by_year": {str(yr): {} for yr in report_years},
    }
    layer1_other_agg = {
        "claims": 0,
        "deductible": 0,
        "coinsurance": 0,
        "copay": 0,
        "noncovered": 0,
        "other_pr": 0,
        "patient_pmt": 0,
        "secondary_pmt": 0,
        "by_year": {str(yr): {} for yr in report_years},
    }

    for company_id, data in company_data_subset.items():
        # FIXED: Initialize with zeros, then sum from year-specific data
        # Previously used unconditional totals which included data from years outside report_years
        company_layer1 = {
            "claims": 0,  # Will be set from year-specific claims
            "deductible": 0,
            "coinsurance": 0,
            "copay": 0,
            "noncovered": 0,
            "other_pr": 0,
            "patient_pmt": 0,
            "secondary_pmt": 0,
            "by_year": {},
            "medicare": {"by_year": {}},
            "other": {"by_year": {}},
        }

        # Sum totals from year-specific data (ensures TOTAL = sum of year columns)
        company_claims_set = set()
        for yr in report_years:
            company_claims_set.update(data.all_claims_by_year.get(yr, set()))
            company_layer1["deductible"] += data.total_pr_by_year.get(yr, 0)
            company_layer1["coinsurance"] += data.coinsurance_by_year.get(yr, 0)
            company_layer1["copay"] += data.copay_by_year.get(yr, 0)
            company_layer1["noncovered"] += data.noncovered_by_year.get(yr, 0)
            company_layer1["other_pr"] += data.other_pr_by_year.get(yr, 0)
            company_layer1["patient_pmt"] += data.patient_collected_for_pr_by_year.get(yr, 0)
            company_layer1["secondary_pmt"] += data.all_secondary_by_year.get(yr, 0)
        company_layer1["claims"] = len(company_claims_set)

        company_layer1["total_pr"] = (
            company_layer1["deductible"]
            + company_layer1["coinsurance"]
            + company_layer1["copay"]
            + company_layer1["noncovered"]
            + company_layer1["other_pr"]
        )
        company_layer1["total_collected"] = company_layer1["patient_pmt"] + company_layer1["secondary_pmt"]
        company_layer1["rate"] = (
            (company_layer1["total_collected"] / company_layer1["total_pr"] * 100)
            if company_layer1["total_pr"] > 0
            else 0
        )

        for yr in report_years:
            yr_str = str(yr)
            # Company's year breakdown
            yr_ded = data.total_pr_by_year.get(yr, 0)
            yr_coins = data.coinsurance_by_year.get(yr, 0)
            yr_copay = data.copay_by_year.get(yr, 0)
            yr_noncov = data.noncovered_by_year.get(yr, 0)
            yr_other = data.other_pr_by_year.get(yr, 0)
            yr_pat = data.patient_collected_for_pr_by_year.get(yr, 0)
            yr_sec = data.all_secondary_by_year.get(yr, 0)
            yr_pr = yr_ded + yr_coins + yr_copay + yr_noncov + yr_other
            yr_coll = yr_pat + yr_sec
            company_layer1["by_year"][yr_str] = {
                "claims": len(data.all_claims_by_year.get(yr, set())),
                "deductible": yr_ded,
                "coinsurance": yr_coins,
                "copay": yr_copay,
                "noncovered": yr_noncov,
                "other_pr": yr_other,
                "total_pr": yr_pr,
                "patient_pmt": yr_pat,
                "secondary_pmt": yr_sec,
                "total_collected": yr_coll,
                "rate": (yr_coll / yr_pr * 100) if yr_pr > 0 else 0,
            }

            # Company's Medicare year breakdown
            med_yr = data.layer1_medicare_by_year.get(yr, {})
            med_ded = med_yr.get("deductible", 0)
            med_coins = med_yr.get("coinsurance", 0)
            med_copay = med_yr.get("copay", 0)
            med_noncov = med_yr.get("noncovered", 0)
            med_other = med_yr.get("other_pr", 0)
            med_pat = med_yr.get("patient_pmt", 0)
            med_sec = med_yr.get("secondary_pmt", 0)
            med_pr = med_ded + med_coins + med_copay + med_noncov + med_other
            med_coll = med_pat + med_sec
            company_layer1["medicare"]["by_year"][yr_str] = {
                "claims": len(med_yr.get("claims", set())),
                "deductible": med_ded,
                "coinsurance": med_coins,
                "copay": med_copay,
                "noncovered": med_noncov,
                "other_pr": med_other,
                "total_pr": med_pr,
                "patient_pmt": med_pat,
                "secondary_pmt": med_sec,
                "total_collected": med_coll,
                "rate": (med_coll / med_pr * 100) if med_pr > 0 else 0,
            }
            # Aggregate Medicare
            for key in [
                "claims",
                "deductible",
                "coinsurance",
                "copay",
                "noncovered",
                "other_pr",
                "patient_pmt",
                "secondary_pmt",
            ]:
                val = company_layer1["medicare"]["by_year"][yr_str].get(key, 0)
                layer1_medicare_agg["by_year"][yr_str][key] = layer1_medicare_agg["by_year"][yr_str].get(key, 0) + val
                layer1_medicare_agg[key] = layer1_medicare_agg.get(key, 0) + val

            # Company's Other year breakdown
            oth_yr = data.layer1_other_by_year.get(yr, {})
            oth_ded = oth_yr.get("deductible", 0)
            oth_coins = oth_yr.get("coinsurance", 0)
            oth_copay = oth_yr.get("copay", 0)
            oth_noncov = oth_yr.get("noncovered", 0)
            oth_other = oth_yr.get("other_pr", 0)
            oth_pat = oth_yr.get("patient_pmt", 0)
            oth_sec = oth_yr.get("secondary_pmt", 0)
            oth_pr = oth_ded + oth_coins + oth_copay + oth_noncov + oth_other
            oth_coll = oth_pat + oth_sec
            company_layer1["other"]["by_year"][yr_str] = {
                "claims": len(oth_yr.get("claims", set())),
                "deductible": oth_ded,
                "coinsurance": oth_coins,
                "copay": oth_copay,
                "noncovered": oth_noncov,
                "other_pr": oth_other,
                "total_pr": oth_pr,
                "patient_pmt": oth_pat,
                "secondary_pmt": oth_sec,
                "total_collected": oth_coll,
                "rate": (oth_coll / oth_pr * 100) if oth_pr > 0 else 0,
            }
            # Aggregate Other
            for key in [
                "claims",
                "deductible",
                "coinsurance",
                "copay",
                "noncovered",
                "other_pr",
                "patient_pmt",
                "secondary_pmt",
            ]:
                val = company_layer1["other"]["by_year"][yr_str].get(key, 0)
                layer1_other_agg["by_year"][yr_str][key] = layer1_other_agg["by_year"][yr_str].get(key, 0) + val
                layer1_other_agg[key] = layer1_other_agg.get(key, 0) + val

        layer1_by_company[company_id] = company_layer1

    # Calculate totals for medicare/other aggregates
    for agg in [layer1_medicare_agg, layer1_other_agg]:
        agg["total_pr"] = agg["deductible"] + agg["coinsurance"] + agg["copay"] + agg["noncovered"] + agg["other_pr"]
        agg["total_collected"] = agg["patient_pmt"] + agg["secondary_pmt"]
        agg["rate"] = (agg["total_collected"] / agg["total_pr"] * 100) if agg["total_pr"] > 0 else 0
        for yr_str in [str(yr) for yr in report_years]:
            yr_data = agg["by_year"][yr_str]
            yr_data["total_pr"] = (
                yr_data.get("deductible", 0)
                + yr_data.get("coinsurance", 0)
                + yr_data.get("copay", 0)
                + yr_data.get("noncovered", 0)
                + yr_data.get("other_pr", 0)
            )
            yr_data["total_collected"] = yr_data.get("patient_pmt", 0) + yr_data.get("secondary_pmt", 0)
            yr_data["rate"] = (yr_data["total_collected"] / yr_data["total_pr"] * 100) if yr_data["total_pr"] > 0 else 0

    # FIXED: Sum claims counts from year-specific data so TOTAL = sum(year columns)
    total_claims_count = sum(len(layer1_by_year[yr]["claims"]) for yr in report_years)
    claims_with_pr_count = sum(len(layer1_by_year[yr]["pr_claims"]) for yr in report_years)
    claims_with_ded_count = sum(len(layer1_by_year[yr]["ded_claims"]) for yr in report_years)

    dashboard_data["overall_context"] = {
        "total_claims": total_claims_count,
        "claims_with_pr": claims_with_pr_count,
        "claims_with_pr_pct": (claims_with_pr_count / total_claims_count * 100) if total_claims_count else 0,
        "claims_with_ded": claims_with_ded_count,
        "claims_with_ded_pct": (claims_with_ded_count / total_claims_count * 100) if total_claims_count else 0,
        "total_pr": total_pr_all,
        "deductible": total_deductible,
        "coinsurance": total_coinsurance,
        "copay": total_copay,
        "noncovered": total_noncovered,
        "other_pr": total_other_pr,
        "patient_pmt": total_patient_pmt_all,
        "secondary_pmt": total_secondary_all,
        "total_collected": total_collected_all,
        "uncollected": total_pr_all - total_collected_all,
        "loss": total_loss_all,
        "rate": (total_collected_all / total_pr_all * 100) if total_pr_all > 0 else 0,
        "by_year": {},
        "by_company": layer1_by_company,
        "medicare": layer1_medicare_agg,
        "other": layer1_other_agg,
    }

    for yr in report_years:
        yr_data = layer1_by_year[yr]
        yr_pr = (
            yr_data["deductible"]
            + yr_data["coinsurance"]
            + yr_data["copay"]
            + yr_data["noncovered"]
            + yr_data["other_pr"]
        )
        yr_coll = yr_data["patient_pmt"] + yr_data["secondary_pmt"]
        dashboard_data["overall_context"]["by_year"][str(yr)] = {
            "claims": len(yr_data["claims"]),
            "pr_claims": len(yr_data["pr_claims"]),
            "ded_claims": len(yr_data["ded_claims"]),
            "total_pr": yr_pr,
            "deductible": yr_data["deductible"],
            "coinsurance": yr_data["coinsurance"],
            "copay": yr_data["copay"],
            "noncovered": yr_data["noncovered"],
            "other_pr": yr_data["other_pr"],
            "patient_pmt": yr_data["patient_pmt"],
            "secondary_pmt": yr_data["secondary_pmt"],
            "total_collected": yr_coll,
            "loss": yr_data["loss"],
            "rate": (yr_coll / yr_pr * 100) if yr_pr > 0 else 0,
        }

    # === LAYER 2: DEDUCTIBLE CLAIMS DEEP DIVE ===
    ded_total_pr = 0
    ded_deductible = 0
    ded_coinsurance = 0
    ded_copay = 0
    ded_noncovered = 0
    ded_other_pr = 0
    ded_patient_pmt = 0
    ded_secondary_pmt = 0
    ded_loss = 0

    layer2_by_year = {
        yr: {
            "total_pr": 0,
            "deductible": 0,
            "coinsurance": 0,
            "copay": 0,
            "noncovered": 0,
            "other_pr": 0,
            "patient_pmt": 0,
            "secondary_pmt": 0,
            "claims": 0,
            "loss": 0,
        }
        for yr in report_years
    }

    for company_id, data in company_data_subset.items():
        # FIXED: Sum ALL totals from year-specific data so TOTAL = sum(year columns)
        # Previously some totals used unconditional data while others used year-specific
        for yr in report_years:
            # Sum to grand totals from year-specific data
            ded_total_pr += data.ded_lines_total_pr_by_year.get(yr, 0)
            ded_deductible += data.ded_lines_deductible_by_year.get(yr, 0)
            ded_coinsurance += data.ded_lines_coinsurance_by_year.get(yr, 0)
            ded_copay += data.ded_lines_copay_by_year.get(yr, 0)
            ded_noncovered += data.ded_lines_noncovered_by_year.get(yr, 0)
            ded_other_pr += data.ded_lines_other_by_year.get(yr, 0)
            ded_patient_pmt += data.patient_collected_by_year.get(yr, 0)
            ded_secondary_pmt += data.secondary_recovery_by_year.get(yr, 0)
            ded_loss += data.loss_by_year.get(yr, 0)

            # Year breakdown (same values as above, just organized by year)
            layer2_by_year[yr]["total_pr"] += data.ded_lines_total_pr_by_year.get(yr, 0)
            layer2_by_year[yr]["deductible"] += data.ded_lines_deductible_by_year.get(yr, 0)
            layer2_by_year[yr]["coinsurance"] += data.ded_lines_coinsurance_by_year.get(yr, 0)
            layer2_by_year[yr]["copay"] += data.ded_lines_copay_by_year.get(yr, 0)
            layer2_by_year[yr]["noncovered"] += data.ded_lines_noncovered_by_year.get(yr, 0)
            layer2_by_year[yr]["other_pr"] += data.ded_lines_other_by_year.get(yr, 0)
            layer2_by_year[yr]["patient_pmt"] += data.patient_collected_by_year.get(yr, 0)
            layer2_by_year[yr]["secondary_pmt"] += data.secondary_recovery_by_year.get(yr, 0)
            layer2_by_year[yr]["claims"] += len(data.claims_with_ded_by_year.get(yr, set()))
            layer2_by_year[yr]["loss"] += data.loss_by_year.get(yr, 0)

    ded_total_coll = ded_patient_pmt + ded_secondary_pmt

    # FIXED: Sum claims count from year-specific data so TOTAL = sum(year columns)
    ded_claims_total = sum(layer2_by_year[yr]["claims"] for yr in report_years)

    dashboard_data["deductible_deep_dive"] = {
        "claims": ded_claims_total,
        "total_pr": ded_total_pr,
        "deductible": ded_deductible,
        "coinsurance": ded_coinsurance,
        "copay": ded_copay,
        "noncovered": ded_noncovered,
        "other_pr": ded_other_pr,
        "patient_pmt": ded_patient_pmt,
        "secondary_pmt": ded_secondary_pmt,
        "total_collected": ded_total_coll,
        "uncollected": ded_total_pr - ded_total_coll,
        "loss": ded_loss,
        "rate": (ded_total_coll / ded_total_pr * 100) if ded_total_pr > 0 else 0,
        "by_year": {},
    }

    for yr in report_years:
        yr_data = layer2_by_year[yr]
        yr_coll = yr_data["patient_pmt"] + yr_data["secondary_pmt"]
        dashboard_data["deductible_deep_dive"]["by_year"][str(yr)] = {
            "claims": yr_data["claims"],
            "total_pr": yr_data["total_pr"],
            "deductible": yr_data["deductible"],
            "coinsurance": yr_data["coinsurance"],
            "copay": yr_data["copay"],
            "noncovered": yr_data["noncovered"],
            "other_pr": yr_data["other_pr"],
            "patient_pmt": yr_data["patient_pmt"],
            "secondary_pmt": yr_data["secondary_pmt"],
            "total_collected": yr_coll,
            "uncollected": yr_data["total_pr"] - yr_coll,
            "loss": yr_data["loss"],
            "rate": (yr_coll / yr_data["total_pr"] * 100) if yr_data["total_pr"] > 0 else 0,
        }

    # === PR COMPONENTS for charts ===
    dashboard_data["pr_components"] = [
        {"name": "Deductible (PR-1)", "code": "PR-1", "amount": ded_deductible, "color": "#f59e0b"},
        {"name": "Coinsurance (PR-2)", "code": "PR-2", "amount": ded_coinsurance, "color": "#3b82f6"},
        {"name": "Copay (PR-3)", "code": "PR-3", "amount": ded_copay, "color": "#8b5cf6"},
        {"name": "Non-Covered", "code": "NC", "amount": ded_noncovered, "color": "#ef4444"},
        {"name": "Other PR", "code": "OTHER", "amount": ded_other_pr, "color": "#6b7280"},
    ]

    # === TOP 50 PATIENTS ===
    all_patients = {}
    for company_id, data in company_data_subset.items():
        for member_id, patient_info in data.patient_deductibles.items():
            if member_id not in all_patients:
                all_patients[member_id] = {
                    "name": patient_info["name"],
                    "member_id": member_id,
                    "claims": 0,
                    "deductible": 0,
                    "total_pr": 0,
                    "collected": 0,
                    "secondary": 0,
                    "loss": 0,
                    "by_year": {
                        str(yr): {
                            "claims": 0,
                            "deductible": 0,
                            "total_pr": 0,
                            "collected": 0,
                            "secondary": 0,
                            "loss": 0,
                        }
                        for yr in report_years
                    },
                    "by_year_medicare": {
                        str(yr): {
                            "claims": 0,
                            "deductible": 0,
                            "total_pr": 0,
                            "collected": 0,
                            "secondary": 0,
                            "loss": 0,
                        }
                        for yr in report_years
                    },
                    "by_year_other": {
                        str(yr): {
                            "claims": 0,
                            "deductible": 0,
                            "total_pr": 0,
                            "collected": 0,
                            "secondary": 0,
                            "loss": 0,
                        }
                        for yr in report_years
                    },
                    "by_company": {},
                }
            all_patients[member_id]["claims"] += len(patient_info["runs"])
            all_patients[member_id]["deductible"] += patient_info["deductible"]
            all_patients[member_id]["total_pr"] += patient_info["total_pr"]
            all_patients[member_id]["collected"] += patient_info["collected"]
            all_patients[member_id]["secondary"] += patient_info["secondary"]
            all_patients[member_id]["loss"] += patient_info["loss"]

            # Add by_company breakdown
            if company_id not in all_patients[member_id]["by_company"]:
                all_patients[member_id]["by_company"][company_id] = {
                    "claims": 0,
                    "deductible": 0,
                    "total_pr": 0,
                    "collected": 0,
                    "secondary": 0,
                    "loss": 0,
                }
            all_patients[member_id]["by_company"][company_id]["claims"] += len(patient_info["runs"])
            all_patients[member_id]["by_company"][company_id]["deductible"] += patient_info["deductible"]
            all_patients[member_id]["by_company"][company_id]["total_pr"] += patient_info["total_pr"]
            all_patients[member_id]["by_company"][company_id]["collected"] += patient_info["collected"]
            all_patients[member_id]["by_company"][company_id]["secondary"] += patient_info["secondary"]
            all_patients[member_id]["by_company"][company_id]["loss"] += patient_info["loss"]

        # Add by_year breakdown from patient_by_year
        for member_id, year_data in data.patient_by_year.items():
            if member_id in all_patients:
                for yr in report_years:
                    if yr in year_data:
                        yr_str = str(yr)
                        yr_info = year_data[yr]
                        all_patients[member_id]["by_year"][yr_str]["claims"] += len(yr_info.get("runs", set()))
                        all_patients[member_id]["by_year"][yr_str]["deductible"] += yr_info.get("deductible", 0)
                        all_patients[member_id]["by_year"][yr_str]["total_pr"] += yr_info.get("total_pr", 0)
                        all_patients[member_id]["by_year"][yr_str]["collected"] += yr_info.get("collected", 0)
                        all_patients[member_id]["by_year"][yr_str]["secondary"] += yr_info.get("secondary", 0)
                        all_patients[member_id]["by_year"][yr_str]["loss"] += yr_info.get("loss", 0)

        # Add by_year_medicare breakdown
        for member_id, year_data in data.patient_by_year_medicare.items():
            if member_id in all_patients:
                for yr in report_years:
                    if yr in year_data:
                        yr_str = str(yr)
                        yr_info = year_data[yr]
                        all_patients[member_id]["by_year_medicare"][yr_str]["claims"] += len(yr_info.get("runs", set()))
                        all_patients[member_id]["by_year_medicare"][yr_str]["deductible"] += yr_info.get(
                            "deductible", 0
                        )
                        all_patients[member_id]["by_year_medicare"][yr_str]["total_pr"] += yr_info.get("total_pr", 0)
                        all_patients[member_id]["by_year_medicare"][yr_str]["collected"] += yr_info.get("collected", 0)
                        all_patients[member_id]["by_year_medicare"][yr_str]["secondary"] += yr_info.get("secondary", 0)
                        all_patients[member_id]["by_year_medicare"][yr_str]["loss"] += yr_info.get("loss", 0)

        # Add by_year_other breakdown
        for member_id, year_data in data.patient_by_year_other.items():
            if member_id in all_patients:
                for yr in report_years:
                    if yr in year_data:
                        yr_str = str(yr)
                        yr_info = year_data[yr]
                        all_patients[member_id]["by_year_other"][yr_str]["claims"] += len(yr_info.get("runs", set()))
                        all_patients[member_id]["by_year_other"][yr_str]["deductible"] += yr_info.get("deductible", 0)
                        all_patients[member_id]["by_year_other"][yr_str]["total_pr"] += yr_info.get("total_pr", 0)
                        all_patients[member_id]["by_year_other"][yr_str]["collected"] += yr_info.get("collected", 0)
                        all_patients[member_id]["by_year_other"][yr_str]["secondary"] += yr_info.get("secondary", 0)
                        all_patients[member_id]["by_year_other"][yr_str]["loss"] += yr_info.get("loss", 0)

    # Calculate Medicare/Other totals for each patient
    for member_id, patient in all_patients.items():
        patient["medicare"] = {
            "claims": sum(patient["by_year_medicare"].get(str(yr), {}).get("claims", 0) for yr in report_years),
            "deductible": sum(patient["by_year_medicare"].get(str(yr), {}).get("deductible", 0) for yr in report_years),
            "total_pr": sum(patient["by_year_medicare"].get(str(yr), {}).get("total_pr", 0) for yr in report_years),
            "collected": sum(patient["by_year_medicare"].get(str(yr), {}).get("collected", 0) for yr in report_years),
            "secondary": sum(patient["by_year_medicare"].get(str(yr), {}).get("secondary", 0) for yr in report_years),
            "loss": sum(patient["by_year_medicare"].get(str(yr), {}).get("loss", 0) for yr in report_years),
        }
        patient["other"] = {
            "claims": sum(patient["by_year_other"].get(str(yr), {}).get("claims", 0) for yr in report_years),
            "deductible": sum(patient["by_year_other"].get(str(yr), {}).get("deductible", 0) for yr in report_years),
            "total_pr": sum(patient["by_year_other"].get(str(yr), {}).get("total_pr", 0) for yr in report_years),
            "collected": sum(patient["by_year_other"].get(str(yr), {}).get("collected", 0) for yr in report_years),
            "secondary": sum(patient["by_year_other"].get(str(yr), {}).get("secondary", 0) for yr in report_years),
            "loss": sum(patient["by_year_other"].get(str(yr), {}).get("loss", 0) for yr in report_years),
        }
        # FIXED: Recalculate main patient totals from year-specific data so TOTAL = sum(year columns)
        patient["claims"] = sum(patient["by_year"].get(str(yr), {}).get("claims", 0) for yr in report_years)
        patient["deductible"] = sum(patient["by_year"].get(str(yr), {}).get("deductible", 0) for yr in report_years)
        patient["total_pr"] = sum(patient["by_year"].get(str(yr), {}).get("total_pr", 0) for yr in report_years)
        patient["collected"] = sum(patient["by_year"].get(str(yr), {}).get("collected", 0) for yr in report_years)
        patient["secondary"] = sum(patient["by_year"].get(str(yr), {}).get("secondary", 0) for yr in report_years)
        patient["loss"] = sum(patient["by_year"].get(str(yr), {}).get("loss", 0) for yr in report_years)

    # Get top patients from multiple perspectives to support filtering
    # Top 50 by deductible amount
    top_by_total = sorted(all_patients.values(), key=lambda x: x["deductible"], reverse=True)[:50]
    # Top 25 by Medicare deductible
    top_by_medicare = sorted(all_patients.values(), key=lambda x: x["medicare"]["deductible"], reverse=True)[:25]
    # Top 25 by Other/Commercial deductible
    top_by_other = sorted(all_patients.values(), key=lambda x: x["other"]["deductible"], reverse=True)[:25]

    # Combine and deduplicate (use member_id as key)
    seen = set()
    top_patients = []
    for p in top_by_total + top_by_medicare + top_by_other:
        if p["member_id"] not in seen:
            seen.add(p["member_id"])
            top_patients.append(p)

    # Mask member IDs for privacy
    for p in top_patients:
        mid = p["member_id"]
        if len(mid) > 6:
            p["member_id_masked"] = mid[:3] + "*" * (len(mid) - 5) + mid[-2:]
        else:
            p["member_id_masked"] = mid[:2] + "***"

    dashboard_data["top_patients"] = top_patients

    # Patient stats with year breakdown
    total_patients = len(all_patients)
    patients_with_loss = sum(1 for p in all_patients.values() if p["loss"] > 0)
    total_ded_all_patients = sum(p["deductible"] for p in all_patients.values())
    total_loss_all_patients = sum(p["loss"] for p in all_patients.values())

    # Calculate year-specific patient stats dynamically
    patient_stats_by_year = {}
    for yr in report_years:
        yr_str = str(yr)
        patient_stats_by_year[yr_str] = {
            "patients": sum(1 for p in all_patients.values() if p["by_year"].get(yr_str, {}).get("deductible", 0) > 0),
            "deductible": sum(p["by_year"].get(yr_str, {}).get("deductible", 0) for p in all_patients.values()),
            "loss": sum(p["by_year"].get(yr_str, {}).get("loss", 0) for p in all_patients.values()),
        }

    dashboard_data["patient_stats"] = {
        "total_patients": total_patients,
        "patients_with_loss": patients_with_loss,
        "patients_with_loss_pct": (patients_with_loss / total_patients * 100) if total_patients > 0 else 0,
        "total_deductible": total_ded_all_patients,
        "total_loss": total_loss_all_patients,
        "by_year": patient_stats_by_year,
    }

    # === TOP 3 COMMERCIAL PAYERS DEEP DIVE ===
    # Aggregate payers across all companies with year breakdown
    aggregate_payers = defaultdict(
        lambda: {
            "total_pr": 0,
            "deductible": 0,
            "patient_pmt": 0,
            "secondary_pmt": 0,
            "loss": 0,
            "claims": set(),
            "is_medicare": False,
            "by_year": {
                yr: {"total_pr": 0, "deductible": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0}
                for yr in report_years
            },
            "by_company": {},
        }
    )

    for company_id, data in company_data_subset.items():
        for payer_name, payer_info in data.payer_deductibles.items():
            aggregate_payers[payer_name]["total_pr"] += payer_info.get("total_pr", 0)
            aggregate_payers[payer_name]["deductible"] += payer_info.get("deductible", 0)
            aggregate_payers[payer_name]["patient_pmt"] += payer_info.get("collected", 0)
            aggregate_payers[payer_name]["secondary_pmt"] += payer_info.get("secondary_collected", 0)
            aggregate_payers[payer_name]["loss"] += data.loss_by_payer.get(payer_name, 0)
            aggregate_payers[payer_name]["claims"].update(payer_info.get("claims", set()))
            aggregate_payers[payer_name]["is_medicare"] = is_medicare_part_b(payer_name)

            # Add company breakdown with year support
            if company_id not in aggregate_payers[payer_name]["by_company"]:
                aggregate_payers[payer_name]["by_company"][company_id] = {
                    "total_pr": 0,
                    "deductible": 0,
                    "patient_pmt": 0,
                    "secondary_pmt": 0,
                    "loss": 0,
                    "claims": 0,
                    "by_year": {
                        str(yr): {
                            "total_pr": 0,
                            "deductible": 0,
                            "patient_pmt": 0,
                            "secondary_pmt": 0,
                            "loss": 0,
                            "claims": 0,
                        }
                        for yr in report_years
                    },
                }
            aggregate_payers[payer_name]["by_company"][company_id]["total_pr"] += payer_info.get("total_pr", 0)
            aggregate_payers[payer_name]["by_company"][company_id]["deductible"] += payer_info.get("deductible", 0)
            aggregate_payers[payer_name]["by_company"][company_id]["patient_pmt"] += payer_info.get("collected", 0)
            aggregate_payers[payer_name]["by_company"][company_id]["secondary_pmt"] += payer_info.get(
                "secondary_collected", 0
            )
            aggregate_payers[payer_name]["by_company"][company_id]["loss"] += data.loss_by_payer.get(payer_name, 0)
            aggregate_payers[payer_name]["by_company"][company_id]["claims"] += len(payer_info.get("claims", set()))

        # Add year breakdown from payer_by_year (both aggregate and per-company)
        for payer_name, year_data in data.payer_by_year.items():
            for yr in report_years:
                if yr in year_data:
                    yr_info = year_data[yr]
                    yr_str = str(yr)
                    # Aggregate year breakdown
                    aggregate_payers[payer_name]["by_year"][yr]["total_pr"] += yr_info.get("total_pr", 0)
                    aggregate_payers[payer_name]["by_year"][yr]["deductible"] += yr_info.get("deductible", 0)
                    aggregate_payers[payer_name]["by_year"][yr]["patient_pmt"] += yr_info.get("patient_pmt", 0)
                    aggregate_payers[payer_name]["by_year"][yr]["secondary_pmt"] += yr_info.get("secondary_pmt", 0)
                    aggregate_payers[payer_name]["by_year"][yr]["loss"] += yr_info.get("loss", 0)
                    aggregate_payers[payer_name]["by_year"][yr]["claims"] += len(yr_info.get("runs", set()))
                    # Per-company year breakdown
                    if company_id in aggregate_payers[payer_name]["by_company"]:
                        aggregate_payers[payer_name]["by_company"][company_id]["by_year"][yr_str]["total_pr"] += (
                            yr_info.get("total_pr", 0)
                        )
                        aggregate_payers[payer_name]["by_company"][company_id]["by_year"][yr_str]["deductible"] += (
                            yr_info.get("deductible", 0)
                        )
                        aggregate_payers[payer_name]["by_company"][company_id]["by_year"][yr_str]["patient_pmt"] += (
                            yr_info.get("patient_pmt", 0)
                        )
                        aggregate_payers[payer_name]["by_company"][company_id]["by_year"][yr_str]["secondary_pmt"] += (
                            yr_info.get("secondary_pmt", 0)
                        )
                        aggregate_payers[payer_name]["by_company"][company_id]["by_year"][yr_str]["loss"] += (
                            yr_info.get("loss", 0)
                        )
                        aggregate_payers[payer_name]["by_company"][company_id]["by_year"][yr_str]["claims"] += len(
                            yr_info.get("runs", set())
                        )

    # FIXED: Recalculate aggregate_payers totals from year-specific data so TOTAL = sum(year columns)
    for payer_name in aggregate_payers:
        info = aggregate_payers[payer_name]
        info["total_pr"] = sum(info["by_year"].get(yr, {}).get("total_pr", 0) for yr in report_years)
        info["deductible"] = sum(info["by_year"].get(yr, {}).get("deductible", 0) for yr in report_years)
        info["patient_pmt"] = sum(info["by_year"].get(yr, {}).get("patient_pmt", 0) for yr in report_years)
        info["secondary_pmt"] = sum(info["by_year"].get(yr, {}).get("secondary_pmt", 0) for yr in report_years)
        info["loss"] = sum(info["by_year"].get(yr, {}).get("loss", 0) for yr in report_years)
        # Note: claims is a set, can't easily sum - use by_year claims count for dashboard
        # Recalculate by_company totals from their by_year data too
        for company_id, company_info in info["by_company"].items():
            company_info["total_pr"] = sum(
                company_info["by_year"].get(str(yr), {}).get("total_pr", 0) for yr in report_years
            )
            company_info["deductible"] = sum(
                company_info["by_year"].get(str(yr), {}).get("deductible", 0) for yr in report_years
            )
            company_info["patient_pmt"] = sum(
                company_info["by_year"].get(str(yr), {}).get("patient_pmt", 0) for yr in report_years
            )
            company_info["secondary_pmt"] = sum(
                company_info["by_year"].get(str(yr), {}).get("secondary_pmt", 0) for yr in report_years
            )
            company_info["loss"] = sum(company_info["by_year"].get(str(yr), {}).get("loss", 0) for yr in report_years)
            company_info["claims"] = sum(
                company_info["by_year"].get(str(yr), {}).get("claims", 0) for yr in report_years
            )

    # Get top 3 commercial (non-Medicare) payers by total_pr
    commercial_payers = [(name, info) for name, info in aggregate_payers.items() if not info["is_medicare"]]
    top_3_commercial = sorted(commercial_payers, key=lambda x: x[1]["total_pr"], reverse=True)[:3]

    for rank, (payer_name, info) in enumerate(top_3_commercial, 1):
        total_coll = info["patient_pmt"] + info["secondary_pmt"]
        dashboard_data["top_payers_deep_dive"].append(
            {
                "rank": rank,
                "name": payer_name,
                "deductible": info["deductible"],
                "total_pr": info["total_pr"],
                "patient_pmt": info["patient_pmt"],
                "secondary_pmt": info["secondary_pmt"],
                "total_collected": total_coll,
                "uncollected": info["total_pr"] - total_coll,
                "loss": info["loss"],
                "claims": sum(
                    info["by_year"].get(yr, {}).get("claims", 0) for yr in report_years
                ),  # FIXED: Use year-specific claims count
                "rate": (total_coll / info["total_pr"] * 100) if info["total_pr"] > 0 else 0,
                "by_year": {str(yr): info["by_year"].get(yr, {}) for yr in report_years},
                "by_company": info["by_company"],
            }
        )

    # Build company data with Medicare/Other breakdown
    for company_id, data in sorted(company_data_subset.items(), key=lambda x: x[1].ded_lines_total_pr, reverse=True):
        cm = company_metrics[company_id]

        company_entry = {
            "id": company_id,
            "name": data.company_name,
            "all": {},
            "medicare": {},
            "other": {},
            "by_year": {str(yr): {} for yr in report_years},
            "by_year_medicare": {str(yr): {} for yr in report_years},
            "by_year_other": {str(yr): {} for yr in report_years},
        }

        # All payers combined - FIXED: Sum ALL totals from year-specific data so TOTAL = sum(year columns)
        total_pr = sum(data.ded_lines_total_pr_by_year.get(yr, 0) for yr in report_years)
        total_patient = sum(data.patient_collected_by_year.get(yr, 0) for yr in report_years)
        total_secondary = sum(data.secondary_recovery_by_year.get(yr, 0) for yr in report_years)
        total_coll = total_patient + total_secondary
        total_deductible = sum(data.ded_lines_deductible_by_year.get(yr, 0) for yr in report_years)
        total_coinsurance = sum(data.ded_lines_coinsurance_by_year.get(yr, 0) for yr in report_years)
        total_copay = sum(data.ded_lines_copay_by_year.get(yr, 0) for yr in report_years)
        total_noncovered = sum(data.ded_lines_noncovered_by_year.get(yr, 0) for yr in report_years)
        total_other_pr = sum(data.ded_lines_other_by_year.get(yr, 0) for yr in report_years)
        total_claims = sum(len(data.claims_with_ded_by_year.get(yr, set())) for yr in report_years)
        total_loss = sum(data.loss_by_year.get(yr, 0) for yr in report_years)
        company_entry["all"] = {
            "total_pr": total_pr,
            "patient_collected": total_patient,
            "secondary_collected": total_secondary,
            "total_collected": total_coll,
            "uncollected": total_pr - total_coll,
            "claims": total_claims,
            "loss": total_loss,
            "rate": (total_coll / total_pr * 100) if total_pr > 0 else 0,
            "deductible": total_deductible,
            "coinsurance": total_coinsurance,
            "copay": total_copay,
            "noncovered": total_noncovered,
            "other_pr": total_other_pr,
        }

        # Medicare totals
        med_pr = sum(cm["medicare"][yr]["total_pr"] for yr in report_years)
        med_patient = sum(cm["medicare"][yr]["patient_collected"] for yr in report_years)
        med_secondary = sum(cm["medicare"][yr]["secondary_collected"] for yr in report_years)
        med_coll = med_patient + med_secondary
        med_claims = sum(cm["medicare"][yr]["claims"] for yr in report_years)
        med_loss = sum(cm["medicare"][yr]["loss"] for yr in report_years)
        med_deductible = sum(cm["medicare"][yr].get("deductible", 0) for yr in report_years)
        med_coinsurance = sum(cm["medicare"][yr].get("coinsurance", 0) for yr in report_years)
        med_copay = sum(cm["medicare"][yr].get("copay", 0) for yr in report_years)
        med_noncovered = sum(cm["medicare"][yr].get("noncovered", 0) for yr in report_years)
        med_other_pr = sum(cm["medicare"][yr].get("other_pr", 0) for yr in report_years)
        company_entry["medicare"] = {
            "total_pr": med_pr,
            "patient_collected": med_patient,
            "secondary_collected": med_secondary,
            "total_collected": med_coll,
            "uncollected": med_pr - med_coll,
            "claims": med_claims,
            "loss": med_loss,
            "rate": (med_coll / med_pr * 100) if med_pr > 0 else 0,
            "deductible": med_deductible,
            "coinsurance": med_coinsurance,
            "copay": med_copay,
            "noncovered": med_noncovered,
            "other_pr": med_other_pr,
        }

        # Other totals
        other_pr = sum(cm["other"][yr]["total_pr"] for yr in report_years)
        other_patient = sum(cm["other"][yr]["patient_collected"] for yr in report_years)
        other_secondary = sum(cm["other"][yr]["secondary_collected"] for yr in report_years)
        other_coll = other_patient + other_secondary
        other_claims = sum(cm["other"][yr]["claims"] for yr in report_years)
        other_loss = sum(cm["other"][yr]["loss"] for yr in report_years)
        other_deductible = sum(cm["other"][yr].get("deductible", 0) for yr in report_years)
        other_coinsurance = sum(cm["other"][yr].get("coinsurance", 0) for yr in report_years)
        other_copay = sum(cm["other"][yr].get("copay", 0) for yr in report_years)
        other_noncovered = sum(cm["other"][yr].get("noncovered", 0) for yr in report_years)
        other_other_pr = sum(cm["other"][yr].get("other_pr", 0) for yr in report_years)
        company_entry["other"] = {
            "total_pr": other_pr,
            "patient_collected": other_patient,
            "secondary_collected": other_secondary,
            "total_collected": other_coll,
            "uncollected": other_pr - other_coll,
            "claims": other_claims,
            "loss": other_loss,
            "rate": (other_coll / other_pr * 100) if other_pr > 0 else 0,
            "deductible": other_deductible,
            "coinsurance": other_coinsurance,
            "copay": other_copay,
            "noncovered": other_noncovered,
            "other_pr": other_other_pr,
        }

        # Year breakdowns
        for yr in report_years:
            yr_str = str(yr)
            yr_pr = data.ded_lines_total_pr_by_year.get(yr, 0)
            yr_patient = data.patient_collected_by_year.get(yr, 0)
            yr_secondary = data.secondary_recovery_by_year.get(yr, 0)
            yr_coll = yr_patient + yr_secondary
            yr_claims = len(data.claims_with_ded_by_year.get(yr, set()))
            yr_loss = data.loss_by_year.get(yr, 0)
            company_entry["by_year"][yr_str] = {
                "total_pr": yr_pr,
                "patient_collected": yr_patient,
                "secondary_collected": yr_secondary,
                "total_collected": yr_coll,
                "uncollected": yr_pr - yr_coll,
                "claims": yr_claims,
                "loss": yr_loss,
                "rate": (yr_coll / yr_pr * 100) if yr_pr > 0 else 0,
                "deductible": data.ded_lines_deductible_by_year.get(yr, 0),
                "coinsurance": data.ded_lines_coinsurance_by_year.get(yr, 0),
                "copay": data.ded_lines_copay_by_year.get(yr, 0),
                "noncovered": data.ded_lines_noncovered_by_year.get(yr, 0),
                "other_pr": data.ded_lines_other_by_year.get(yr, 0),
            }

            # Medicare by year
            m = cm["medicare"][yr]
            m_coll = m["patient_collected"] + m["secondary_collected"]
            company_entry["by_year_medicare"][yr_str] = {
                "total_pr": m["total_pr"],
                "patient_collected": m["patient_collected"],
                "secondary_collected": m["secondary_collected"],
                "total_collected": m_coll,
                "uncollected": m["total_pr"] - m_coll,
                "claims": m["claims"],
                "loss": m["loss"],
                "rate": (m_coll / m["total_pr"] * 100) if m["total_pr"] > 0 else 0,
                "deductible": m.get("deductible", 0),
                "coinsurance": m.get("coinsurance", 0),
                "copay": m.get("copay", 0),
                "noncovered": m.get("noncovered", 0),
                "other_pr": m.get("other_pr", 0),
            }

            # Other by year
            o = cm["other"][yr]
            o_coll = o["patient_collected"] + o["secondary_collected"]
            company_entry["by_year_other"][yr_str] = {
                "total_pr": o["total_pr"],
                "patient_collected": o["patient_collected"],
                "secondary_collected": o["secondary_collected"],
                "total_collected": o_coll,
                "uncollected": o["total_pr"] - o_coll,
                "claims": o["claims"],
                "loss": o["loss"],
                "rate": (o_coll / o["total_pr"] * 100) if o["total_pr"] > 0 else 0,
                "deductible": o.get("deductible", 0),
                "coinsurance": o.get("coinsurance", 0),
                "copay": o.get("copay", 0),
                "noncovered": o.get("noncovered", 0),
                "other_pr": o.get("other_pr", 0),
            }

        dashboard_data["companies"].append(company_entry)

    # Build payer data with company and year breakdown
    all_payers = defaultdict(
        lambda: {
            "total_pr": 0,
            "patient_pmt": 0,
            "secondary_pmt": 0,
            "loss": 0,
            "claims": 0,
            "is_medicare": False,
            "by_company": {},
            "by_year": {
                yr: {"total_pr": 0, "patient_pmt": 0, "secondary_pmt": 0, "loss": 0, "claims": 0} for yr in report_years
            },
        }
    )

    for company_id, payers in payer_data.items():
        for payer_name, pdata in payers.items():
            all_payers[payer_name]["total_pr"] += pdata["total_pr"]
            all_payers[payer_name]["patient_pmt"] += pdata["patient_pmt"]
            all_payers[payer_name]["secondary_pmt"] += pdata["secondary_pmt"]
            all_payers[payer_name]["loss"] += pdata["loss"]
            all_payers[payer_name]["claims"] += len(pdata["claims"])
            all_payers[payer_name]["is_medicare"] = pdata.get("is_medicare", False)
            all_payers[payer_name]["by_company"][company_id] = {
                "total_pr": pdata["total_pr"],
                "patient_pmt": pdata["patient_pmt"],
                "secondary_pmt": pdata["secondary_pmt"],
                "loss": pdata["loss"],
                "claims": len(pdata["claims"]),
            }

    # Add year breakdown from payer_by_year data
    for company_id, data in company_data_subset.items():
        for payer_name, year_data in data.payer_by_year.items():
            for yr in report_years:
                if yr in year_data:
                    yr_info = year_data[yr]
                    all_payers[payer_name]["by_year"][yr]["total_pr"] += yr_info.get("total_pr", 0)
                    all_payers[payer_name]["by_year"][yr]["patient_pmt"] += yr_info.get("patient_pmt", 0)
                    all_payers[payer_name]["by_year"][yr]["secondary_pmt"] += yr_info.get("secondary_pmt", 0)
                    all_payers[payer_name]["by_year"][yr]["loss"] += yr_info.get("loss", 0)
                    all_payers[payer_name]["by_year"][yr]["claims"] += len(yr_info.get("runs", set()))

    # FIXED: Recalculate payer totals from year-specific data so TOTAL = sum(year columns)
    for payer_name in all_payers:
        pdata = all_payers[payer_name]
        pdata["total_pr"] = sum(pdata["by_year"].get(yr, {}).get("total_pr", 0) for yr in report_years)
        pdata["patient_pmt"] = sum(pdata["by_year"].get(yr, {}).get("patient_pmt", 0) for yr in report_years)
        pdata["secondary_pmt"] = sum(pdata["by_year"].get(yr, {}).get("secondary_pmt", 0) for yr in report_years)
        pdata["loss"] = sum(pdata["by_year"].get(yr, {}).get("loss", 0) for yr in report_years)
        pdata["claims"] = sum(pdata["by_year"].get(yr, {}).get("claims", 0) for yr in report_years)

    for payer_name, pdata in sorted(all_payers.items(), key=lambda x: x[1]["total_pr"], reverse=True):
        if pdata["total_pr"] > 0:
            total_coll = pdata["patient_pmt"] + pdata["secondary_pmt"]
            dashboard_data["payers"].append(
                {
                    "name": payer_name,
                    "is_medicare": pdata["is_medicare"],
                    "total_pr": pdata["total_pr"],
                    "patient_pmt": pdata["patient_pmt"],
                    "secondary_pmt": pdata["secondary_pmt"],
                    "total_collected": total_coll,
                    "uncollected": pdata["total_pr"] - total_coll,
                    "claims": pdata["claims"],
                    "loss": pdata["loss"],
                    "rate": (total_coll / pdata["total_pr"] * 100) if pdata["total_pr"] > 0 else 0,
                    "by_company": pdata["by_company"],
                    "by_year": {str(yr): pdata["by_year"].get(yr, {}) for yr in report_years},
                }
            )

    # Build monthly data with company and payer type breakdown
    all_months = defaultdict(
        lambda: defaultdict(
            lambda: {
                "medicare": {"total_pr": 0, "patient_collected": 0, "secondary_collected": 0, "claims": 0, "loss": 0},
                "other": {"total_pr": 0, "patient_collected": 0, "secondary_collected": 0, "claims": 0, "loss": 0},
            }
        )
    )

    for company_id, months in monthly_data.items():
        for year_month, mdata in months.items():
            for payer_type in ["medicare", "other"]:
                pt = mdata[payer_type]
                all_months[year_month][company_id][payer_type]["total_pr"] += pt["total_pr"]
                all_months[year_month][company_id][payer_type]["patient_collected"] += pt["patient_collected"]
                all_months[year_month][company_id][payer_type]["secondary_collected"] += pt["secondary_collected"]
                all_months[year_month][company_id][payer_type]["claims"] += (
                    len(pt["claims"]) if isinstance(pt["claims"], set) else pt["claims"]
                )
                all_months[year_month][company_id][payer_type]["loss"] += pt["loss"]

    for year_month in sorted(all_months.keys()):
        parts = year_month.split("-")
        yr = int(parts[0])
        month = int(parts[1])
        month_name = get_month_name(month)

        # Aggregate across companies
        total_medicare = {"total_pr": 0, "patient_collected": 0, "secondary_collected": 0, "claims": 0, "loss": 0}
        total_other = {"total_pr": 0, "patient_collected": 0, "secondary_collected": 0, "claims": 0, "loss": 0}
        by_company = {}

        for company_id, cdata in all_months[year_month].items():
            by_company[company_id] = {"medicare": cdata["medicare"].copy(), "other": cdata["other"].copy()}
            for key in total_medicare:
                total_medicare[key] += cdata["medicare"][key]
                total_other[key] += cdata["other"][key]

        def calc_rate(m):
            coll = m["patient_collected"] + m["secondary_collected"]
            return (coll / m["total_pr"] * 100) if m["total_pr"] > 0 else 0

        dashboard_data["monthly"].append(
            {
                "year_month": year_month,
                "label": f"{month_name[:3]} {yr}",
                "year": yr,
                "month": month,
                "medicare": {
                    **total_medicare,
                    "total_collected": total_medicare["patient_collected"] + total_medicare["secondary_collected"],
                    "uncollected": total_medicare["total_pr"]
                    - (total_medicare["patient_collected"] + total_medicare["secondary_collected"]),
                    "rate": calc_rate(total_medicare),
                },
                "other": {
                    **total_other,
                    "total_collected": total_other["patient_collected"] + total_other["secondary_collected"],
                    "uncollected": total_other["total_pr"]
                    - (total_other["patient_collected"] + total_other["secondary_collected"]),
                    "rate": calc_rate(total_other),
                },
                "by_company": by_company,
            }
        )

    # PR Adjustments - aggregate with company, year, and payer type breakdown
    aggregate_pr = defaultdict(
        lambda: {
            "amount": 0,
            "claims": 0,
            "with_ded_amount": 0,
            "with_ded_claims": 0,
            "by_year": defaultdict(lambda: {"amount": 0, "claims": 0}),
            "with_ded_by_year": defaultdict(lambda: {"amount": 0, "claims": 0}),  # ADDED: Year-specific with_ded data
            "by_company": {},
            "medicare": {
                "amount": 0,
                "claims": 0,
                "by_year": {str(yr): {"amount": 0, "claims": 0} for yr in report_years},
            },
            "other": {
                "amount": 0,
                "claims": 0,
                "by_year": {str(yr): {"amount": 0, "claims": 0} for yr in report_years},
            },
        }
    )

    for company_id, data in company_data_subset.items():
        for code, amount in data.pr_adjustments.items():
            aggregate_pr[code]["amount"] += amount
            aggregate_pr[code]["claims"] += len(data.pr_adjustments_claims.get(code, set()))
            if company_id not in aggregate_pr[code]["by_company"]:
                aggregate_pr[code]["by_company"][company_id] = {
                    "amount": 0,
                    "claims": 0,
                    "by_year": {str(yr): {"amount": 0, "claims": 0} for yr in report_years},
                    "medicare": {"amount": 0, "claims": 0},
                    "other": {"amount": 0, "claims": 0},
                }
            aggregate_pr[code]["by_company"][company_id]["amount"] += amount
            aggregate_pr[code]["by_company"][company_id]["claims"] += len(data.pr_adjustments_claims.get(code, set()))

        # FIXED: Accumulate with_ded from year-specific data (only report_years)
        for yr in report_years:
            yr_data = data.pr_adjustments_with_ded_by_year.get(yr, {})
            for code, amount in yr_data.items():
                aggregate_pr[code]["with_ded_by_year"][yr]["amount"] += amount
                aggregate_pr[code]["with_ded_by_year"][yr]["claims"] += len(
                    data.pr_adjustments_claims_with_ded_by_year.get(yr, {}).get(code, set())
                )

        # Year breakdown (only years with data)
        for yr, yr_data in data.pr_adjustments_by_year.items():
            if yr not in report_years:
                continue
            yr_str = str(yr)
            for code, amount in yr_data.items():
                aggregate_pr[code]["by_year"][yr]["amount"] += amount
                aggregate_pr[code]["by_year"][yr]["claims"] += len(
                    data.pr_adjustments_claims_by_year.get(yr, {}).get(code, set())
                )
                # Company-year breakdown
                if company_id in aggregate_pr[code]["by_company"]:
                    aggregate_pr[code]["by_company"][company_id]["by_year"][yr_str]["amount"] += amount
                    aggregate_pr[code]["by_company"][company_id]["by_year"][yr_str]["claims"] += len(
                        data.pr_adjustments_claims_by_year.get(yr, {}).get(code, set())
                    )

        # Medicare breakdown (aggregate)
        for code, amount in data.pr_adjustments_medicare.items():
            aggregate_pr[code]["medicare"]["amount"] += amount
            aggregate_pr[code]["medicare"]["claims"] += len(data.pr_adjustments_claims_medicare.get(code, set()))
            # Company-medicare breakdown
            if company_id in aggregate_pr[code]["by_company"]:
                aggregate_pr[code]["by_company"][company_id]["medicare"]["amount"] += amount
                aggregate_pr[code]["by_company"][company_id]["medicare"]["claims"] += len(
                    data.pr_adjustments_claims_medicare.get(code, set())
                )

        # Medicare by year breakdown (only years with data)
        for yr, yr_data in data.pr_adjustments_medicare_by_year.items():
            if yr not in report_years:
                continue
            yr_str = str(yr)
            for code, amount in yr_data.items():
                aggregate_pr[code]["medicare"]["by_year"][yr_str]["amount"] += amount
                aggregate_pr[code]["medicare"]["by_year"][yr_str]["claims"] += len(
                    data.pr_adjustments_claims_medicare_by_year.get(yr, {}).get(code, set())
                )

        # Other breakdown (aggregate)
        for code, amount in data.pr_adjustments_other.items():
            aggregate_pr[code]["other"]["amount"] += amount
            aggregate_pr[code]["other"]["claims"] += len(data.pr_adjustments_claims_other.get(code, set()))
            # Company-other breakdown
            if company_id in aggregate_pr[code]["by_company"]:
                aggregate_pr[code]["by_company"][company_id]["other"]["amount"] += amount
                aggregate_pr[code]["by_company"][company_id]["other"]["claims"] += len(
                    data.pr_adjustments_claims_other.get(code, set())
                )

        # Other by year breakdown (only years with data)
        for yr, yr_data in data.pr_adjustments_other_by_year.items():
            if yr not in report_years:
                continue
            yr_str = str(yr)
            for code, amount in yr_data.items():
                aggregate_pr[code]["other"]["by_year"][yr_str]["amount"] += amount
                aggregate_pr[code]["other"]["by_year"][yr_str]["claims"] += len(
                    data.pr_adjustments_claims_other_by_year.get(yr, {}).get(code, set())
                )

    # FIXED: Recalculate PR adjustment totals from year-specific data so TOTAL = sum(year columns)
    for code in aggregate_pr:
        amounts = aggregate_pr[code]
        amounts["amount"] = sum(amounts["by_year"].get(yr, {}).get("amount", 0) for yr in report_years)
        amounts["claims"] = sum(amounts["by_year"].get(yr, {}).get("claims", 0) for yr in report_years)
        # Recalculate with_ded totals from year-specific data
        amounts["with_ded_amount"] = sum(
            amounts["with_ded_by_year"].get(yr, {}).get("amount", 0) for yr in report_years
        )
        amounts["with_ded_claims"] = sum(
            amounts["with_ded_by_year"].get(yr, {}).get("claims", 0) for yr in report_years
        )
        # Also recalculate Medicare/Other totals from their by_year data
        amounts["medicare"]["amount"] = sum(
            amounts["medicare"]["by_year"].get(str(yr), {}).get("amount", 0) for yr in report_years
        )
        amounts["medicare"]["claims"] = sum(
            amounts["medicare"]["by_year"].get(str(yr), {}).get("claims", 0) for yr in report_years
        )
        amounts["other"]["amount"] = sum(
            amounts["other"]["by_year"].get(str(yr), {}).get("amount", 0) for yr in report_years
        )
        amounts["other"]["claims"] = sum(
            amounts["other"]["by_year"].get(str(yr), {}).get("claims", 0) for yr in report_years
        )
        # Recalculate by_company totals from their by_year data
        for company_id, company_info in amounts["by_company"].items():
            company_info["amount"] = sum(
                company_info["by_year"].get(str(yr), {}).get("amount", 0) for yr in report_years
            )
            company_info["claims"] = sum(
                company_info["by_year"].get(str(yr), {}).get("claims", 0) for yr in report_years
            )

    for code, amounts in sorted(aggregate_pr.items(), key=lambda x: abs(x[1]["amount"]), reverse=True):
        reason_num = code.replace("PR-", "")
        desc = CARC_DESCRIPTIONS.get(reason_num, "Other Patient Responsibility")
        dashboard_data["pr_adjustments"].append(
            {
                "code": code,
                "description": desc,
                "total_amount": amounts["amount"],
                "total_claims": amounts["claims"],
                "with_ded_amount": amounts["with_ded_amount"],
                "with_ded_claims": amounts["with_ded_claims"],
                "by_year": {str(yr): amounts["by_year"].get(yr, {"amount": 0, "claims": 0}) for yr in report_years},
                "by_company": amounts["by_company"],
                "medicare": amounts["medicare"],
                "other": amounts["other"],
            }
        )

    # Convert to JSON
    data_json = json.dumps(dashboard_data, default=str)

    # Determine title
    if single_company_id:
        title = f"Deductible Collection Analysis - {company_name}"
    else:
        title = "Deductible Collection Analysis - All Companies"

    # Generate HTML - Simple Tabular Dashboard
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: 'IBM Plex Sans', -apple-system, sans-serif;
            background: #1e293b;
            color: #1e293b;
            line-height: 1.5;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ font-size: 1.5rem; font-weight: 700; margin-bottom: 0.25rem; color: #f1f5f9; }}
        h2 {{ font-size: 1.1rem; font-weight: 600; margin: 1.5rem 0 0.75rem; color: #334155; border-bottom: 2px solid #e2e8f0; padding-bottom: 0.5rem; }}
        h3 {{ font-size: 0.95rem; font-weight: 600; margin: 1rem 0 0.5rem; color: #475569; }}
        .subtitle {{ color: #94a3b8; font-size: 0.875rem; margin-bottom: 1rem; }}

        /* Filter Bar - sticky at top */
        .filter-bar {{
            position: sticky;
            top: 0;
            z-index: 100;
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 1rem;
            margin-bottom: 1.5rem;
            display: flex;
            gap: 1.5rem;
            flex-wrap: wrap;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            align-items: center;
        }}
        .filter-group {{ display: flex; align-items: center; gap: 0.5rem; }}
        .filter-group label {{ font-size: 0.8rem; font-weight: 600; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }}
        .filter-group select {{
            padding: 0.4rem 0.75rem;
            border: 1px solid #cbd5e1;
            border-radius: 4px;
            font-size: 0.875rem;
            background: #fff;
            min-width: 140px;
        }}
        .filter-group select:focus {{ outline: 2px solid #0ea5e9; border-color: #0ea5e9; }}

        /* Tables */
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875rem;
            background: #fff;
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            overflow: hidden;
            margin-bottom: 1rem;
        }}
        th {{
            background: #f1f5f9;
            font-weight: 600;
            text-align: left;
            padding: 0.6rem 0.75rem;
            color: #475569;
            border-bottom: 2px solid #e2e8f0;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }}
        td {{
            padding: 0.5rem 0.75rem;
            border-bottom: 1px solid #f1f5f9;
        }}
        tr:hover {{ background: #f8fafc; }}
        tr.total-row {{
            background: #f1f5f9 !important;
            font-weight: 600;
            border-top: 2px solid #cbd5e1;
        }}
        tr.total-row td {{ border-bottom: none; }}
        .text-right {{ text-align: right; }}
        .text-center {{ text-align: center; }}
        .mono {{ font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem; }}
        .currency {{ color: #0f172a; }}
        .positive {{ color: #059669; }}
        .negative {{ color: #dc2626; }}
        .warning {{ color: #d97706; }}
        .rate {{ font-weight: 600; }}
        .rate-good {{ color: #059669; }}
        .rate-mid {{ color: #d97706; }}
        .rate-bad {{ color: #dc2626; }}

        /* Section Cards */
        .section {{
            background: #fff;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            padding: 1rem 1.25rem;
            margin-bottom: 1.5rem;
        }}
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
            padding-bottom: 0.75rem;
            border-bottom: 1px solid #f1f5f9;
        }}
        .section-title {{ font-size: 1rem; font-weight: 600; color: #1e293b; }}
        .section-subtitle {{ font-size: 0.8rem; color: #64748b; }}
        .loss-formula {{ font-size: 0.75rem; color: #334155; font-style: normal; background: #f1f5f9; padding: 0.5rem 0.75rem; border-radius: 4px; border: 1px solid #cbd5e1; }}
        .loss-formula strong {{ font-weight: 600; color: #1e293b; }}
        .loss-formula .definition {{ display: block; margin-bottom: 0.25rem; }}
        .loss-formula .explanation {{ display: block; font-size: 0.7rem; color: #475569; }}

        /* Summary Stats */
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }}
        .summary-stat {{
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            padding: 0.75rem 1rem;
        }}
        .summary-stat .label {{ font-size: 0.75rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.25rem; }}
        .summary-stat .value {{ font-family: 'IBM Plex Mono', monospace; font-size: 1.1rem; font-weight: 600; color: #1e293b; }}
        .summary-stat.highlight {{ background: #0f172a; border-color: #0f172a; }}
        .summary-stat.highlight .label {{ color: #94a3b8; }}
        .summary-stat.highlight .value {{ color: #fff; }}

        /* Two column layout */
        .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }}
        @media (max-width: 900px) {{ .two-col {{ grid-template-columns: 1fr; }} }}

        /* Sticky table headers for scrollable containers */
        #topPatientsTable, #prAdjTable {{
            overflow: visible;
            border-collapse: separate;
            border-spacing: 0;
        }}
        #payerTable thead, #topPatientsTable thead, #prAdjTable thead {{
            position: sticky;
            top: 0;
            z-index: 10;
            background: #f1f5f9;
        }}
        .sticky-header {{
            position: sticky;
            top: 0;
            background: #f1f5f9;
            z-index: 10;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}

        /* Sortable column headers */
        .sortable {{
            cursor: pointer;
            user-select: none;
            transition: background-color 0.15s;
        }}
        .sortable:hover {{
            background: #e2e8f0;
        }}
        .sort-icon {{
            display: inline-block;
            margin-left: 4px;
            opacity: 0.3;
            font-size: 0.7rem;
        }}
        .sort-icon::after {{
            content: '↕';
        }}
        .sortable.sort-asc .sort-icon::after {{
            content: '↑';
            opacity: 1;
        }}
        .sortable.sort-desc .sort-icon::after {{
            content: '↓';
            opacity: 1;
        }}
        .sortable.sort-asc .sort-icon,
        .sortable.sort-desc .sort-icon {{
            opacity: 1;
            color: #0ea5e9;
        }}

        /* Loading indicator */
        .loading {{ position: relative; pointer-events: none; }}
        .loading .container {{ opacity: 0.6; }}
        .loading::after {{
            content: '';
            position: fixed;
            top: 50%;
            left: 50%;
            width: 40px;
            height: 40px;
            margin: -20px 0 0 -20px;
            border: 4px solid #e2e8f0;
            border-top-color: #0ea5e9;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            z-index: 9999;
        }}
        @keyframes spin {{ to {{ transform: rotate(360deg); }} }}

        /* Print styles */
        @media print {{
            body {{ background: #fff; padding: 0; font-size: 10pt; }}
            .filter-bar {{ display: none; }}
            .section {{ break-inside: avoid; border: none; padding: 0; margin-bottom: 1rem; }}
            table {{ font-size: 9pt; }}
            h2 {{ font-size: 12pt; }}
        }}

        /* ═══════════════════════════════════════════════════════════════
           MOBILE RESPONSIVE STYLES
           These styles ONLY apply to screens 768px and below.
           Desktop layout remains completely unchanged.
           ═══════════════════════════════════════════════════════════════ */
        @media (max-width: 768px) {{
            /* Base adjustments */
            body {{ padding: 12px; font-size: 0.9rem; }}
            .container {{ max-width: 100%; padding: 0; }}

            /* Header */
            header {{ margin-bottom: 1rem !important; }}
            h1 {{ font-size: 1.2rem; line-height: 1.3; }}
            .subtitle {{ font-size: 0.75rem; line-height: 1.4; }}

            /* Filter bar - stack vertically */
            .filter-bar {{
                flex-direction: column;
                gap: 0.75rem;
                padding: 0.75rem;
            }}
            .filter-group {{
                width: 100%;
                flex-direction: column;
                align-items: stretch;
                gap: 0.35rem;
            }}
            .filter-group label {{ font-size: 0.7rem; }}
            .filter-group select {{
                width: 100%;
                min-width: unset;
                padding: 0.6rem 0.75rem;
                font-size: 16px; /* Prevents iOS zoom */
                border-radius: 6px;
            }}
            .filter-bar > div[style*="margin-left: auto"] {{
                width: 100%;
                text-align: center;
                padding-top: 0.5rem;
                border-top: 1px solid #334155;
                margin-top: 0.25rem;
                margin-left: 0 !important;
            }}

            /* Stack all two-column and side-by-side layouts */
            .two-col {{
                grid-template-columns: 1fr !important;
                gap: 1rem !important;
            }}
            div[style*="grid-template-columns: 1fr 1fr"] {{
                display: flex !important;
                flex-direction: column !important;
                gap: 1rem !important;
            }}

            /* Section cards */
            .section {{
                padding: 0.75rem;
                margin-bottom: 1rem;
                border-radius: 8px;
            }}
            .section-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 0.5rem;
            }}
            .section-title {{ font-size: 0.9rem; }}
            .section-subtitle {{ font-size: 0.7rem; }}
            .loss-formula {{
                width: 100%;
                font-size: 0.65rem;
                padding: 0.4rem;
            }}
            .loss-formula .explanation {{ font-size: 0.6rem; }}

            /* Summary stats grid */
            .summary-grid {{
                grid-template-columns: 1fr 1fr !important;
                gap: 0.5rem;
            }}
            .summary-stat {{
                padding: 0.5rem 0.6rem;
            }}
            .summary-stat .label {{ font-size: 0.6rem; }}
            .summary-stat .value {{ font-size: 0.9rem; }}

            /* Tables - horizontal scroll */
            div[style*="overflow-y: auto"] {{
                max-height: none !important;
                overflow-x: auto !important;
                overflow-y: visible !important;
                -webkit-overflow-scrolling: touch;
            }}
            table {{
                font-size: 0.75rem;
                min-width: 550px;
            }}
            th {{
                font-size: 0.65rem;
                padding: 0.4rem 0.5rem;
                white-space: nowrap;
            }}
            td {{
                padding: 0.35rem 0.5rem;
                white-space: nowrap;
            }}
            .mono {{ font-size: 0.7rem; }}

            /* Rate indicators */
            .rate {{ font-size: 0.75rem; }}

            /* Touch-friendly rows */
            tr:active {{ background: #e2e8f0 !important; }}

            /* Sortable headers */
            .sortable {{ min-height: 44px; }}
        }}

        /* Extra small phones */
        @media (max-width: 380px) {{
            body {{ padding: 8px; }}
            h1 {{ font-size: 1.1rem; }}
            .filter-bar {{ padding: 0.5rem; }}
            .summary-grid {{ grid-template-columns: 1fr !important; }}
            table {{ min-width: 480px; font-size: 0.7rem; }}
            th, td {{ padding: 0.3rem 0.4rem; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header style="margin-bottom: 1.5rem;">
            <h1>{title}</h1>
            <p class="subtitle">Generated: {dashboard_data['generated_at']} | Source: 835 Remittance Data + Trip Credits</p>
        </header>

        <!-- Filters -->
        <div class="filter-bar">
            <div class="filter-group">
                <label for="yearFilter">Year</label>
                <select id="yearFilter" onchange="debouncedApplyFilters()">
                    <option value="all">All Years</option>
                    {''.join([f'<option value="{yr}">{yr}</option>' for yr in dashboard_data.get('available_years', [2024, 2025])])}
                </select>
            </div>
            <div class="filter-group">
                <label for="payerTypeFilter">Payer Type</label>
                <select id="payerTypeFilter" onchange="debouncedApplyFilters()">
                    <option value="all">All Payers</option>
                    <option value="medicare">Medicare Only</option>
                    <option value="other">Commercial/Other</option>
                </select>
            </div>
            {'<div class="filter-group"><label for="companyFilter">Company</label><select id="companyFilter" onchange="debouncedApplyFilters()"><option value="all">All Companies</option>' + ''.join([f'<option value="{c["id"]}">{c["name"][:30]}</option>' for c in dashboard_data.get("companies", [])]) + '</select></div>' if not dashboard_data.get('is_single_company') else ''}
            <div style="margin-left: auto; font-size: 0.8rem; color: #cbd5e1; font-weight: 500;">
                <span id="filterStatus">Showing: All Data</span>
            </div>
        </div>

        <!-- SIDE BY SIDE: ALL CLAIMS vs DEDUCTIBLE CLAIMS -->
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; margin-bottom: 1.5rem;">
            <!-- LEFT: ALL CLAIMS -->
            <div class="section" style="margin-bottom: 0;">
                <div class="section-header">
                    <div>
                        <div class="section-title">ALL CLAIMS</div>
                        <div class="section-subtitle">Patient responsibility across all claims</div>
                    </div>
                </div>

                <table id="prBreakdownTable">
                    <thead>
                        <tr>
                            <th>Component</th>
                            {''.join([f'<th class="text-right">{yr}</th>' for yr in dashboard_data.get('available_years', [2024, 2025])])}
                            <th class="text-right">TOTAL</th>
                        </tr>
                    </thead>
                    <tbody id="prBreakdownBody">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>

                <table id="collectionTable" style="margin-top: 0.5rem;">
                    <thead>
                        <tr>
                            <th>Metric</th>
                            {''.join([f'<th class="text-right">{yr}</th>' for yr in dashboard_data.get('available_years', [2024, 2025])])}
                            <th class="text-right">TOTAL</th>
                        </tr>
                    </thead>
                    <tbody id="collectionBody">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>
            </div>

            <!-- RIGHT: DEDUCTIBLE CLAIMS -->
            <div class="section" style="margin-bottom: 0;">
                <div class="section-header">
                    <div>
                        <div class="section-title">DEDUCTIBLE CLAIMS (PR-1)</div>
                        <div class="section-subtitle">Claims with deductible adjustments only</div>
                    </div>
                </div>

                <table id="dedPRTable">
                    <thead>
                        <tr>
                            <th>Component</th>
                            {''.join([f'<th class="text-right">{yr}</th>' for yr in dashboard_data.get('available_years', [2024, 2025])])}
                            <th class="text-right">TOTAL</th>
                        </tr>
                    </thead>
                    <tbody id="dedPRBody">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>

                <table id="dedCollTable" style="margin-top: 0.5rem;">
                    <thead>
                        <tr>
                            <th>Metric</th>
                            {''.join([f'<th class="text-right">{yr}</th>' for yr in dashboard_data.get('available_years', [2024, 2025])])}
                            <th class="text-right">TOTAL</th>
                        </tr>
                    </thead>
                    <tbody id="dedCollBody">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>
            </div>
        </div>

        <!-- COMPANY COMPARISON (if aggregate) -->
        {'<div class="section" id="companySection"><div class="section-header"><div><div class="section-title">Company Comparison</div><div class="section-subtitle">Performance by company (deductible claims)</div></div><div class="loss-formula"><span class="definition"><strong>LOSS</strong> = Deductible on claims where NO patient payment AND NO secondary payment was received</span><span class="explanation">These represent the true collectable deductible amounts with zero recovery.</span></div></div><table id="companyTable"><thead><tr><th class="sortable" data-sort="name" onclick="sortCompanies(this.dataset.sort)">Company <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="claims" onclick="sortCompanies(this.dataset.sort)">Claims <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="pr" onclick="sortCompanies(this.dataset.sort)">Total PR <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="pat" onclick="sortCompanies(this.dataset.sort)">Patient Pmt <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="sec" onclick="sortCompanies(this.dataset.sort)">Secondary <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="coll" onclick="sortCompanies(this.dataset.sort)">Total Coll <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="uncoll" onclick="sortCompanies(this.dataset.sort)">Uncollected <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="rate" onclick="sortCompanies(this.dataset.sort)">Rate <span class="sort-icon"></span></th><th class="sortable text-right" data-sort="loss" onclick="sortCompanies(this.dataset.sort)">LOSS <span class="sort-icon"></span></th></tr></thead><tbody id="companyBody"></tbody></table></div>' if not dashboard_data.get('is_single_company') else ''}

        <!-- PAYER ANALYSIS -->
        <div class="section">
            <div class="section-header">
                <div>
                    <div class="section-title">Payer Analysis</div>
                    <div class="section-subtitle">Performance by insurance payer (deductible claims)</div>
                </div>
                <div class="loss-formula"><span class="definition"><strong>LOSS</strong> = Deductible on claims where NO patient payment AND NO secondary payment was received</span><span class="explanation">These represent the true collectable deductible amounts with zero recovery.</span></div>
            </div>
            <div style="max-height: 500px; overflow-y: auto; border: 1px solid #e2e8f0; border-radius: 4px;">
                <table id="payerTable" style="margin: 0; border-collapse: separate; border-spacing: 0; overflow: visible;">
                    <thead style="position: sticky; top: 0; z-index: 10;">
                        <tr>
                            <th class="sortable sticky-header" data-sort="payer" onclick="sortPayers('payer')">Payer <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="claims" onclick="sortPayers('claims')">Claims <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="pr" onclick="sortPayers('pr')">Total PR <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="pat" onclick="sortPayers('pat')">Patient Pmt <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="sec" onclick="sortPayers('sec')">Secondary <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="coll" onclick="sortPayers('coll')">Total Coll <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="uncoll" onclick="sortPayers('uncoll')">Uncollected <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="rate" onclick="sortPayers('rate')">Rate <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="loss" onclick="sortPayers('loss')">LOSS <span class="sort-icon"></span></th>
                        </tr>
                    </thead>
                    <tbody id="payerBody">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>
            </div>
        </div>

        <!-- MONTHLY TREND -->
        <div class="section">
            <div class="section-header">
                <div>
                    <div class="section-title">Monthly Analysis</div>
                    <div class="section-subtitle">Collection trends by month (deductible claims)</div>
                </div>
                <div class="loss-formula"><span class="definition"><strong>LOSS</strong> = Deductible on claims where NO patient payment AND NO secondary payment was received</span><span class="explanation">These represent the true collectable deductible amounts with zero recovery.</span></div>
            </div>
            <table id="monthlyTable">
                <thead>
                    <tr>
                        <th class="sortable" data-sort="label" onclick="sortMonthly('label')">Month <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="claims" onclick="sortMonthly('claims')">Claims <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="pr" onclick="sortMonthly('pr')">Total PR <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="pat" onclick="sortMonthly('pat')">Patient Pmt <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="sec" onclick="sortMonthly('sec')">Secondary <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="coll" onclick="sortMonthly('coll')">Total Coll <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="uncoll" onclick="sortMonthly('uncoll')">Uncollected <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="rate" onclick="sortMonthly('rate')">Rate <span class="sort-icon"></span></th>
                        <th class="sortable text-right" data-sort="loss" onclick="sortMonthly('loss')">LOSS <span class="sort-icon"></span></th>
                    </tr>
                </thead>
                <tbody id="monthlyBody">
                    <!-- Populated by JavaScript -->
                </tbody>
            </table>
        </div>

        <!-- TOP PATIENTS TABLE -->
        <div class="section">
            <div class="section-header">
                <div>
                    <div class="section-title">Patients with Highest Deductible</div>
                    <div class="section-subtitle">Top 50 patients by deductible amount (filtered by payer type)</div>
                </div>
            </div>
            <div style="max-height: 500px; overflow-y: auto; border: 1px solid #e2e8f0; border-radius: 4px; position: relative;">
                <table id="topPatientsTable" style="margin: 0; border-collapse: separate; border-spacing: 0;">
                    <thead>
                        <tr>
                            <th class="sortable sticky-header" data-sort="name" onclick="sortPatients('name')">Patient Name <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header" data-sort="member_id" onclick="sortPatients('member_id')">Member ID <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="claims" onclick="sortPatients('claims')">Claims <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="deductible" onclick="sortPatients('deductible')">Deductible <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="total_pr" onclick="sortPatients('total_pr')">Total PR <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="collected" onclick="sortPatients('collected')">Collected <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="secondary" onclick="sortPatients('secondary')">Secondary <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="uncollected" onclick="sortPatients('uncollected')">Uncollected <span class="sort-icon"></span></th>
                        </tr>
                    </thead>
                    <tbody id="topPatientsBody">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>
            </div>
        </div>

        <!-- PR ADJUSTMENTS TABLE (at bottom with scroll) -->
        <div class="section">
            <div class="section-header">
                <div>
                    <div class="section-title">PR Adjustment Breakdown</div>
                    <div class="section-subtitle">All patient responsibility adjustment codes</div>
                </div>
            </div>
            <div style="max-height: 400px; overflow-y: auto; border: 1px solid #e2e8f0; border-radius: 4px;">
                <table id="prAdjTable" style="margin: 0; border-collapse: separate; border-spacing: 0; overflow: visible;">
                    <thead style="position: sticky; top: 0; z-index: 10;">
                        <tr>
                            <th class="sortable sticky-header" data-sort="code" onclick="sortPRAdj('code')">Code <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header" data-sort="description" onclick="sortPRAdj('description')">Description <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="claims_2024" onclick="sortPRAdj('claims_2024')">{dashboard_data.get('available_years', [2024, 2025])[0]} Claims <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="amount_2024" onclick="sortPRAdj('amount_2024')">{dashboard_data.get('available_years', [2024, 2025])[0]} Amount <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="claims_2025" onclick="sortPRAdj('claims_2025')">{dashboard_data.get('available_years', [2024, 2025])[1] if len(dashboard_data.get('available_years', [2024, 2025])) > 1 else ''} Claims <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="amount_2025" onclick="sortPRAdj('amount_2025')">{dashboard_data.get('available_years', [2024, 2025])[1] if len(dashboard_data.get('available_years', [2024, 2025])) > 1 else ''} Amount <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="total_claims" onclick="sortPRAdj('total_claims')">Total Claims <span class="sort-icon"></span></th>
                            <th class="sortable sticky-header text-right" data-sort="total_amount" onclick="sortPRAdj('total_amount')">Total Amount <span class="sort-icon"></span></th>
                        </tr>
                    </thead>
                    <tbody id="prAdjBody">
                        <!-- Populated by JavaScript -->
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        // Dashboard data
        const DATA = {data_json};

        // Formatting functions
        const fmt = (v) => new Intl.NumberFormat('en-US', {{style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0}}).format(v || 0);
        const fmtNum = (v) => new Intl.NumberFormat('en-US').format(v || 0);
        const fmtPct = (v) => (v || 0).toFixed(1) + '%';
        const rateClass = (v) => v >= 30 ? 'rate-good' : v >= 15 ? 'rate-mid' : 'rate-bad';

        // Dynamic year references from data
        const YEARS = DATA.available_years || [2024, 2025];
        const YR1 = String(YEARS[0] || 2024);
        const YR2 = String(YEARS[1] || 2025);
        // Debounce utility - prevents UI thrashing on rapid filter changes
        function debounce(func, wait) {{
            let timeout;
            return function executedFunction(...args) {{
                clearTimeout(timeout);
                timeout = setTimeout(() => func.apply(this, args), wait);
            }};
        }}

        // Sort state for all tables
        let patientSortField = 'deductible';
        let patientSortAsc = false;
        let payerSortField = 'pr';
        let payerSortAsc = false;
        let monthlySortField = 'label';
        let monthlySortAsc = true;
        let prAdjSortField = 'total_amount';
        let prAdjSortAsc = false;
        let companySortField = 'pr';
        let companySortAsc = false;

        // Generic sort function for tables
        function sortTable(tableId, field, sortState) {{
            // Toggle direction if same field
            if (field === sortState.field) {{
                sortState.asc = !sortState.asc;
            }} else {{
                sortState.field = field;
                // Text fields start ascending, numeric fields start descending
                const textFields = ['name', 'member_id', 'payer', 'label', 'code', 'description'];
                sortState.asc = textFields.includes(field);
            }}

            // Update header classes
            document.querySelectorAll(`#${{tableId}} .sortable`).forEach(th => {{
                th.classList.remove('sort-asc', 'sort-desc');
                if (th.dataset.sort === field) {{
                    th.classList.add(sortState.asc ? 'sort-asc' : 'sort-desc');
                }}
            }});
        }}

        // Sort patients table
        function sortPatients(field) {{
            const state = {{ field: patientSortField, asc: patientSortAsc }};
            sortTable('topPatientsTable', field, state);
            patientSortField = state.field;
            patientSortAsc = state.asc;
            renderTopPatients();
        }}

        // Sort payers table
        function sortPayers(field) {{
            const state = {{ field: payerSortField, asc: payerSortAsc }};
            sortTable('payerTable', field, state);
            payerSortField = state.field;
            payerSortAsc = state.asc;
            renderPayers();
        }}

        // Sort monthly table
        function sortMonthly(field) {{
            const state = {{ field: monthlySortField, asc: monthlySortAsc }};
            sortTable('monthlyTable', field, state);
            monthlySortField = state.field;
            monthlySortAsc = state.asc;
            renderMonthly();
        }}

        // Sort PR adjustments table
        function sortPRAdj(field) {{
            const state = {{ field: prAdjSortField, asc: prAdjSortAsc }};
            sortTable('prAdjTable', field, state);
            prAdjSortField = state.field;
            prAdjSortAsc = state.asc;
            renderPRAdjustments();
        }}

        // Sort companies table
        function sortCompanies(field) {{
            const state = {{ field: companySortField, asc: companySortAsc }};
            sortTable('companyTable', field, state);
            companySortField = state.field;
            companySortAsc = state.asc;
            renderCompanies();
        }}

        // Get filter values
        function getFilters() {{
            return {{
                year: document.getElementById('yearFilter')?.value || 'all',
                payerType: document.getElementById('payerTypeFilter')?.value || 'all',
                company: document.getElementById('companyFilter')?.value || 'all'
            }};
        }}

        // Apply filters and re-render
        function applyFilters() {{
            // Save scroll position before re-render
            const scrollY = window.scrollY;
            const scrollX = window.scrollX;

            const f = getFilters();
            let status = [];
            if (f.year !== 'all') status.push(f.year);
            if (f.payerType !== 'all') status.push(f.payerType === 'medicare' ? 'Medicare' : 'Commercial');
            if (f.company !== 'all') {{
                const comp = DATA.companies.find(c => c.id === f.company);
                status.push(comp ? comp.name.substring(0, 20) : f.company);
            }}
            document.getElementById('filterStatus').textContent = status.length ? 'Filtered: ' + status.join(', ') : 'Showing: All Data';
            renderAll();

            // Restore scroll position after re-render
            requestAnimationFrame(() => {{
                window.scrollTo(scrollX, scrollY);
            }});
        }}

        // Debounced version for dropdown changes (150ms delay)
        const debouncedApplyFilters = debounce(applyFilters, 150);

        // Render Layer 1 stats - ALL CLAIMS (supports year, company, payer type filters)
        // DYNAMICALLY handles ALL years in YEARS array
        function renderLayer1() {{
            const f = getFilters();
            const oc = DATA.overall_context;
            const zeroData = {{ claims: 0, total_pr: 0, deductible: 0, coinsurance: 0, copay: 0, noncovered: 0, other_pr: 0, patient_pmt: 0, secondary_pmt: 0, loss: 0, rate: 0 }};

            // Get data source based on filters
            let source = oc;
            if (f.company !== 'all' && f.payerType !== 'all') {{
                const companyData = oc.by_company?.[f.company];
                source = f.payerType === 'medicare' ? companyData?.medicare : companyData?.other;
                source = source || {{}};
            }} else if (f.company !== 'all') {{
                source = oc.by_company?.[f.company] || {{}};
            }} else if (f.payerType !== 'all') {{
                source = f.payerType === 'medicare' ? oc.medicare : oc.other;
                source = source || {{}};
            }}

            // Build yearData object for ALL years dynamically
            const yearData = {{}};
            YEARS.forEach(yr => {{
                const yrStr = String(yr);
                if (f.year !== 'all' && f.year !== yrStr) {{
                    yearData[yrStr] = zeroData;
                }} else {{
                    yearData[yrStr] = source.by_year?.[yrStr] || zeroData;
                }}
            }});

            // Calculate totals from all displayed years
            let d = {{
                claims: 0, deductible: 0, coinsurance: 0, copay: 0, noncovered: 0, other_pr: 0,
                patient_pmt: 0, secondary_pmt: 0, total_pr: 0
            }};
            YEARS.forEach(yr => {{
                const yd = yearData[String(yr)];
                d.claims += yd.claims || 0;
                d.deductible += yd.deductible || 0;
                d.coinsurance += yd.coinsurance || 0;
                d.copay += yd.copay || 0;
                d.noncovered += yd.noncovered || 0;
                d.other_pr += yd.other_pr || 0;
                d.patient_pmt += yd.patient_pmt || 0;
                d.secondary_pmt += yd.secondary_pmt || 0;
                d.total_pr += yd.total_pr || 0;
            }});
            d.total_collected = d.patient_pmt + d.secondary_pmt;
            d.rate = d.total_pr > 0 ? (d.total_collected / d.total_pr * 100) : 0;

            // Helper to generate year cells dynamically
            const genYrCells = (field, formatter) => YEARS.map(yr =>
                `<td class="text-right mono">${{formatter(yearData[String(yr)]?.[field] || 0)}}</td>`
            ).join('');
            const genYrCellsBold = (field, formatter) => YEARS.map(yr =>
                `<td class="text-right mono"><strong>${{formatter(yearData[String(yr)]?.[field] || 0)}}</strong></td>`
            ).join('');

            // PR Breakdown table with dynamic year columns
            document.getElementById('prBreakdownBody').innerHTML = `
                <tr><td>Claims</td>${{genYrCells('claims', fmtNum)}}<td class="text-right mono"><strong>${{fmtNum(d.claims)}}</strong></td></tr>
                <tr><td>Deductible (PR-1)</td>${{genYrCells('deductible', fmt)}}<td class="text-right mono"><strong>${{fmt(d.deductible)}}</strong></td></tr>
                <tr><td>Coinsurance (PR-2)</td>${{genYrCells('coinsurance', fmt)}}<td class="text-right mono"><strong>${{fmt(d.coinsurance)}}</strong></td></tr>
                <tr><td>Copay (PR-3)</td>${{genYrCells('copay', fmt)}}<td class="text-right mono"><strong>${{fmt(d.copay)}}</strong></td></tr>
                <tr><td>Non-Covered</td>${{genYrCells('noncovered', fmt)}}<td class="text-right mono"><strong>${{fmt(d.noncovered)}}</strong></td></tr>
                <tr><td>Other PR</td>${{genYrCells('other_pr', fmt)}}<td class="text-right mono"><strong>${{fmt(d.other_pr)}}</strong></td></tr>
                <tr class="total-row"><td><strong>TOTAL PR</strong></td>${{genYrCellsBold('total_pr', fmt)}}<td class="text-right mono"><strong>${{fmt(d.total_pr)}}</strong></td></tr>
            `;

            // Collection table - compute collected/rate per year
            const yrColl = {{}};
            const yrRate = {{}};
            YEARS.forEach(yr => {{
                const yrStr = String(yr);
                const yd = yearData[yrStr];
                yrColl[yrStr] = (yd.patient_pmt || 0) + (yd.secondary_pmt || 0);
                yrRate[yrStr] = yd.total_pr > 0 ? (yrColl[yrStr] / yd.total_pr * 100) : 0;
            }});
            const patPmt = d.patient_pmt || 0;
            const secPmt = d.secondary_pmt || 0;
            const totalColl = patPmt + secPmt;
            const uncoll = (d.total_pr || 0) - totalColl;
            const rate = d.total_pr > 0 ? (totalColl / d.total_pr * 100) : 0;

            // Generate collection rows with dynamic year columns
            const genCollYrCells = (field) => YEARS.map(yr =>
                `<td class="text-right mono positive">${{fmt(yearData[String(yr)]?.[field] || 0)}}</td>`
            ).join('');
            const genCollYrCellsBold = () => YEARS.map(yr =>
                `<td class="text-right mono positive"><strong>${{fmt(yrColl[String(yr)])}}</strong></td>`
            ).join('');
            const genUncollYrCells = () => YEARS.map(yr => {{
                const yd = yearData[String(yr)];
                return `<td class="text-right mono negative">${{fmt((yd.total_pr||0) - yrColl[String(yr)])}}</td>`;
            }}).join('');
            const genRateYrCells = () => YEARS.map(yr =>
                `<td class="text-right mono ${{rateClass(yrRate[String(yr)])}}"><strong>${{fmtPct(yrRate[String(yr)])}}</strong></td>`
            ).join('');

            document.getElementById('collectionBody').innerHTML = `
                <tr><td>Patient Payments</td>${{genCollYrCells('patient_pmt')}}<td class="text-right mono positive"><strong>${{fmt(patPmt)}}</strong></td></tr>
                <tr><td>Secondary Payments</td>${{genCollYrCells('secondary_pmt')}}<td class="text-right mono positive"><strong>${{fmt(secPmt)}}</strong></td></tr>
                <tr class="total-row"><td><strong>TOTAL COLLECTED</strong></td>${{genCollYrCellsBold()}}<td class="text-right mono positive"><strong>${{fmt(totalColl)}}</strong></td></tr>
                <tr><td>Uncollected</td>${{genUncollYrCells()}}<td class="text-right mono negative"><strong>${{fmt(uncoll)}}</strong></td></tr>
                <tr class="total-row"><td><strong>RATE</strong></td>${{genRateYrCells()}}<td class="text-right mono ${{rateClass(rate)}}"><strong>${{fmtPct(rate)}}</strong></td></tr>
            `;
        }}

        // Render Layer 2 (Deductible Deep Dive) - aggregate from companies when filtered
        // DYNAMICALLY handles ALL years in YEARS array
        function renderLayer2() {{
            const f = getFilters();
            const dd = DATA.deductible_deep_dive;
            const zeroData = {{ claims: 0, total_pr: 0, deductible: 0, coinsurance: 0, copay: 0, noncovered: 0, other_pr: 0, patient_pmt: 0, secondary_pmt: 0, loss: 0, rate: 0 }};

            // Helper to get source data based on filters
            const getSource = (c, yr) => {{
                if (f.payerType === 'medicare') return c.by_year_medicare?.[yr] || {{}};
                if (f.payerType === 'other') return c.by_year_other?.[yr] || {{}};
                return c.by_year?.[yr] || {{}};
            }};

            // Build yearData object for ALL years dynamically
            const yearData = {{}};
            let companies = DATA.companies || [];
            if (f.company !== 'all') {{
                companies = companies.filter(c => c.id === f.company);
            }}

            // Aggregate data for each year
            YEARS.forEach(yr => {{
                const yrStr = String(yr);
                if (f.year !== 'all' && f.year !== yrStr) {{
                    yearData[yrStr] = zeroData;
                }} else if (f.payerType !== 'all' || f.company !== 'all') {{
                    // Aggregate from companies
                    let t = {{ claims: 0, total_pr: 0, deductible: 0, coinsurance: 0, copay: 0, noncovered: 0, other_pr: 0, patient_pmt: 0, secondary_pmt: 0, loss: 0 }};
                    companies.forEach(c => {{
                        const src = getSource(c, yrStr);
                        t.claims += src.claims || 0;
                        t.total_pr += src.total_pr || 0;
                        t.deductible += src.deductible || 0;
                        t.coinsurance += src.coinsurance || 0;
                        t.copay += src.copay || 0;
                        t.noncovered += src.noncovered || 0;
                        t.other_pr += src.other_pr || 0;
                        t.patient_pmt += src.patient_collected || 0;
                        t.secondary_pmt += src.secondary_collected || 0;
                        t.loss += src.loss || 0;
                    }});
                    const coll = t.patient_pmt + t.secondary_pmt;
                    t.rate = t.total_pr > 0 ? (coll / t.total_pr * 100) : 0;
                    yearData[yrStr] = t;
                }} else {{
                    yearData[yrStr] = dd.by_year?.[yrStr] || zeroData;
                }}
            }});

            // Calculate totals from all displayed years
            let d = {{
                claims: 0, deductible: 0, coinsurance: 0, copay: 0, noncovered: 0, other_pr: 0,
                patient_pmt: 0, secondary_pmt: 0, total_pr: 0, loss: 0
            }};
            YEARS.forEach(yr => {{
                const yd = yearData[String(yr)];
                d.claims += yd.claims || 0;
                d.deductible += yd.deductible || 0;
                d.coinsurance += yd.coinsurance || 0;
                d.copay += yd.copay || 0;
                d.noncovered += yd.noncovered || 0;
                d.other_pr += yd.other_pr || 0;
                d.patient_pmt += yd.patient_pmt || 0;
                d.secondary_pmt += yd.secondary_pmt || 0;
                d.total_pr += yd.total_pr || 0;
                d.loss += yd.loss || 0;
            }});
            d.total_collected = d.patient_pmt + d.secondary_pmt;
            d.uncollected = d.total_pr - d.total_collected;
            d.rate = d.total_pr > 0 ? (d.total_collected / d.total_pr * 100) : 0;

            // Helper to generate year cells dynamically
            const genYrCells = (field, formatter) => YEARS.map(yr =>
                `<td class="text-right mono">${{formatter(yearData[String(yr)]?.[field] || 0)}}</td>`
            ).join('');
            const genYrCellsBold = (field, formatter) => YEARS.map(yr =>
                `<td class="text-right mono"><strong>${{formatter(yearData[String(yr)]?.[field] || 0)}}</strong></td>`
            ).join('');

            // PR Table with dynamic year columns
            document.getElementById('dedPRBody').innerHTML = `
                <tr><td>Claims</td>${{genYrCells('claims', fmtNum)}}<td class="text-right mono"><strong>${{fmtNum(d.claims)}}</strong></td></tr>
                <tr><td>Deductible (PR-1)</td>${{genYrCells('deductible', fmt)}}<td class="text-right mono"><strong>${{fmt(d.deductible)}}</strong></td></tr>
                <tr><td>Coinsurance (PR-2)</td>${{genYrCells('coinsurance', fmt)}}<td class="text-right mono"><strong>${{fmt(d.coinsurance)}}</strong></td></tr>
                <tr><td>Copay (PR-3)</td>${{genYrCells('copay', fmt)}}<td class="text-right mono"><strong>${{fmt(d.copay)}}</strong></td></tr>
                <tr><td>Non-Covered</td>${{genYrCells('noncovered', fmt)}}<td class="text-right mono"><strong>${{fmt(d.noncovered)}}</strong></td></tr>
                <tr><td>Other PR</td>${{genYrCells('other_pr', fmt)}}<td class="text-right mono"><strong>${{fmt(d.other_pr)}}</strong></td></tr>
                <tr class="total-row"><td><strong>TOTAL PR</strong></td>${{genYrCellsBold('total_pr', fmt)}}<td class="text-right mono"><strong>${{fmt(d.total_pr)}}</strong></td></tr>
            `;

            // Collection table - compute collected/rate per year
            const yrColl = {{}};
            const yrRate = {{}};
            YEARS.forEach(yr => {{
                const yrStr = String(yr);
                const yd = yearData[yrStr];
                yrColl[yrStr] = (yd.patient_pmt || 0) + (yd.secondary_pmt || 0);
                yrRate[yrStr] = yd.total_pr > 0 ? (yrColl[yrStr] / yd.total_pr * 100) : 0;
            }});

            // Generate collection rows with dynamic year columns
            const genCollYrCells = (field) => YEARS.map(yr =>
                `<td class="text-right mono positive">${{fmt(yearData[String(yr)]?.[field] || 0)}}</td>`
            ).join('');
            const genCollYrCellsBold = () => YEARS.map(yr =>
                `<td class="text-right mono positive"><strong>${{fmt(yrColl[String(yr)])}}</strong></td>`
            ).join('');
            const genUncollYrCells = () => YEARS.map(yr => {{
                const yd = yearData[String(yr)];
                return `<td class="text-right mono negative">${{fmt((yd.total_pr||0) - yrColl[String(yr)])}}</td>`;
            }}).join('');
            const genRateYrCells = () => YEARS.map(yr =>
                `<td class="text-right mono ${{rateClass(yrRate[String(yr)])}}"><strong>${{fmtPct(yrRate[String(yr)])}}</strong></td>`
            ).join('');

            document.getElementById('dedCollBody').innerHTML = `
                <tr><td>Patient Payments</td>${{genCollYrCells('patient_pmt')}}<td class="text-right mono positive"><strong>${{fmt(d.patient_pmt)}}</strong></td></tr>
                <tr><td>Secondary Payments</td>${{genCollYrCells('secondary_pmt')}}<td class="text-right mono positive"><strong>${{fmt(d.secondary_pmt)}}</strong></td></tr>
                <tr class="total-row"><td><strong>TOTAL COLLECTED</strong></td>${{genCollYrCellsBold()}}<td class="text-right mono positive"><strong>${{fmt(d.total_collected)}}</strong></td></tr>
                <tr><td>Uncollected</td>${{genUncollYrCells()}}<td class="text-right mono negative"><strong>${{fmt(d.uncollected)}}</strong></td></tr>
                <tr class="total-row"><td><strong>RATE</strong></td>${{genRateYrCells()}}<td class="text-right mono ${{rateClass(d.rate)}}"><strong>${{fmtPct(d.rate)}}</strong></td></tr>
            `;
        }}

        // Render PR Adjustments
        function renderPRAdjustments() {{
            const f = getFilters();
            let rows = DATA.pr_adjustments || [];

            // Transform to include all sortable fields with filter support
            let transformed = rows.map(r => {{
                let src = r;

                // Apply company filter
                if (f.company !== 'all' && r.by_company?.[f.company]) {{
                    src = {{ ...r, ...r.by_company[f.company] }};
                }}

                // Apply payer type filter
                if (f.payerType === 'medicare') {{
                    const med = f.company !== 'all' ? (r.by_company?.[f.company]?.medicare || r.medicare) : r.medicare;
                    src = {{
                        ...r,
                        total_amount: med?.amount || 0,
                        total_claims: med?.claims || 0,
                        by_year: med?.by_year || {{}}
                    }};
                }} else if (f.payerType === 'other') {{
                    const oth = f.company !== 'all' ? (r.by_company?.[f.company]?.other || r.other) : r.other;
                    src = {{
                        ...r,
                        total_amount: oth?.amount || 0,
                        total_claims: oth?.claims || 0,
                        by_year: oth?.by_year || {{}}
                    }};
                }}

                // Get year data (use filtered source if company/payer type applied)
                let y24, y25;
                if (f.company !== 'all' && f.payerType === 'all') {{
                    y24 = r.by_company?.[f.company]?.by_year?.[YR1] || {{}};
                    y25 = r.by_company?.[f.company]?.by_year?.[YR2] || {{}};
                }} else {{
                    y24 = src.by_year?.[YR1] || {{}};
                    y25 = src.by_year?.[YR2] || {{}};
                }}

                return {{
                    code: r.code,
                    description: r.description || '',
                    claims_2024: y24.claims || 0,
                    amount_2024: y24.amount || 0,
                    claims_2025: y25.claims || 0,
                    amount_2025: y25.amount || 0,
                    total_claims: src.total_claims || 0,
                    total_amount: src.total_amount || 0
                }};
            }}).filter(r => r.total_amount !== 0 || r.total_claims > 0);

            // Apply dynamic sort
            transformed.sort((a, b) => {{
                let aVal = a[prAdjSortField];
                let bVal = b[prAdjSortField];
                if (typeof aVal === 'string') {{
                    return prAdjSortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }}
                return prAdjSortAsc ? (aVal - bVal) : (bVal - aVal);
            }});

            let totals = {{ c24: 0, a24: 0, c25: 0, a25: 0, cTot: 0, aTot: 0 }};
            let html = '';

            transformed.forEach(r => {{
                totals.c24 += r.claims_2024;
                totals.a24 += r.amount_2024;
                totals.c25 += r.claims_2025;
                totals.a25 += r.amount_2025;
                totals.cTot += r.total_claims;
                totals.aTot += r.total_amount;

                html += `<tr>
                    <td class="mono">${{r.code}}</td>
                    <td style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${{r.description}}">${{r.description?.substring(0,35) || ''}}</td>
                    <td class="text-right mono">${{fmtNum(r.claims_2024)}}</td>
                    <td class="text-right mono">${{fmt(r.amount_2024)}}</td>
                    <td class="text-right mono">${{fmtNum(r.claims_2025)}}</td>
                    <td class="text-right mono">${{fmt(r.amount_2025)}}</td>
                    <td class="text-right mono"><strong>${{fmtNum(r.total_claims)}}</strong></td>
                    <td class="text-right mono"><strong>${{fmt(r.total_amount)}}</strong></td>
                </tr>`;
            }});

            html += `<tr class="total-row">
                <td colspan="2"><strong>TOTAL</strong></td>
                <td class="text-right mono"><strong>${{fmtNum(totals.c24)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.a24)}}</strong></td>
                <td class="text-right mono"><strong>${{fmtNum(totals.c25)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.a25)}}</strong></td>
                <td class="text-right mono"><strong>${{fmtNum(totals.cTot)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.aTot)}}</strong></td>
            </tr>`;

            document.getElementById('prAdjBody').innerHTML = html;
        }}

        // Render Top Patients (with payer type filter)
        function renderTopPatients() {{
            const f = getFilters();
            let patients = DATA.top_patients || [];

            // Filter and transform based on year and payer type
            let filtered = patients.map(p => {{
                let d;
                if (f.payerType === 'medicare') {{
                    d = f.year !== 'all' ? (p.by_year_medicare?.[f.year] || {{}}) : p.medicare;
                }} else if (f.payerType === 'other') {{
                    d = f.year !== 'all' ? (p.by_year_other?.[f.year] || {{}}) : p.other;
                }} else {{
                    d = f.year !== 'all' ? (p.by_year?.[f.year] || {{}}) : p;
                }}
                const deductible = d.deductible || 0;
                const total_pr = d.total_pr || 0;
                const collected = d.collected || 0;
                const secondary = d.secondary || 0;
                const totalColl = collected + secondary;
                const uncollected = total_pr - totalColl;
                return {{
                    name: p.name,
                    member_id: p.member_id_masked || p.member_id,
                    claims: d.claims || 0,
                    deductible: deductible,
                    total_pr: total_pr,
                    collected: collected,
                    secondary: secondary,
                    uncollected: uncollected
                }};
            }}).filter(p => p.deductible > 0);

            // Sort based on current sort state
            filtered.sort((a, b) => {{
                let aVal = a[patientSortField];
                let bVal = b[patientSortField];
                // Handle string comparison for name and member_id
                if (typeof aVal === 'string') {{
                    aVal = aVal.toLowerCase();
                    bVal = (bVal || '').toLowerCase();
                    return patientSortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }}
                // Numeric comparison
                return patientSortAsc ? (aVal - bVal) : (bVal - aVal);
            }});

            let totals = {{ claims: 0, deductible: 0, total_pr: 0, collected: 0, secondary: 0, uncollected: 0 }};
            let html = '';

            filtered.forEach(p => {{
                totals.claims += p.claims;
                totals.deductible += p.deductible;
                totals.total_pr += p.total_pr;
                totals.collected += p.collected;
                totals.secondary += p.secondary;
                totals.uncollected += p.uncollected;

                html += `<tr>
                    <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${{p.name}}">${{p.name?.substring(0,25) || 'Unknown'}}</td>
                    <td class="mono">${{p.member_id}}</td>
                    <td class="text-right mono">${{fmtNum(p.claims)}}</td>
                    <td class="text-right mono">${{fmt(p.deductible)}}</td>
                    <td class="text-right mono">${{fmt(p.total_pr)}}</td>
                    <td class="text-right mono positive">${{fmt(p.collected)}}</td>
                    <td class="text-right mono positive">${{fmt(p.secondary)}}</td>
                    <td class="text-right mono negative">${{fmt(p.uncollected)}}</td>
                </tr>`;
            }});

            html += `<tr class="total-row">
                <td colspan="2"><strong>TOTAL (Top 50)</strong></td>
                <td class="text-right mono"><strong>${{fmtNum(totals.claims)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.deductible)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.total_pr)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.collected)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.secondary)}}</strong></td>
                <td class="text-right mono negative"><strong>${{fmt(totals.uncollected)}}</strong></td>
            </tr>`;

            document.getElementById('topPatientsBody').innerHTML = html;
        }}

        // Render Company table
        function renderCompanies() {{
            const el = document.getElementById('companyBody');
            if (!el) return;

            const f = getFilters();
            let companies = DATA.companies || [];

            // Filter by selected company
            if (f.company !== 'all') {{
                companies = companies.filter(c => c.id === f.company);
            }}

            // Build sortable row data
            let rows = [];
            companies.forEach(c => {{
                let d = c.all;
                if (f.year !== 'all') {{
                    d = c.by_year?.[f.year] || {{}};
                }}
                if (f.payerType === 'medicare') {{
                    d = f.year !== 'all' ? (c.by_year_medicare?.[f.year] || {{}}) : c.medicare;
                }} else if (f.payerType === 'other') {{
                    d = f.year !== 'all' ? (c.by_year_other?.[f.year] || {{}}) : c.other;
                }}

                const pr = d.total_pr || 0;
                const pat = d.patient_collected || 0;
                const sec = d.secondary_collected || 0;
                const coll = pat + sec;
                const uncoll = pr - coll;
                const rate = pr > 0 ? coll / pr * 100 : 0;
                const loss = d.loss || 0;
                const claims = d.claims || 0;

                if (pr > 0 || claims > 0) {{
                    rows.push({{
                        name: c.name,
                        claims: claims,
                        pr: pr,
                        pat: pat,
                        sec: sec,
                        coll: coll,
                        uncoll: uncoll,
                        rate: rate,
                        loss: loss
                    }});
                }}
            }});

            // Sort rows
            rows.sort((a, b) => {{
                let aVal = a[companySortField];
                let bVal = b[companySortField];
                if (typeof aVal === 'string') {{
                    return companySortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }}
                return companySortAsc ? aVal - bVal : bVal - aVal;
            }});

            // Calculate totals and render
            let totals = {{ claims: 0, pr: 0, pat: 0, sec: 0, coll: 0, uncoll: 0, loss: 0 }};
            let html = '';

            rows.forEach(r => {{
                totals.claims += r.claims;
                totals.pr += r.pr;
                totals.pat += r.pat;
                totals.sec += r.sec;
                totals.coll += r.coll;
                totals.uncoll += r.uncoll;
                totals.loss += r.loss;

                html += `<tr>
                    <td>${{r.name}}</td>
                    <td class="text-right mono">${{fmtNum(r.claims)}}</td>
                    <td class="text-right mono">${{fmt(r.pr)}}</td>
                    <td class="text-right mono positive">${{fmt(r.pat)}}</td>
                    <td class="text-right mono positive">${{fmt(r.sec)}}</td>
                    <td class="text-right mono">${{fmt(r.coll)}}</td>
                    <td class="text-right mono negative">${{fmt(r.uncoll)}}</td>
                    <td class="text-right mono ${{rateClass(r.rate)}}">${{fmtPct(r.rate)}}</td>
                    <td class="text-right mono warning">${{fmt(r.loss)}}</td>
                </tr>`;
            }});

            const totalRate = totals.pr > 0 ? totals.coll / totals.pr * 100 : 0;
            html += `<tr class="total-row">
                <td><strong>TOTAL</strong></td>
                <td class="text-right mono"><strong>${{fmtNum(totals.claims)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.pr)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.pat)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.sec)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.coll)}}</strong></td>
                <td class="text-right mono negative"><strong>${{fmt(totals.uncoll)}}</strong></td>
                <td class="text-right mono ${{rateClass(totalRate)}}"><strong>${{fmtPct(totalRate)}}</strong></td>
                <td class="text-right mono warning"><strong>${{fmt(totals.loss)}}</strong></td>
            </tr>`;

            el.innerHTML = html;
        }}

        // Render Payer table
        function renderPayers() {{
            const f = getFilters();
            let payers = DATA.payers || [];

            // Filter by payer type
            if (f.payerType === 'medicare') {{
                payers = payers.filter(p => p.is_medicare);
            }} else if (f.payerType === 'other') {{
                payers = payers.filter(p => !p.is_medicare);
            }}

            // Filter by company and use company-specific data
            if (f.company !== 'all') {{
                payers = payers.filter(p => p.by_company && p.by_company[f.company]);
                // Replace aggregate data with company-specific data
                payers = payers.map(p => {{
                    const cd = p.by_company[f.company] || {{}};
                    // If year filter also applied, use company+year specific data
                    if (f.year !== 'all' && cd.by_year && cd.by_year[f.year]) {{
                        const cyd = cd.by_year[f.year];
                        return {{
                            ...p,
                            total_pr: cyd.total_pr || 0,
                            patient_pmt: cyd.patient_pmt || 0,
                            secondary_pmt: cyd.secondary_pmt || 0,
                            loss: cyd.loss || 0,
                            claims: cyd.claims || 0
                        }};
                    }}
                    return {{
                        ...p,
                        total_pr: cd.total_pr || 0,
                        patient_pmt: cd.patient_pmt || 0,
                        secondary_pmt: cd.secondary_pmt || 0,
                        loss: cd.loss || 0,
                        claims: cd.claims || 0
                    }};
                }}).filter(p => p.total_pr > 0);
            }}

            // Apply year filter (when no company filter)
            if (f.year !== 'all' && f.company === 'all') {{
                payers = payers.map(p => {{
                    const yd = p.by_year?.[f.year] || {{}};
                    return {{
                        ...p,
                        total_pr: yd.total_pr || 0,
                        patient_pmt: yd.patient_pmt || 0,
                        secondary_pmt: yd.secondary_pmt || 0,
                        loss: yd.loss || 0,
                        claims: yd.claims || 0
                    }};
                }}).filter(p => p.total_pr > 0);
            }}

            // Transform to include all sortable fields
            let transformed = payers.map(p => {{
                const pr = p.total_pr || 0;
                const pat = p.patient_pmt || 0;
                const sec = p.secondary_pmt || 0;
                const coll = pat + sec;
                const uncoll = pr - coll;
                const rate = pr > 0 ? coll / pr * 100 : 0;
                return {{
                    payer: p.name || '',
                    is_medicare: p.is_medicare,
                    claims: p.claims || 0,
                    pr, pat, sec, coll, uncoll, rate,
                    loss: p.loss || 0
                }};
            }});

            // Apply dynamic sort
            transformed.sort((a, b) => {{
                let aVal = a[payerSortField];
                let bVal = b[payerSortField];
                if (typeof aVal === 'string') {{
                    return payerSortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }}
                return payerSortAsc ? (aVal - bVal) : (bVal - aVal);
            }});

            // Show all payers (scrollable container with sticky headers)
            let totals = {{ claims: 0, pr: 0, pat: 0, sec: 0, coll: 0, uncoll: 0, loss: 0 }};
            let html = '';

            transformed.forEach(p => {{
                totals.claims += p.claims;
                totals.pr += p.pr;
                totals.pat += p.pat;
                totals.sec += p.sec;
                totals.coll += p.coll;
                totals.uncoll += p.uncoll;
                totals.loss += p.loss;

                html += `<tr>
                    <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${{p.payer}}">${{p.payer?.substring(0,30) || ''}}${{p.is_medicare ? ' ⚕️' : ''}}</td>
                    <td class="text-right mono">${{fmtNum(p.claims)}}</td>
                    <td class="text-right mono">${{fmt(p.pr)}}</td>
                    <td class="text-right mono positive">${{fmt(p.pat)}}</td>
                    <td class="text-right mono positive">${{fmt(p.sec)}}</td>
                    <td class="text-right mono">${{fmt(p.coll)}}</td>
                    <td class="text-right mono negative">${{fmt(p.uncoll)}}</td>
                    <td class="text-right mono ${{rateClass(p.rate)}}">${{fmtPct(p.rate)}}</td>
                    <td class="text-right mono warning">${{fmt(p.loss)}}</td>
                </tr>`;
            }});

            const totalRate = totals.pr > 0 ? totals.coll / totals.pr * 100 : 0;
            html += `<tr class="total-row">
                <td><strong>TOTAL (${{transformed.length}} payers)</strong></td>
                <td class="text-right mono"><strong>${{fmtNum(totals.claims)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.pr)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.pat)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.sec)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.coll)}}</strong></td>
                <td class="text-right mono negative"><strong>${{fmt(totals.uncoll)}}</strong></td>
                <td class="text-right mono ${{rateClass(totalRate)}}"><strong>${{fmtPct(totalRate)}}</strong></td>
                <td class="text-right mono warning"><strong>${{fmt(totals.loss)}}</strong></td>
            </tr>`;

            document.getElementById('payerBody').innerHTML = html;
        }}

        // Render Monthly table
        function renderMonthly() {{
            const f = getFilters();
            let monthly = DATA.monthly || [];

            // Filter by year
            if (f.year !== 'all') {{
                monthly = monthly.filter(m => m.year === parseInt(f.year));
            }}

            // Filter to years in data (dynamic)
            const minYear = Math.min(...(DATA.available_years || [2024])); monthly = monthly.filter(m => m.year >= minYear);

            // Transform to include all sortable fields
            let transformed = monthly.map(m => {{
                let d = {{}};
                let src = m;
                if (f.company !== 'all' && m.by_company?.[f.company]) {{
                    src = {{ medicare: m.by_company[f.company].medicare || {{}}, other: m.by_company[f.company].other || {{}} }};
                }}

                if (f.payerType === 'medicare') {{
                    d = src.medicare || {{}};
                }} else if (f.payerType === 'other') {{
                    d = src.other || {{}};
                }} else {{
                    d = {{
                        total_pr: (src.medicare?.total_pr || 0) + (src.other?.total_pr || 0),
                        patient_collected: (src.medicare?.patient_collected || 0) + (src.other?.patient_collected || 0),
                        secondary_collected: (src.medicare?.secondary_collected || 0) + (src.other?.secondary_collected || 0),
                        claims: (src.medicare?.claims || 0) + (src.other?.claims || 0),
                        loss: (src.medicare?.loss || 0) + (src.other?.loss || 0)
                    }};
                }}

                const pr = d.total_pr || 0;
                const pat = d.patient_collected || 0;
                const sec = d.secondary_collected || 0;
                const coll = pat + sec;
                const uncoll = pr - coll;
                const rate = pr > 0 ? coll / pr * 100 : 0;
                return {{
                    label: m.label,
                    sortKey: m.year * 12 + m.month,  // Chronological sort key
                    claims: d.claims || 0,
                    pr, pat, sec, coll, uncoll, rate,
                    loss: d.loss || 0
                }};
            }}).filter(m => m.pr > 0 || m.claims > 0);

            // Apply dynamic sort
            transformed.sort((a, b) => {{
                let aVal = a[monthlySortField];
                let bVal = b[monthlySortField];
                // Use sortKey for chronological ordering when sorting by label
                if (monthlySortField === 'label') {{
                    return monthlySortAsc ? (a.sortKey - b.sortKey) : (b.sortKey - a.sortKey);
                }}
                if (typeof aVal === 'string') {{
                    return monthlySortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }}
                return monthlySortAsc ? (aVal - bVal) : (bVal - aVal);
            }});

            let totals = {{ claims: 0, pr: 0, pat: 0, sec: 0, coll: 0, uncoll: 0, loss: 0 }};
            let html = '';

            transformed.forEach(m => {{
                totals.claims += m.claims;
                totals.pr += m.pr;
                totals.pat += m.pat;
                totals.sec += m.sec;
                totals.coll += m.coll;
                totals.uncoll += m.uncoll;
                totals.loss += m.loss;

                html += `<tr>
                    <td class="mono">${{m.label}}</td>
                    <td class="text-right mono">${{fmtNum(m.claims)}}</td>
                    <td class="text-right mono">${{fmt(m.pr)}}</td>
                    <td class="text-right mono positive">${{fmt(m.pat)}}</td>
                    <td class="text-right mono positive">${{fmt(m.sec)}}</td>
                    <td class="text-right mono">${{fmt(m.coll)}}</td>
                    <td class="text-right mono negative">${{fmt(m.uncoll)}}</td>
                    <td class="text-right mono ${{rateClass(m.rate)}}">${{fmtPct(m.rate)}}</td>
                    <td class="text-right mono warning">${{fmt(m.loss)}}</td>
                </tr>`;
            }});

            const totalRate = totals.pr > 0 ? totals.coll / totals.pr * 100 : 0;
            html += `<tr class="total-row">
                <td><strong>TOTAL</strong></td>
                <td class="text-right mono"><strong>${{fmtNum(totals.claims)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.pr)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.pat)}}</strong></td>
                <td class="text-right mono positive"><strong>${{fmt(totals.sec)}}</strong></td>
                <td class="text-right mono"><strong>${{fmt(totals.coll)}}</strong></td>
                <td class="text-right mono negative"><strong>${{fmt(totals.uncoll)}}</strong></td>
                <td class="text-right mono ${{rateClass(totalRate)}}"><strong>${{fmtPct(totalRate)}}</strong></td>
                <td class="text-right mono warning"><strong>${{fmt(totals.loss)}}</strong></td>
            </tr>`;

            document.getElementById('monthlyBody').innerHTML = html;
        }}


        // Update table headers with dynamic years from data
        function updateYearHeaders() {{
            const years = DATA.available_years || [2024, 2025];
            const [yr1, yr2] = years.length >= 2 ? [years[0], years[1]] : [years[0] || 2024, years[1] || 2025];
            // Update all year column headers
            document.querySelectorAll('th').forEach(th => {{
                if (th.textContent.trim() === '2024') th.textContent = yr1;
                if (th.textContent.trim() === '2025') th.textContent = yr2;
                if (th.textContent.includes('2024 Claims')) th.innerHTML = th.innerHTML.replace('2024', yr1);
                if (th.textContent.includes('2024 Amount')) th.innerHTML = th.innerHTML.replace('2024', yr1);
                if (th.textContent.includes('2025 Claims')) th.innerHTML = th.innerHTML.replace('2025', yr2);
                if (th.textContent.includes('2025 Amount')) th.innerHTML = th.innerHTML.replace('2025', yr2);
            }});
        }}

        // Render all
        function renderAll() {{
            document.body.classList.add('loading');
            // Use requestAnimationFrame to allow the loading indicator to render
            requestAnimationFrame(() => {{
                renderLayer1();
                renderLayer2();
                renderTopPatients();
                renderPRAdjustments();
                renderCompanies();
                renderPayers();
                renderMonthly();
                document.body.classList.remove('loading');
            }});
        }}

        // Initialize on load
        document.addEventListener('DOMContentLoaded', () => {{ updateYearHeaders(); renderAll(); }});
    </script>
</body>
</html>"""

    # Write the HTML file
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    return str(output_path)


def generate_all_dashboards(all_company_data, output_dir):
    """Generate aggregate dashboard and individual company dashboards."""
    from pathlib import Path

    output_dir = Path(output_dir)

    # Generate aggregate dashboard
    aggregate_path = output_dir / "Deductible_Dashboard_ALL_COMPANIES.html"
    try:
        generate_interactive_dashboard(all_company_data, aggregate_path)
        if aggregate_path.exists():
            print(f"  {aggregate_path.name}")
        else:
            print(f"  WARNING: {aggregate_path.name} was not created!")
    except Exception as e:
        print(f"  ERROR generating {aggregate_path.name}: {e}")

    # Generate individual company dashboards
    for company_id, data in sorted(all_company_data.items()):
        company_name_safe = data.company_name.replace(" ", "_").replace(",", "").replace("/", "_")[:25]
        company_path = output_dir / f"Deductible_Dashboard_{company_id}_{company_name_safe}.html"
        generate_interactive_dashboard(all_company_data, company_path, single_company_id=company_id)
        print(f"  {company_path.name}")


def generate_from_database(trips_path: str, output_dir: str, db_path: Optional[str] = None) -> str:
    """
    Generate deductible reports from database.

    This is the entry point called by the GUI button. It produces reports
    IDENTICAL to the CSV-based main() function by using the exact same
    processing logic.

    Args:
        trips_path: Path to Fair Health ZIP CSV file (contains RUN and PT PAID columns)
        output_dir: Output directory for reports
        db_path: Optional database path (None = default AppData location)

    Returns:
        Path to generated reports directory
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    # Create timestamped debug log file that captures ALL print output
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    debug_log_path = output_dir / f"Deductible_Report_Log_{run_timestamp}.txt"
    debug_log = open(debug_log_path, "w", encoding="utf-8")

    # Redirect stdout to capture all print output (including from sub-functions)
    import sys

    class TeeOutput:
        """Write to both original stdout and log file."""

        def __init__(self, original, log_file):
            self.original = original
            self.log_file = log_file

        def write(self, text):
            self.original.write(text)
            self.log_file.write(text)
            self.log_file.flush()

        def flush(self):
            self.original.flush()
            self.log_file.flush()

    original_stdout = sys.stdout
    sys.stdout = TeeOutput(original_stdout, debug_log)

    print("=" * 70)
    print("DEDUCTIBLE COLLECTION REPORT GENERATOR (Database Mode)")
    print("=" * 70)
    print()

    # Load Patient Payments from Fair Health ZIP CSV (if provided)
    if trips_path and Path(trips_path).exists():
        run_payments = load_trip_credits(trips_path)
    else:
        print("No Fair Health ZIP file provided - patient payments will show as $0")
        run_payments = defaultdict(lambda: {"total": 0.0, "payment_count": 0})
    print()

    # Load data from database (wrapped in FlexibleRow for column name compatibility)
    rows = load_data_from_database(db_path)
    print()

    if not rows:
        print("ERROR: No data found in database.")
        return str(output_dir)

    # First pass: collect data by company
    all_company_data = {}

    print("Pass 1: Collecting data by company with collection matching...")

    row_count = 0
    for row in rows:
        row_count += 1
        if row_count % 50000 == 0:
            print(f"  Processed {row_count:,} rows...")

        filename = row.get("Filename_File", "")
        parts = filename.split(".")
        if len(parts) < 2:
            continue

        company_id = parts[1]
        company_name = row.get("COMPANY", "") or "UNKNOWN"

        if company_id not in all_company_data:
            all_company_data[company_id] = CompanyData(company_id, company_name)

        data = all_company_data[company_id]
        data.total_records += 1
        data.rows.append(row)

        # Track unique claims
        claim_num = row.get("CLM_PatientControlNumber_L2100_CLP", "") or row.get("CLAIM NUMBER", "")
        if claim_num:
            data.all_claims.add(claim_num)

        calc_ded = parse_currency(row.get("CALCULATED DEDUCTIBLE", 0))
        data.calculated_deductible_total += calc_ded

        # Capture all PR components for total patient responsibility
        calc_coins = parse_currency(row.get("CALCULATED COINSURANCE", 0))
        calc_copay = parse_currency(row.get("CALCULATED COPAY", 0))
        calc_noncov = parse_currency(row.get("CALCULATED PATIENT NON COVERED", 0))
        calc_other = parse_currency(row.get("CALCULATED PATIENT OTHER", 0))

        data.total_coinsurance += calc_coins
        data.total_copay += calc_copay
        data.total_noncovered += calc_noncov
        data.total_other_pr += calc_other

        # Total patient responsibility for this service line
        total_pr = calc_ded + calc_coins + calc_copay + calc_noncov + calc_other

        run_num = row.get("RUN", "")
        collected = run_payments.get(run_num, {}).get("total", 0.0)

        # Track claims with any patient responsibility
        if total_pr > 0:
            if claim_num:
                data.claims_with_pr_set.add(claim_num)
            data.patient_collected_for_pr += collected

        run_collection_not_counted = calc_ded != 0 and run_num and run_num not in data.ded_runs_collected

        if calc_ded != 0:
            data.claims_with_deductible += 1
            if claim_num:
                data.claims_with_ded_set.add(claim_num)

            if run_collection_not_counted:
                data.patient_collected_total += collected
                data.ded_runs_collected.add(run_num)
                if collected > 0:
                    data.claims_with_collection += 1

            # Track PR specifically from deductible lines (will NET correctly with reversals)
            data.ded_lines_total_pr += total_pr
            data.ded_lines_other_pr += total_pr - calc_ded
            data.ded_lines_coinsurance += calc_coins
            data.ded_lines_copay += calc_copay
            data.ded_lines_noncovered += calc_noncov
            data.ded_lines_other += calc_other

        payer_name = row.get("PAYOR PAID", "") or row.get("Effective_PayerName", "") or "UNKNOWN"
        payer_name = payer_name.strip().upper() if payer_name else "UNKNOWN"
        is_medicare = is_medicare_part_b(payer_name)  # Determine early for PR adjustments

        service_date_str = row.get("SERVICE DATE", "") or row.get("DATE OF SERVICE", "")
        service_date = parse_date(service_date_str)

        # === YEAR TRACKING ===
        year = None
        if service_date:
            year = service_date.year
            # Track DOS range per year
            if year not in data.dos_min_by_year or service_date < data.dos_min_by_year[year]:
                data.dos_min_by_year[year] = service_date
            if year not in data.dos_max_by_year or service_date > data.dos_max_by_year[year]:
                data.dos_max_by_year[year] = service_date

            # Layer 1: Track ALL claims by year
            if claim_num:
                data.all_claims_by_year[year].add(claim_num)
            data.total_pr_by_year[year] += calc_ded
            data.coinsurance_by_year[year] += calc_coins
            data.copay_by_year[year] += calc_copay
            data.noncovered_by_year[year] += calc_noncov
            data.other_pr_by_year[year] += calc_other

            # Layer 1: Track ALL claims by year AND payer type (Medicare vs Other)
            layer1_payer = data.layer1_medicare_by_year if is_medicare else data.layer1_other_by_year
            if claim_num:
                layer1_payer[year]["claims"].add(claim_num)
            layer1_payer[year]["deductible"] += calc_ded
            layer1_payer[year]["coinsurance"] += calc_coins
            layer1_payer[year]["copay"] += calc_copay
            layer1_payer[year]["noncovered"] += calc_noncov
            layer1_payer[year]["other_pr"] += calc_other

            # Track claims with PR by year
            if total_pr > 0:
                if claim_num:
                    data.claims_with_pr_by_year[year].add(claim_num)
                    layer1_payer[year]["pr_claims"].add(claim_num)
                # Match text report behavior: add collected for every row with PR
                data.patient_collected_for_pr_by_year[year] += collected
                layer1_payer[year]["patient_pmt"] += collected

        # Year-specific deductible tracking
        run_collection_not_counted_year = (
            calc_ded != 0 and run_num and year and run_num not in data.ded_runs_collected_by_year[year]
        )

        if calc_ded != 0 and year:
            if claim_num:
                data.claims_with_ded_by_year[year].add(claim_num)

            if run_collection_not_counted_year:
                data.patient_collected_by_year[year] += collected
                data.ded_runs_collected_by_year[year].add(run_num)

            # Layer 2: Track PR on deductible lines by year
            data.ded_lines_total_pr_by_year[year] += total_pr
            data.ded_lines_deductible_by_year[year] += calc_ded
            data.ded_lines_coinsurance_by_year[year] += calc_coins
            data.ded_lines_copay_by_year[year] += calc_copay
            data.ded_lines_noncovered_by_year[year] += calc_noncov
            data.ded_lines_other_by_year[year] += calc_other

        if calc_ded != 0:
            if run_num:
                data.payer_deductibles[payer_name]["runs"].add(run_num)
            if claim_num:
                data.payer_deductibles[payer_name]["claims"].add(claim_num)
            data.payer_deductibles[payer_name]["total"] += calc_ded
            data.payer_deductibles[payer_name]["total_pr"] += total_pr
            if run_collection_not_counted:
                data.payer_deductibles[payer_name]["collected"] += collected

            if service_date and year:
                month = service_date.month
                year_month = (year, month)  # Use tuple key for year-month tracking
                if is_medicare:
                    if run_num:
                        data.medicare_by_year_month[year_month]["runs"].add(run_num)
                    data.medicare_by_year_month[year_month]["amount"] += calc_ded
                    data.medicare_by_year_month[year_month]["total_pr"] += total_pr
                    if run_collection_not_counted:
                        data.medicare_by_year_month[year_month]["collected"] += collected
                    # === YEAR: Medicare by year ===
                    if run_num:
                        data.medicare_by_year[year]["runs"].add(run_num)
                    data.medicare_by_year[year]["deductible"] += calc_ded
                    data.medicare_by_year[year]["total_pr"] += total_pr
                    data.medicare_by_year[year]["coinsurance"] += calc_coins
                    data.medicare_by_year[year]["copay"] += calc_copay
                    data.medicare_by_year[year]["noncovered"] += calc_noncov
                    data.medicare_by_year[year]["other_pr"] += calc_other
                    if run_collection_not_counted_year:
                        data.medicare_by_year[year]["patient_pmt"] += collected
                else:
                    if run_num:
                        data.other_by_year_month[year_month]["runs"].add(run_num)
                    data.other_by_year_month[year_month]["amount"] += calc_ded
                    data.other_by_year_month[year_month]["total_pr"] += total_pr
                    if run_collection_not_counted:
                        data.other_by_year_month[year_month]["collected"] += collected
                    # === YEAR: Other by year ===
                    if run_num:
                        data.other_by_year[year]["runs"].add(run_num)
                    data.other_by_year[year]["deductible"] += calc_ded
                    data.other_by_year[year]["total_pr"] += total_pr
                    data.other_by_year[year]["coinsurance"] += calc_coins
                    data.other_by_year[year]["copay"] += calc_copay
                    data.other_by_year[year]["noncovered"] += calc_noncov
                    data.other_by_year[year]["other_pr"] += calc_other
                    if run_collection_not_counted_year:
                        data.other_by_year[year]["patient_pmt"] += collected

                # Store RUN -> month mapping for secondary allocation
                if run_num and run_num not in data.run_to_month:
                    data.run_to_month[run_num] = {"month": month, "is_medicare": is_medicare, "year": year}

                # Track RUN deductible info for LOSS calculation
                if run_num:
                    if run_num not in data.run_ded_info:
                        data.run_ded_info[run_num] = {
                            "deductible": 0,
                            "payer": payer_name,
                            "month": month,
                            "is_medicare": is_medicare,
                            "member_id": "",
                            "year": year,
                        }
                    data.run_ded_info[run_num]["deductible"] += calc_ded
                    # Track per-payer deductible within RUN for accurate LOSS attribution
                    data.run_payer_info[run_num][payer_name]["deductible"] += calc_ded
                    data.run_payer_info[run_num][payer_name]["month"] = month
                    data.run_payer_info[run_num][payer_name]["is_medicare"] = is_medicare
                    data.run_payer_info[run_num][payer_name]["year"] = year

            # === YEAR: Payer by year ===
            if year:
                if run_num:
                    data.payer_by_year[payer_name][year]["runs"].add(run_num)
                data.payer_by_year[payer_name][year]["deductible"] += calc_ded
                data.payer_by_year[payer_name][year]["total_pr"] += total_pr
                if run_collection_not_counted_year:
                    data.payer_by_year[payer_name][year]["patient_pmt"] += collected

            # Track patient deductibles
            member_id = str(row.get("MEMBER ID", "") or "").strip()
            patient_name = str(row.get("NAME", "") or "").strip()
            if member_id:
                if run_num:
                    data.patient_deductibles[member_id]["runs"].add(run_num)
                    # Store member_id in run_ded_info for LOSS by patient calculation
                    if run_num in data.run_ded_info:
                        data.run_ded_info[run_num]["member_id"] = member_id
                if not data.patient_deductibles[member_id]["name"]:
                    data.patient_deductibles[member_id]["name"] = patient_name
                data.patient_deductibles[member_id]["deductible"] += calc_ded
                data.patient_deductibles[member_id]["total_pr"] += total_pr
                if run_collection_not_counted:
                    data.patient_deductibles[member_id]["collected"] += collected

                # === YEAR: Patient by year ===
                if year:
                    if run_num:
                        data.patient_by_year[member_id][year]["runs"].add(run_num)
                    if not data.patient_by_year[member_id][year]["name"]:
                        data.patient_by_year[member_id][year]["name"] = patient_name
                    data.patient_by_year[member_id][year]["deductible"] += calc_ded
                    data.patient_by_year[member_id][year]["total_pr"] += total_pr
                    if run_collection_not_counted_year:
                        data.patient_by_year[member_id][year]["collected"] += collected

                    # === YEAR: Patient by year by payer type ===
                    if is_medicare:
                        payer_dict = data.patient_by_year_medicare
                    else:
                        payer_dict = data.patient_by_year_other
                    if run_num:
                        payer_dict[member_id][year]["runs"].add(run_num)
                    if not payer_dict[member_id][year]["name"]:
                        payer_dict[member_id][year]["name"] = patient_name
                    payer_dict[member_id][year]["deductible"] += calc_ded
                    payer_dict[member_id][year]["total_pr"] += total_pr
                    if run_collection_not_counted_year:
                        payer_dict[member_id][year]["collected"] += collected

            if is_medicare:
                data.medicare_amounts.append(calc_ded)
                # Track Medicare member deductibles
                if member_id:
                    data.medicare_members[member_id]["total"] += calc_ded
                    data.medicare_members[member_id]["total_pr"] += total_pr
                    data.medicare_members[member_id]["claims"] += 1
                    medicare_run_not_counted = run_num and run_num not in data.medicare_runs_collected
                    if medicare_run_not_counted:
                        data.medicare_members[member_id]["collected"] += collected
                        data.medicare_runs_collected.add(run_num)

        # === TRACK TOTAL_PR FOR NON-DEDUCTIBLE LINES ===
        # This ensures patient total_pr includes ALL their PR (coinsurance, copay, etc.)
        # even when there's no deductible on the line (e.g., Medicare patients after meeting deductible)
        if calc_ded == 0 and total_pr > 0:
            member_id = str(row.get("MEMBER ID", "") or "").strip()
            patient_name = str(row.get("NAME", "") or "").strip()
            if member_id:
                # Update aggregate patient data (if patient already exists from deductible lines)
                if not data.patient_deductibles[member_id]["name"]:
                    data.patient_deductibles[member_id]["name"] = patient_name
                data.patient_deductibles[member_id]["total_pr"] += total_pr

                # Year-specific tracking
                if year:
                    if not data.patient_by_year[member_id][year]["name"]:
                        data.patient_by_year[member_id][year]["name"] = patient_name
                    data.patient_by_year[member_id][year]["total_pr"] += total_pr

                    # Payer type tracking (Medicare vs Other)
                    if is_medicare:
                        payer_dict = data.patient_by_year_medicare
                    else:
                        payer_dict = data.patient_by_year_other
                    if not payer_dict[member_id][year]["name"]:
                        payer_dict[member_id][year]["name"] = patient_name
                    payer_dict[member_id][year]["total_pr"] += total_pr

                # Medicare member tracking (for text reports)
                if is_medicare:
                    data.medicare_members[member_id]["total_pr"] += total_pr

        for i in range(1, 6):
            group = row.get(f"SVC_CAS{i}_Group_L2110_CAS", "")
            reason = row.get(f"SVC_CAS{i}_Reason_L2110_CAS", "")
            amount = parse_currency(row.get(f"SVC_CAS{i}_Amount_L2110_CAS", 0))

            if group == "PR":
                pr_key = f"PR-{reason}"
                data.pr_adjustments[pr_key] += amount
                if claim_num:
                    data.pr_adjustments_claims[pr_key].add(claim_num)
                if year:
                    data.pr_adjustments_by_year[year][pr_key] += amount
                    if claim_num:
                        data.pr_adjustments_claims_by_year[year][pr_key].add(claim_num)
                # Track by payer type (medicare vs other)
                if is_medicare:
                    data.pr_adjustments_medicare[pr_key] += amount
                    if claim_num:
                        data.pr_adjustments_claims_medicare[pr_key].add(claim_num)
                    if year:
                        data.pr_adjustments_medicare_by_year[year][pr_key] += amount
                        if claim_num:
                            data.pr_adjustments_claims_medicare_by_year[year][pr_key].add(claim_num)
                else:
                    data.pr_adjustments_other[pr_key] += amount
                    if claim_num:
                        data.pr_adjustments_claims_other[pr_key].add(claim_num)
                    if year:
                        data.pr_adjustments_other_by_year[year][pr_key] += amount
                        if claim_num:
                            data.pr_adjustments_claims_other_by_year[year][pr_key].add(claim_num)
                if calc_ded > 0:
                    data.pr_adjustments_with_ded[pr_key] += amount
                    if claim_num:
                        data.pr_adjustments_claims_with_ded[pr_key].add(claim_num)
                    if year:
                        data.pr_adjustments_with_ded_by_year[year][pr_key] += amount
                        if claim_num:
                            data.pr_adjustments_claims_with_ded_by_year[year][pr_key].add(claim_num)
                if reason == "1":
                    data.pr1_cas_total += amount

        # === TRACK SECONDARY PAYER RECOVERY ===
        # Match ANY primary claim to ANY secondary claim by RUN (not just FORWARDED)
        is_primary_status = row.get("IS PRIMARY", "")
        service_payment = parse_currency(row.get("SERVICE PAYMENT", 0))

        # Track ALL PRIMARY claims (regardless of deductible or FORWARDED status)
        # Exclude: SECONDARY, TERTIARY, DENIED, REVERSAL
        is_primary_record = (
            "PRIMARY" in is_primary_status
            and "SECONDARY" not in is_primary_status
            and "TERTIARY" not in is_primary_status
            and "DENIED" not in is_primary_status
            and "REVERSAL" not in is_primary_status
        )

        if is_primary_record and run_num:
            secondary_payer_name = row.get("SecondaryPayer_Name_L1000A_N1", "") or ""
            # Store/update ALL primary claim info (aggregate if multiple lines per RUN)
            if run_num not in data.all_forwarded_claims:
                data.all_forwarded_claims[run_num] = {
                    "total_pr": 0.0,
                    "primary_payer": payer_name,
                    "secondary_payer_name": secondary_payer_name,
                    "year": year,  # Track year for secondary reconciliation
                }
            data.all_forwarded_claims[run_num]["total_pr"] += total_pr

            if calc_ded != 0:
                if run_num not in data.forwarded_claims:
                    data.forwarded_claims[run_num] = {
                        "deductible": 0.0,
                        "total_pr": 0.0,
                        "primary_payer": payer_name,
                        "secondary_payer_name": secondary_payer_name,
                    }
                data.forwarded_claims[run_num]["deductible"] += calc_ded
                data.forwarded_claims[run_num]["total_pr"] += total_pr

        # Track SECONDARY processed claims with payments
        if "SECONDARY" in is_primary_status and run_num and service_payment > 0:
            secondary_payer = payer_name  # The payer processing as secondary
            # Store/update secondary claim payment (aggregate if multiple lines)
            if run_num not in data.secondary_claims:
                data.secondary_claims[run_num] = {"payment": 0.0, "payer_name": secondary_payer}
            data.secondary_claims[run_num]["payment"] += service_payment

    print(f"  Total rows processed: {row_count:,}")
    print(f"  Companies found: {len(all_company_data)}")
    print()

    # === RECONCILE SECONDARY PAYER RECOVERY ===
    # Match primary claims to secondary payments by RUN (regardless of FORWARDED flag)
    print("Reconciling secondary payer recovery (matching primary to secondary by RUN)...")

    # Tracking for ALL claims
    all_total_primary = 0
    all_total_secondary_paid = 0
    all_total_secondary_payments = 0.0

    # Tracking for DEDUCTIBLE claims only
    ded_total_primary = 0
    ded_total_secondary_paid = 0
    ded_total_secondary_payments = 0.0

    for company_id, data in all_company_data.items():
        # === FIRST: Reconcile ALL primary claims ===
        for run_num, fwd_info in data.all_forwarded_claims.items():
            data.all_claims_forwarded_count += 1
            data.all_pr_forwarded_total += fwd_info["total_pr"]

            # Check if secondary payer made a payment for this RUN
            if run_num in data.secondary_claims:
                sec_info = data.secondary_claims[run_num]
                sec_payment = sec_info["payment"]

                if sec_payment > 0:
                    data.all_claims_secondary_paid_count += 1
                    data.all_secondary_recovery_total += sec_payment
                    # Track by year for dashboard filtering
                    claim_year = fwd_info.get("year")
                    if claim_year:
                        data.all_secondary_by_year[claim_year] += sec_payment
                        # Track by payer type for Layer 1 filtering
                        primary_payer = fwd_info.get("primary_payer", "")
                        if is_medicare_part_b(primary_payer):
                            data.layer1_medicare_by_year[claim_year]["secondary_pmt"] += sec_payment
                        else:
                            data.layer1_other_by_year[claim_year]["secondary_pmt"] += sec_payment

        # === SECOND: Reconcile DEDUCTIBLE-specific primary claims ===
        for run_num, fwd_info in data.forwarded_claims.items():
            data.claims_forwarded_count += 1
            data.deductible_forwarded_total += fwd_info["deductible"]

            # Check if secondary payer made a payment for this RUN
            if run_num in data.secondary_claims:
                sec_info = data.secondary_claims[run_num]
                sec_payment = sec_info["payment"]
                sec_payer = sec_info["payer_name"]

                if sec_payment > 0:
                    data.claims_secondary_paid_count += 1
                    data.secondary_recovery_total += sec_payment

                    # Track by secondary payer
                    pri_ded = fwd_info["deductible"]
                    pri_total_pr = fwd_info["total_pr"]
                    data.secondary_payer_recovery[sec_payer]["claims"] += 1
                    data.secondary_payer_recovery[sec_payer]["primary_deductible"] += pri_ded
                    data.secondary_payer_recovery[sec_payer]["primary_total_pr"] += pri_total_pr
                    data.secondary_payer_recovery[sec_payer]["secondary_payment"] += sec_payment

                    # Track secondary payment by PRIMARY payer (who applied the deductible)
                    primary_payer = fwd_info["primary_payer"]
                    data.payer_deductibles[primary_payer]["secondary_collected"] += sec_payment

                    # Allocate secondary payment by year-month using run_to_month mapping
                    # Use is_medicare_part_b(primary_payer) for classification to match Section 2A Overall
                    # (primary_payer is who applied the deductible, not the secondary payer)
                    if run_num in data.run_to_month:
                        month_info = data.run_to_month[run_num]
                        month = month_info["month"]
                        yr = month_info.get("year")
                        if yr:
                            year_month = (yr, month)
                            is_medicare = is_medicare_part_b(primary_payer)
                            if is_medicare:
                                data.medicare_by_year_month[year_month]["secondary_collected"] += sec_payment
                                # === YEAR: Secondary by year for Medicare ===
                                data.medicare_by_year[yr]["secondary_pmt"] += sec_payment
                            else:
                                data.other_by_year_month[year_month]["secondary_collected"] += sec_payment
                                # === YEAR: Secondary by year for Other ===
                                data.other_by_year[yr]["secondary_pmt"] += sec_payment
                            # === YEAR: Secondary recovery by year overall ===
                            data.secondary_recovery_by_year[yr] += sec_payment
                            # Track by payer and year
                            data.payer_by_year[primary_payer][yr]["secondary_pmt"] += sec_payment

        # Aggregate totals
        all_total_primary += data.all_claims_forwarded_count
        all_total_secondary_paid += data.all_claims_secondary_paid_count
        all_total_secondary_payments += data.all_secondary_recovery_total

        ded_total_primary += data.claims_forwarded_count
        ded_total_secondary_paid += data.claims_secondary_paid_count
        ded_total_secondary_payments += data.secondary_recovery_total

    print(
        f"  ALL CLAIMS: {all_total_primary:,} primary, {all_total_secondary_paid:,} with secondary pmt, ${all_total_secondary_payments:,.2f} total"
    )
    print(
        f"  DEDUCTIBLE CLAIMS: {ded_total_primary:,} primary, {ded_total_secondary_paid:,} with secondary pmt, ${ded_total_secondary_payments:,.2f} total"
    )
    print()

    # === CALCULATE LOSS (deductible on zero-collection claims) ===
    print("Calculating LOSS (deductible on zero-collection claims)...")
    total_loss = 0.0
    for company_id, data in all_company_data.items():
        for run_num, info in data.run_ded_info.items():
            # Check if this RUN has zero collection (patient payment is per-RUN)
            patient_pmt = run_payments.get(run_num, {}).get("total", 0)
            secondary_pmt = data.secondary_claims.get(run_num, {}).get("payment", 0)

            if patient_pmt == 0 and secondary_pmt == 0:
                # Zero collection - attribute LOSS per-payer within this RUN
                data.loss_runs.add(run_num)
                member_id = info.get("member_id", "")  # Patient is per-RUN (correct)
                run_total_loss = 0.0  # Track total LOSS for this RUN (for patient tracking)

                # Iterate through each payer's contribution within this RUN
                for payer, payer_info in data.run_payer_info[run_num].items():
                    ded_amt = payer_info["deductible"]
                    if ded_amt <= 0:
                        continue

                    month = payer_info["month"]
                    is_medicare = payer_info["is_medicare"]
                    yr = payer_info["year"]

                    data.loss_total += ded_amt
                    data.loss_by_payer[payer] += ded_amt
                    run_total_loss += ded_amt

                    if is_medicare:
                        data.loss_medicare += ded_amt
                        if yr:
                            data.loss_by_year_month_medicare[(yr, month)] += ded_amt
                    else:
                        data.loss_other += ded_amt
                        if yr:
                            data.loss_by_year_month_other[(yr, month)] += ded_amt

                    # === YEAR: LOSS by year ===
                    if yr:
                        data.loss_by_year[yr] += ded_amt
                        data.loss_by_payer_year[payer][yr] += ded_amt
                        if is_medicare:
                            data.loss_medicare_by_year[yr] += ded_amt
                            data.medicare_by_year[yr]["loss"] += ded_amt
                        else:
                            data.loss_other_by_year[yr] += ded_amt
                            data.other_by_year[yr]["loss"] += ded_amt
                        # Track payer LOSS by year
                        data.payer_by_year[payer][yr]["loss"] += ded_amt

                # Track LOSS by patient (patient gets total LOSS for the RUN)
                if member_id and member_id in data.patient_deductibles and run_total_loss > 0:
                    data.patient_deductibles[member_id]["loss"] += run_total_loss
                    # For patient year/payer-type tracking, use run-level metadata
                    yr = info.get("year")
                    is_medicare = info.get("is_medicare", False)
                    # === YEAR: Patient LOSS by year ===
                    if yr and member_id in data.patient_by_year:
                        data.patient_by_year[member_id][yr]["loss"] += run_total_loss
                    # === YEAR: Patient LOSS by year by payer type ===
                    if yr:
                        if is_medicare:
                            data.patient_by_year_medicare[member_id][yr]["loss"] += run_total_loss
                        else:
                            data.patient_by_year_other[member_id][yr]["loss"] += run_total_loss

        # Also add secondary payments to patient tracking
        for run_num, sec_info in data.secondary_claims.items():
            if run_num in data.run_ded_info:
                member_id = data.run_ded_info[run_num].get("member_id", "")
                yr = data.run_ded_info[run_num].get("year")
                is_medicare = data.run_ded_info[run_num].get("is_medicare", False)
                if member_id and member_id in data.patient_deductibles:
                    sec_pmt = sec_info.get("payment", 0)
                    data.patient_deductibles[member_id]["secondary"] += sec_pmt
                    # === YEAR: Patient secondary by year ===
                    if yr and member_id in data.patient_by_year:
                        data.patient_by_year[member_id][yr]["secondary"] += sec_pmt
                    # === YEAR: Patient secondary by year by payer type ===
                    if yr:
                        if is_medicare:
                            data.patient_by_year_medicare[member_id][yr]["secondary"] += sec_pmt
                        else:
                            data.patient_by_year_other[member_id][yr]["secondary"] += sec_pmt

        total_loss += data.loss_total

    print(f"  Total LOSS across all companies: ${total_loss:,.2f}")
    print()

    # Generate individual company reports
    print("Generating individual company reports with collection data...")
    db_source = "SQLite Database + Trip_Credits.csv"
    for company_id, data in sorted(all_company_data.items()):
        output_path = (
            output_dir
            / f"Deductible_Collection_{company_id}_{data.company_name.replace(' ', '_').replace(',', '')[:25]}.txt"
        )
        total_ded, total_coll = generate_company_report(data, run_payments, output_path, source_description=db_source)
        rate = (total_coll / total_ded * 100) if total_ded > 0 else 0
        print(f"  {company_id}: ${total_ded:,.2f} deductible, ${total_coll:,.2f} collected ({rate:.1f}%)")
        data.rows = []

    print()

    # Generate aggregate report
    print("Generating aggregate report...")
    aggregate_path = output_dir / "Deductible_Collection_AGGREGATE_ALL_COMPANIES.txt"
    grand_ded, grand_coll = generate_aggregate_report(
        all_company_data, run_payments, aggregate_path, source_description=db_source
    )
    grand_rate = (grand_coll / grand_ded * 100) if grand_ded > 0 else 0
    print(f"  Aggregate: ${grand_ded:,.2f} deductible, ${grand_coll:,.2f} collected ({grand_rate:.1f}%)")

    # Generate interactive HTML dashboards (aggregate + individual companies)
    print()
    print("Generating interactive HTML dashboards...")
    generate_all_dashboards(all_company_data, output_dir)

    print()
    print("=" * 70)
    print("REPORT GENERATION COMPLETE")
    print("=" * 70)
    print(f"Output directory: {output_dir}")
    print()
    print("Generated files:")
    # Only list files generated by this report (not all files in the directory)
    for f in sorted(output_dir.glob("Deductible_Collection_*.txt")):
        print(f"  {f.name}")
    for f in sorted(output_dir.glob("Deductible_Dashboard_*.html")):
        print(f"  {f.name}")

    print()
    print(f"Debug log saved: {debug_log_path.name}")

    # Restore stdout and close debug log file
    sys.stdout = original_stdout
    debug_log.close()

    return str(output_dir)


if __name__ == "__main__":
    main()
