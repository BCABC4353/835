import dictionary


def categorize_adjustment(group_code, reason_code, amount):
    """
    Categorize a CAS adjustment into standardized buckets.

    GROUP CODE DRIVES PRIMARY CATEGORIZATION:
    - CO = Contractual Obligation (Provider responsibility)
    - PR = Patient Responsibility
    - OA = Other Adjustment (e.g., sequestration)
    - PI = Payer Initiated reduction
    - NC = Non-Covered Charge

    CARC codes provide sub-categorization within group.

    Per X12 835 Specification Section 1.10.2.1:
    - POSITIVE adjustments DECREASE the payment
    - NEGATIVE adjustments INCREASE the payment

    Args:
        group_code: CAS group code (CO, PR, OA, PI, NC, etc.)
        reason_code: CARC reason code (e.g., '45', '1', '23')
        amount: Amount as string (e.g., '70' or '-70.00')

    Returns:
        Dictionary with amounts in each category
    """
    # Initialize all categories to 0.0
    result = {
        "Contractual": 0.0,
        "Copay": 0.0,
        "Coinsurance": 0.0,
        "Deductible": 0.0,
        "Denied": 0.0,
        "OtherAdjustments": 0.0,
        "Sequestration": 0.0,
        "COB": 0.0,
        "HCRA": 0.0,
        "QMB": 0.0,  # QMB/Dual-Eligible write-off (CO-303) - NEVER bill patient
        "PR_NonCovered": 0.0,
        "OtherPatientResp": 0.0,
        "AuditFlag": "",  # Flags group/CARC mismatches for analyst review
    }

    # Convert amount to float
    try:
        amt = float(amount) if amount else 0.0
    except (ValueError, TypeError):
        amt = 0.0

    if amt == 0.0:
        return result

    # CARC code sets for sub-categorization
    # Source: X12 CARC code list and dictionary.py get_carc_classifications()

    # Patient Responsibility - Standard
    DEDUCTIBLE_CARCS = {"1", "37", "66", "168", "247"}
    # 1=Deductible, 37=Balance doesn't exceed deductible, 66=Blood deductible
    # 168=Blood deductible, 247=Deductible for professional service by facility

    COINSURANCE_CARCS = {"2", "248"}
    # 2=Coinsurance Amount, 248=Coinsurance for professional service by facility

    COPAY_CARCS = {"3", "36"}
    # 3=Co-payment Amount, 36=Balance doesn't exceed copay

    # Non-Covered Services (patient pays because service not covered)
    NONCOVERED_CARCS = {
        "48",  # Not covered unless work-related injury
        "49",  # Non-covered service
        "50",  # Non-covered services
        "53",  # Immediate relative/household not covered
        "54",  # Multiple physicians not covered
        "78",  # Non-covered days/room charge
        "96",  # Non-covered charge(s)
        "109",  # Not covered by this payer
        "111",  # Not covered unless accepts assignment
        "167",  # Diagnosis not covered
        "202",  # Personal comfort not covered
        "204",  # Service/equipment/drug not covered
        "212",  # Admin surcharges not covered
        "219",  # Extent of injury - not covered
        "258",  # Not covered in this facility type
        "269",  # Anesthesia not covered for this service
        "293",  # Non-covered - routine care
        "295",  # Incident to non-covered service
        "B1",  # Non-covered visits
        "D25",  # Billing for non-covered charges
    }

    # Sequestration (federal payment reduction)
    SEQUESTRATION_CARCS = {"217", "253"}
    # Both mean: Sequestration - reduction in federal payment

    # HCRA - Health Care Related Adjustment
    # NOTE: No standard HCRA CARC codes exist. Payers may use proprietary codes.
    # If you see HCRA adjustments, identify the actual codes used and add here.
    HCRA_CARCS = set()  # Empty - add payer-specific codes if needed

    # Coordination of Benefits / Other Payer / Other Insurance
    # Expanded to include all COB/Other Insurance CARCs per dictionary.py
    COB_CARCS = {
        # Standard COB/Prior Payer
        "22",  # Care may be covered by another payer
        "23",  # Impact of prior payer adjudication
        "89",  # Not covered due to other insurance
        "129",  # Prior processing info appears incorrect
        "136",  # Failure to follow prior payer's coverage
        "213",  # Non-compliance with prior payer requirements
        "275",  # Prior payer payments not used in calculation
        "276",  # Services denied by prior payer not included
        "277",  # Claim adjusted based on prior payer info
        "282",  # Prior payer allowed exceeds billed
        "300",  # Benefits not available - forwarded to Behavioral Health
        "A3",  # Medicare Secondary Payer liability met
        "B13",  # Previously paid - forwarded to another payer
        "B15",  # Partially/fully furnished by another provider
        "B20",  # Partially/fully furnished by another provider
        # Work-Related / Liability / No-Fault (Other Insurance)
        "19",  # Work-related injury/illness
        "20",  # Covered by liability carrier
        "21",  # Liability of no-fault carrier
        "90",  # Workers' Compensation adjustment
        "92",  # Not covered under workers' compensation
        "191",  # Not a work-related injury/illness
        "201",  # Workers' Compensation case settled
        "214",  # Workers' Comp claim adjudicated as non-covered
        # Forwarded to Other Plans
        "304",  # Submit to patient's hearing plan
        "305",  # Forwarded to patient's hearing plan
        # Property & Casualty / Workers' Comp Block
        "P2",  # Not work-related; not covered by WC
        "P3",  # Workers' Compensation case settled
        "P4",  # Workers' Comp claim adjudicated as non-covered
        "P12",  # WC jurisdictional fee schedule adjustment
        "P13",  # Reduced/denied based on WC jurisdictional regulations
        "P15",  # WC Medical Treatment Guideline Adjustment
        "P16",  # Provider not authorized for WC in this jurisdiction
        "P21",  # Denied based on MPC/PIP regulations
        "P22",  # Adjusted based on MPC/PIP regulations
    }

    # QMB/Dual-Eligible (federally mandated write-off - NEVER bill patient)
    QMB_CARCS = {"303"}
    # 303 = Prior payer patient responsibility not covered for QMB beneficiaries

    # GROUP CODE DRIVES PRIMARY CATEGORIZATION

    if group_code == "NC":
        # Non-Covered Charge - patient responsible for non-covered services
        result["PR_NonCovered"] = amt
        return result

    if group_code == "CO":
        # Contractual Obligation - provider write-off
        # Check for special carve-outs that override standard CO = Contractual
        if reason_code in SEQUESTRATION_CARCS:
            # CO-253/217: Federal sequestration - report separately
            result["Sequestration"] = amt
        elif reason_code in QMB_CARCS:
            # CO-303: QMB/Dual-Eligible write-off - NEVER bill patient per CMS rules
            result["QMB"] = amt
        else:
            result["Contractual"] = amt
            # Audit flag: CARC suggests COB but payer sent CO group
            # This surfaces potential payer miscoding for analyst review
            if reason_code in COB_CARCS:
                result["AuditFlag"] = f"CO-{reason_code}: Dictionary suggests COB but payer declared CO (Contractual)"
        return result

    if group_code == "PR":
        # Patient Responsibility - use CARC for sub-categorization
        if reason_code in DEDUCTIBLE_CARCS:
            result["Deductible"] = amt
        elif reason_code in COINSURANCE_CARCS:
            result["Coinsurance"] = amt
        elif reason_code in COPAY_CARCS:
            result["Copay"] = amt
        elif reason_code in NONCOVERED_CARCS:
            result["PR_NonCovered"] = amt
        else:
            # PR group but unrecognized CARC - STILL patient responsibility!
            result["OtherPatientResp"] = amt
        return result

    if group_code == "OA":
        # Other Adjustment - use CARC for sub-categorization
        if reason_code in SEQUESTRATION_CARCS:
            result["Sequestration"] = amt
        elif reason_code in QMB_CARCS:
            # OA-303: Unusual but still QMB - route correctly
            result["QMB"] = amt
            result["AuditFlag"] = f"OA-{reason_code}: QMB CARC expected with CO group"
        elif reason_code in HCRA_CARCS:
            result["HCRA"] = amt
        elif reason_code in COB_CARCS:
            result["COB"] = amt
        else:
            result["OtherAdjustments"] = amt
        return result

    if group_code == "PI":
        # Payer Initiated - typically denials or reductions
        if reason_code in NONCOVERED_CARCS:
            result["Denied"] = amt
        else:
            result["OtherAdjustments"] = amt
        return result

    if group_code == "MA":
        # Medicare Secondary Payer Adjustment - treated as COB
        if reason_code in QMB_CARCS:
            # MA-303: QMB on Medicare Secondary - route to QMB
            result["QMB"] = amt
        elif reason_code in COB_CARCS:
            result["COB"] = amt
        elif reason_code in SEQUESTRATION_CARCS:
            result["Sequestration"] = amt
        else:
            result["OtherAdjustments"] = amt
        return result

    if group_code == "CR":
        # Correction/Reversal - must be paired with original group code
        # Use CARC-based classification since CR itself doesn't indicate category
        # Amount is typically negative (reversing previous adjustment)
        pass  # Fall through to CARC-based classification below

    # Fallback for unknown group codes (including CR): use CARC-based classification
    carc_data = dictionary.get_carc_classifications()

    if reason_code in carc_data:
        category = carc_data[reason_code].get("category", "")

        if category == "QMB/Dual Eligible":
            result["QMB"] = amt
        elif category == "Contractual":
            result["Contractual"] = amt
        elif category == "Patient Responsibility":
            if reason_code in DEDUCTIBLE_CARCS:
                result["Deductible"] = amt
            elif reason_code in COINSURANCE_CARCS:
                result["Coinsurance"] = amt
            elif reason_code in COPAY_CARCS:
                result["Copay"] = amt
            else:
                result["OtherPatientResp"] = amt
        elif category in ["COB/Other Payer", "Other Insurance"]:
            result["COB"] = amt
        elif "Sequestration" in category:
            result["Sequestration"] = amt
        elif "HCRA" in category:
            result["HCRA"] = amt
        elif category == "Coverage" or "not covered" in category.lower():
            result["Denied"] = amt
        else:
            result["OtherAdjustments"] = amt
    else:
        # Unknown CARC - put in OtherAdjustments
        result["OtherAdjustments"] = amt

    return result
