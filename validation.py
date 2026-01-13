"""
Zero-Fail 835 CSV Validation System
===================================

This module provides comprehensive validation for 835 EDI to CSV conversion with:
- 100% field coverage tracking
- Mathematical accuracy validation
- Edge case handling
- Detailed error reporting with payer/state tracking
"""

import decimal
import html
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, Overflow
from typing import Any, Dict, List

import colloquial
import dictionary
from categorization import categorize_adjustment

# Configure module logger
logger = logging.getLogger(__name__)

# Lazy-load DISPLAY_COLUMN_NAMES to avoid circular import with parser_835
_DISPLAY_TO_INTERNAL = None
_INTERNAL_TO_DISPLAY = None


def _get_display_mappings():
    """Lazy-load display column mappings to avoid circular import."""
    global _DISPLAY_TO_INTERNAL, _INTERNAL_TO_DISPLAY
    if _DISPLAY_TO_INTERNAL is None:
        from parser_835 import DISPLAY_COLUMN_NAMES

        _DISPLAY_TO_INTERNAL = {v: k for k, v in DISPLAY_COLUMN_NAMES.items()}
        _INTERNAL_TO_DISPLAY = DISPLAY_COLUMN_NAMES.copy()
    return _DISPLAY_TO_INTERNAL, _INTERNAL_TO_DISPLAY


def get_csv_field(row: dict, internal_name: str, default: Any = "") -> Any:
    """
    Get a field from CSV row, trying internal name first then display name.

    The CSV may use either internal field names or display-friendly names
    (from DISPLAY_COLUMN_NAMES). This helper handles both cases.

    Args:
        row: CSV row dictionary
        internal_name: The internal field name (e.g., 'CLM_ChargeAmount_L2100_CLP')
        default: Default value if field not found

    Returns:
        Field value or default
    """
    # Try internal name first
    if internal_name in row:
        return row[internal_name]
    # Try display name
    _, internal_to_display = _get_display_mappings()
    display_name = internal_to_display.get(internal_name)
    if display_name and display_name in row:
        return row[display_name]
    return default


def normalize_csv_rows(csv_rows: List[dict]) -> List[dict]:
    """
    Normalize CSV rows to use internal field names instead of display names.

    The CSV output uses display-friendly column names (e.g., "CLAIM CHARGE" instead
    of "CLM_ChargeAmount_L2100_CLP"). This function converts display names back to
    internal names so validation code can use consistent field names.

    Args:
        csv_rows: List of CSV row dictionaries with display names

    Returns:
        List of CSV row dictionaries with internal names
    """
    if not csv_rows:
        return csv_rows

    display_to_internal, _ = _get_display_mappings()

    # Check if normalization is needed (first row has display names)
    first_row = csv_rows[0]
    needs_normalization = any(display_name in first_row for display_name in display_to_internal)

    if not needs_normalization:
        return csv_rows

    # Normalize all rows
    normalized = []
    for row in csv_rows:
        new_row = {}
        for key, value in row.items():
            # Convert display name to internal name if mapping exists
            internal_name = display_to_internal.get(key, key)
            new_row[internal_name] = value
        normalized.append(new_row)

    return normalized


class ValidationError:
    """Structured validation error for detailed reporting"""

    def __init__(
        self,
        error_type: str,
        message: str,
        location: str = None,
        segment: str = None,
        field: str = None,
        expected: Any = None,
        actual: Any = None,
        payer_info: Dict = None,
        edi_context: List[str] = None,
    ):
        self.type = error_type
        self.message = message
        self.location = location
        self.segment = segment
        self.field = field
        self.expected = expected
        self.actual = actual
        self.payer_info = payer_info or {}
        self.edi_context = edi_context or []

    def __str__(self):
        parts = [f"[{self.type}]"]
        if self.location:
            parts.append(f"{self.location}:")
        parts.append(self.message)
        if self.expected is not None and self.actual is not None:
            parts.append(f"(Expected: {self.expected}, Actual: {self.actual})")
        return " ".join(parts)

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            "type": self.type,
            "message": self.message,
            "location": self.location,
            "segment": self.segment,
            "field": self.field,
            "expected": str(self.expected) if self.expected is not None else None,
            "actual": str(self.actual) if self.actual is not None else None,
            "payer_info": self.payer_info,
            "edi_context": self.edi_context,
        }


class SegmentFieldMap:
    """Complete mapping of ALL 835 segments to CSV fields"""

    BPR_MAP = {
        "01": "CHK_TransactionHandling_Header_BPR",
        "02": "CHK_PaymentAmount_Header_BPR",
        "03": "CHK_CreditDebitFlag_Header_BPR",
        "04": "CHK_PaymentMethod_Header_BPR",
        "05": "CHK_Format_Header_BPR",
        "06": "CHK_PayerDFI_Qualifier_Header_BPR",
        "07": "CHK_PayerDFI_ID_Header_BPR",
        "08": "CHK_PayerAccountQualifier_Header_BPR",
        "09": "CHK_PayerAccountNumber_Header_BPR",
        "10": "CHK_OriginatingCompanyID_Header_BPR",
        "11": "CHK_OriginatingCompanySupplemental_Header_BPR",
        "12": "CHK_PayeeDFI_Qualifier_Header_BPR",
        "13": "CHK_PayeeDFI_ID_Header_BPR",
        "14": "CHK_PayeeAccountQualifier_Header_BPR",
        "15": "CHK_PayeeAccountNumber_Header_BPR",
        "16": "CHK_EffectiveDate_Header_BPR",
        "17": "CHK_BusinessFunctionCode_Header_BPR",
        "18": "CHK_DFI_Qualifier_3_Header_BPR",
        "19": "CHK_DFI_ID_3_Header_BPR",
        "20": "CHK_AccountQualifier_3_Header_BPR",
        "21": "CHK_AccountNumber_3_Header_BPR",
    }
    TRN_MAP = {
        "01": "CHK_TraceType_Header_TRN",
        "02": "CHK_TraceNumber_Header_TRN",
        "03": "CHK_OriginatingCompanyID_TRN_Header_TRN",
        "04": "CHK_ReferenceIDSecondary_Header_TRN",
    }
    CLP_MAP = {
        "01": "CLM_PatientControlNumber_L2100_CLP",
        "02": "CLM_Status_L2100_CLP",
        "03": "CLM_ChargeAmount_L2100_CLP",
        "04": "CLM_PaymentAmount_L2100_CLP",
        "05": "CLM_PatientResponsibility_L2100_CLP",
        "06": "CLM_FilingIndicator_L2100_CLP",
        "07": "CLM_PayerControlNumber_L2100_CLP",
        "08": "CLM_FacilityTypeCode_L2100_CLP",
        "09": "CLM_FrequencyCode_L2100_CLP",
        "10": "CLM_PatientConditionCode_L2100_CLP",
        "11": "CLM_DRGCode_L2100_CLP",
        "12": "CLM_DRGWeight_L2100_CLP",
        "13": "CLM_DischargeFraction_L2100_CLP",
        "14": "CLM_YesNoCondition_L2100_CLP",
        "16": "Not Used",
        "17": "Not Used",
        "18": "Not Used",
        "19": "CLM_PaymentTypology_L2100_CLP",
    }
    SVC_MAP = {
        "01": "SVC_ProcedureCode_L2110_SVC",
        "02": "SVC_ChargeAmount_L2110_SVC",
        "03": "SVC_PaymentAmount_L2110_SVC",
        # '04': Not validated - rarely populated; SEQ column is generated row number
        "05": "SVC_Units_L2110_SVC",
        "06": "SVC_OriginalProcedure_L2110_SVC",
        "07": "SVC_OriginalUnits_L2110_SVC",
    }
    MOA_MAP = {
        "01": "MOA_ReimbursementRate_L2100_MOA",
        "02": "MOA_ClaimHCPCSPayableAmount_L2100_MOA",
        "03": "MOA_ClaimPaymentRemarkCode1_L2100_MOA",
        "04": "MOA_ClaimPaymentRemarkCode2_L2100_MOA",
        "05": "MOA_ClaimPaymentRemarkCode3_L2100_MOA",
        "06": "MOA_ClaimPaymentRemarkCode4_L2100_MOA",
        "07": "MOA_ClaimPaymentRemarkCode5_L2100_MOA",
        "08": "MOA_ESRDPaymentAmount_L2100_MOA",
        "09": "MOA_NonpayableProfessionalComponent_L2100_MOA",
    }
    MIA_MAP = {
        "01": "MIA_CoveredDaysOrVisitsCount_L2100_MIA",
        "02": "MIA_PPSOperatingOutlierAmount_L2100_MIA",
        "03": "MIA_LifetimePsychiatricDaysCount_L2100_MIA",
        "04": "MIA_ClaimDRGAmount_L2100_MIA",
        "05": "MIA_ClaimPaymentRemarkCode_L2100_MIA",
        "06": "MIA_ClaimDisproportionateShareAmount_L2100_MIA",
        "07": "MIA_ClaimMSPPassThroughAmount_L2100_MIA",
        "08": "MIA_ClaimPPSCapitalAmount_L2100_MIA",
        "09": "MIA_PPSCapitalFSPDRGAmount_L2100_MIA",
        "10": "MIA_PPSCapitalHSPDRGAmount_L2100_MIA",
        "11": "MIA_PPSCapitalDSHDRGAmount_L2100_MIA",
        "12": "MIA_OldCapitalAmount_L2100_MIA",
        "13": "MIA_PPSCapitalIMEAmount_L2100_MIA",
        "14": "MIA_PPSOperatingHospitalSpecificDRGAmount_L2100_MIA",
        "15": "MIA_CostReportDayCount_L2100_MIA",
        "16": "MIA_PPSOperatingFederalSpecificDRGAmount_L2100_MIA",
        "17": "MIA_ClaimPPSCapitalOutlierAmount_L2100_MIA",
        "18": "MIA_ClaimIndirectTeachingAmount_L2100_MIA",
        "19": "MIA_NonpayableProfessionalComponentAmount_L2100_MIA",
        "20": "MIA_ClaimPaymentRemarkCode2_L2100_MIA",
        "21": "MIA_ClaimPaymentRemarkCode3_L2100_MIA",
        "22": "MIA_ClaimPaymentRemarkCode4_L2100_MIA",
        "23": "MIA_ClaimPaymentRemarkCode5_L2100_MIA",
        "24": "MIA_PPSCapitalExceptionAmount_L2100_MIA",
    }
    PLB_MAP = {
        # PLB01-02: Provider ID and Fiscal Period Date
        "01": "PLB_ProviderID_PLB",
        "02": "PLB_FiscalPeriodDate_PLB",
        # PLB03-14: Up to 6 adjustment pairs (composite ID + amount)
        # Note: PLB03/05/07/09/11/13 are composites containing reason_code:reference_id
        # The parser flattens these into PLB_Adj#_ReasonCode_PLB and PLB_Adj#_RefID_PLB
        "03": "PLB_Adj1_ReasonCode_PLB",  # Composite: reason_code:reference_id
        "04": "PLB_Adj1_Amount_PLB",
        "05": "PLB_Adj2_ReasonCode_PLB",
        "06": "PLB_Adj2_Amount_PLB",
        "07": "PLB_Adj3_ReasonCode_PLB",
        "08": "PLB_Adj3_Amount_PLB",
        "09": "PLB_Adj4_ReasonCode_PLB",
        "10": "PLB_Adj4_Amount_PLB",
        "11": "PLB_Adj5_ReasonCode_PLB",
        "12": "PLB_Adj5_Amount_PLB",
        "13": "PLB_Adj6_ReasonCode_PLB",
        "14": "PLB_Adj6_Amount_PLB",
    }
    REF_QUALIFIER_MAP = {
        "EV": "CHK_ReceiverID_Header_REF",
        "F2": "CHK_VersionID",
        "TJ": "Provider_TaxID_L1000B_REF",
        "0B": "CLM_StateMedicalAssistanceNumber_L2100_REF",
        "0K": "CLM_PolicyNumber_L2100_REF",
        "1A": "CLM_BlueCrossProviderNumber_L2100_REF",
        "1B": "CLM_BlueShieldProviderNumber_L2100_REF",
        "1C": "CLM_MedicareProviderNumber_L2100_REF",
        "1D": "CLM_MedicaidProviderNumber_L2100_REF",
        "1G": "CLM_ProviderUPINNumber_L2100_REF",
        "1H": "CLM_CHAMPUSIdentificationNumber_L2100_REF",
        "1K": "CLM_PayersClaimNumber_L2100_REF",
        "1L": "CLM_GroupNumber_L2100_REF",
        "6P": "CLM_GroupNumber_L2100_REF",  # Also maps to Group Number
        "1W": "CLM_MemberID_L2100_NM1",
        "28": "CLM_EmployeeIdentificationNumber_L2100_REF",
        "2U": "CLM_PayerIdentificationNumber_L2100_REF",
        "6R": "CLM_ProviderControl_L2100_REF",
        "9A": "CLM_RepricedClaimRefNumber_L2100_REF",
        "9C": "CLM_AdjustmentIdentifier_L2100_REF",
        "9F": "CLM_ReferralNumber_L2100_REF",
        "A6": "CLM_EmployeeIdentificationNumber_L2100_REF",
        "BB": "CLM_AuthorizationNumber_L2100_REF",
        "CE": "CLM_PlanName_L2100_REF",
        "D9": "CLM_ClaimNumber_L2100_REF",
        "EA": "CLM_MedicalRecord_L2100_REF",
        "F4": "CLM_HierarchicalParentId_L2100_REF",
        "F8": "CLM_OriginalRef_L2100_REF",
        "G1": "CLM_PriorAuth_L2100_REF",
        "G3": "CLM_PredeterminationNumber_L2100_REF",
        "HPI": "CLM_HPID_L2100_REF",
        "IG": "CLM_InsurancePolicyNumber_L2100_REF",
        "LU": "SVC_FacilityTypeCode_L2110_REF",
        "LX": "CLM_QualifiedProductsList_L2100_REF",
        "PQ": "CLM_PayeeIdentification_L2100_REF",
        "SY": "CLM_SSN_L2100_NM1",
        "Y8": "CLM_AgencyClaimNumber_L2100_REF",
        "E9": "SVC_LineItemControl_L2110_REF",
        # Repricing Reference Numbers
        "APC": "CLM_AmbulatoryPaymentClassification_L2100_REF",
        "NF": "CLM_NAICCode_L2100_REF",
    }
    DTM_QUALIFIER_MAP = {
        "009": "CLM_ProcessDate_L2100_DTM",
        "036": "CLM_ExpirationDate_L2100_DTM",
        "050": "CLM_ReceivedDate_L2100_DTM",
        "232": "CLM_ServiceStartDate_L2100_DTM",
        "233": "CLM_ServiceEndDate_L2100_DTM",
        "434": "CLM_StatementFromDate_L2100_DTM",
        "435": "CLM_StatementToDate_L2100_DTM",
        "150": "SVC_ServiceStartDate_L2110_DTM",
        "151": "SVC_ServiceEndDate_L2110_DTM",
        "472": "SVC_ServiceStartDate_L2110_DTM",
        # Production Date
        "405": "CHK_ProductionDate_Header_DTM405",
    }
    AMT_QUALIFIER_MAP = {
        "AU": "CLM_CoverageAmount_L2100_AMT",
        "D8": "CLM_DiscountAmount",
        "DY": "CLM_PerDayLimitAmount",
        "F5": "CLM_PatientAmountPaid_L2100_AMT",
        "I": "CLM_InterestAmount_L2100_AMT",
        "NL": "CLM_PromptPaymentDiscount",
        "T": ["CLM_TaxAmount_L2100_AMT", "SVC_TaxAmount_L2110_AMT"],
        "T2": ["CLM_TotalClaimBeforeTaxes", "SVC_TotalServiceBeforeTaxes"],
        "ZK": "CLM_FederalMedicareCreditAmount",
        "ZL": "CLM_FederalMedicareBuyInAmount",
        "ZM": "CLM_FederalMedicareBloodDeductible",
        "ZN": "CLM_CoinsuranceAmount",
        "ZO": "CLM_ZOAmount",
        "ZZ": "CLM_MutuallyDefined",
        "B6": "SVC_AllowedAmount_L2110_AMT",
        "KH": "SVC_DeductibleAmount",
    }
    QTY_QUALIFIER_MAP = {
        "PS": "CLM_PrescriptionCount",
        "VS": "CLM_VisitCount",
        "ZK": "CLM_UnitsDenied",
        "ZL": "CLM_UnitsNotCertified",
        "PT": "SVC_AmbulancePatientCount_L2110_QTY",
        # Covered Actual
        "CA": "CLM_CoveredActual_L2100_QTY",
    }
    NM1_ENTITY_MAP = {
        "IL": "CLM_SubscriberName",
        "QC": "CLM_PatientName",
        "74": "CLM_CorrectedInsuredName",
        "82": "SVC_RenderingProvider",
        "TT": "CLM_TransferToProvider",
        "77": "CLM_ServiceFacility",
        "PR": "Payer_Name",
        "PE": "Provider_Name",
        "GB": "CLM_OtherPayer",
        "IC": "CLM_IntermediaryBank",
        "P3": "CLM_PrimaryCareProvider",
        "71": "CLM_AttendingPhysician",
        "72": "CLM_OperatingPhysician",
        "ZZ": "CLM_MutuallyDefinedEntity",
        "QJ": "AMB_PickupName",
        "45": "AMB_DropoffName",
    }
    CAS_GROUP_CODES = ["CO", "CR", "DE", "MA", "OA", "PI", "PR"]
    CUR_MAP = {}
    RDM_MAP = {
        "01": "RDM_TransmissionCode_Header_RDM",
        "02": "RDM_Name_Header_RDM",
        "03": "RDM_CommunicationNumber_Header_RDM",
        "04": "RDM_ReferenceID_Header_RDM",
        "05": "RDM_ReferenceID2_Header_RDM",
        "06": "RDM_CommunicationNumber2_Header_RDM",
        "07": "RDM_ContactFunction_Header_RDM",
    }
    ISA_MAP = {
        "01": "ENV_AuthorizationQualifier_Envelope_ISA",
        "02": "ENV_AuthorizationInfo_Envelope_ISA",
        "03": "ENV_SecurityQualifier_Envelope_ISA",
        "04": "ENV_SecurityInfo_Envelope_ISA",
        "05": "ENV_SenderIDQualifier_Envelope_ISA",
        "06": "ENV_SenderID_Envelope_ISA",
        "07": "ENV_ReceiverIDQualifier_Envelope_ISA",
        "08": "ENV_ReceiverID_Envelope_ISA",
        "09": "ENV_InterchangeDate_Envelope_ISA",
        "10": "ENV_InterchangeTime_Envelope_ISA",
        "11": "ENV_RepetitionSeparator_Envelope_ISA",
        "12": "ENV_VersionNumber_Envelope_ISA",
        "13": "ENV_InterchangeControlNumber_Envelope_ISA",
        "14": "ENV_AcknowledgmentRequested_Envelope_ISA",
        "15": "ENV_UsageIndicator_Envelope_ISA",
        "16": "ENV_ComponentSeparator_Envelope_ISA",
    }
    GS_MAP = {
        "01": "ENV_FunctionalIDCode_Envelope_GS",
        "02": "ENV_ApplicationSenderCode_Envelope_GS",
        "03": "ENV_ApplicationReceiverCode_Envelope_GS",
        "04": "ENV_Date_Envelope_GS",
        "05": "ENV_Time_Envelope_GS",
        "06": "ENV_GroupControlNumber_Envelope_GS",
        "07": "ENV_ResponsibleAgencyCode_Envelope_GS",
        "08": "ENV_VersionReleaseID_Envelope_GS",
    }
    N1_MAP = {"01": "EntityIDCode", "02": "Name", "03": "IDQualifier", "04": "IDCode"}
    N2_MAP = {"01": "AdditionalNameLine1", "02": "AdditionalNameLine2"}
    N3_MAP = {"01": "Address", "02": "Address2"}
    N4_MAP = {
        "01": "City",
        "02": "State",
        "03": "Zip",
        "04": "Country",
        "05": "LocationQualifier",
        "06": "LocationID",
        "07": "CountrySubdivisionCode",
    }
    PER_MAP = {
        "01": "Contact_Function_Code",
        "02": "Name",
        "03": "Communication_Number_Qualifier_1",
        "04": "Communication_Number_1",
        "05": "Communication_Number_Qualifier_2",
        "06": "Communication_Number_2",
        "07": "Communication_Number_Qualifier_3",
        "08": "Communication_Number_3",
        "09": "Contact_Inquiry_Reference",
    }
    LQ_MAP = {"01": "Code_List_Qualifier", "02": "Industry_Code"}
    LQ_QUALIFIER_MAP = {
        # Healthcare Remark Codes (RARC)
        "HE": "CLM_HealthcareRemarkCodes_L2100_LQ",
    }
    ST_MAP = {
        "01": "File_TransactionType_Header_ST",
        "02": "File_TransactionControlNumber_Header_ST",
        "03": "File_ImplementationConventionRef_Header_ST",
    }

    @classmethod
    def get_all_segments(cls) -> Dict[str, Dict[str, str]]:
        """Return all segment field mappings"""
        return {
            "ISA": cls.ISA_MAP,
            "GS": cls.GS_MAP,
            "ST": cls.ST_MAP,
            "BPR": cls.BPR_MAP,
            "TRN": cls.TRN_MAP,
            "CLP": cls.CLP_MAP,
            "SVC": cls.SVC_MAP,
            "PLB": cls.PLB_MAP,
            "RDM": cls.RDM_MAP,
            "N1": cls.N1_MAP,
            "N2": cls.N2_MAP,
            "N3": cls.N3_MAP,
            "N4": cls.N4_MAP,
            "PER": cls.PER_MAP,
            "LQ": cls.LQ_MAP,
        }

    @classmethod
    def get_qualifier_maps(cls) -> Dict[str, Dict[str, str]]:
        """Return all qualifier mappings"""
        return {
            "REF": cls.REF_QUALIFIER_MAP,
            "DTM": cls.DTM_QUALIFIER_MAP,
            "AMT": cls.AMT_QUALIFIER_MAP,
            "QTY": cls.QTY_QUALIFIER_MAP,
            "NM1": cls.NM1_ENTITY_MAP,
        }

    @classmethod
    def get_cas_category_fields(cls) -> Dict[str, List[str]]:
        """Return CAS categorization fields for validation"""
        return {
            "claim": [
                "CLM_Contractual_L2100_CAS",
                "CLM_Copay_L2100_CAS",
                "CLM_Coinsurance_L2100_CAS",
                "CLM_Deductible_L2100_CAS",
                "CLM_Denied_L2100_CAS",
                "CLM_OtherAdjustments_L2100_CAS",
                "CLM_Sequestration_L2100_CAS",
                "CLM_COB_L2100_CAS",
                "CLM_HCRA_L2100_CAS",
                "CLM_QMB_L2100_CAS",
            ],
            "service": [
                "SVC_Contractual_L2110_CAS",
                "SVC_Copay_L2110_CAS",
                "SVC_Coinsurance_L2110_CAS",
                "SVC_Deductible_L2110_CAS",
                "SVC_Denied_L2110_CAS",
                "SVC_OtherAdjustments_L2110_CAS",
                "SVC_Sequestration_L2110_CAS",
                "SVC_COB_L2110_CAS",
                "SVC_HCRA_L2110_CAS",
                "SVC_QMB_L2110_CAS",
                "Patient_NonCovered",
            ],
        }


class EDIElementPresenceTracker:
    """Lightweight tracker to find EDI elements with data but no CSV mapping"""

    ELEMENT_DESCRIPTIONS = {
        "CLP*08": "Facility Type Code (Institutional claims - hospital/SNF type)",
        "CLP*10": "Patient Status Code (Institutional - discharge status)",
        "CLP*11": "DRG Code (Institutional - Diagnosis Related Group)",
        "CLP*12": "DRG Weight (Institutional - DRG payment weight)",
        "CLP*13": "Discharge Fraction (Institutional - percent of stay)",
        "CLP*15": "Exchange Rate (Foreign currency claims)",
        "MOA*01": "Reimbursement Rate (Medicare outpatient payment rate)",
        "MOA*02": "HCPCS Payable Amount (Medicare outpatient allowable)",
        "MOA*03": "Remark Code 1 (Medicare outpatient remark)",
        "MOA*04": "Remark Code 2 (Medicare outpatient remark)",
        "MOA*05": "Remark Code 3 (Medicare outpatient remark)",
        "MOA*06": "Remark Code 4 (Medicare outpatient remark)",
        "MOA*07": "Remark Code 5 (Medicare outpatient remark)",
        "MOA*08": "ESRD Payment Amount (End-Stage Renal Disease)",
        "MOA*09": "Non-Payable Professional Component (Medicare)",
        "MIA*01": "Covered Days/Visits Count (Medicare inpatient)",
        "MIA*02": "PPS Operating Outlier Amount (Medicare inpatient)",
        "MIA*03": "Lifetime Psychiatric Days Count (Medicare inpatient)",
        "MIA*04": "DRG Amount (Medicare inpatient DRG payment)",
        "MIA*05": "Remark Code (Medicare inpatient remark)",
        "MIA*06": "Disproportionate Share Amount (Medicare DSH)",
        "MIA*07": "MSP Pass-Through Amount (Medicare Secondary Payer)",
        "MIA*08": "PPS Capital Amount (Medicare capital payment)",
        "MIA*09": "PPS Capital FSP DRG Amount (Medicare)",
        "MIA*10": "PPS Capital HSP DRG Amount (Medicare)",
        "MIA*11": "PPS Capital DSH DRG Amount (Medicare)",
        "MIA*12": "Old Capital Amount (Medicare)",
        "MIA*13": "PPS Capital IME Amount (Medicare teaching adjustment)",
        "MIA*14": "PPS Operating Hospital-Specific DRG Amount (Medicare)",
        "MIA*15": "Cost Report Day Count (Medicare)",
        "MIA*16": "PPS Operating Federal-Specific DRG Amount (Medicare)",
        "MIA*17": "PPS Capital Outlier Amount (Medicare)",
        "MIA*18": "Indirect Teaching Amount (Medicare IME)",
        "MIA*19": "Non-Payable Professional Component (Medicare)",
        "MIA*20": "Remark Code 2 (Medicare inpatient remark)",
        "MIA*21": "Remark Code 3 (Medicare inpatient remark)",
        "MIA*22": "Remark Code 4 (Medicare inpatient remark)",
        "MIA*23": "Remark Code 5 (Medicare inpatient remark)",
        "MIA*24": "PPS Capital Exception Amount (Medicare)",
        "SVC*04": "Bundled/Unbundled Line Number (line item reference)",
        "CUR*02": "Currency Code (foreign currency identifier)",
        "CUR*03": "Exchange Rate (foreign currency rate)",
    }
    # Qualifier-based segments that need special tracking
    QUALIFIER_SEGMENTS = {"REF", "DTM", "AMT", "QTY", "LQ"}

    def __init__(self):
        self.element_presence = defaultdict(lambda: defaultdict(set))
        self.element_payers = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        # Qualifier tracking: {segment_type: {qualifier: {file_idx set}}}
        self.qualifier_presence = defaultdict(lambda: defaultdict(set))
        # Qualifier payer counts: {segment_type: {qualifier: {payer: count}}}
        self.qualifier_payers = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self.files_processed = 0
        self.current_file_idx = 0
        self.current_payer = "Unknown"

    def track_segment(self, segment: str, elements: list, delimiter: str):
        """Track which element positions have data in this segment"""
        if not elements:
            return
        seg_id = elements[0]

        # Track qualifier-based segments separately
        if seg_id in self.QUALIFIER_SEGMENTS and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2] if len(elements) > 2 else ""
            if qualifier and value and str(value).strip():
                self.qualifier_presence[seg_id][qualifier].add(self.current_file_idx)
                self.qualifier_payers[seg_id][qualifier][self.current_payer] += 1

        # Track all element positions (for non-qualifier segments)
        for pos, value in enumerate(elements[1:], 1):
            if value and str(value).strip():
                self.element_presence[seg_id][pos].add(self.current_file_idx)
                self.element_payers[seg_id][pos][self.current_payer] += 1

    def new_file(self, payer_name: str = "Unknown"):
        """Mark start of a new file"""
        self.files_processed += 1
        self.current_file_idx = self.files_processed
        self.current_payer = payer_name

    def get_unmapped_elements(self) -> dict:
        """Compare tracked elements to segment maps and find gaps"""
        all_maps = {
            "BPR": SegmentFieldMap.BPR_MAP,
            "TRN": SegmentFieldMap.TRN_MAP,
            "CLP": SegmentFieldMap.CLP_MAP,
            "SVC": SegmentFieldMap.SVC_MAP,
            "PLB": SegmentFieldMap.PLB_MAP,
            "MIA": SegmentFieldMap.MIA_MAP,
            "MOA": SegmentFieldMap.MOA_MAP,
            "CUR": SegmentFieldMap.CUR_MAP if hasattr(SegmentFieldMap, "CUR_MAP") else {},
        }
        # Envelope/structural segments we intentionally skip
        envelope_segments = {
            "ISA",
            "GS",
            "GE",
            "IEA",
            "ST",
            "SE",
        }
        # Segments handled by entity/context logic (not qualifier-based)
        context_segments = {
            "N1",
            "N2",
            "N3",
            "N4",
            "NM1",
            "PER",
            "CAS",
            "LX",
            "RDM",
            "TS2",
            "TS3",
        }
        unmapped = {}

        # Check position-based segments
        for seg_id, positions in self.element_presence.items():
            if seg_id in envelope_segments or seg_id in context_segments or seg_id in self.QUALIFIER_SEGMENTS:
                continue
            seg_map = all_maps.get(seg_id, {})
            for pos, file_set in positions.items():
                pos_key = f"{pos:02d}"
                if pos_key not in seg_map or seg_map.get(pos_key) in [None, "", "Not Used"]:
                    key = f"{seg_id}*{pos:02d}"
                    files_with_data = len(file_set)
                    payer_counts = dict(self.element_payers[seg_id][pos])
                    sorted_payers = sorted(payer_counts.items(), key=lambda x: x[1], reverse=True)
                    unmapped[key] = {
                        "segment": seg_id,
                        "position": pos,
                        "description": self.ELEMENT_DESCRIPTIONS.get(key, "Unknown field"),
                        "files_with_data": files_with_data,
                        "total_files": self.files_processed,
                        "pct": round(100 * files_with_data / self.files_processed, 1)
                        if self.files_processed > 0
                        else 0,
                        "payers": sorted_payers[:10],
                        "total_occurrences": sum(payer_counts.values()),
                        "type": "position",
                    }

        return dict(sorted(unmapped.items(), key=lambda x: x[1]["files_with_data"], reverse=True))

    def get_unmapped_qualifiers(self) -> dict:
        """Find qualifier-based segments with data but no CSV mapping"""
        qualifier_maps = {
            "REF": SegmentFieldMap.REF_QUALIFIER_MAP,
            "DTM": SegmentFieldMap.DTM_QUALIFIER_MAP,
            "AMT": SegmentFieldMap.AMT_QUALIFIER_MAP,
            "QTY": SegmentFieldMap.QTY_QUALIFIER_MAP,
            "LQ": SegmentFieldMap.LQ_QUALIFIER_MAP,
        }

        unmapped = {}
        for seg_id, qualifiers in self.qualifier_presence.items():
            qual_map = qualifier_maps.get(seg_id, {})
            for qualifier, file_set in qualifiers.items():
                if qualifier not in qual_map:
                    key = f"{seg_id}*{qualifier}"
                    files_with_data = len(file_set)
                    payer_counts = dict(self.qualifier_payers[seg_id][qualifier])
                    sorted_payers = sorted(payer_counts.items(), key=lambda x: x[1], reverse=True)
                    unmapped[key] = {
                        "segment": seg_id,
                        "qualifier": qualifier,
                        "description": f"Unmapped {seg_id} qualifier",
                        "files_with_data": files_with_data,
                        "total_files": self.files_processed,
                        "pct": round(100 * files_with_data / self.files_processed, 1)
                        if self.files_processed > 0
                        else 0,
                        "payers": sorted_payers[:10],
                        "total_occurrences": sum(payer_counts.values()),
                        "type": "qualifier",
                    }

        return dict(sorted(unmapped.items(), key=lambda x: x[1]["files_with_data"], reverse=True))

    def get_summary(self) -> str:
        """Generate summary report of unmapped elements and qualifiers"""
        unmapped_elements = self.get_unmapped_elements()
        unmapped_qualifiers = self.get_unmapped_qualifiers()

        if not unmapped_elements and not unmapped_qualifiers:
            return "All EDI elements with data have CSV mappings."

        lines = []

        if unmapped_elements:
            lines.append(f"UNMAPPED EDI ELEMENTS ({len(unmapped_elements)} found)")
            lines.append("These EDI positions have data but no CSV column captures them:")
            lines.append("-" * 60)
            for key, info in unmapped_elements.items():
                lines.append(f"  {key}: data in {info['files_with_data']}/{info['total_files']} files ({info['pct']}%)")
                lines.append(f"       → {info['description']}")
                if info["payers"]:
                    lines.append(f"       Payers: {', '.join(f'{p[0]} ({p[1]})' for p in info['payers'][:5])}")

        if unmapped_qualifiers:
            if lines:
                lines.append("")
            lines.append(f"UNMAPPED QUALIFIER-BASED FIELDS ({len(unmapped_qualifiers)} found)")
            lines.append("These qualifier codes have data but no CSV column mapping:")
            lines.append("-" * 60)
            for key, info in unmapped_qualifiers.items():
                lines.append(f"  {key}: data in {info['files_with_data']}/{info['total_files']} files ({info['pct']}%)")
                lines.append(f"       → {info['total_occurrences']:,} total occurrences")
                if info["payers"]:
                    lines.append(f"       Payers: {', '.join(f'{p[0]} ({p[1]})' for p in info['payers'][:5])}")

        return "\n".join(lines)


class EDIFieldTracker:
    """Tracks which fields were extracted from EDI segments"""

    def __init__(self):
        self.segments_processed = []
        self.fields_extracted = defaultdict(dict)
        self.missing_mappings = defaultdict(set)
        self.claim_context = {}
        self.service_context = {}

    def track_segment(self, segment: str, elements: List[str], delimiter: str):
        """Track that a segment was processed"""
        seg_id = elements[0] if elements else ""
        self.segments_processed.append(
            {"segment_id": seg_id, "raw": segment, "elements": elements, "element_count": len(elements)}
        )

    def track_field(self, segment: str, position: int, value: Any, csv_field: str = None):
        """Track that a field was extracted"""
        key = f"{segment}_{position:02d}"
        self.fields_extracted[key] = {
            "segment": segment,
            "position": position,
            "value": value,
            "csv_field": csv_field,
            "timestamp": datetime.now(),
        }

    def track_missing_mapping(self, segment: str, qualifier: str, value: str, field_type: str):
        """Track unmapped codes/qualifiers"""
        self.missing_mappings[field_type].add((segment, qualifier, value))

    def set_claim_context(self, claim_id: str, payer_info: Dict):
        """Set current claim context for error reporting"""
        self.claim_context = {"claim_id": claim_id, "payer_info": payer_info}

    def set_service_context(self, service_info: Dict):
        """Set current service context"""
        self.service_context = service_info


class ZeroFailValidator:
    """Zero-tolerance validation for 835 to CSV conversion"""

    def __init__(self, debug: bool = False, payer_keys: Dict = None):
        self.errors = []
        self.warnings = []
        self.debug = debug
        self.payer_keys = payer_keys or {}  # Maps file index to payer key for overrides
        self.field_tracker = EDIFieldTracker()
        self.segment_maps = SegmentFieldMap.get_all_segments()
        self.qualifier_maps = SegmentFieldMap.get_qualifier_maps()
        self.debug_counts = defaultdict(lambda: defaultdict(int))
        self.debug_limit = 3
        if self.debug:
            logger.debug("ZeroFailValidator initialized with debug mode ENABLED")
            logger.debug("Debug output limited to failed validations only")
            logger.debug("Maximum 3 debug outputs per error type per payer")
        self.stats = {
            "total_segments": 0,
            "total_fields": 0,
            "fields_validated": 0,
            "calculations_checked": 0,
            "missing_mappings": defaultdict(list),
            "payers_missing_mileage_units": defaultdict(int),
            "payer_data_quality_issues": defaultdict(lambda: defaultdict(int)),
            "priority_rarc_codes": defaultdict(lambda: defaultdict(int)),  # Track payer-specific priority RARC codes
        }
        self.transaction_balance_tolerances = {
            "PROSPECT MEDICAL SYSTEMS": Decimal("10.00"),
            "PROSPECT HEALTH SOURCE": Decimal("10.00"),
            "PROSPECT MEDICAL SD": Decimal("10.00"),
            "PROSPECT MEDICAL": Decimal("10.00"),
            "EMPLOYERS MUTUAL": Decimal("1000.00"),
        }
        self.current_file_idx = 0  # Track current file for payer key lookup

    def _should_debug(self, payer_name: str, error_type: str) -> bool:
        """Check if debug output should be shown for this payer/error combination"""
        if not self.debug:
            return False
        if self.debug_counts[payer_name][error_type] >= self.debug_limit:
            return False
        self.debug_counts[payer_name][error_type] += 1
        return True

    def _get_transaction_tolerance(self, payer_name: str) -> Decimal:
        """Return payer-specific tolerance for transaction balancing."""
        if not payer_name:
            return Decimal("0.01")
        upper_name = payer_name.upper()
        for key, tolerance in self.transaction_balance_tolerances.items():
            if key in upper_name:
                return tolerance
        return Decimal("0.01")

    def _build_transaction_balance_context(
        self,
        transaction_segments: List[str],
        delimiter: str,
        check_total: Decimal,
        clp_payments: List[Decimal],
        plb_total: Decimal,
        expected: Decimal,
        diff: Decimal,
    ) -> List[str]:
        """
        Build comprehensive debugging context for transaction balance errors.

        Returns a list of formatted strings showing:
        - Calculation breakdown with formula
        - All BPR segments with parsed amounts
        - All PLB segments with parsed adjustment details
        - CLP summary with statistics and samples
        """
        context = []

        # === CALCULATION BREAKDOWN ===
        context.append("=" * 60)
        context.append("CALCULATION BREAKDOWN")
        context.append("=" * 60)
        context.append("")
        context.append("Formula: BPR02 = Sum(CLP04) - Sum(PLB adjustments)")
        context.append("")

        clp_sum = sum(clp_payments)
        context.append(f"  BPR02 (Check Amount):      ${float(check_total):,.2f}")
        context.append(f"  Sum of CLP04 ({len(clp_payments)} claims): ${float(clp_sum):,.2f}")
        context.append(f"  Sum of PLB adjustments:    ${float(plb_total):,.2f}")
        context.append("  ----------------------------------------")
        context.append(f"  Expected (CLP04 - PLB):    ${float(expected):,.2f}")
        context.append(f"  Actual (BPR02):            ${float(check_total):,.2f}")
        context.append(f"  DIFFERENCE:                ${float(diff):,.2f}")
        context.append("")

        # === BPR SEGMENTS ===
        context.append("=" * 60)
        context.append("BPR SEGMENTS (Check/Payment Information)")
        context.append("=" * 60)
        bpr_segments = []
        for seg in transaction_segments:
            if seg.split(delimiter)[0] == "BPR":
                bpr_segments.append(seg)
                elements = seg.split(delimiter)
                if len(elements) > 2:
                    try:
                        amt = Decimal(str(elements[2]))
                        context.append(f"  Amount: ${float(amt):,.2f}")
                    except (ValueError, TypeError):
                        context.append(f"  Amount: {elements[2]} (parse error)")
                context.append(f"  Raw: {seg}")
                context.append("")

        # === PLB SEGMENTS ===
        context.append("=" * 60)
        context.append("PLB SEGMENTS (Provider-Level Adjustments)")
        context.append("=" * 60)
        plb_segments = []
        plb_detail_total = Decimal("0")
        for seg in transaction_segments:
            if seg.split(delimiter)[0] == "PLB":
                plb_segments.append(seg)
                elements = seg.split(delimiter)
                context.append(f"  Raw: {seg}")
                context.append(f"  Provider ID: {elements[1] if len(elements) > 1 else 'N/A'}")
                context.append(f"  Fiscal Period: {elements[2] if len(elements) > 2 else 'N/A'}")
                # Parse adjustment pairs (reason:reference, amount)
                adj_num = 1
                for i in range(3, min(len(elements), 15), 2):
                    reason = elements[i] if i < len(elements) else ""
                    amount_str = elements[i + 1] if i + 1 < len(elements) else ""
                    if amount_str:
                        try:
                            amt = Decimal(str(amount_str))
                            plb_detail_total += amt
                            context.append(f"    Adjustment {adj_num}: ${float(amt):,.2f} ({reason})")
                        except (ValueError, TypeError):
                            context.append(f"    Adjustment {adj_num}: {amount_str} ({reason}) - PARSE ERROR")
                        adj_num += 1
                context.append("")

        if not plb_segments:
            context.append("  (No PLB segments in this transaction)")
            context.append("")

        if plb_segments:
            context.append(f"  PLB TOTAL: ${float(plb_detail_total):,.2f}")
            if plb_detail_total != plb_total:
                context.append(f"  WARNING: Parsed total differs from calculated total (${float(plb_total):,.2f})")
            context.append("")

        # === CLP SUMMARY ===
        context.append("=" * 60)
        context.append("CLP SEGMENTS (Claim Payment Summary)")
        context.append("=" * 60)
        context.append(f"  Total Claims: {len(clp_payments)}")
        if clp_payments:
            context.append(f"  Sum of Payments: ${float(clp_sum):,.2f}")
            context.append(f"  Min Payment: ${float(min(clp_payments)):,.2f}")
            context.append(f"  Max Payment: ${float(max(clp_payments)):,.2f}")
            context.append(f"  Avg Payment: ${float(clp_sum / len(clp_payments)):,.2f}")
            context.append("")

            # Show breakdown by payment sign (positive, negative, zero)
            positive = [p for p in clp_payments if p > 0]
            negative = [p for p in clp_payments if p < 0]
            zero = [p for p in clp_payments if p == 0]
            context.append("  Payment Distribution:")
            context.append(f"    Positive payments: {len(positive)} (${float(sum(positive)):,.2f})")
            context.append(f"    Negative payments: {len(negative)} (${float(sum(negative)):,.2f})")
            context.append(f"    Zero payments: {len(zero)}")
            context.append("")

        # === CLP SAMPLES ===
        context.append("=" * 60)
        context.append("CLP SEGMENT SAMPLES (First 10 claims)")
        context.append("=" * 60)
        clp_count = 0
        for seg in transaction_segments:
            elements = seg.split(delimiter)
            if elements[0] == "CLP":
                clp_count += 1
                if clp_count <= 10:
                    claim_id = elements[1] if len(elements) > 1 else "N/A"
                    status = elements[2] if len(elements) > 2 else "N/A"
                    charge = elements[3] if len(elements) > 3 else "N/A"
                    payment = elements[4] if len(elements) > 4 else "N/A"
                    context.append(f"  {clp_count}. Claim: {claim_id}")
                    context.append(f"     Status: {status}, Charge: ${charge}, Payment: ${payment}")
                    context.append(f"     Raw: {seg[:100]}{'...' if len(seg) > 100 else ''}")
                    context.append("")

        if clp_count > 10:
            context.append(f"  ... and {clp_count - 10} more CLP segments")
            context.append("")

        # === ANALYSIS ===
        context.append("=" * 60)
        context.append("ANALYSIS")
        context.append("=" * 60)
        context.append("")
        context.append("Possible causes of the discrepancy:")
        context.append("  1. Missing or extra CLP segments in the file")
        context.append("  2. PLB adjustments not fully captured")
        context.append("  3. Payer data error in the original 835 file")
        context.append("  4. Multiple BPR segments with amounts not summed correctly")
        context.append("")

        return context

    def _build_claim_balance_context(
        self,
        claim_id: str,
        claim_row: dict,
        all_rows: List[dict],
        claim_charge: Decimal,
        claim_payment: Decimal,
        claim_adj_total: Decimal,
        expected: Decimal,
    ) -> List[str]:
        """
        Build comprehensive debugging context for claim balance errors.

        Returns a list of formatted strings showing:
        - Calculation breakdown with formula
        - Claim-level CAS adjustments
        - Service line details with their adjustments
        """
        context = []
        diff = abs(claim_payment - expected)

        # === CALCULATION BREAKDOWN ===
        context.append("=" * 60)
        context.append("CLAIM BALANCE CALCULATION BREAKDOWN")
        context.append("=" * 60)
        context.append("")
        context.append(f"Claim ID: {claim_id}")
        context.append("")
        context.append("Formula: CLP03 (Charge) - CAS Adjustments = CLP04 (Payment)")
        context.append("")

        context.append(f"  CLP03 (Charge Amount):     ${float(claim_charge):,.2f}")
        context.append(f"  Total CAS Adjustments:     ${float(claim_adj_total):,.2f}")
        context.append("  ----------------------------------------")
        context.append(f"  Expected Payment:          ${float(expected):,.2f}")
        context.append(f"  CLP04 (Actual Payment):    ${float(claim_payment):,.2f}")
        context.append(f"  DIFFERENCE:                ${float(diff):,.2f}")
        context.append("")

        # === CLAIM-LEVEL CAS ADJUSTMENTS ===
        context.append("=" * 60)
        context.append("CLAIM-LEVEL CAS ADJUSTMENTS")
        context.append("=" * 60)
        claim_level_adj_total = Decimal("0")
        has_claim_cas = False
        for cas_idx in range(1, 6):
            group_field = f"CLM_CAS{cas_idx}_Group_L2100_CAS"
            reason_field = f"CLM_CAS{cas_idx}_Reason_L2100_CAS"
            amount_field = f"CLM_CAS{cas_idx}_Amount_L2100_CAS"
            group = claim_row.get(group_field, "")
            reason = claim_row.get(reason_field, "")
            amount_str = claim_row.get(amount_field, "")
            if amount_str:
                has_claim_cas = True
                try:
                    amount = self._parse_currency_decimal(amount_str)
                    claim_level_adj_total += amount
                    context.append(f"  CAS {cas_idx}: ${float(amount):,.2f}")
                    context.append(f"    Group: {group}, Reason: {reason}")
                except (ValueError, TypeError):
                    context.append(f"  CAS {cas_idx}: {amount_str} (parse error)")
                    context.append(f"    Group: {group}, Reason: {reason}")
        if not has_claim_cas:
            context.append("  (No claim-level CAS adjustments)")
        else:
            context.append("  ----------------------------------------")
            context.append(f"  Claim-Level CAS Total: ${float(claim_level_adj_total):,.2f}")
        context.append("")

        # === SERVICE LINE DETAILS ===
        context.append("=" * 60)
        context.append("SERVICE LINE DETAILS")
        context.append("=" * 60)
        service_rows = [r for r in all_rows if r.get("SVC_ChargeAmount_L2110_SVC")]
        if not service_rows:
            context.append("  (No service lines for this claim)")
        else:
            context.append(f"  Total Service Lines: {len(service_rows)}")
            context.append("")
            svc_adj_total = Decimal("0")
            for svc_idx, svc_row in enumerate(service_rows, 1):
                svc_proc = svc_row.get("SVC_Procedure_L2110_SVC", "N/A")
                svc_charge = svc_row.get("SVC_ChargeAmount_L2110_SVC", "0")
                svc_payment = svc_row.get("SVC_PaymentAmount_L2110_SVC", "0")
                svc_units = svc_row.get("SVC_Units_L2110_SVC", "")
                context.append(f"  Service {svc_idx}: {svc_proc}")
                context.append(f"    Charge: ${svc_charge}, Payment: ${svc_payment}, Units: {svc_units}")

                # Service-level CAS adjustments
                svc_line_adj = Decimal("0")
                svc_cas_parts = []
                for cas_idx in range(1, 6):
                    cas_amount_field = f"SVC_CAS{cas_idx}_Amount_L2110_CAS"
                    cas_group_field = f"SVC_CAS{cas_idx}_Group_L2110_CAS"
                    cas_reason_field = f"SVC_CAS{cas_idx}_Reason_L2110_CAS"
                    cas_amt = svc_row.get(cas_amount_field, "")
                    if cas_amt:
                        try:
                            amt = self._parse_currency_decimal(cas_amt)
                            svc_line_adj += amt
                            svc_adj_total += amt
                            group = svc_row.get(cas_group_field, "")
                            reason = svc_row.get(cas_reason_field, "")
                            svc_cas_parts.append(f"${float(amt):,.2f} ({group}/{reason})")
                        except (ValueError, TypeError):
                            svc_cas_parts.append(f"{cas_amt} (parse error)")
                if svc_cas_parts:
                    context.append(f"    CAS Adjustments: {', '.join(svc_cas_parts)}")
                context.append("")

            context.append(f"  Service-Level CAS Total: ${float(svc_adj_total):,.2f}")
            context.append("")

        # === RECONCILIATION ===
        context.append("=" * 60)
        context.append("ADJUSTMENT RECONCILIATION")
        context.append("=" * 60)
        service_rows = [r for r in all_rows if r.get("SVC_ChargeAmount_L2110_SVC")]
        recalc_svc_adj = Decimal("0")
        for svc_row in service_rows:
            for cas_idx in range(1, 6):
                cas_amt = svc_row.get(f"SVC_CAS{cas_idx}_Amount_L2110_CAS", "")
                if cas_amt:
                    try:
                        recalc_svc_adj += self._parse_currency_decimal(cas_amt)
                    except (ValueError, TypeError):
                        pass
        context.append(f"  Claim-Level CAS:   ${float(claim_level_adj_total):,.2f}")
        context.append(f"  Service-Level CAS: ${float(recalc_svc_adj):,.2f}")
        context.append("  ----------------------------------------")
        context.append(f"  Total Adjustments: ${float(claim_level_adj_total + recalc_svc_adj):,.2f}")
        context.append(f"  (Reported Total:   ${float(claim_adj_total):,.2f})")
        context.append("")

        # === RAW DATA ===
        context.append("=" * 60)
        context.append("RAW CSV DATA (Key Fields)")
        context.append("=" * 60)
        context.append(f"  Payer: {claim_row.get('Payer_Name_L1000A_N1', 'N/A')}")
        context.append(f"  Claim Status: {claim_row.get('CLM_Status_L2100_CLP', 'N/A')}")
        context.append(f"  Patient Name: {claim_row.get('Patient_Name_L2100_NM1', 'N/A')}")
        context.append(f"  Service Date: {claim_row.get('CLM_ServiceStartDate_L2100_DTM', 'N/A')}")
        context.append("")

        return context

    def _get_payer_key_for_file(self, file_idx: int) -> str:
        """Get the payer key for a given file index."""
        return self.payer_keys.get(file_idx)

    def _allows_generic_payer_id(self, file_idx: int) -> bool:
        """Check if the payer for this file allows generic payer IDs (e.g., 999999)."""
        payer_key = self._get_payer_key_for_file(file_idx)
        if payer_key:
            return colloquial.allows_generic_payer_id(payer_key)
        return False

    def _is_priority_rarc(self, file_idx: int, rarc_code: str) -> bool:
        """Check if a RARC code is a priority code for this file's payer."""
        payer_key = self._get_payer_key_for_file(file_idx)
        if payer_key:
            return colloquial.is_payer_priority_rarc(payer_key, rarc_code)
        return False

    def _is_field_not_used_for_payer(self, payer_key: str, field_name: str) -> bool:
        """
        Check if a field is marked as 'Not Used' for a specific payer.

        Some payers (like Indiana Medicaid/IHCP) explicitly do not populate certain
        optional fields per their companion guides. This method checks the payer's
        parsing_rules to determine if missing/empty data for a field should be
        suppressed from validation warnings.

        Args:
            payer_key: The payer identifier (e.g., "INDIANA_MEDICAID")
            field_name: The field name to check (e.g., "SVC_Modifier1_L2110_SVC")

        Returns:
            True if the field is marked as 'not used' for this payer
        """
        if not payer_key:
            return False

        parsing_rules = colloquial.get_parsing_rules(payer_key)
        if not parsing_rules:
            return False

        # Map field names to parsing_rules keys
        # Indiana Medicaid: SVC01-7 (Procedure Code Description) not used
        # Indiana Medicaid: SVC06 (Original Procedure Code) not used
        field_to_rule_map = {
            # SVC01-7: Procedure Code Description
            "SVC_CodeDescription_L2110_SVC": "svc01_7_not_used",
            "SVC_ProcedureCodeDescription_L2110_SVC": "svc01_7_not_used",
            # SVC06: Original/Adjudicated Procedure Code composite
            "SVC_OriginalProcedure_L2110_SVC": "svc06_not_used",
            "SVC_OriginalProcedureCode_L2110_SVC": "svc06_not_used",
            "SVC_AdjudicatedProcedure_L2110_SVC": "svc06_not_used",
        }

        rule_key = field_to_rule_map.get(field_name)
        if rule_key:
            return parsing_rules.get(rule_key, False)

        return False

    def _get_payer_key_from_name(self, payer_name: str) -> str:
        """
        Identify payer key from payer name for validation purposes.

        Args:
            payer_name: The payer name from N1*PR segment

        Returns:
            Payer key if identified, None otherwise
        """
        if not payer_name:
            return None
        return colloquial.identify_payer(payer_name=payer_name)

    def _parse_currency(self, value) -> float:
        """Parse a currency string (e.g., '-$1,712.00') to float."""
        if value is None or value == "":
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        # Remove currency formatting: $, commas, and handle negatives
        s = str(value).strip()
        s = s.replace("$", "").replace(",", "")
        if not s:
            return 0.0
        try:
            return float(s)
        except ValueError:
            return 0.0

    def _parse_currency_decimal(self, value) -> Decimal:
        """Parse a currency string (e.g., '-$1,712.00') to Decimal for precision."""
        if value is None or value == "":
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        # Remove currency formatting: $, commas, and handle negatives
        s = str(value).strip()
        s = s.replace("$", "").replace(",", "")
        if not s:
            return Decimal("0")
        try:
            return Decimal(s)
        except (ValueError, InvalidOperation, Overflow):
            return Decimal("0")

    def _derive_mileage_units(self, proc_code: str, charge_amt: float) -> float:
        """
        Derive mileage units from charge amount when SVC05/SVC07 are missing.

        Per CMS fee schedule (approximate rates):
        - A0425: Ground mileage ~$18/mile
        - A0435: Fixed wing mileage ~$19/mile
        - A0436: Rotor wing mileage ~$36/mile

        Returns estimated units or None if cannot derive.
        """
        if charge_amt <= 0:
            return None

        # Approximate per-mile rates from CMS fee schedule
        rate_map = {
            "A0425": 18.0,  # Ground ambulance mileage
            "A0435": 19.0,  # Fixed wing air mileage
            "A0436": 36.0,  # Rotary wing air mileage
        }

        rate = rate_map.get(proc_code)
        if rate:
            return round(charge_amt / rate, 1)
        return None

    def _normalize_amount(self, edi_value: str) -> str:
        """Convert EDI amount to CSV format (handle implied decimals)"""
        if not edi_value:
            return ""
        return edi_value

    def _is_amount_field(self, csv_field: str, seg_id: str, pos: str) -> bool:
        """Check if a field contains monetary amounts that need normalization"""
        if seg_id == "BPR" and pos == "02":
            return True
        if seg_id == "CLP" and pos in ["03", "04", "05"]:
            return True
        if seg_id == "SVC" and pos in ["02", "03"]:
            return True
        if seg_id == "MOA" and pos in ["02", "08"]:
            return True
        if seg_id == "MIA" and pos in [
            "02",
            "04",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
            "13",
            "14",
            "16",
            "17",
            "18",
            "19",
            "24",
        ]:
            return True
        if seg_id == "PLB" and pos in ["04", "06", "08", "10", "12", "14"]:
            return True
        return False

    def _parse_cas_from_segments(self, segments_list: List[str], delimiter: str) -> List[dict]:
        """
        Parse CAS segments into structured adjustment data
        CAS format: CAS*GroupCode*ReasonCode1*Amount1*Qty1*ReasonCode2*Amount2*Qty2*...
        Can have up to 6 reason/amount/quantity triplets per CAS segment

        Note: Reason codes are normalized using colloquial.normalize_carc_code() to handle
        payer-specific variations like Medi-Cal's leading zero codes (e.g., 0012 -> 12).
        """
        cas_entries = []
        for segment in segments_list:
            if not segment.strip().startswith("CAS"):
                continue
            elements = segment.split(delimiter)
            if len(elements) < 4:
                continue
            group_code = elements[1] if len(elements) > 1 else ""
            idx = 2
            while idx + 1 < len(elements):
                reason_code = elements[idx].strip() if idx < len(elements) else ""
                amount = elements[idx + 1].strip() if idx + 1 < len(elements) else ""
                quantity = elements[idx + 2].strip() if idx + 2 < len(elements) else ""
                if reason_code and amount:
                    # Normalize reason code to handle payer-specific variations (e.g., leading zeros)
                    normalized_reason = colloquial.normalize_carc_code(reason_code)
                    cas_entries.append(
                        {
                            "group_code": group_code,
                            "reason_code": normalized_reason,
                            "amount": amount,
                            "quantity": quantity,
                        }
                    )
                idx += 3
                if idx > 19:
                    break
        return cas_entries

    def _validate_cas_adjustments(
        self, claim_id: str, rows: List[dict], edi_cas_segments: List[str], level: str, delimiter: str
    ):
        """
        Validate that CAS adjustment categorization in CSV matches EDI source
        X12 835 Section 1.10.2.4 Claim Adjustment and Service Adjustment Segment Theory:
        'The Claim Adjustment and Service Adjustment Segments provide the reasons,
        amounts, and quantities of any adjustments that the payer made either to
        the original submitted charge or to the units related to the claim or service(s).'
        CAS01 = Group Code (PR, CO, PI, OA)
        CAS02, 05, 08, 11, 14, 17 = Reason Codes
        CAS03, 06, 09, 12, 15, 18 = Monetary Amounts
        CAS04, 07, 10, 13, 16, 19 = Quantities (optional)
        Args:
            claim_id: Claim identifier
            rows: CSV rows for this claim
            edi_cas_segments: Raw CAS segments from EDI
            level: 'claim' or 'service'
            delimiter: EDI element delimiter
        """
        if not edi_cas_segments:
            return
        cas_entries = self._parse_cas_from_segments(edi_cas_segments, delimiter)
        if not cas_entries:
            return
        expected_categories = {
            "Contractual": Decimal("0"),
            "Copay": Decimal("0"),
            "Coinsurance": Decimal("0"),
            "Deductible": Decimal("0"),
            "Denied": Decimal("0"),
            "OtherAdjustments": Decimal("0"),
            "Sequestration": Decimal("0"),
            "COB": Decimal("0"),
            "HCRA": Decimal("0"),
            "QMB": Decimal("0"),
            "PR_NonCovered": Decimal("0"),
            "OtherPatientResp": Decimal("0"),
        }
        for cas in cas_entries:
            categories = categorize_adjustment(cas["group_code"], cas["reason_code"], cas["amount"])
            for cat, amt in categories.items():
                if cat == "AuditFlag":
                    continue  # Skip audit flag - it's a string, not an amount
                expected_categories[cat] += Decimal(str(amt))
        prefix = "CLM" if level == "claim" else "SVC"
        suffix = "L2100_CAS" if level == "claim" else "L2110_CAS"
        if level == "claim":
            target_row = next(
                (
                    r
                    for r in rows
                    if r.get("CLM_PatientControlNumber_L2100_CLP") == claim_id
                    and not r.get("SVC_ProcedureCode_L2110_SVC")
                ),
                None,
            )
        else:
            target_row = next(
                (
                    r
                    for r in rows
                    if r.get("CLM_PatientControlNumber_L2100_CLP") == claim_id and r.get("SVC_ProcedureCode_L2110_SVC")
                ),
                None,
            )
        if not target_row:
            return
        for category, expected_amt in expected_categories.items():
            if expected_amt == 0:
                continue
            field_name = f"{prefix}_{category}_{suffix}"
            actual_value = target_row.get(field_name, "")
            actual_amt = self._parse_currency_decimal(actual_value) if actual_value else Decimal("0")
            if abs(expected_amt - actual_amt) > Decimal("0.01"):
                self.errors.append(
                    ValidationError(
                        "CAS_CATEGORY",
                        f"CAS {category} categorization mismatch",
                        location=f"Claim {claim_id}" + (f" ({level} level)" if level == "service" else ""),
                        field=field_name,
                        expected=float(expected_amt),
                        actual=float(actual_amt),
                    )
                )
        self.stats["calculations_checked"] += 1

    def validate_all(
        self,
        edi_segments: List[str],
        csv_rows: List[dict],
        delimiter: str = "*",
        enable_warnings: bool = True,
        verbose: bool = False,
        status_callback=None,
    ) -> Dict:
        """Run complete validation suite per X12 835 Implementation Guide
        Validates:
        1. Service Line Balancing (X12 Section 1.10.2.1.1)
        2. Claim Balancing (X12 Section 1.10.2.1.2)
        3. Transaction Balancing (X12 Section 1.10.2.1.3)
        4. CAS Adjustments (X12 Section 1.10.2.4)
        5. Date Logic (DTM segments per X12 Loop specifications)
        6. Claim Status Codes (CLP02 per X12 spec)
        """

        def update_status(msg):
            if verbose:
                logger.info(msg)
            if status_callback:
                status_callback(msg)

        self.errors = []
        self.warnings = []
        self.stats = {
            "total_segments": len(edi_segments),
            "total_fields": 0,
            "fields_validated": 0,
            "calculations_checked": 0,
            "missing_mappings": defaultdict(list),
            "payers_missing_mileage_units": defaultdict(int),
            "payer_data_quality_issues": defaultdict(lambda: defaultdict(int)),
            "priority_rarc_codes": defaultdict(lambda: defaultdict(int)),
        }

        # Normalize CSV rows to use internal field names (convert display names back)
        csv_rows = normalize_csv_rows(csv_rows)

        update_status("  [1/10] Parsing EDI structure...")
        edi_data = self._parse_edi_data(edi_segments, delimiter)
        update_status("  [2/10] Validating loop structure...")
        self._validate_loop_structure(edi_data)
        update_status("  [3/10] Validating segment sequences...")
        self._validate_critical_sequences(edi_segments, delimiter)
        update_status("  [4/10] Grouping claims...")
        csv_by_claim = self._group_csv_by_claim(csv_rows)
        update_status("  [5/10] Validating calculations & balancing...")
        self._validate_calculations(csv_rows, csv_by_claim, edi_segments, delimiter, edi_data, verbose=verbose)
        update_status("  [6/10] Validating field completeness...")
        self._validate_completeness(edi_data, csv_rows, delimiter, verbose=verbose)
        update_status("  [7/10] Validating composite fields...")
        self._validate_composite_fields(edi_data, csv_rows)
        update_status("  [8/10] Validating field mappings & descriptions...")
        self._validate_field_mappings(csv_rows, verbose=verbose)
        self._validate_description_fields(csv_rows, verbose=verbose)
        update_status("  [9/10] Validating date formats...")
        self._validate_date_formats(csv_rows, verbose=verbose)
        update_status("  [10/10] Validating special cases...")
        self._validate_edge_cases(csv_rows, csv_by_claim, verbose=verbose)
        update_status("  Building validation report...")
        return self._build_validation_report(edi_segments, csv_rows)

    def validate_all_by_file(
        self, edi_segments_by_file: List[dict], csv_rows: List[dict], verbose: bool = False, status_callback=None
    ) -> Dict:
        """Run validation processing each file separately (like the parser does).

        This avoids the duplicate claim ID problem by using file|claim_id as keys.

        Args:
            edi_segments_by_file: List of {file, segments, delimiter} dicts
            csv_rows: List of CSV row dictionaries
            verbose: Enable verbose output
            status_callback: Optional callback for GUI status updates
        """

        def update_status(msg):
            if verbose:
                logger.info(msg)
            if status_callback:
                status_callback(msg)

        self.errors = []
        self.warnings = []

        # Count total segments across all files
        total_segments = sum(len(fd["segments"]) for fd in edi_segments_by_file)
        self.stats = {
            "total_segments": total_segments,
            "total_fields": 0,
            "fields_validated": 0,
            "calculations_checked": 0,
            "missing_mappings": defaultdict(list),
            "payers_missing_mileage_units": defaultdict(int),
            "payer_data_quality_issues": defaultdict(lambda: defaultdict(int)),
            "priority_rarc_codes": defaultdict(lambda: defaultdict(int)),
        }

        # Normalize CSV rows to use internal field names (convert display names back)
        csv_rows = normalize_csv_rows(csv_rows)

        update_status("  [1/10] Parsing EDI structure (per-file)...")
        edi_data = self._parse_edi_data_by_file(edi_segments_by_file)

        # Combine segments for reporting, but validate per-file to handle different delimiters
        all_segments = []
        default_delimiter = "*"
        for idx, file_data in enumerate(edi_segments_by_file):
            all_segments.extend(file_data["segments"])
            if idx == 0:
                default_delimiter = file_data.get("delimiter", "*")

        update_status("  [2/10] Validating loop structure...")
        self._validate_loop_structure(edi_data)
        update_status("  [3/10] Validating segment sequences...")
        # Validate each file separately with its own delimiter
        for file_data in edi_segments_by_file:
            file_segments = file_data["segments"]
            file_delimiter = file_data.get("delimiter", "*")
            self._validate_critical_sequences(file_segments, file_delimiter)
        update_status("  [4/10] Grouping claims...")
        csv_by_claim = self._group_csv_by_claim(csv_rows)
        update_status("  [5/10] Validating calculations & balancing...")
        # Pass edi_segments_by_file for per-file delimiter handling
        self._validate_calculations(
            csv_rows,
            csv_by_claim,
            all_segments,
            default_delimiter,
            edi_data,
            verbose=verbose,
            edi_segments_by_file=edi_segments_by_file,
        )
        update_status("  [6/10] Validating field completeness...")
        self._validate_completeness(edi_data, csv_rows, edi_segments_by_file, verbose=verbose)
        update_status("  [7/10] Validating composite fields (per-file)...")
        self._validate_composite_fields_by_file(edi_data, csv_rows)
        update_status("  [8/10] Validating field mappings & descriptions...")
        self._validate_field_mappings(csv_rows, verbose=verbose)
        self._validate_description_fields(csv_rows, verbose=verbose)
        update_status("  [9/10] Validating date formats...")
        self._validate_date_formats(csv_rows, verbose=verbose)
        update_status("  [10/10] Validating special cases...")
        self._validate_edge_cases(csv_rows, csv_by_claim, verbose=verbose)
        update_status("  Building validation report...")
        return self._build_validation_report(all_segments, csv_rows)

    def _parse_edi_data_by_file(self, edi_segments_by_file: List[dict]) -> Dict:
        """Parse EDI segments with file context - prevents duplicate claim ID overwrites.

        Uses file|claim_id as keys, matching how the parser processes files.
        """
        edi_data = {
            "header": {},
            "payer_loop": {},
            "payee_loop": {},
            "claims": defaultdict(dict),
            "claims_by_file": defaultdict(lambda: defaultdict(dict)),  # file -> claim_id -> data
            "services": defaultdict(list),
            "segment_counts": defaultdict(int),
            "all_segments": [],
            "current_loop": "header",
        }

        for file_data in edi_segments_by_file:
            file_name = file_data["file"]
            segments = file_data["segments"]
            delimiter = file_data.get("delimiter", "*")

            current_claim_key = None  # claim_id|occurrence key
            current_service = None
            current_loop_level = "header"

            # Track claim occurrences within this file (same claim can appear multiple times)
            claim_occurrence_tracker = {}

            for segment in segments:
                if not segment.strip():
                    continue
                elements = segment.split(delimiter)
                seg_id = elements[0]
                self.field_tracker.track_segment(segment, elements, delimiter)
                edi_data["segment_counts"][seg_id] += 1
                edi_data["all_segments"].append(
                    {"segment": segment, "elements": elements, "seg_id": seg_id, "file": file_name}
                )
                self.stats["total_fields"] += len(elements)

                if seg_id == "CLP":
                    current_loop_level = "claim"
                    base_claim_id = elements[1] if len(elements) > 1 else "UNKNOWN"

                    # Track occurrence (same claim can appear multiple times: reversal, correction, etc.)
                    if base_claim_id not in claim_occurrence_tracker:
                        claim_occurrence_tracker[base_claim_id] = 0
                    claim_occurrence_tracker[base_claim_id] += 1
                    occurrence = claim_occurrence_tracker[base_claim_id]

                    # Use claim_id|occurrence as the key to handle duplicates
                    current_claim_key = f"{base_claim_id}|{occurrence}"
                    current_service = None

                    # Normalize file path to basename for consistent matching with CSV
                    normalized_file = self._normalize_file_path(file_name)
                    # Store by file AND claim_id|occurrence (like the parser does)
                    edi_data["claims_by_file"][normalized_file][current_claim_key] = {
                        "segments": {"CLP": elements},
                        "services": [],
                        "file": normalized_file,
                        "occurrence": occurrence,
                    }
                    # Also store with composite key for backward compatibility
                    composite_key = f"{normalized_file}|{current_claim_key}"
                    edi_data["claims"][composite_key] = edi_data["claims_by_file"][normalized_file][current_claim_key]

                elif seg_id == "SVC" and current_claim_key:
                    current_loop_level = "service"
                    normalized_file = self._normalize_file_path(file_name)
                    current_service = len(edi_data["claims_by_file"][normalized_file][current_claim_key]["services"])
                    service_data = {"SVC": elements, "segments": {}}
                    edi_data["claims_by_file"][normalized_file][current_claim_key]["services"].append(service_data)

                elif current_service is not None and current_claim_key:
                    normalized_file = self._normalize_file_path(file_name)
                    services = edi_data["claims_by_file"][normalized_file][current_claim_key]["services"]
                    if services:
                        if seg_id in ["DTM", "CAS", "REF", "AMT", "QTY", "LQ"]:
                            services[-1]["segments"][seg_id] = elements
                        else:
                            current_service = None
                            edi_data["claims_by_file"][normalized_file][current_claim_key]["segments"][seg_id] = (
                                elements
                            )
                elif current_claim_key and current_service is None:
                    normalized_file = self._normalize_file_path(file_name)
                    if seg_id not in ["BPR", "TRN", "N1", "N2", "N3", "N4", "PER", "REF", "CUR", "RDM"]:
                        edi_data["claims_by_file"][normalized_file][current_claim_key]["segments"][seg_id] = elements
                else:
                    if current_loop_level == "payer":
                        if seg_id not in ["CLP", "SVC"]:
                            # Keep first file's values to match CSV extraction (uses first row)
                            if seg_id not in edi_data["payer_loop"]:
                                edi_data["payer_loop"][seg_id] = elements
                    elif current_loop_level == "payee":
                        if seg_id not in ["CLP", "SVC"]:
                            if seg_id not in edi_data["payee_loop"]:
                                edi_data["payee_loop"][seg_id] = elements
                    else:
                        if seg_id not in ["CLP", "SVC"]:
                            if seg_id not in edi_data["header"]:
                                edi_data["header"][seg_id] = elements

        return edi_data

    def _validate_composite_fields_by_file(self, edi_data: Dict, csv_rows: List[dict]):
        """Validate composite fields using file context - matches how parser works."""
        # Group CSV rows by file and claim|occurrence (normalize file paths to basename)
        # This matches how the parser tracks claim occurrences for reversals/adjustments
        csv_by_file_and_claim = defaultdict(lambda: defaultdict(list))
        for r in csv_rows:
            claim_id = r.get("CLM_PatientControlNumber_L2100_CLP")
            occurrence = r.get("CLM_Occurrence_L2100_CLP", 1)
            file_name = self._normalize_file_path(r.get("Filename_File", ""))
            if claim_id and r.get("SVC_ProcedureCode_L2110_SVC"):
                # Key by claim_id|occurrence to match EDI parsing
                claim_key = f"{claim_id}|{occurrence}"
                csv_by_file_and_claim[file_name][claim_key].append(r)

        # Get component delimiter from first CSV row
        component_delimiter = ":"
        if csv_rows:
            component_delimiter = csv_rows[0].get("ENV_ComponentSeparator_Envelope_ISA", ":") or ":"

        # Validate per-file (like the parser processes)
        for file_name, claims in edi_data.get("claims_by_file", {}).items():
            csv_claims = csv_by_file_and_claim.get(file_name, {})

            for claim_key, claim_data in claims.items():
                # claim_key is now "claim_id|occurrence"
                service_rows = csv_claims.get(claim_key, [])

                # Extract display-friendly claim ID and occurrence
                if "|" in claim_key:
                    display_claim_id, occurrence = claim_key.rsplit("|", 1)
                    display_location = f"Claim {display_claim_id} (occurrence {occurrence})"
                else:
                    display_claim_id = claim_key
                    display_location = f"Claim {display_claim_id}"

                for svc_idx, service_data in enumerate(claim_data.get("services", [])):
                    svc_segment = service_data.get("SVC")
                    if not svc_segment or len(svc_segment) < 2:
                        continue

                    svc01_composite = svc_segment[1]
                    components = svc01_composite.split(component_delimiter)
                    if len(components) < 2:
                        continue

                    expected_code = components[1] if len(components) > 1 else ""
                    expected_modifiers = components[2:6]

                    if svc_idx >= len(service_rows):
                        payer_name = "Unknown"
                        payer_state = "Unknown State"
                        if service_rows:
                            payer_name = service_rows[0].get("Payer_Name_L1000A_N1", "Unknown")
                            payer_state = service_rows[0].get("Payer_State_L1000A_N4", "Unknown State")
                        self.errors.append(
                            ValidationError(
                                "COMPOSITE_PARSE",
                                f"Service {svc_idx + 1} exists in EDI but not in CSV",
                                location=f"File: {file_name}, {display_location}, Service {svc_idx + 1}",
                                expected=f"Service with code {expected_code}",
                                actual="Not found in CSV",
                                payer_info={"name": payer_name, "state": payer_state},
                            )
                        )
                        continue

                    csv_row = service_rows[svc_idx]
                    payer_name = csv_row.get("Payer_Name_L1000A_N1", "Unknown")
                    payer_state = csv_row.get("Payer_State_L1000A_N4", "Unknown State")
                    payer_info = {"name": payer_name, "state": payer_state}
                    actual_code = csv_row.get("SVC_ProcedureCode_L2110_SVC", "")

                    if expected_code != actual_code:
                        self.errors.append(
                            ValidationError(
                                "COMPOSITE_PARSE",
                                "Procedure code not correctly extracted from composite field",
                                location=f"File: {file_name}, {display_location}, Service {svc_idx + 1}",
                                field="SVC_ProcedureCode_L2110_SVC",
                                expected=expected_code,
                                actual=actual_code,
                                payer_info=payer_info,
                            )
                        )

                    for mod_idx, expected_mod in enumerate(expected_modifiers, 1):
                        if not expected_mod:
                            continue
                        field_name = f"SVC_Modifier{mod_idx}_L2110_SVC"
                        actual_mod = csv_row.get(field_name, "")
                        if expected_mod != actual_mod:
                            self.errors.append(
                                ValidationError(
                                    "COMPOSITE_PARSE",
                                    f"Modifier {mod_idx} not correctly extracted from composite",
                                    location=f"File: {file_name}, {display_location}, Service {svc_idx + 1}",
                                    field=field_name,
                                    expected=expected_mod,
                                    actual=actual_mod,
                                    payer_info=payer_info,
                                )
                            )

    def _parse_edi_data(self, segments: List[str], delimiter: str) -> Dict:
        """Parse all EDI segments into structured data"""
        edi_data = {
            "header": {},
            "payer_loop": {},
            "payee_loop": {},
            "claims": defaultdict(dict),
            "services": defaultdict(list),
            "segment_counts": defaultdict(int),
            "all_segments": [],
            "current_loop": "header",
        }

        current_claim = None
        current_service = None

        current_loop_level = "header"
        for segment in segments:
            if not segment.strip():
                continue
            elements = segment.split(delimiter)
            seg_id = elements[0]
            self.field_tracker.track_segment(segment, elements, delimiter)
            edi_data["segment_counts"][seg_id] += 1
            edi_data["all_segments"].append({"segment": segment, "elements": elements, "seg_id": seg_id})
            self.stats["total_fields"] += len(elements)
            if seg_id == "N1" and len(elements) > 1:
                entity_code = elements[1]
                if entity_code == "PR":
                    current_loop_level = "payer"
                elif entity_code == "PE":
                    current_loop_level = "payee"
            if seg_id == "CLP":
                current_loop_level = "claim"
                current_claim = elements[1] if len(elements) > 1 else "UNKNOWN"
                current_service = None
                edi_data["claims"][current_claim] = {"segments": {"CLP": elements}, "services": []}
            elif seg_id == "SVC" and current_claim:
                current_loop_level = "service"
                current_service = len(edi_data["claims"][current_claim]["services"])
                service_data = {"SVC": elements, "segments": {}}
                edi_data["claims"][current_claim]["services"].append(service_data)
            elif current_service is not None and current_claim:
                services = edi_data["claims"][current_claim]["services"]
                if services:
                    if seg_id in ["DTM", "CAS", "REF", "AMT", "QTY", "LQ"]:
                        services[-1]["segments"][seg_id] = elements
                    else:
                        current_service = None
                        edi_data["claims"][current_claim]["segments"][seg_id] = elements
            elif current_claim and current_service is None:
                if "segments" not in edi_data["claims"][current_claim]:
                    edi_data["claims"][current_claim]["segments"] = {}
                if seg_id not in ["BPR", "TRN", "N1", "N2", "N3", "N4", "PER", "REF", "CUR", "RDM"]:
                    edi_data["claims"][current_claim]["segments"][seg_id] = elements
            else:
                if current_loop_level == "payer":
                    if seg_id not in ["CLP", "SVC"]:
                        # Keep first file's values to match CSV extraction (uses first row)
                        if seg_id not in edi_data["payer_loop"]:
                            edi_data["payer_loop"][seg_id] = elements
                elif current_loop_level == "payee":
                    if seg_id not in ["CLP", "SVC"]:
                        if seg_id not in edi_data["payee_loop"]:
                            edi_data["payee_loop"][seg_id] = elements
                else:
                    if seg_id not in ["CLP", "SVC"]:
                        if seg_id not in edi_data["header"]:
                            edi_data["header"][seg_id] = elements
        return edi_data

    def _group_csv_by_claim(self, csv_rows: List[dict]) -> Dict[str, List[dict]]:
        """Group CSV rows by (file, claim_id, occurrence) composite key to handle:
        - Duplicate claim IDs across files (COB scenarios)
        - Multiple occurrences of same claim (reversals, corrections)
        """
        grouped = defaultdict(list)
        for row in csv_rows:
            claim_id = row.get("CLM_PatientControlNumber_L2100_CLP", "UNKNOWN")
            file_name = row.get("Filename_File", "")
            occurrence = row.get("CLM_Occurrence_L2100_CLP", "1")
            occurrence = str(occurrence).strip() if occurrence else "1"
            if file_name:
                # Normalize file path to basename for consistent matching
                normalized_file = self._normalize_file_path(file_name)
                composite_key = f"{normalized_file}|{claim_id}|{occurrence}"
            else:
                composite_key = f"{claim_id}|{occurrence}"
            grouped[composite_key].append(row)
        return grouped

    def _normalize_file_path(self, file_path: str) -> str:
        """Normalize file path to just basename for consistent matching.

        This handles cases where CSV has paths with '_testing' folder but
        EDI files are read from original location. Also normalizes case
        since Windows is case-insensitive but Python string comparison is not.
        """
        if not file_path:
            return ""
        # Extract just the filename (basename) and normalize to uppercase for consistent matching
        import os

        return os.path.basename(file_path).upper()

    def _split_transactions(self, segments: List[str], delimiter: str) -> List[List[str]]:
        """Split file segments into ST/SE transaction segments."""
        transactions = []
        current = []
        in_transaction = False
        for segment in segments:
            seg_id = segment.split(delimiter)[0] if delimiter in segment else segment.split("*")[0]
            if seg_id == "ST":
                if current:
                    transactions.append(current)
                    current = []
                in_transaction = True
            current.append(segment)
            if seg_id == "SE" and in_transaction:
                transactions.append(current)
                current = []
                in_transaction = False
        if current:
            transactions.append(current)
        if not transactions:
            return [segments]
        return transactions

    def _extract_claim_id_from_composite(self, composite_key: str) -> tuple:
        """Extract file name and claim ID from composite key"""
        if "|" in composite_key:
            parts = composite_key.split("|", 1)
            return parts[0], parts[1]
        else:
            return "", composite_key

    def _validate_calculations(
        self,
        csv_rows: List[dict],
        csv_by_claim: Dict[str, List[dict]],
        edi_segments: List[str],
        delimiter: str,
        edi_data: Dict,
        verbose: bool = False,
        edi_segments_by_file: List[dict] = None,
    ):
        """Layer 1: Validate all calculations"""

        file_segment_groups = []

        if edi_segments_by_file:
            for file_data in edi_segments_by_file:
                segments = file_data.get("segments", [])
                file_delimiter = file_data.get("delimiter", "*")
                file_name = self._normalize_file_path(file_data.get("file", ""))
                file_segment_groups.append((segments, file_delimiter, file_name))
        else:
            current_file_segments = []
            for segment in edi_segments:
                seg_id = segment.split(delimiter)[0] if delimiter in segment else segment.split("*")[0]
                current_file_segments.append(segment)

                if seg_id == "IEA":
                    file_segment_groups.append((current_file_segments[:], delimiter, ""))
                    current_file_segments = []
            if not file_segment_groups:
                file_segment_groups.append((edi_segments, delimiter, ""))

        for file_segments, file_delimiter, file_name in file_segment_groups:
            delimiter = file_delimiter  # Use this file's delimiter
            transactions = self._split_transactions(file_segments, delimiter)
            for transaction_segments in transactions:
                check_total = None
                clp_payments = []
                plb_total = Decimal("0")
                payer_name = "Unknown"
                payer_state = "Unknown State"
                found_pr = False
                for segment in transaction_segments:
                    if segment.startswith("N1" + delimiter + "PR"):
                        elements = segment.split(delimiter)
                        if len(elements) > 2:
                            payer_name = elements[2]
                        found_pr = True
                    elif found_pr and segment.startswith("N4" + delimiter):
                        elements = segment.split(delimiter)
                        if len(elements) > 2:
                            payer_state = elements[2]
                        break

                saw_bpr_segment = False
                notification_only_check = True
                zero_amount_tolerance = Decimal("0.01")

                for segment in transaction_segments:
                    elements = segment.split(delimiter)
                    seg_id = elements[0]

                    if seg_id == "BPR" and len(elements) > 2:
                        method_code = elements[4].strip().upper() if len(elements) > 4 and elements[4] else ""
                        amount = None
                        try:
                            amount = Decimal(str(elements[2]))
                            if check_total is None:
                                check_total = amount
                            else:
                                check_total += amount
                        except (ValueError, TypeError, decimal.InvalidOperation, decimal.Overflow):
                            logger.warning(
                                "Invalid BPR check amount in file %s: '%s'", file_name or "unknown", elements[2]
                            )
                        saw_bpr_segment = True
                        if notification_only_check:
                            is_zero_non_payment = (
                                amount is not None and abs(amount) <= zero_amount_tolerance and method_code == "NON"
                            )
                            if not is_zero_non_payment:
                                notification_only_check = False

                    elif seg_id == "CLP" and len(elements) > 4:
                        try:
                            payment = Decimal(str(elements[4]))
                            clp_payments.append(payment)
                        except (ValueError, TypeError, decimal.InvalidOperation, decimal.Overflow):
                            claim_id = elements[1] if len(elements) > 1 else "unknown"
                            logger.warning(
                                "Invalid CLP payment amount for claim %s in file %s: '%s'",
                                claim_id,
                                file_name or "unknown",
                                elements[4],
                            )

                    elif seg_id == "PLB" and len(elements) >= 3:
                        for i in range(3, min(len(elements), 15), 2):
                            if i + 1 < len(elements) and elements[i + 1]:
                                try:
                                    amount = Decimal(str(elements[i + 1]))
                                    plb_total += amount
                                except (ValueError, TypeError):
                                    logger.warning(
                                        "Invalid PLB adjustment amount at element %d in file %s: '%s'",
                                        i + 1,
                                        file_name or "unknown",
                                        elements[i + 1],
                                    )

                skip_transaction_balance = False
                payer_key = f"{payer_name}|{payer_state}"
                if check_total is None or not clp_payments:
                    skip_transaction_balance = True
                    self.stats["payer_data_quality_issues"][payer_key][
                        "Transaction balance skip: Missing BPR02 or no CLP claims"
                    ] += 1
                elif check_total.copy_abs() <= zero_amount_tolerance:
                    skip_transaction_balance = True
                    self.stats["transaction_balances_skipped_non_payment"] = (
                        self.stats.get("transaction_balances_skipped_non_payment", 0) + 1
                    )
                    self.stats["payer_data_quality_issues"][payer_key][
                        "Transaction balance skip: Zero/near-zero amount"
                    ] += 1
                elif saw_bpr_segment and notification_only_check:
                    skip_transaction_balance = True
                    self.stats["transaction_balances_skipped_non_payment"] = (
                        self.stats.get("transaction_balances_skipped_non_payment", 0) + 1
                    )
                    self.stats["payer_data_quality_issues"][payer_key][
                        "Transaction balance skip: Notification-only transaction"
                    ] += 1

                if not skip_transaction_balance and check_total is not None and clp_payments:
                    expected = sum(clp_payments) - plb_total
                    diff = abs(check_total - expected)
                    tolerance = self._get_transaction_tolerance(payer_name)
                    if diff > tolerance:
                        if self._should_debug(payer_name, "TransactionBalance"):
                            logger.debug(
                                "Transaction balance error - Payer: %s, BPR02: %s, Sum(CLP04): %s, PLB: %s, Expected: %s, Diff: %s",
                                payer_name,
                                check_total,
                                sum(clp_payments),
                                plb_total,
                                expected,
                                diff,
                            )
                        # Build comprehensive EDI context for debugging
                        edi_context = self._build_transaction_balance_context(
                            transaction_segments,
                            delimiter,
                            check_total,
                            clp_payments,
                            plb_total,
                            expected,
                            diff,
                        )
                        self.errors.append(
                            ValidationError(
                                "CALC",
                                "Check total doesn't balance per X12 835 Section 1.10.2.1.3",
                                location="Transaction Level (BPR02 vs CLP04-PLB)",
                                expected=float(expected),
                                actual=float(check_total),
                                payer_info={"name": payer_name, "state": payer_state},
                                edi_context=edi_context,
                            )
                        )
                    self.stats["calculations_checked"] += 1

        # Build CAS segments by composite key (file|claim_id) to match csv_by_claim structure
        # This prevents collision when multiple files share the same claim ID
        cas_by_claim = defaultdict(lambda: {"claim": [], "service": []})
        current_context = None
        current_claim_id = None
        current_file = None
        current_composite_key = None
        for seg_data in edi_data["all_segments"]:
            seg_id = seg_data["seg_id"]
            elements = seg_data["elements"]
            # Track file changes - 'file' field is added when processing multi-file EDI data
            seg_file = seg_data.get("file", "")
            if seg_file:
                current_file = self._normalize_file_path(seg_file)

            if seg_id == "CLP":
                current_claim_id = elements[1] if len(elements) > 1 else None
                current_context = "claim"
                # Build composite key matching csv_by_claim format
                if current_file and current_claim_id:
                    current_composite_key = f"{current_file}|{current_claim_id}"
                else:
                    current_composite_key = current_claim_id

            elif seg_id == "SVC" and current_claim_id:
                current_context = "service"

            elif seg_id == "CAS" and current_composite_key:
                if current_context == "claim":
                    cas_by_claim[current_composite_key]["claim"].append(seg_data["segment"])
                elif current_context == "service":
                    cas_by_claim[current_composite_key]["service"].append(seg_data["segment"])
        total_claims = len(csv_by_claim)
        for idx, (claim_id, rows) in enumerate(csv_by_claim.items(), 1):
            if verbose and idx % 100 == 0:
                logger.info("      Validating claim %s of %s...", f"{idx:,}", f"{total_claims:,}")
            self._validate_claim_calculations(claim_id, rows)

            # Use composite key directly - both csv_by_claim and cas_by_claim now use same format
            claim_cas_segments = cas_by_claim.get(claim_id, {}).get("claim", [])
            service_cas_segments = cas_by_claim.get(claim_id, {}).get("service", [])

            if claim_cas_segments:
                self._validate_cas_adjustments(claim_id, rows, claim_cas_segments, "claim", delimiter)

            if service_cas_segments:
                self._validate_cas_adjustments(claim_id, rows, service_cas_segments, "service", delimiter)

    def _validate_claim_calculations(self, claim_id: str, rows: List[dict]):
        """Validate calculations for a single claim
        X12 835 Section 1.10.2.1.2 Claim Balancing:
        'Balancing must occur within each Claim Payment loop so that the submitted
        charges for the claim minus the sum of all monetary adjustments equals the
        claim paid amount.'
        Formula: CLP03 (charge) - CAS adjustments = CLP04 (payment)
        """
        file_name, actual_claim_id = self._extract_claim_id_from_composite(claim_id)
        display_claim_id = actual_claim_id if file_name else claim_id
        claim_row = next(
            (r for r in rows if r.get("CLM_ChargeAmount_L2100_CLP") and not r.get("SVC_ChargeAmount_L2110_SVC")), None
        )
        if not claim_row:
            return
        payer_name = claim_row.get("Payer_Name_L1000A_N1", "Unknown")
        payer_state = claim_row.get("Payer_State_L1000A_N4", "Unknown State")
        try:
            claim_charge = self._parse_currency_decimal(claim_row.get("CLM_ChargeAmount_L2100_CLP", 0))
            claim_payment = self._parse_currency_decimal(claim_row.get("CLM_PaymentAmount_L2100_CLP", 0))
            claim_status = claim_row.get("CLM_Status_L2100_CLP", "")
        except (ValueError, TypeError, InvalidOperation, Overflow):
            return
        if claim_status == "25":
            if claim_payment != 0:
                error_location = f"Claim {display_claim_id}"
                if file_name:
                    error_location += f" (File: {file_name})"
                if self._should_debug(payer_name, "Predetermination"):
                    logger.debug(
                        "Predetermination claim error - Claim: %s, Payer: %s, Status: %s, Payment: %s (should be zero)",
                        display_claim_id,
                        payer_name,
                        claim_status,
                        claim_payment,
                    )
                self.errors.append(
                    ValidationError(
                        "EDGE",
                        "Predetermination claim (Status 25) should have zero payment per X12 spec section 1.10.2.7",
                        location=error_location,
                        expected=0.0,
                        actual=float(claim_payment),
                        field="CLM_PaymentAmount_L2100_CLP",
                    )
                )
            return
        claim_adj_total = Decimal("0")

        for cas_idx in range(1, 6):
            cas_amount_field = f"CLM_CAS{cas_idx}_Amount_L2100_CAS"
            val = claim_row.get(cas_amount_field)
            if val:
                amount = self._parse_currency_decimal(val)
                claim_adj_total += amount

        service_rows = [r for r in rows if r.get("SVC_ChargeAmount_L2110_SVC")]
        # Track empty claims (no service lines)
        if not service_rows and claim_status != "25":  # Predeterminations may not have services
            payer_key = f"{payer_name}|{payer_state}"
            self.stats["payer_data_quality_issues"][payer_key]["Empty claim"] += 1
        for svc_row in service_rows:
            for cas_idx in range(1, 6):
                cas_amount_field = f"SVC_CAS{cas_idx}_Amount_L2110_CAS"
                val = svc_row.get(cas_amount_field)
                if val:
                    amount = self._parse_currency_decimal(val)
                    claim_adj_total += amount
        expected = claim_charge - claim_adj_total
        if abs(claim_payment - expected) > Decimal("0.01"):
            error_location = f"Claim {display_claim_id}"
            if file_name:
                error_location += f" (File: {file_name})"
            if self._should_debug(payer_name, "ClaimBalance"):
                logger.debug(
                    "Claim balance error - Claim: %s, Payer: %s, Charge: %s, Adj: %s, Payment: %s, Expected: %s, Diff: %s",
                    display_claim_id,
                    payer_name,
                    claim_charge,
                    claim_adj_total,
                    claim_payment,
                    expected,
                    abs(claim_payment - expected),
                )
            # Build detailed claim balance context
            edi_context = self._build_claim_balance_context(
                display_claim_id, claim_row, rows, claim_charge, claim_payment, claim_adj_total, expected
            )
            self.errors.append(
                ValidationError(
                    "CALC",
                    f"Claim doesn't balance: Charge({claim_charge}) - Adjustments({claim_adj_total}) should equal Payment({claim_payment})",
                    location=error_location,
                    expected=float(expected),
                    actual=float(claim_payment),
                    field="CLM_PaymentAmount_L2100_CLP",
                    edi_context=edi_context,
                )
            )
        self.stats["calculations_checked"] += 1
        service_rows = [r for r in rows if r.get("SVC_ChargeAmount_L2110_SVC")]
        service_charge_total = Decimal("0")
        service_payment_total = Decimal("0")
        for svc_row in service_rows:
            self._validate_service_calculations(display_claim_id, svc_row, file_name)
            service_charge_total += self._parse_currency_decimal(svc_row.get("SVC_ChargeAmount_L2110_SVC", 0))
            service_payment_total += self._parse_currency_decimal(svc_row.get("SVC_PaymentAmount_L2110_SVC", 0))
        if len(service_rows) > 0:
            try:
                if abs(claim_charge - service_charge_total) > Decimal("0.01"):
                    error_location = f"Claim {display_claim_id}"
                    if file_name:
                        error_location += f" (File: {file_name})"
                    # Build edi_context showing each service line
                    svc_context = self._build_service_sum_context(
                        display_claim_id, claim_charge, service_charge_total, service_rows, "charge"
                    )
                    self.errors.append(
                        ValidationError(
                            "CALC",
                            f"Service charges don't sum to claim total: CLP03({claim_charge}) should equal sum of SVC02({service_charge_total})",
                            location=error_location,
                            expected=float(claim_charge),
                            actual=float(service_charge_total),
                            field="CLM_ChargeAmount_L2100_CLP",
                            edi_context=svc_context,
                        )
                    )
            except (ValueError, TypeError, decimal.InvalidOperation, decimal.Overflow):
                pass
            try:
                if abs(claim_payment - service_payment_total) > Decimal("0.01"):
                    error_location = f"Claim {display_claim_id}"
                    if file_name:
                        error_location += f" (File: {file_name})"
                    # Build edi_context showing each service line
                    svc_context = self._build_service_sum_context(
                        display_claim_id, claim_payment, service_payment_total, service_rows, "payment"
                    )
                    self.errors.append(
                        ValidationError(
                            "CALC",
                            f"Service payments don't sum to claim payment: CLP04({claim_payment}) should equal sum of SVC03({service_payment_total})",
                            location=error_location,
                            expected=float(claim_payment),
                            actual=float(service_payment_total),
                            field="CLM_PaymentAmount_L2100_CLP",
                            edi_context=svc_context,
                        )
                    )
            except (ValueError, TypeError, decimal.InvalidOperation, decimal.Overflow):
                pass

    def _validate_service_calculations(self, claim_id: str, row: dict, file_name: str = ""):
        """Validate service line calculations
        X12 835 Section 1.10.2.1.1 Service Line Balancing:
        'The submitted service charge plus or minus the sum of all monetary
        adjustments must equal the amount paid for this service line.'
        Formula: SVC02 (charge) - CAS adjustments = SVC03 (payment)
        Where CAS adjustments = sum of CAS03, 06, 09, 12, 15, and 18
        """
        payer_name = row.get("Payer_Name_L1000A_N1", "Unknown")
        charge = self._parse_currency_decimal(row.get("SVC_ChargeAmount_L2110_SVC", 0))
        payment = self._parse_currency_decimal(row.get("SVC_PaymentAmount_L2110_SVC", 0))
        adj_total = Decimal("0")
        for cas_idx in range(1, 6):
            cas_amount_field = f"SVC_CAS{cas_idx}_Amount_L2110_CAS"
            val = row.get(cas_amount_field)
            if val:
                adj_total += self._parse_currency_decimal(val)
        expected = charge - adj_total
        if abs(payment - expected) > Decimal("0.01"):
            proc = row.get("SVC_ProcedureCode_L2110_SVC", "")
            error_location = f"Claim {claim_id}, Service {proc}"
            if file_name:
                error_location += f" (File: {file_name})"
            if self._should_debug(payer_name, "ServiceBalance"):
                logger.debug(
                    "Service balance error - Proc: %s, Claim: %s, Payer: %s, Charge: %s, Adj: %s, Payment: %s, Expected: %s",
                    proc,
                    claim_id,
                    payer_name,
                    charge,
                    adj_total,
                    payment,
                    expected,
                )
            # Build detailed service balance context
            edi_context = self._build_service_balance_context(claim_id, row, charge, payment, adj_total, expected)
            self.errors.append(
                ValidationError(
                    "CALC",
                    f"Service doesn't balance: Charge({charge}) - Adjustments({adj_total}) should equal Payment({payment})",
                    location=error_location,
                    expected=float(expected),
                    actual=float(payment),
                    field="SVC_PaymentAmount_L2110_SVC",
                    edi_context=edi_context,
                )
            )
        self.stats["calculations_checked"] += 1

    def _build_service_balance_context(
        self,
        claim_id: str,
        row: dict,
        charge: Decimal,
        payment: Decimal,
        adj_total: Decimal,
        expected: Decimal,
    ) -> List[str]:
        """
        Build comprehensive debugging context for service balance errors.
        """
        context = []
        diff = abs(payment - expected)
        proc = row.get("SVC_Procedure_L2110_SVC", "N/A")
        units = row.get("SVC_Units_L2110_SVC", "N/A")

        # === CALCULATION BREAKDOWN ===
        context.append("=" * 60)
        context.append("SERVICE LINE BALANCE CALCULATION BREAKDOWN")
        context.append("=" * 60)
        context.append("")
        context.append(f"Claim ID: {claim_id}")
        context.append(f"Procedure: {proc}")
        context.append(f"Units: {units}")
        context.append("")
        context.append("Formula: SVC02 (Charge) - CAS Adjustments = SVC03 (Payment)")
        context.append("")

        context.append(f"  SVC02 (Charge):           ${float(charge):,.2f}")
        context.append(f"  CAS Adjustments:          ${float(adj_total):,.2f}")
        context.append("  ----------------------------------------")
        context.append(f"  Expected Payment:         ${float(expected):,.2f}")
        context.append(f"  SVC03 (Actual Payment):   ${float(payment):,.2f}")
        context.append(f"  DIFFERENCE:               ${float(diff):,.2f}")
        context.append("")

        # === CAS ADJUSTMENTS ===
        context.append("=" * 60)
        context.append("SERVICE CAS ADJUSTMENTS")
        context.append("=" * 60)
        has_cas = False
        cas_detail_total = Decimal("0")
        for cas_idx in range(1, 6):
            group_field = f"SVC_CAS{cas_idx}_Group_L2110_CAS"
            reason_field = f"SVC_CAS{cas_idx}_Reason_L2110_CAS"
            amount_field = f"SVC_CAS{cas_idx}_Amount_L2110_CAS"
            group = row.get(group_field, "")
            reason = row.get(reason_field, "")
            amount_str = row.get(amount_field, "")
            if amount_str:
                has_cas = True
                try:
                    amount = self._parse_currency_decimal(amount_str)
                    cas_detail_total += amount
                    context.append(f"  CAS {cas_idx}: ${float(amount):,.2f}")
                    context.append(f"    Group: {group}, Reason: {reason}")
                except (ValueError, TypeError):
                    context.append(f"  CAS {cas_idx}: {amount_str} (parse error)")
                    context.append(f"    Group: {group}, Reason: {reason}")
        if not has_cas:
            context.append("  (No CAS adjustments for this service line)")
        else:
            context.append("  ----------------------------------------")
            context.append(f"  CAS Total: ${float(cas_detail_total):,.2f}")
        context.append("")

        # === RAW DATA ===
        context.append("=" * 60)
        context.append("RAW CSV DATA (Service Line Fields)")
        context.append("=" * 60)
        context.append(f"  Payer: {row.get('Payer_Name_L1000A_N1', 'N/A')}")
        context.append(f"  Patient: {row.get('Patient_Name_L2100_NM1', 'N/A')}")
        context.append(f"  Procedure: {row.get('SVC_Procedure_L2110_SVC', 'N/A')}")
        context.append(f"  Modifier: {row.get('SVC_Modifier_L2110_SVC', 'N/A')}")
        context.append(f"  Charge: {row.get('SVC_ChargeAmount_L2110_SVC', 'N/A')}")
        context.append(f"  Payment: {row.get('SVC_PaymentAmount_L2110_SVC', 'N/A')}")
        context.append(f"  Units: {row.get('SVC_Units_L2110_SVC', 'N/A')}")
        context.append(f"  Service Date: {row.get('SVC_ServiceStartDate_L2110_DTM', 'N/A')}")
        context.append("")

        return context

    def _build_service_sum_context(
        self,
        claim_id: str,
        claim_total: Decimal,
        service_total: Decimal,
        service_rows: List[dict],
        sum_type: str,
    ) -> List[str]:
        """
        Build comprehensive debugging context for service sum errors (charge or payment).
        """
        context = []
        diff = abs(claim_total - service_total)
        field_prefix = "SVC_ChargeAmount" if sum_type == "charge" else "SVC_PaymentAmount"
        clp_field = "CLP03" if sum_type == "charge" else "CLP04"
        svc_field = "SVC02" if sum_type == "charge" else "SVC03"

        context.append("=" * 60)
        context.append(f"SERVICE {sum_type.upper()} SUM BREAKDOWN")
        context.append("=" * 60)
        context.append("")
        context.append(f"Claim ID: {claim_id}")
        context.append(f"Total Service Lines: {len(service_rows)}")
        context.append("")
        context.append(
            f"Formula: Sum of {svc_field} (Service {sum_type.title()}s) should equal {clp_field} (Claim {sum_type.title()})"
        )
        context.append("")
        context.append(f"  {clp_field} (Claim {sum_type.title()}):  ${float(claim_total):,.2f}")
        context.append(f"  Sum of {svc_field}:              ${float(service_total):,.2f}")
        context.append("  ----------------------------------------")
        context.append(f"  DIFFERENCE:                  ${float(diff):,.2f}")
        context.append("")

        # List all service lines
        context.append("=" * 60)
        context.append("SERVICE LINE BREAKDOWN")
        context.append("=" * 60)
        running_total = Decimal("0")
        for idx, svc_row in enumerate(service_rows, 1):
            proc = svc_row.get("SVC_ProcedureCode_L2110_SVC", "N/A")
            amount_str = svc_row.get(f"{field_prefix}_L2110_SVC", "0")
            try:
                amount = self._parse_currency_decimal(amount_str)
            except (ValueError, TypeError):
                amount = Decimal("0")
            running_total += amount
            context.append(f"  {idx}. Procedure: {proc}")
            context.append(f"     {sum_type.title()}: ${float(amount):,.2f}")
            context.append(f"     Running Total: ${float(running_total):,.2f}")
        context.append("")
        context.append(f"Final Sum: ${float(running_total):,.2f}")
        context.append("")

        return context

    def _validate_completeness(self, edi_data: Dict, csv_rows: List[dict], delimiter_or_files, verbose: bool = False):
        """Layer 2: Validate 100% field coverage"""

        # Use first file's delimiter as default for legacy compatibility
        delimiter = "*"
        if isinstance(delimiter_or_files, list):
            if delimiter_or_files:
                delimiter = delimiter_or_files[0].get("delimiter", "*")
        else:
            delimiter = delimiter_or_files or "*"

        if verbose:
            logger.info("      Detecting duplicate claim numbers...")
        from collections import Counter

        claim_numbers = [
            row.get("CLM_PatientControlNumber_L2100_CLP")
            for row in csv_rows
            if row.get("CLM_PatientControlNumber_L2100_CLP")
        ]
        claim_counts = Counter(claim_numbers)
        duplicate_claim_numbers = {cn for cn, count in claim_counts.items() if count > 1}
        # Track duplicate claims in payer_data_quality_issues
        if duplicate_claim_numbers:
            for dup_claim in duplicate_claim_numbers:
                # Find payer for this claim
                claim_row = next(
                    (r for r in csv_rows if r.get("CLM_PatientControlNumber_L2100_CLP") == dup_claim), None
                )
                if claim_row:
                    payer_name = claim_row.get("Payer_Name_L1000A_N1", "Unknown")
                    payer_state = claim_row.get("Payer_State_L1000A_N4", "Unknown State")
                    payer_key = f"{payer_name}|{payer_state}"
                    self.stats["payer_data_quality_issues"][payer_key][f"Duplicate claim: {dup_claim}"] = claim_counts[
                        dup_claim
                    ]
        if verbose:
            logger.info("      Building expected fields from %d claims...", len(edi_data.get("claims", {})))
        expected_fields = self._build_expected_fields(edi_data, delimiter)

        if verbose:
            logger.info("      Extracting actual fields from %s CSV rows...", f"{len(csv_rows):,}")
        actual_fields = self._extract_actual_fields(csv_rows, verbose=verbose)
        if verbose:
            logger.info("      Comparing %s expected fields...", f"{len(expected_fields):,}")

        total_fields = len(expected_fields)
        for idx, (field_key, expected_value) in enumerate(expected_fields.items(), 1):
            if verbose and idx % 1000 == 0:
                logger.info("      Validating field %s of %s...", f"{idx:,}", f"{total_fields:,}")
            if "_CAS_Present_" in field_key:
                continue

            location = expected_value.get("location", "")

            is_loop_level = location in ["Header", "Payer Loop", "Payee Loop"]

            if field_key not in actual_fields:
                if is_loop_level:
                    continue

                segment = expected_value.get("segment", "")
                if segment in ["MOA", "MIA", "PER", "AMT", "QTY", "DTM", "REF", "CAS", "LQ"]:
                    continue

                self.errors.append(
                    ValidationError(
                        "MISSING",
                        "Field not found in CSV",
                        segment=expected_value["segment"],
                        field=expected_value["csv_field"],
                        expected=expected_value["value"],
                        location=expected_value.get("location"),
                    )
                )

            elif actual_fields[field_key] != expected_value["value"]:
                if not self._values_match(actual_fields[field_key], expected_value["value"]):
                    if is_loop_level and not actual_fields[field_key]:
                        continue
                    location_str = expected_value.get("location") or ""
                    if location_str and "Claim" in location_str:
                        claim_from_location = location_str.split("Claim")[1].strip().split(",")[0].strip()
                        if claim_from_location in duplicate_claim_numbers:
                            continue

                    self.errors.append(
                        ValidationError(
                            "MISMATCH",
                            "Field value mismatch",
                            segment=expected_value["segment"],
                            field=expected_value["csv_field"],
                            expected=expected_value["value"],
                            actual=actual_fields[field_key],
                            location=expected_value.get("location"),
                        )
                    )

            self.stats["fields_validated"] += 1

    def _build_expected_fields(self, edi_data: Dict, delimiter: str) -> Dict:
        """Build complete list of expected fields from EDI

        Uses file|claim_id composite keys to handle claims that appear in multiple
        EDI files (e.g., coordination of benefits scenarios where the same claim
        is processed by both primary and secondary payers).
        """
        expected = {}
        for seg_id, elements in edi_data["header"].items():
            self._add_expected_segment_fields(expected, seg_id, elements, "Header")
        for seg_id, elements in edi_data["payer_loop"].items():
            self._add_expected_segment_fields(expected, seg_id, elements, "Payer Loop")
        for seg_id, elements in edi_data["payee_loop"].items():
            self._add_expected_segment_fields(expected, seg_id, elements, "Payee Loop")
        for claim_id, claim_data in edi_data["claims"].items():
            # Extract file and claim ID from composite key (file|claim_id)
            # Keep the file in the location to distinguish same claim across different files
            file_part, actual_claim_id = self._extract_claim_id_from_composite(claim_id)
            normalized_file = self._normalize_file_path(file_part) if file_part else ""

            # Use file|claim_id as display to match CSV extraction
            if normalized_file:
                display_claim_id = f"{normalized_file}|{actual_claim_id}"
            else:
                display_claim_id = actual_claim_id if actual_claim_id else claim_id

            for seg_id, elements in claim_data.get("segments", {}).items():
                if seg_id not in [
                    "ISA",
                    "GS",
                    "ST",
                    "SE",
                    "GE",
                    "IEA",
                    "BPR",
                    "TRN",
                    "N1",
                    "N2",
                    "N3",
                    "N4",
                    "PER",
                    "REF",
                    "CUR",
                    "RDM",
                    "DTM",
                    "PLB",
                    "LX",
                    "TS3",
                    "TS2",
                ]:
                    self._add_expected_segment_fields(expected, seg_id, elements, f"Claim {display_claim_id}")

            for svc_idx, service_data in enumerate(claim_data.get("services", [])):
                for seg_id, elements in service_data.get("segments", {}).items():
                    location = f"Claim {display_claim_id}, Service {svc_idx + 1}"
                    self._add_expected_segment_fields(expected, seg_id, elements, location)
        return expected

    def _add_expected_segment_fields(self, expected: Dict, seg_id: str, elements: List[str], location: str):
        """Add expected fields for a specific segment"""
        from dictionary import (
            get_bpr_transaction_handling_description,
        )

        if seg_id in self.segment_maps:
            field_map = self.segment_maps[seg_id]
            for pos, csv_field in field_map.items():
                if csv_field != "Not Used":
                    element_index = int(pos)
                    if element_index < len(elements):
                        value = elements[element_index]
                        if value:
                            if csv_field == "CHK_TransactionHandling_Header_BPR":
                                if seg_id == "BPR" and pos == "01":
                                    value = get_bpr_transaction_handling_description(value)
                            if seg_id in ["N1", "N2", "N3", "N4"] and location in ["Payer Loop", "Payee Loop"]:
                                if location == "Payer Loop":
                                    csv_field = f"Payer_{csv_field}_L1000A_{seg_id}"
                                elif location == "Payee Loop":
                                    csv_field = f"Provider_{csv_field}_L1000B_{seg_id}"

                            if seg_id == "PER" and location == "Payer Loop":
                                function_code = elements[1] if len(elements) > 1 else ""
                                if function_code:
                                    if csv_field == "Contact_Function_Code":
                                        csv_field = f"Contact_{function_code}_Function_L1000A_PER"
                                    elif csv_field == "Name":
                                        csv_field = f"Contact_{function_code}_Name_L1000A_PER"
                                    elif csv_field == "Communication_Number_Qualifier_1":
                                        csv_field = f"Contact_{function_code}_Phone_Qualifier_L1000A_PER"
                                    elif csv_field == "Communication_Number_1":
                                        csv_field = f"Contact_{function_code}_Phone_L1000A_PER"
                                    elif csv_field == "Communication_Number_Qualifier_2":
                                        csv_field = f"Contact_{function_code}_Comm2_Qualifier_L1000A_PER"
                                    elif csv_field == "Communication_Number_2":
                                        csv_field = f"Contact_{function_code}_Comm2_Number_L1000A_PER"

                            key = f"{location}:{csv_field}"
                            expected[key] = {
                                "segment": seg_id,
                                "position": pos,
                                "csv_field": csv_field,
                                "value": value,
                                "location": location,
                            }
        if seg_id in self.qualifier_maps:
            self._add_qualifier_based_fields(expected, seg_id, elements, location)
        if seg_id == "CAS" and len(elements) >= 4:
            group_code = elements[1] if len(elements) > 1 else ""
            cas_adjustments = []
            idx = 2
            while idx + 1 < len(elements):
                reason = elements[idx] if idx < len(elements) else ""
                amount = elements[idx + 1] if idx + 1 < len(elements) else ""
                qty = elements[idx + 2] if idx + 2 < len(elements) else ""
                if reason and amount:
                    cas_adjustments.append({"group": group_code, "reason": reason, "amount": amount, "qty": qty})
                idx += 3
                if idx > 19:
                    break
            if cas_adjustments:
                if location and "Service" in location:
                    field_suffix = "L2110_CAS"
                    prefix = "SVC"
                else:
                    field_suffix = "L2100_CAS"
                    prefix = "CLM"
                key = f"{location}:{prefix}_CAS_Present_{field_suffix}"
                expected[key] = {
                    "segment": seg_id,
                    "csv_field": f"{prefix}_*_{field_suffix}",
                    "value": "EXPECTED",
                    "location": location,
                    "cas_data": cas_adjustments,
                }

    def _add_qualifier_based_fields(self, expected: Dict, seg_id: str, elements: List[str], location: str):
        """Add expected fields for qualifier-based segments"""
        if seg_id == "REF" and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps["REF"]:
                csv_field = self.qualifier_maps["REF"][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    "segment": seg_id,
                    "qualifier": qualifier,
                    "csv_field": csv_field,
                    "value": value,
                    "location": location,
                }
        elif seg_id == "DTM" and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps["DTM"]:
                csv_field = self.qualifier_maps["DTM"][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    "segment": seg_id,
                    "qualifier": qualifier,
                    "csv_field": csv_field,
                    "value": value,
                    "location": location,
                }
        elif seg_id == "AMT" and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps["AMT"]:
                csv_field = self.qualifier_maps["AMT"][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    "segment": seg_id,
                    "qualifier": qualifier,
                    "csv_field": csv_field,
                    "value": value,
                    "location": location,
                }
        elif seg_id == "QTY" and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps["QTY"]:
                csv_field = self.qualifier_maps["QTY"][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    "segment": seg_id,
                    "qualifier": qualifier,
                    "csv_field": csv_field,
                    "value": value,
                    "location": location,
                }

    def _extract_actual_fields(self, csv_rows: List[dict], verbose: bool = False) -> Dict:
        """Extract all actual field values from CSV

        Uses file|claim_id|occurrence composite keys to handle claims that appear in multiple
        EDI files (e.g., coordination of benefits scenarios where the same claim
        is processed by both primary and secondary payers) and multiple occurrences
        (e.g., reversals and corrections of the same claim).
        """
        actual = {}

        service_counts = defaultdict(int)
        total_rows = len(csv_rows)

        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                logger.info("        Processing CSV row %s of %s...", f"{idx:,}", f"{total_rows:,}")
            raw_claim_id = row.get("CLM_PatientControlNumber_L2100_CLP", "")
            claim_id = str(raw_claim_id).strip() if raw_claim_id is not None else ""
            if not claim_id:
                continue

            # Get source file and normalize to basename for consistent matching with EDI
            file_name = row.get("Filename_File", "")
            normalized_file = self._normalize_file_path(file_name) if file_name else ""

            # Get occurrence to match EDI parsing (same claim can appear multiple times)
            occurrence = row.get("CLM_Occurrence_L2100_CLP", "1")
            occurrence = str(occurrence).strip() if occurrence else "1"

            # Build claim key with file|claim_id|occurrence to match _build_expected_fields
            # EDI parser uses: file|claim_id|occurrence
            if normalized_file:
                claim_key = f"{normalized_file}|{claim_id}|{occurrence}"
            else:
                claim_key = f"{claim_id}|{occurrence}"

            has_service = bool(row.get("SVC_ProcedureCode_L2110_SVC"))

            if not actual:
                for field, value in row.items():
                    if field.startswith(("CHK_", "File_", "Contact_", "ENV_")):
                        key = f"Header:{field}"
                        actual[key] = value

                for field, value in row.items():
                    if field.startswith("Payer_"):
                        key = f"Payer Loop:{field}"
                        actual[key] = value

                for field, value in row.items():
                    if field.startswith("Provider_"):
                        key = f"Payee Loop:{field}"
                        actual[key] = value

            if claim_key:
                claim_location = f"Claim {claim_key}"

                if not has_service:
                    service_counts[claim_key] = 0

                for field, value in row.items():
                    if field.startswith("CLM_") and value:
                        key = f"{claim_location}:{field}"
                        actual[key] = value

                if has_service:
                    service_counts[claim_key] += 1
                    service_location = f"Claim {claim_key}, Service {service_counts[claim_key]}"
                    for field, value in row.items():
                        if field.startswith("SVC_") and value:
                            key = f"{service_location}:{field}"
                            actual[key] = value

        return actual

    def _values_match(self, actual: Any, expected: Any) -> bool:
        """Check if values match, allowing for formatting differences"""
        if actual is None and expected == "":
            return True
        if expected is None and actual == "":
            return True
        actual_str = str(actual).strip() if actual is not None else ""
        expected_str = str(expected).strip() if expected is not None else ""
        if actual_str == expected_str:
            return True
        # Try whitespace-normalized comparison (multiple spaces -> single space)
        import re

        actual_normalized = re.sub(r"\s+", " ", actual_str)
        expected_normalized = re.sub(r"\s+", " ", expected_str)
        if actual_normalized == expected_normalized:
            return True
        # Try numeric comparison (handles currency formatting: $, commas)
        try:
            actual_clean = actual_str.replace("$", "").replace(",", "")
            expected_clean = expected_str.replace("$", "").replace(",", "")
            actual_num = Decimal(actual_clean)
            expected_num = Decimal(expected_clean)
            return abs(actual_num - expected_num) <= Decimal("0.01")
        except (ValueError, InvalidOperation, Overflow):
            pass

        # Try date comparison (handles YYYYMMDD, YYMMDD, MM/DD/YY, MM/DD/YYYY)
        def _try_parse_date(value: str):
            if not value:
                return None
            # Build format list based on value characteristics
            date_formats = []
            # Check for slashes (MM/DD/YYYY or MM/DD/YY)
            if "/" in value:
                date_formats.extend(["%m/%d/%Y", "%m/%d/%y"])
            # Check for hyphens (ISO format)
            elif "-" in value:
                date_formats.append("%Y-%m-%d")
            # Pure numeric - check length to determine format
            elif value.isdigit():
                if len(value) == 8:
                    date_formats.append("%Y%m%d")  # YYYYMMDD
                elif len(value) == 6:
                    date_formats.append("%y%m%d")  # YYMMDD (X12 ISA09 format)
            else:
                # Fallback: try all formats
                date_formats = ["%m/%d/%Y", "%m/%d/%y", "%Y%m%d", "%y%m%d"]
            for fmt in date_formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            return None

        actual_date = _try_parse_date(actual_str)
        expected_date = _try_parse_date(expected_str)
        if actual_date and expected_date:
            if actual_date == expected_date:
                return True
        if actual_str.lower() == expected_str.lower():
            return True
        return False

    def _validate_field_mappings(self, csv_rows: List[dict], verbose: bool = False):
        """Layer 3: Validate field mappings and data quality"""
        self._track_dictionary_gaps(csv_rows, verbose=verbose)
        if verbose:
            logger.info("      Validating data types for %s rows...", f"{len(csv_rows):,}")
        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                logger.info("        Validating data types for row %s of %s...", f"{idx:,}", f"{len(csv_rows):,}")
            self._validate_field_data_types(row)

    def _track_dictionary_gaps(self, csv_rows: List[dict], verbose: bool = False):
        """Identify all codes that have missing dictionary entries - OPTIMIZED"""
        import dictionary

        if not csv_rows:
            return
        total_rows = len(csv_rows)
        header_field_lookups = {
            "CHK_PaymentMethod_Header_BPR": dictionary.get_payment_method_description,
            "CHK_Format_Header_BPR": dictionary.get_payment_format_description,
            "CHK_TraceType_Header_TRN": dictionary.get_trace_type_description,
            "CHK_CreditDebitFlag_Header_BPR": dictionary.get_credit_debit_indicator_description,
            "CHK_BusinessFunctionCode_Header_BPR": dictionary.get_business_function_code_description,
            "CHK_PayerDFI_Qualifier_Header_BPR": dictionary.get_dfi_id_number_qualifier_description,
            "CHK_PayerAccountQualifier_Header_BPR": dictionary.get_account_number_qualifier_description,
            "CHK_PayeeDFI_Qualifier_Header_BPR": dictionary.get_dfi_id_number_qualifier_description,
            "CHK_PayeeAccountQualifier_Header_BPR": dictionary.get_account_number_qualifier_description,
            "CHK_DFI_Qualifier_3_Header_BPR": dictionary.get_dfi_id_number_qualifier_description,
            "CHK_AccountQualifier_3_Header_BPR": dictionary.get_account_number_qualifier_description,
            "Payer_IDQualifier_L1000A_REF": dictionary.get_id_code_qualifier_description,
            "Provider_IDQualifier_L1000B_N1": dictionary.get_id_code_qualifier_description,
        }
        row_level_lookups = {
            "CLM_Status_L2100_CLP": dictionary.get_claim_status_description,
            "CLM_FilingIndicator_L2100_CLP": dictionary.get_claim_filing_indicator_description,
            "CLM_FrequencyCode_L2100_CLP": dictionary.get_claim_frequency_description,
            "SVC_Qualifier_L2110_SVC": dictionary.get_service_qualifier_description,
        }
        carc_fields = [
            "CLM_CAS1_Reason_L2100_CAS",
            "CLM_CAS2_Reason_L2100_CAS",
            "CLM_CAS3_Reason_L2100_CAS",
            "CLM_CAS4_Reason_L2100_CAS",
            "CLM_CAS5_Reason_L2100_CAS",
            "SVC_CAS1_Reason_L2110_CAS",
            "SVC_CAS2_Reason_L2110_CAS",
            "SVC_CAS3_Reason_L2110_CAS",
            "SVC_CAS4_Reason_L2110_CAS",
            "SVC_CAS5_Reason_L2110_CAS",
        ]
        ambulance_code_field = "SVC_ProcedureCode_L2110_SVC"
        modifier_fields = [
            "SVC_Modifier1_L2110_SVC",
            "SVC_Modifier2_L2110_SVC",
            "SVC_Modifier3_L2110_SVC",
            "SVC_Modifier4_L2110_SVC",
        ]
        ambulance_service_codes = {
            "A0425",
            "A0426",
            "A0427",
            "A0428",
            "A0429",
            "A0430",
            "A0431",
            "A0432",
            "A0433",
            "A0434",
        }
        invalid_wound_modifiers_for_ambulance = {"A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"}
        # Remark code fields contain comma-separated values (e.g., "N130, N381")
        # Also check MOA/MIA remark code fields which are individual
        remark_fields_csv = [
            "CLM_RemarkCodes_L2100_LQ",  # Comma-separated claim-level remark codes
            "SVC_RemarkCodes_L2110_LQ",  # Comma-separated service-level remark codes
        ]
        remark_fields_individual = [
            "MOA_ClaimPaymentRemarkCode1_L2100_MOA",
            "MOA_ClaimPaymentRemarkCode2_L2100_MOA",
            "MOA_ClaimPaymentRemarkCode3_L2100_MOA",
            "MOA_ClaimPaymentRemarkCode4_L2100_MOA",
            "MOA_ClaimPaymentRemarkCode5_L2100_MOA",
            "MIA_ClaimPaymentRemarkCode_L2100_MIA",
            "MIA_ClaimPaymentRemarkCode2_L2100_MIA",
            "MIA_ClaimPaymentRemarkCode3_L2100_MIA",
            "MIA_ClaimPaymentRemarkCode4_L2100_MIA",
            "MIA_ClaimPaymentRemarkCode5_L2100_MIA",
        ]
        if verbose:
            logger.info("      Checking header-level codes (once)...")
        first_row = csv_rows[0]
        payer_info = {
            "name": first_row.get("Payer_Name_L1000A_N1", "Unknown"),
            "state": first_row.get("Payer_State_L1000A_N4", "Unknown"),
            "id": first_row.get("CHK_PayerID_L1000A_REF", "Unknown"),
        }
        for code_field, lookup_func in header_field_lookups.items():
            code = first_row.get(code_field, "")
            if not code or not code.strip():
                continue
            desc = lookup_func(code)
            is_gap = not desc or desc == "" or desc.startswith("Unknown") or desc == code
            if is_gap:
                payer_key = f"{payer_info['name']}|{payer_info['state']}"
                self.stats["missing_mappings"][payer_key].append(
                    {"field": code_field, "code": code, "claim": "Header", "lookup_result": desc}
                )
                self.stats["payer_data_quality_issues"][payer_key][f"Missing dictionary: {code_field}"] += 1
        if verbose:
            logger.info("      Collecting unique codes from %s rows...", f"{total_rows:,}")
        code_payer_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        for _idx, row in enumerate(csv_rows, 1):
            payer_key = f"{row.get('Payer_Name_L1000A_N1', 'Unknown')}|{row.get('Payer_State_L1000A_N4', 'Unknown')}"
            for code_field in row_level_lookups.keys():
                code = row.get(code_field, "")
                if code and code.strip():
                    code_payer_counts[code_field][code.strip()][payer_key] += 1
            for carc_field in carc_fields:
                code = row.get(carc_field, "")
                if code and code.strip():
                    code_payer_counts[carc_field][code.strip()][payer_key] += 1
            code = row.get(ambulance_code_field, "")
            if code and code.strip():
                code_payer_counts[ambulance_code_field][code.strip()][payer_key] += 1
            for mod_field in modifier_fields:
                code = row.get(mod_field, "")
                if code and code.strip():
                    code_payer_counts[mod_field][code.strip()][payer_key] += 1
            # Handle comma-separated remark code fields (LQ segments)
            for remark_field in remark_fields_csv:
                codes_str = row.get(remark_field, "")
                if codes_str and codes_str.strip():
                    # Split comma-separated codes and count each individually
                    for code in codes_str.split(","):
                        code = code.strip()
                        if code:
                            code_payer_counts["RARC"][code][payer_key] += 1
            # Handle individual remark code fields (MOA/MIA segments)
            for remark_field in remark_fields_individual:
                code = row.get(remark_field, "")
                if code and code.strip():
                    code_payer_counts["RARC"][code.strip()][payer_key] += 1
        if verbose:
            total_unique = sum(len(codes) for codes in code_payer_counts.values())
            logger.info("      Validating %s unique codes...", f"{total_unique:,}")
        carc_classifications = dictionary.get_carc_classifications()
        for code_field, codes_dict in code_payer_counts.items():
            for code, payer_counts in codes_dict.items():
                is_gap = False
                if code_field in row_level_lookups:
                    desc = row_level_lookups[code_field](code)
                    is_gap = not desc or desc == "" or desc.startswith("Unknown") or desc == code
                elif code_field in carc_fields:
                    normalized = colloquial.normalize_carc_code(code)
                    is_gap = normalized not in carc_classifications
                elif code_field == ambulance_code_field:
                    desc = dictionary.get_ambulance_code_description(code)
                    is_gap = desc.startswith("Unknown")
                elif code_field in modifier_fields:
                    svc_code = row.get(ambulance_code_field, "").strip()
                    if svc_code in ambulance_service_codes and code in invalid_wound_modifiers_for_ambulance:
                        location = (
                            f"Claim {row.get('CLM_PatientControlNumber_L2100_CLP', '')}, Service {row.get('SEQ', '')}"
                        )
                        payer_info = {
                            "name": row.get("Payer_Name_L1000A_N1", ""),
                            "state": row.get("Payer_State_L1000A_N4", ""),
                            "id": row.get("CHK_PayerID_L1000A_REF", ""),
                        }
                        self.errors.append(
                            ValidationError(
                                "INVALID_AMBULANCE_MODIFIER",
                                f"Invalid ambulance modifier for EMS (wound dressing series): {code}",
                                location=location,
                                field=code_field,
                                actual=code,
                                payer_info=payer_info,
                            )
                        )
                        continue
                    desc = dictionary.get_ambulance_modifier_description(code)
                    is_gap = desc == code
                elif code_field == "RARC":
                    # RARC = Remittance Advice Remark Code (from LQ, MOA, MIA segments)
                    desc = dictionary.get_remark_code_description(code)
                    is_gap = desc.startswith("Unknown")
                    # Track priority RARC codes - attribute only to payers that list it as priority
                    # Each payer's count is attributed only if that payer has it as a priority RARC
                    for payer_key, count in payer_counts.items():
                        for file_idx, pk in self.payer_keys.items():
                            if pk == payer_key and self._is_priority_rarc(file_idx, code):
                                self.stats["priority_rarc_codes"][payer_key][code] += count
                                break  # Found matching payer for this payer_key
                if is_gap:
                    sum(payer_counts.values())
                    for payer_key, count in payer_counts.items():
                        # Check if this field is "not used" for this payer per their companion guide
                        # payer_key format is "PAYER_NAME|STATE", need to extract payer name
                        payer_name_from_key = payer_key.split("|")[0] if "|" in payer_key else payer_key
                        identified_payer = colloquial.identify_payer(payer_name=payer_name_from_key)
                        if identified_payer and self._is_field_not_used_for_payer(identified_payer, code_field):
                            # Skip this warning - field is documented as "Not Used" for this payer
                            continue

                        self.stats["missing_mappings"][payer_key].append(
                            {
                                "field": code_field,
                                "code": code,
                                "claim": f"{count} occurrences",
                                "lookup_result": "Unknown",
                            }
                        )
                        # Categorize the missing code type for reporting
                        if "CAS" in code_field:
                            issue_key = f"Missing CARC: {code}"
                        elif code_field == "RARC":
                            issue_key = f"Missing RARC: {code}"
                        else:
                            issue_key = f"Missing: {code_field}"
                        self.stats["payer_data_quality_issues"][payer_key][issue_key] += count

    def _validate_field_data_types(self, row: dict):
        """Validate field data types"""
        amount_fields = [
            field
            for field in row.keys()
            if (
                "Amount" in field
                or ("Payment" in field and "PaymentMethod" not in field)
                or "Charge" in field
                or "Responsibility" in field
                or "Deductible" in field
                or "Copay" in field
                or "Coinsurance" in field
            )
            and not field.endswith("Desc")
            and not field.endswith("Method")
            and not field.endswith("MethodDesc")
            and not field == "CHK_PaymentMethod_Header_BPR"
            and "RemarkCode" not in field
            and "RemarkDesc" not in field
            and "Date" not in field
        ]  # Exclude date fields (e.g., PaymentDate)
        for field in amount_fields:
            value = row.get(field)
            if value and value != "":
                try:
                    # Strip currency formatting ($, commas) before parsing
                    clean_value = str(value).replace("$", "").replace(",", "").strip()
                    if clean_value:
                        Decimal(clean_value)
                except (ValueError, InvalidOperation, Overflow):
                    self.errors.append(
                        ValidationError(
                            "MAPPING",
                            "Non-numeric value in amount field",
                            field=field,
                            actual=value,
                            location=row.get("CLM_PatientControlNumber_L2100_CLP"),
                        )
                    )
        date_fields = [field for field in row.keys() if "Date" in field]
        for field in date_fields:
            value = row.get(field)
            if value and value != "":
                str_value = str(value)
                # Accept multiple date formats: YYYYMMDD (raw), MM/DD/YY (formatted)
                is_valid = False
                if len(str_value) == 8 and str_value.isdigit():
                    try:
                        datetime.strptime(str_value, "%Y%m%d")
                        is_valid = True
                    except ValueError:
                        pass
                elif "/" in str_value or "-" in str_value:
                    date_formats = ["%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"]
                    for fmt in date_formats:
                        try:
                            datetime.strptime(str_value, fmt)
                            is_valid = True
                            break
                        except ValueError:
                            continue
                if not is_valid and str_value:
                    self.errors.append(
                        ValidationError(
                            "MAPPING",
                            "Invalid date format",
                            field=field,
                            actual=value,
                            expected="CCYYMMDD or MM/DD/YY",
                            location=row.get("CLM_PatientControlNumber_L2100_CLP"),
                        )
                    )

    def _validate_date_formats(self, csv_rows: List[dict], verbose: bool = False):
        """Validate date formats in all date columns and report format coverage.

        Checks that all dates can be parsed and converted to MM/DD/YY format.
        Tracks which date formats are encountered for transparency.
        """
        if not csv_rows:
            return

        # Define known date formats we support (from redactor.py)
        known_formats = [
            ("%Y%m%d", "YYYYMMDD (EDI DTM)"),
            ("%y%m%d", "YYMMDD (EDI ISA)"),  # ISA segment uses 6-digit dates
            ("%Y-%m-%d", "YYYY-MM-DD (ISO)"),
            ("%m/%d/%Y", "MM/DD/YYYY"),
            ("%m-%d-%Y", "MM-DD-YYYY"),
            ("%m/%d/%y", "MM/DD/YY"),
            ("%m-%d-%y", "MM-DD-YY"),
            ("%Y/%m/%d", "YYYY/MM/DD"),
            ("%Y-%m-%d %H:%M:%S", "ISO with time"),
            ("%Y-%m-%dT%H:%M:%S", "ISO with T"),
        ]

        # Identify all date columns based on EDI segment structure
        # This uses the actual EDI structure rather than guessing from field name patterns
        date_columns = set()

        # Specific known date fields from envelope and header segments
        known_date_fields = [
            "INTERCHANGEDATE",  # ISA09 - Interchange Date
            "DATE_ENVELOPE_GS",  # GS04 - Functional Group Date
            "PAYMENTDATE",  # BPR16 - Payment/Effective Date
            "EFFECTIVEDATE",  # BPR16 - Check/EFT Effective Date
        ]

        for field in csv_rows[0].keys():
            field_upper = field.upper()

            # Exclude time-only columns
            if "TIME" in field_upper and "DATE" not in field_upper:
                continue

            # DTM segment fields are ALWAYS dates (contains _DTM)
            if "_DTM" in field_upper:
                date_columns.add(field)
                continue

            # Check for specific known date fields
            if any(known in field_upper for known in known_date_fields):
                date_columns.add(field)

        if verbose:
            logger.info("      Found %d date columns to validate", len(date_columns))

        # Track statistics
        format_counts = defaultdict(lambda: defaultdict(int))  # column -> format -> count
        unrecognized_dates = defaultdict(list)  # column -> list of (value, claim_id)
        total_dates_checked = 0
        total_valid_dates = 0

        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                logger.info("        Checking date formats: row %s of %s...", f"{idx:,}", f"{len(csv_rows):,}")

            claim_id = row.get("CLM_PatientControlNumber_L2100_CLP", f"Row_{idx}")
            payer_name = row.get("Payer_Name_L1000A_N1", "Unknown")
            payer_state = row.get("Payer_State_L1000A_N4", "Unknown")

            for col in date_columns:
                value = row.get(col)
                if not value or value == "" or value is None:
                    continue

                total_dates_checked += 1
                value_str = str(value).strip()

                # Try to match against known formats
                matched_format = None
                for fmt, fmt_name in known_formats:
                    try:
                        # Handle formats with time by trimming microseconds
                        test_value = value_str.split(".")[0] if "." in value_str else value_str
                        datetime.strptime(test_value, fmt)
                        matched_format = fmt_name
                        break
                    except ValueError:
                        continue

                if matched_format:
                    format_counts[col][matched_format] += 1
                    total_valid_dates += 1
                else:
                    # Check if it's already in MM/DD/YY format (our target format)
                    if re.match(r"^\d{2}/\d{2}/\d{2}$", value_str):
                        format_counts[col]["MM/DD/YY"] += 1
                        total_valid_dates += 1
                    else:
                        unrecognized_dates[col].append((value_str, claim_id))
                        payer_key = f"{payer_name}|{payer_state}"
                        self.stats["payer_data_quality_issues"][payer_key][f"Unrecognized date format: {col}"] += 1

        # Add date format stats to the report
        self.stats["date_format_validation"] = {
            "date_columns_found": list(date_columns),
            "total_dates_checked": total_dates_checked,
            "total_valid_dates": total_valid_dates,
            "format_distribution": {col: dict(formats) for col, formats in format_counts.items()},
            "unrecognized_count": sum(len(v) for v in unrecognized_dates.values()),
        }

        # Report unrecognized date formats as warnings
        for col, issues in unrecognized_dates.items():
            if len(issues) > 0:
                # Only report first few examples per column
                examples = issues[:3]
                example_str = ", ".join([f"'{v}' (claim {c})" for v, c in examples])
                if len(issues) > 3:
                    example_str += f" ... and {len(issues) - 3} more"

                self.warnings.append(
                    ValidationError(
                        "DATE_FORMAT",
                        f"Unrecognized date format in {col}: {example_str}",
                        field=col,
                        actual=f"{len(issues)} values",
                    )
                )

        if verbose:
            logger.info("      Date format validation complete:")
            logger.info("        - Total dates checked: %s", f"{total_dates_checked:,}")
            logger.info("        - Valid dates: %s", f"{total_valid_dates:,}")
            logger.info("        - Unrecognized: %s", f"{sum(len(v) for v in unrecognized_dates.values()):,}")
            if format_counts:
                logger.info("        - Formats found:")
                all_formats = set()
                for formats in format_counts.values():
                    all_formats.update(formats.keys())
                for fmt in sorted(all_formats):
                    total = sum(formats.get(fmt, 0) for formats in format_counts.values())
                    logger.info("          %s: %s", fmt, f"{total:,}")

    def _validate_description_fields(self, csv_rows: List[dict], verbose: bool = False):
        """Validate that description fields are populated when code fields have values"""
        total_rows = len(csv_rows)
        if verbose:
            logger.info("      Validating description fields for %s rows...", f"{total_rows:,}")
        code_desc_pairs = [
            ("CHK_PaymentMethod_Header_BPR", "CHK_PaymentMethodDesc_Header_BPR"),
            ("CHK_Format_Header_BPR", "CHK_FormatDesc_Header_BPR"),
            ("CHK_TraceType_Header_TRN", "CHK_TraceTypeDesc_Header_TRN"),
            ("CHK_CreditDebitFlag_Header_BPR", "CHK_CreditDebitFlagDesc_Header_BPR"),
            ("CHK_BusinessFunctionCode_Header_BPR", "CHK_BusinessFunctionDesc_Header_BPR"),
            ("CHK_PayerDFI_Qualifier_Header_BPR", "CHK_PayerDFI_QualifierDesc_Header_BPR"),
            ("CHK_PayerAccountQualifier_Header_BPR", "CHK_PayerAccountQualifierDesc_Header_BPR"),
            ("CHK_PayeeDFI_Qualifier_Header_BPR", "CHK_PayeeDFI_QualifierDesc_Header_BPR"),
            ("CHK_PayeeAccountQualifier_Header_BPR", "CHK_PayeeAccountQualifierDesc_Header_BPR"),
            ("CHK_DFI_Qualifier_3_Header_BPR", "CHK_DFI_Qualifier_3_Desc_Header_BPR"),
            ("CHK_AccountQualifier_3_Header_BPR", "CHK_AccountQualifier_3_Desc_Header_BPR"),
            ("CLM_Status_L2100_CLP", "CLM_StatusDescr_L2100_CLP"),
            ("CLM_FilingIndicator_L2100_CLP", "CLM_FilingIndicatorDesc_L2100_CLP"),
            ("CLM_FrequencyCode_L2100_CLP", "CLM_FrequencyCodeDesc_L2100_CLP"),
            ("Payer_IDQualifier_L1000A_REF", "Payer_IDQualifierDesc_L1000A_REF"),
            ("Provider_IDQualifier_L1000B_N1", "Provider_IDQualifierDesc_L1000B_N1"),
        ]
        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                logger.info("        Validating descriptions for row %s of %s...", f"{idx:,}", f"{total_rows:,}")
            for code_field, desc_field in code_desc_pairs:
                code_value = row.get(code_field, "")
                desc_value = row.get(desc_field, "")
                if code_value and code_value.strip() and not desc_value:
                    self.errors.append(
                        ValidationError(
                            "DESC_MISSING",
                            "Code field has value but description field is empty",
                            field=desc_field,
                            location=row.get("CLM_PatientControlNumber_L2100_CLP", ""),
                            actual=f"Code={code_value}, Desc=empty",
                        )
                    )

    def _validate_composite_fields(self, edi_data: Dict, csv_rows: List[dict]):
        """Validate that composite fields were properly parsed.

        Note: This legacy method works with flat segment lists and may have issues
        with duplicate claim IDs across files. Use validate_all_by_file() for proper
        per-file validation.
        """
        csv_by_file_and_claim = defaultdict(lambda: defaultdict(list))
        for r in csv_rows:
            claim_id = r.get("CLM_PatientControlNumber_L2100_CLP")
            file_name = r.get("Filename_File", "")
            # Normalize file path to basename for consistent matching
            normalized_file = self._normalize_file_path(file_name)
            if claim_id and r.get("SVC_ProcedureCode_L2110_SVC"):
                csv_by_file_and_claim[normalized_file][claim_id].append(r)
        claims_by_file = defaultdict(dict)
        for claim_id, claim_data in edi_data.get("claims", {}).items():
            # Extract normalized file from composite claim_id
            file_part, actual_claim_id = self._extract_claim_id_from_composite(claim_id)
            normalized_edi_file = self._normalize_file_path(file_part) if file_part else ""
            for file_name, file_claims in csv_by_file_and_claim.items():
                if normalized_edi_file == file_name and actual_claim_id in file_claims:
                    claims_by_file[file_name][actual_claim_id] = claim_data
                    break
        for file_name, file_claims in claims_by_file.items():
            csv_claims = csv_by_file_and_claim[file_name]
            component_delimiter = ":"
            for claim_rows in csv_claims.values():
                if claim_rows:
                    component_delimiter = claim_rows[0].get("ENV_ComponentSeparator_Envelope_ISA", ":") or ":"
                    break
            for claim_id, claim_data in file_claims.items():
                service_rows = csv_claims.get(claim_id, [])
                for svc_idx, service_data in enumerate(claim_data.get("services", [])):
                    svc_segment = service_data.get("SVC")
                    if not svc_segment or len(svc_segment) < 2:
                        continue
                    svc01_composite = svc_segment[1]
                    components = svc01_composite.split(component_delimiter)
                    if len(components) < 2:
                        continue
                    components[0] if len(components) > 0 else ""
                    expected_code = components[1] if len(components) > 1 else ""
                    expected_modifiers = components[2:6]
                    if svc_idx >= len(service_rows):
                        payer_name = "Unknown"
                        payer_state = "Unknown State"
                        if service_rows:
                            payer_name = service_rows[0].get("Payer_Name_L1000A_N1", "Unknown")
                            payer_state = service_rows[0].get("Payer_State_L1000A_N4", "Unknown State")
                        self.errors.append(
                            ValidationError(
                                "COMPOSITE_PARSE",
                                f"Service {svc_idx + 1} exists in EDI but not in CSV",
                                location=f"File: {file_name}, Claim {claim_id}, Service {svc_idx + 1}",
                                expected=f"Service with code {expected_code}",
                                actual="Not found in CSV",
                                payer_info={"name": payer_name, "state": payer_state},
                            )
                        )
                        continue
                    csv_row = service_rows[svc_idx]
                    payer_name = csv_row.get("Payer_Name_L1000A_N1", "Unknown")
                    payer_state = csv_row.get("Payer_State_L1000A_N4", "Unknown State")
                    payer_info = {"name": payer_name, "state": payer_state}
                    actual_code = csv_row.get("SVC_ProcedureCode_L2110_SVC", "")
                    if expected_code != actual_code:
                        self.errors.append(
                            ValidationError(
                                "COMPOSITE_PARSE",
                                "Procedure code not correctly extracted from composite field",
                                location=f"File: {file_name}, Claim {claim_id}, Service {svc_idx + 1}",
                                field="SVC_ProcedureCode_L2110_SVC",
                                expected=expected_code,
                                actual=actual_code,
                                payer_info=payer_info,
                            )
                        )
                    for mod_idx, expected_mod in enumerate(expected_modifiers, 1):
                        if not expected_mod:
                            continue
                        field_name = f"SVC_Modifier{mod_idx}_L2110_SVC"
                        actual_mod = csv_row.get(field_name, "")
                        if expected_mod != actual_mod:
                            self.errors.append(
                                ValidationError(
                                    "COMPOSITE_PARSE",
                                    f"Modifier {mod_idx} not correctly extracted from composite",
                                    location=f"File: {file_name}, Claim {claim_id}, Service {svc_idx + 1}",
                                    field=field_name,
                                    expected=expected_mod,
                                    actual=actual_mod,
                                    payer_info=payer_info,
                                )
                            )

    def _validate_edge_cases(self, csv_rows: List[dict], csv_by_claim: Dict[str, List[dict]], verbose: bool = False):
        """Validate edge cases and anomalies"""
        total_claims = len(csv_by_claim)
        for idx, (claim_id, rows) in enumerate(csv_by_claim.items(), 1):
            if verbose and idx % 500 == 0:
                logger.info("      Checking edge cases for claim %s of %s...", f"{idx:,}", f"{total_claims:,}")
            file_name, actual_claim_id = self._extract_claim_id_from_composite(claim_id)
            display_claim_id = actual_claim_id if file_name else claim_id
            claim_row = next((r for r in rows if r.get("CLM_Status_L2100_CLP")), None)
            if not claim_row:
                continue
            claim_payer_name = claim_row.get("Payer_Name_L1000A_N1", "Unknown")
            claim_payer_state = claim_row.get("Payer_State_L1000A_N4", "Unknown State")
            claim_payer_info = {"name": claim_payer_name, "state": claim_payer_state}
            status = claim_row.get("CLM_Status_L2100_CLP")
            if status == "22":
                charge = self._parse_currency(claim_row.get("CLM_ChargeAmount_L2100_CLP", 0))
                payment = self._parse_currency(claim_row.get("CLM_PaymentAmount_L2100_CLP", 0))
                # Per X12 835 spec section 1.10.2.8, reversal claims should have negative values
                # However, $0 payment is valid when reversing a denied claim (no payment to reverse)
                if charge > 0:  # Positive charge is always wrong for reversal
                    if self._should_debug(claim_payer_name, "ReversalCharge"):
                        logger.debug(
                            "Reversal claim charge error - Claim: %s, Payer: %s, Charge: %s (should be negative or zero)",
                            display_claim_id,
                            claim_payer_name,
                            charge,
                        )
                    self.errors.append(
                        ValidationError(
                            "EDGE",
                            "Reversal claim (Status 22) has positive charge - should be negative per X12 spec section 1.10.2.8",
                            location=f"Claim {display_claim_id}" + (f" (File: {file_name})" if file_name else ""),
                            expected="Negative or zero value",
                            actual=charge,
                            payer_info=claim_payer_info,
                        )
                    )
                if payment > 0:  # Positive payment is always wrong for reversal; $0 is valid for denied claim reversals
                    if self._should_debug(claim_payer_name, "ReversalPayment"):
                        logger.debug(
                            "Reversal claim payment error - Claim: %s, Payer: %s, Payment: %s (should be negative or zero)",
                            display_claim_id,
                            claim_payer_name,
                            payment,
                        )
                    self.errors.append(
                        ValidationError(
                            "EDGE",
                            "Reversal claim (Status 22) has positive payment - should be negative per X12 spec section 1.10.2.8",
                            location=f"Claim {display_claim_id}" + (f" (File: {file_name})" if file_name else ""),
                            expected="Negative or zero value",
                            actual=payment,
                            payer_info=claim_payer_info,
                        )
                    )
            elif status == "4":
                pass
            remark_codes = []
            for i in range(1, 6):
                code = claim_row.get(f"CLM_RemarkCode{i}")
                if code:
                    remark_codes.append(code)
            nsa_codes = ["N864", "N865", "N866", "N875"]
            if any(code in remark_codes for code in nsa_codes):
                if not claim_row.get("CLM_QPA_Amount"):
                    self.warnings.append(
                        ValidationError(
                            "EDGE",
                            "No Surprises Act claim may be missing QPA amount",
                            location=f"Claim ID: {display_claim_id}",
                        )
                    )
            service_rows = [r for r in rows if r.get("SVC_ProcedureCode_L2110_SVC")]
            for svc_row in service_rows:
                proc = svc_row.get("SVC_ProcedureCode_L2110_SVC", "")
                if proc in ["A0425", "A0435", "A0436"]:
                    if status == "22":
                        continue
                    paid_units = svc_row.get("SVC_Units_L2110_SVC", "").strip()
                    original_units = svc_row.get("SVC_OriginalUnits_L2110_SVC", "").strip()
                    payer_name = svc_row.get("Payer_Name_L1000A_N1", "Unknown")
                    payer_state = svc_row.get("Payer_State_L1000A_N4", "")

                    # Parse payment/charge amounts (handles currency formatting)
                    payment_amt = self._parse_currency(svc_row.get("SVC_PaymentAmount_L2110_SVC", 0))
                    self._parse_currency(svc_row.get("SVC_ChargeAmount_L2110_SVC", 0))

                    # Apply X12 835 default rules for missing units:
                    # - SVC05 (paid_units): "If not present, the value is assumed to be one" (X12 TR3)
                    # - SVC07 (original_units): "Required when paid units differ from submitted" (X12 TR3)
                    #   If omitted, original units = paid units
                    # However, for denied claims (payment=0), payers often truncate SVC segment entirely

                    both_missing = not paid_units and not original_units

                    if both_missing:
                        # Per X12 835 TR3 Section 2.3.2.3.2:
                        # "SVC05 - If not present, the value is assumed to be one"
                        # Missing units field = 1 unit per spec, NOT a data quality issue
                        # This is compliant behavior, not an error or warning
                        #
                        # Common valid scenarios where units are omitted:
                        # 1. Base/loaded mile billing (1 unit = base rate)
                        # 2. Flat rate mileage contracts
                        # 3. Single-unit services
                        #
                        # Only flag if charge amount is grossly inconsistent with 1 unit
                        # (e.g., $240 charge at ~$8/mile suggests ~30 miles, not 1)
                        continue
                    else:
                        # At least one unit field is present
                        try:
                            units = float(paid_units) if paid_units else None
                            if units is None or units <= 0:
                                units = float(original_units) if original_units else None
                        except (ValueError, TypeError):
                            units = None

                        # Handle units=0 with payment>0
                        if (units is None or units <= 0) and payment_amt > 0:
                            # Check if this is a SECONDARY claim (CLP02 = '2')
                            # Secondary payers correctly send units=0 because the PRIMARY owns the units
                            if status == "2":
                                # Secondary claim - units=0 is expected and valid
                                continue

                            # Check if filing indicator suggests COB/secondary situation
                            # Filing codes 12-16, 41-47 are Medicare COB related
                            filing_indicator = claim_row.get("CLM_FilingIndicator_L2100_CLP", "")
                            cob_filing_codes = ["12", "13", "14", "15", "16", "41", "42", "43", "44", "45", "46", "47"]
                            if filing_indicator in cob_filing_codes:
                                # COB/Medicare secondary situation - units=0 may be valid
                                # Downgrade to info level, not error
                                continue

                            # For primary claims with non-COB filing, this could be:
                            # 1. Flat rate mileage payment (valid)
                            # 2. Base/loaded mile scenario (valid)
                            # 3. Data quality issue
                            # Downgrade to warning, not error
                            self.warnings.append(
                                ValidationError(
                                    "EDGE",
                                    f"Mileage {proc} paid with zero units (may be flat rate or base mile)",
                                    location=f"Claim {display_claim_id}, Service {proc}",
                                    actual=f"Paid: {paid_units}, Original: {original_units}, Payment: ${payment_amt:.2f}",
                                    payer_info={"name": payer_name, "state": payer_state},
                                )
                            )
            # Suppressed warning: certain payers omit REF*F8/XZ for frequency-7/8 claims
            # frequency_code = claim_row.get('CLM_FrequencyCode_L2100_CLP', '')
            # if frequency_code in ['7', '8']:
            #     original_ref = claim_row.get('CLM_OriginalRef_L2100_REF', '')
            #     if not original_ref:
            #         self.warnings.append(ValidationError(
            #         'EDGE',
            #         f"Void/Replace claim (freq={frequency_code}) missing original reference",
            #         location=f"Claim ID: {display_claim_id}"
            #     ))
            if not display_claim_id or display_claim_id == "UNKNOWN":
                source_file = claim_row.get("Filename_File", "UNKNOWN_FILE")
                display_claim_id = f"EMPTY_ID_IN_{os.path.basename(source_file)}"
            # DISABLED: Claim-level date validation (DTM*232/233)
            # Reason: Claim-level dates are often unreliable (swapped, typos).
            # Service-level dates (DTM*150/472/151) are more accurate.
            # See DATE_STRUCTURE_ANALYSIS.md for details.
            charge = self._parse_currency(claim_row.get("CLM_ChargeAmount_L2100_CLP", 0))
            payment = self._parse_currency(claim_row.get("CLM_PaymentAmount_L2100_CLP", 0))
            self._parse_currency(claim_row.get("CLM_InterestAmount_L2100_AMT", 0))

            # Validate Allowed Amount calculations
            # Service-level: Method1 (Charge - CO) should equal Method2 (Payment + PR)
            for svc_row in service_rows:
                try:
                    svc_method1 = self._parse_currency(svc_row.get("Allowed_Amount", 0))
                    svc_method2 = self._parse_currency(svc_row.get("Allowed_Verification", 0))

                    # Skip if both are zero (likely no service data)
                    if svc_method1 == 0 and svc_method2 == 0:
                        continue

                    # Check if methods match (within $0.01 tolerance)
                    if abs(svc_method1 - svc_method2) > 0.01:
                        proc = svc_row.get("SVC_ProcedureCode_L2110_SVC", "Unknown")
                        svc_charge = self._parse_currency(svc_row.get("SVC_ChargeAmount_L2110_SVC", 0))
                        svc_payment = self._parse_currency(svc_row.get("SVC_PaymentAmount_L2110_SVC", 0))
                        note = ""
                        if claim_payer_name and "PROSPECT" in claim_payer_name.upper():
                            note = " Prospect encodes sequestration in CO-253, so Method1 includes it while Method2 (Payment+PR) does not. CSV values match source EDI."
                        actual_text = f"Difference: ${abs(svc_method1 - svc_method2):.2f}"
                        if note:
                            actual_text += note
                        self.warnings.append(
                            ValidationError(
                                "CALC",
                                f"Service allowed amount mismatch: Method1 (Charge-CO)=${svc_method1:.2f} vs Method2 (Payment+PR)=${svc_method2:.2f}",
                                location=f"Claim {display_claim_id} | Service {proc}",
                                expected=f"Methods should match. Charge=${svc_charge:.2f}, Payment=${svc_payment:.2f}",
                                actual=actual_text,
                                payer_info=claim_payer_info,
                            )
                        )
                except (ValueError, TypeError):
                    proc = svc_row.get("SVC_ProcedureCode_L2110_SVC", "unknown")
                    logger.warning(
                        "Invalid service allowed amount values for claim %s, service %s: allowed_amount='%s', allowed_verification='%s'",
                        display_claim_id,
                        proc,
                        svc_row.get("Allowed_Amount"),
                        svc_row.get("Allowed_Verification"),
                    )

    def _validate_loop_structure(self, edi_data: Dict):
        """Validate loop structure follows X12 835 rules"""
        if "BPR" not in edi_data["header"]:
            self.errors.append(
                ValidationError(
                    "MISSING_REQUIRED", "Required BPR segment missing from transaction header", segment="BPR"
                )
            )
        if "TRN" not in edi_data["header"]:
            self.errors.append(
                ValidationError(
                    "MISSING_REQUIRED", "Required TRN segment missing from transaction header", segment="TRN"
                )
            )
        for claim_id, claim_data in edi_data["claims"].items():
            if "CLP" not in claim_data.get("segments", {}) and "CLP" not in claim_data:
                self.errors.append(
                    ValidationError(
                        "MISSING_REQUIRED",
                        "Required CLP segment missing from claim",
                        location=f"Claim {claim_id}",
                        segment="CLP",
                    )
                )
        for claim_id, claim_data in edi_data["claims"].items():
            for svc_idx, service_data in enumerate(claim_data.get("services", [])):
                if "SVC" not in service_data and not service_data.get("SVC"):
                    self.errors.append(
                        ValidationError(
                            "MISSING_REQUIRED",
                            "Required SVC segment missing from service line",
                            location=f"Claim {claim_id}, Service {svc_idx + 1}",
                            segment="SVC",
                        )
                    )

    def _validate_critical_sequences(self, segments: List[str], delimiter: str):
        """Validate critical segment sequences per X12 835 spec"""
        segment_positions = {}
        for idx, segment in enumerate(segments):
            if not segment.strip():
                continue
            elements = segment.split(delimiter)
            seg_id = elements[0]
            if seg_id not in segment_positions:
                segment_positions[seg_id] = idx
        if "BPR" in segment_positions and "TRN" in segment_positions:
            if segment_positions["TRN"] < segment_positions["BPR"]:
                self.errors.append(
                    ValidationError(
                        "SEQUENCE_VIOLATION",
                        "TRN segment must follow BPR segment",
                        segment="TRN",
                        location=f'Segment position {segment_positions["TRN"]}',
                    )
                )
        if "CLP" in segment_positions and "SVC" in segment_positions:
            if segment_positions["SVC"] < segment_positions["CLP"]:
                self.errors.append(
                    ValidationError(
                        "SEQUENCE_VIOLATION",
                        "SVC segment cannot appear before first CLP segment",
                        segment="SVC",
                        location=f'Segment position {segment_positions["SVC"]}',
                    )
                )

    def _build_validation_report(self, edi_segments: List[str], csv_rows: List[dict]) -> Dict:
        """Build comprehensive validation report"""
        errors_by_type = defaultdict(list)
        errors_by_payer = defaultdict(lambda: defaultdict(list))
        for error in self.errors:
            errors_by_type[error.type].append(error)
            if error.payer_info:
                payer_key = f"{error.payer_info.get('name', 'Unknown')} ({error.payer_info.get('state', 'Unknown')})"
                errors_by_payer[payer_key][error.type].append(error)
        warnings_list = []
        for warning in self.warnings:
            if isinstance(warning, ValidationError):
                warnings_list.append(warning.to_dict())
            else:
                warnings_list.append({"message": str(warning), "type": "WARNING"})
        report = {
            "summary": {
                "total_segments": len(edi_segments),
                "total_fields": self.stats["total_fields"],
                "fields_validated": self.stats["fields_validated"],
                "calculations_checked": self.stats["calculations_checked"],
                "error_count": len(self.errors),
                "warning_count": len(self.warnings),
                "validation_status": "PASS" if len(self.errors) == 0 else "FAIL",
            },
            "errors_by_type": {
                error_type: [e.to_dict() for e in errors] for error_type, errors in errors_by_type.items()
            },
            "errors_by_payer": {
                payer: {error_type: [e.to_dict() for e in errors] for error_type, errors in payer_errors.items()}
                for payer, payer_errors in errors_by_payer.items()
            },
            "warnings": warnings_list,
            "missing_mappings": dict(self.stats["missing_mappings"]),
            "payers_missing_mileage_units": dict(self.stats["payers_missing_mileage_units"]),
            "payer_data_quality_issues": self._reorganize_quality_issues(),
            "priority_rarc_codes": {k: dict(v) for k, v in self.stats["priority_rarc_codes"].items()},
            "date_format_validation": self.stats.get("date_format_validation", {}),
            "sample_errors": self._get_sample_errors(csv_rows),
            "validation_timestamp": datetime.now().isoformat(),
        }
        return report

    def _reorganize_quality_issues(self) -> Dict:
        """Reorganize payer_data_quality_issues into structured categories for reporting"""
        raw_issues = self.stats["payer_data_quality_issues"]
        result = {
            "missing_carc_codes": defaultdict(dict),
            "missing_rarc_codes": defaultdict(dict),
            "missing_dictionary_entries": defaultdict(dict),
            "unrecognized_date_formats": defaultdict(int),
            "transaction_balance_skips": defaultdict(int),
            "empty_claims_count": 0,
            "duplicate_claims": defaultdict(int),
            "other_issues": defaultdict(dict),
        }
        for payer_key, issues in raw_issues.items():
            for issue_key, count in issues.items():
                if issue_key.startswith("Missing CARC: "):
                    code = issue_key.replace("Missing CARC: ", "")
                    result["missing_carc_codes"][code][payer_key] = count
                elif issue_key.startswith("Missing RARC: "):
                    code = issue_key.replace("Missing RARC: ", "")
                    result["missing_rarc_codes"][code][payer_key] = count
                elif issue_key.startswith("Missing dictionary: "):
                    entry_type = issue_key.replace("Missing dictionary: ", "")
                    result["missing_dictionary_entries"][entry_type][payer_key] = count
                elif issue_key.startswith("Unrecognized date format: "):
                    date_val = issue_key.replace("Unrecognized date format: ", "")
                    result["unrecognized_date_formats"][date_val] += count
                elif issue_key.startswith("Transaction balance skip: "):
                    reason = issue_key.replace("Transaction balance skip: ", "")
                    result["transaction_balance_skips"][reason] += count
                elif issue_key == "Empty claim":
                    result["empty_claims_count"] += count
                elif issue_key.startswith("Duplicate claim: "):
                    claim_id = issue_key.replace("Duplicate claim: ", "")
                    result["duplicate_claims"][claim_id] += count
                else:
                    result["other_issues"][issue_key][payer_key] = count
        # Convert defaultdicts to regular dicts
        return {
            "missing_carc_codes": dict(result["missing_carc_codes"]),
            "missing_rarc_codes": dict(result["missing_rarc_codes"]),
            "missing_dictionary_entries": dict(result["missing_dictionary_entries"]),
            "unrecognized_date_formats": dict(result["unrecognized_date_formats"]),
            "transaction_balance_skips": dict(result["transaction_balance_skips"]),
            "empty_claims_count": result["empty_claims_count"],
            "duplicate_claims": dict(result["duplicate_claims"]),
            "other_issues": dict(result["other_issues"]),
        }

    def _get_sample_errors(self, csv_rows: List[dict], max_samples: int = 5) -> List[Dict]:
        """Get sample errors with redacted data"""
        samples = []
        for error in self.errors[:max_samples]:
            sample = {"error": error.to_dict(), "context": {}}
            if error.location:
                claim_id = error.location.split(",")[0].replace("Claim ", "")
                matching_row = next(
                    (r for r in csv_rows if r.get("CLM_PatientControlNumber_L2100_CLP") == claim_id), None
                )
                if matching_row:
                    if error.type == "CALC":
                        sample["context"] = {
                            field: matching_row.get(field)
                            for field in matching_row
                            if "Amount" in field or "Payment" in field or "Charge" in field
                        }
                    elif error.field:
                        sample["context"] = {
                            field: matching_row.get(field)
                            for field in matching_row
                            if error.field and (error.field in field or field in error.field)
                        }
            samples.append(sample)
        return samples


def generate_executive_dashboard(validation_result: Dict) -> str:
    """Generate executive summary dashboard grouped by issue type for data quality analysis"""
    summary = validation_result["summary"]
    lines = []
    lines.append("=" * 100)
    lines.append("VALIDATION EXECUTIVE DASHBOARD - GROUPED BY ISSUE TYPE")
    lines.append("=" * 100)
    lines.append("")
    status = summary["validation_status"]
    status_symbol = "[PASS]" if status == "PASS" else "[FAIL]"
    lines.append(f"{status_symbol} OVERALL STATUS: {status}")
    lines.append("")
    # Show data loading issues prominently at the top
    if validation_result.get("data_load_issues"):
        lines.append("DATA LOADING ISSUES:")
        lines.append("-" * 100)
        for issue in validation_result["data_load_issues"]:
            severity = issue.get("severity", "WARNING")
            issue_type = issue.get("type", "UNKNOWN")
            message = issue.get("message", "Unknown error")
            lines.append(f"  [{severity}] {issue_type}: {message}")
        lines.append("")
    lines.append("KEY METRICS:")
    lines.append(f"  Segments Processed: {summary['total_segments']:,}")
    lines.append(f"  Fields Validated: {summary['fields_validated']:,}")
    lines.append(f"  Calculations Checked: {summary['calculations_checked']:,}")
    lines.append(f"  Errors: {summary['error_count']:,}")
    lines.append(f"  Warnings: {summary['warning_count']:,}")
    lines.append("")
    if validation_result.get("errors_by_type"):
        lines.append("ISSUES GROUPED BY TYPE:")
        lines.append("-" * 100)
        from collections import defaultdict

        issues_by_type = defaultdict(lambda: defaultdict(list))
        for error_type, errors in (validation_result.get("errors_by_type") or {}).items():
            for error in errors:
                if not isinstance(error, dict):
                    continue
                msg = error.get("message") or ""
                if "doesn't balance" in msg:
                    issue_key = "Balance Error"
                elif "Service start date is after end date" in msg:
                    issue_key = "Date Logic Error"
                elif "Predetermination" in msg:
                    issue_key = "Predetermination Error"
                elif "missing" in msg.lower():
                    issue_key = "Missing Data"
                elif "mismatch" in msg.lower():
                    issue_key = "Data Mismatch"
                else:
                    issue_key = error_type
                payer_info = error.get("payer_info", {})
                payer_name = payer_info.get("name", "Unknown")
                payer_state = payer_info.get("state", "Unknown")
                payer_key = f"{payer_name}|{payer_state}"
                issues_by_type[issue_key][payer_key].append(error)
        for issue_type in sorted(issues_by_type.keys()):
            payer_data = issues_by_type[issue_type]
            total_count = sum(len(errors) for errors in payer_data.values())
            lines.append(f"\n{issue_type.upper()} ({total_count:,} total):")
            lines.append("  Payers affected:")
            for payer_key in sorted(payer_data.keys()):
                errors = payer_data[payer_key]
                payer_name, payer_state = payer_key.split("|")
                lines.append(f"    - {payer_name} ({payer_state}): {len(errors):,} instances")
                example = errors[0]
                location = example.get("location", "Unknown")
                if example.get("expected") is not None and example.get("actual") is not None:
                    lines.append(f"      Example: {location}")
                    lines.append(f"        Expected: {example['expected']}, Actual: {example['actual']}")
                else:
                    lines.append(f"      Example: {location}")
                if example.get("edi_context"):
                    lines.append("      Source EDI Data:")
                    for seg in example["edi_context"]:
                        lines.append(f"        {seg}")
        lines.append("")
    if validation_result.get("missing_mappings"):
        total_gaps = sum(len(codes) for codes in validation_result["missing_mappings"].values())
        payer_count = len(validation_result["missing_mappings"])
        lines.append(f"DICTIONARY GAPS: {total_gaps:,} missing codes across {payer_count} payers")
        gap_counts = [(payer, len(codes)) for payer, codes in validation_result["missing_mappings"].items()]
        gap_counts.sort(key=lambda x: x[1], reverse=True)
        lines.append("  Top payers with missing codes:")
        for payer, count in gap_counts[:5]:
            payer_name = payer.split("|")[0]
            lines.append(f"    - {payer_name}: {count} missing codes")
        lines.append("")
    # Date Format Validation Summary
    if validation_result.get("date_format_validation"):
        date_stats = validation_result["date_format_validation"]
        lines.append("DATE FORMAT VALIDATION:")
        lines.append(f"  Date columns found: {len(date_stats.get('date_columns_found', []))}")
        lines.append(f"  Total dates checked: {date_stats.get('total_dates_checked', 0):,}")
        lines.append(f"  Valid dates: {date_stats.get('total_valid_dates', 0):,}")
        lines.append(f"  Unrecognized formats: {date_stats.get('unrecognized_count', 0):,}")

        if date_stats.get("format_distribution"):
            all_formats = set()
            for formats in date_stats["format_distribution"].values():
                all_formats.update(formats.keys())
            if all_formats:
                lines.append("  Formats encountered:")
                for fmt in sorted(all_formats):
                    total = sum(formats.get(fmt, 0) for formats in date_stats["format_distribution"].values())
                    lines.append(f"    - {fmt}: {total:,}")
        lines.append("")

    if validation_result.get("payers_missing_mileage_units"):
        payer_data = validation_result.get("payers_missing_mileage_units", {})
        total_missing = sum(payer_data.values())
        payer_count = len(payer_data)
        lines.append(f"PAYERS MISSING MILEAGE UNIT DATA: {total_missing:,} instances across {payer_count} payers")
        payer_list = sorted(payer_data.items(), key=lambda x: x[1], reverse=True)
        lines.append("  Payers with incomplete mileage data:")
        for payer, count in payer_list:
            payer_parts = payer.split("|")
            payer_name = payer_parts[0]
            payer_state = payer_parts[1] if len(payer_parts) > 1 else ""
            display = f"{payer_name} ({payer_state})" if payer_state else payer_name
            lines.append(f"    - {display}: {count} service line(s)")
        lines.append("")
    # Payer Data Quality Issues - CARC/RARC codes, etc.
    if validation_result.get("payer_data_quality_issues"):
        quality_issues = validation_result["payer_data_quality_issues"]
        has_issues = any(
            quality_issues.get(k)
            for k in [
                "missing_carc_codes",
                "missing_rarc_codes",
                "missing_dictionary_entries",
                "unrecognized_date_formats",
                "transaction_balance_skips",
                "empty_claims_count",
                "duplicate_claims",
            ]
        )
        if has_issues:
            lines.append("PAYER DATA QUALITY ISSUES:")
            lines.append("-" * 100)
            # Missing CARC codes summary
            if quality_issues.get("missing_carc_codes"):
                carc_data = quality_issues["missing_carc_codes"]
                carc_count = len(carc_data)
                lines.append(f"  Missing CARC Codes: {carc_count} code(s) not in dictionary")
                for code in sorted(carc_data.keys())[:5]:
                    payers = carc_data[code]
                    payer_list = list(payers.keys()) if isinstance(payers, dict) else list(payers)
                    lines.append(f"    - Code {code}: used by {len(payer_list)} payer(s)")
                if carc_count > 5:
                    lines.append(f"    ... and {carc_count - 5} more codes")
            # Missing RARC codes summary
            if quality_issues.get("missing_rarc_codes"):
                rarc_data = quality_issues["missing_rarc_codes"]
                rarc_count = len(rarc_data)
                lines.append(f"  Missing RARC Codes: {rarc_count} code(s) not in dictionary")
                for code in sorted(rarc_data.keys())[:5]:
                    payers = rarc_data[code]
                    payer_list = list(payers.keys()) if isinstance(payers, dict) else list(payers)
                    lines.append(f"    - Code {code}: used by {len(payer_list)} payer(s)")
                if rarc_count > 5:
                    lines.append(f"    ... and {rarc_count - 5} more codes")
            # Transaction balance skips
            if quality_issues.get("transaction_balance_skips"):
                skip_data = quality_issues["transaction_balance_skips"]
                total_skips = sum(skip_data.values())
                lines.append(f"  Transaction Balance Skips: {total_skips} transaction(s) skipped")
                for reason, count in sorted(skip_data.items(), key=lambda x: x[1], reverse=True):
                    lines.append(f"    - {reason}: {count}")
            # Empty claims
            if quality_issues.get("empty_claims_count"):
                lines.append(f"  Empty Claims: {quality_issues['empty_claims_count']} claim(s) with no service lines")
            # Duplicate claims
            if quality_issues.get("duplicate_claims"):
                dup_count = len(quality_issues["duplicate_claims"])
                lines.append(f"  Duplicate Claims: {dup_count} claim ID(s) appear multiple times")
            # Unrecognized date formats
            if quality_issues.get("unrecognized_date_formats"):
                date_count = len(quality_issues["unrecognized_date_formats"])
                lines.append(f"  Unrecognized Date Formats: {date_count} unique format(s)")
            lines.append("")
    lines.append("RECOMMENDED ACTIONS:")
    if summary["error_count"] == 0:
        lines.append("  [OK] No critical issues found - system operating correctly")
    else:
        priority_actions = []
        if validation_result.get("errors_by_type", {}).get("CALC"):
            priority_actions.append("CRITICAL: Fix mathematical balancing errors (may indicate parser bugs)")
        if validation_result.get("errors_by_type", {}).get("MISSING"):
            priority_actions.append("HIGH: Investigate missing required fields")
        if validation_result.get("errors_by_type", {}).get("CAS_CATEGORY"):
            priority_actions.append("HIGH: Review CAS adjustment categorization logic")
        if validation_result.get("missing_mappings"):
            priority_actions.append("MEDIUM: Update dictionary with missing code mappings")
        if validation_result.get("errors_by_type", {}).get("COMPOSITE_PARSE"):
            priority_actions.append("MEDIUM: Fix composite field parsing")
        for idx, action in enumerate(priority_actions, 1):
            lines.append(f"  {idx}. {action}")
    lines.append("")
    lines.append("=" * 100)
    return "\n".join(lines)


def generate_validation_report(
    validation_result: Dict, output_format: str = "text", output_file: str = None, redact: bool = True
) -> str:
    """Generate a formatted validation report"""
    dashboard = generate_executive_dashboard(validation_result)
    if output_format == "html":
        report_content = generate_html_report(validation_result, redact)
    else:
        detail_report = generate_text_report(validation_result, redact)
        report_content = dashboard + "\n\n" + detail_report
    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report_content)
    return report_content


def generate_text_report(validation_result: Dict, redact: bool = True) -> str:
    """Generate text format validation report"""
    lines = []
    lines.append("=" * 80)
    lines.append("VALIDATION REPORT - ZERO FAIL MODE")
    lines.append("=" * 80)
    lines.append("")
    summary = validation_result["summary"]
    lines.append(f"Validation Status: {summary['validation_status']}")
    lines.append(f"Generated: {validation_result.get('validation_timestamp', '')}")
    lines.append("")
    lines.append(f"Total Segments: {summary['total_segments']:,}")
    lines.append(f"Total Fields: {summary['total_fields']:,}")
    lines.append(f"Fields Validated: {summary['fields_validated']:,}")
    lines.append(f"Calculations Checked: {summary['calculations_checked']:,}")
    lines.append("")
    lines.append(f"Errors Found: {summary['error_count']}")
    lines.append(f"Warnings: {summary['warning_count']}")
    lines.append("")
    # Show data loading issues
    if validation_result.get("data_load_issues"):
        lines.append("-" * 80)
        lines.append("DATA LOADING ISSUES")
        lines.append("-" * 80)
        lines.append("")
        lines.append("The following issues occurred while loading external data files:")
        lines.append("")
        for issue in validation_result["data_load_issues"]:
            severity = issue.get("severity", "WARNING")
            issue_type = issue.get("type", "UNKNOWN")
            message = issue.get("message", "Unknown error")
            lines.append(f"[{severity}] {issue_type}")
            lines.append(f"  {message}")
            lines.append("")
    lines.append("-" * 80)
    if validation_result.get("errors_by_type"):
        lines.append("ERRORS BY TYPE")
        lines.append("-" * 80)
        for error_type, errors in validation_result["errors_by_type"].items():
            lines.append(f"\n{error_type} Errors ({len(errors)} found):")
            lines.append("-" * 40)
            for i, error in enumerate(errors[:10], 1):
                lines.append(f"{i}. {error.get('message', 'Unknown error')}")
                if error.get("location"):
                    lines.append(f"   Location: {error['location']}")
                if error.get("expected") is not None and error.get("actual") is not None:
                    lines.append(f"   Expected: {error['expected']}, Actual: {error['actual']}")
                if error.get("field"):
                    lines.append(f"   Field: {error['field']}")
                if error.get("edi_context"):
                    lines.append("   Source EDI Data:")
                    for seg in error["edi_context"]:
                        lines.append(f"      {seg}")
                lines.append("")
            if len(errors) > 10:
                lines.append(f"   ... and {len(errors) - 10} more {error_type} errors")
        lines.append("")
        lines.append("-" * 80)
    if validation_result.get("warnings"):
        lines.append("WARNINGS")
        lines.append("-" * 80)
        for i, warning in enumerate(validation_result["warnings"][:20], 1):
            lines.append(f"{i}. {warning.get('message', 'Unknown warning')}")
            if warning.get("location"):
                lines.append(f"   Location: {warning['location']}")
            if warning.get("actual"):
                lines.append(f"   Details: {warning['actual']}")
            lines.append("")
        if len(validation_result["warnings"]) > 20:
            lines.append(f"   ... and {len(validation_result['warnings']) - 20} more warnings")
        lines.append("")
        lines.append("-" * 80)
    if validation_result.get("missing_mappings"):
        lines.append("MISSING DICTIONARY MAPPINGS")
        lines.append("-" * 80)
        for payer_key, mappings in validation_result["missing_mappings"].items():
            payer_name, state = payer_key.split("|")
            lines.append(f"\nPayer: {payer_name}")
            lines.append(f"State: {state}")
            lines.append("")
            by_field = defaultdict(list)
            for mapping in mappings:
                by_field[mapping["field"]].append(mapping["code"])
            for field, codes in by_field.items():
                unique_codes = list(set(codes))
                lines.append(f"Field: {field}")
                lines.append(f"Missing Codes: {', '.join(unique_codes[:5])}")
                if len(unique_codes) > 5:
                    lines.append(f"   ... and {len(unique_codes) - 5} more")
                lines.append("")
        lines.append("-" * 80)
    if validation_result.get("payers_missing_mileage_units"):
        lines.append("PAYERS WITH MISSING MILEAGE UNIT DATA")
        lines.append("-" * 80)
        lines.append("")
        lines.append("The following payers sent mileage service lines (A0425/A0435/A0436)")
        lines.append("without ANY unit data in SVC05 or SVC07. This is a payer data quality issue.")
        lines.append("")
        payer_data = validation_result.get("payers_missing_mileage_units", {})
        payer_list = sorted(payer_data.items(), key=lambda x: x[1], reverse=True)
        for payer, count in payer_list:
            payer_parts = payer.split("|")
            payer_name = payer_parts[0]
            payer_state = payer_parts[1] if len(payer_parts) > 1 else "N/A"
            lines.append(f"Payer: {payer_name}")
            lines.append(f"State: {payer_state}")
            lines.append(f"Missing Unit Count: {count} service line(s)")
            lines.append("")
        lines.append("-" * 80)
    if validation_result.get("sample_errors"):
        lines.append("SAMPLE ERRORS WITH CONTEXT")
        lines.append("-" * 80)
        for i, sample in enumerate(validation_result["sample_errors"], 1):
            error = sample["error"]
            context = sample.get("context", {})
            lines.append(f"\nExample {i}: {error.get('type', 'UNKNOWN')} - {error.get('message', 'Unknown error')}")
            if error.get("location"):
                lines.append(f"Location: {error['location']}")
            if context:
                lines.append("Context Data:")
                for field, value in sorted(context.items()):
                    if value is not None and value != "":
                        if redact and any(sensitive in field for sensitive in ["Name", "SSN", "MemberID", "Address"]):
                            value = "[REDACTED]"
                        lines.append(f"  {field}: {value}")
            lines.append("")
        lines.append("-" * 80)
    if validation_result.get("errors_by_payer"):
        lines.append("SUMMARY BY PAYER")
        lines.append("-" * 80)
        for payer, payer_errors in validation_result["errors_by_payer"].items():
            error_counts = {error_type: len(errors) for error_type, errors in payer_errors.items()}
            total = sum(error_counts.values())
            summary_parts = [f"{count} {error_type}" for error_type, count in error_counts.items()]
            lines.append(f"{payer}: {', '.join(summary_parts)} (Total: {total})")
        lines.append("")
    # Payer Data Quality Issues - CARC/RARC codes, date formats, etc.
    if validation_result.get("payer_data_quality_issues"):
        lines.append("-" * 80)
        lines.append("PAYER DATA QUALITY ISSUES")
        lines.append("-" * 80)
        lines.append("")
        lines.append("Issues detected that may require payer follow-up or dictionary updates:")
        lines.append("")
        quality_issues = validation_result["payer_data_quality_issues"]
        # Missing CARC codes
        if quality_issues.get("missing_carc_codes"):
            lines.append("MISSING CARC CODES (Claim Adjustment Reason Codes):")
            lines.append("-" * 40)
            carc_data = quality_issues["missing_carc_codes"]
            for code, payers in sorted(carc_data.items()):
                payer_list = list(payers.keys()) if isinstance(payers, dict) else list(payers)
                lines.append(f"  Code: {code}")
                lines.append(f"    Payers: {', '.join(payer_list[:5])}")
                if len(payer_list) > 5:
                    lines.append(f"    ... and {len(payer_list) - 5} more payers")
            lines.append("")
        # Missing RARC codes
        if quality_issues.get("missing_rarc_codes"):
            lines.append("MISSING RARC CODES (Remittance Advice Remark Codes):")
            lines.append("-" * 40)
            rarc_data = quality_issues["missing_rarc_codes"]
            for code, payers in sorted(rarc_data.items()):
                payer_list = list(payers.keys()) if isinstance(payers, dict) else list(payers)
                lines.append(f"  Code: {code}")
                lines.append(f"    Payers: {', '.join(payer_list[:5])}")
                if len(payer_list) > 5:
                    lines.append(f"    ... and {len(payer_list) - 5} more payers")
            lines.append("")
        # Missing dictionary entries
        if quality_issues.get("missing_dictionary_entries"):
            lines.append("MISSING DICTIONARY ENTRIES:")
            lines.append("-" * 40)
            dict_data = quality_issues["missing_dictionary_entries"]
            for entry_type, entries in sorted(dict_data.items()):
                entry_list = list(entries.keys()) if isinstance(entries, dict) else list(entries)
                lines.append(f"  Type: {entry_type}")
                lines.append(f"    Missing: {', '.join(str(e) for e in entry_list[:10])}")
                if len(entry_list) > 10:
                    lines.append(f"    ... and {len(entry_list) - 10} more entries")
            lines.append("")
        # Unrecognized date formats
        if quality_issues.get("unrecognized_date_formats"):
            lines.append("UNRECOGNIZED DATE FORMATS:")
            lines.append("-" * 40)
            date_data = quality_issues["unrecognized_date_formats"]
            for date_val, count in sorted(date_data.items(), key=lambda x: x[1], reverse=True)[:10]:
                lines.append(f"  '{date_val}' - {count} occurrence(s)")
            if len(date_data) > 10:
                lines.append(f"  ... and {len(date_data) - 10} more formats")
            lines.append("")
        # Transaction balance skips
        if quality_issues.get("transaction_balance_skips"):
            lines.append("TRANSACTION BALANCE SKIPS:")
            lines.append("-" * 40)
            skip_data = quality_issues["transaction_balance_skips"]
            for reason, count in sorted(skip_data.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"  {reason}: {count} transaction(s)")
            lines.append("")
        # Empty claims
        if quality_issues.get("empty_claims_count"):
            lines.append(f"EMPTY CLAIMS: {quality_issues['empty_claims_count']} claim(s) with no service lines")
            lines.append("")
        # Duplicate claims
        if quality_issues.get("duplicate_claims"):
            lines.append("DUPLICATE CLAIMS DETECTED:")
            lines.append("-" * 40)
            dup_data = quality_issues["duplicate_claims"]
            for claim_id, count in sorted(dup_data.items(), key=lambda x: x[1], reverse=True)[:10]:
                lines.append(f"  Claim {claim_id}: {count} occurrence(s)")
            if len(dup_data) > 10:
                lines.append(f"  ... and {len(dup_data) - 10} more duplicates")
            lines.append("")
        # Other issues not categorized above
        if quality_issues.get("other_issues"):
            lines.append("OTHER DATA QUALITY ISSUES:")
            lines.append("-" * 40)
            other_data = quality_issues["other_issues"]
            for issue_key, payers in sorted(other_data.items()):
                total = sum(payers.values()) if isinstance(payers, dict) else payers
                lines.append(f"  {issue_key}: {total} occurrence(s)")
            lines.append("")
        lines.append("-" * 80)
    return "\n".join(lines)


def generate_html_report(validation_result: Dict, redact: bool = True) -> str:
    """Generate HTML format validation report"""
    html_parts = []
    html_parts.append("""
<!DOCTYPE html>
<html>
<head>
    <title>835 CSV Validation Report</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        h1, h2, h3 { color: #333; }
        .pass { color: #28a745; }
        .fail { color: #dc3545; }
        .warning { color: #ffc107; }
        .summary-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .summary-table td, .summary-table th {
            padding: 10px;
            border: 1px solid #ddd;
            text-align: left;
        }
        .summary-table th {
            background-color: #f8f9fa;
            font-weight: bold;
        }
        .error-box {
            background-color: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
        }
        .warning-box {
            background-color: #fff3cd;
            border: 1px solid #ffeaa7;
            color: #856404;
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
        }
        .code-sample {
            background-color: #f4f4f4;
            border: 1px solid #ddd;
            padding: 10px;
            font-family: monospace;
            font-size: 12px;
            overflow-x: auto;
        }
        .error-type {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 11px;
            font-weight: bold;
            margin-right: 5px;
        }
        .error-type.calc { background-color: #ff6b6b; color: white; }
        .error-type.missing { background-color: #ffa500; color: white; }
        .error-type.mismatch { background-color: #dc3545; color: white; }
        .error-type.mapping { background-color: #6c757d; color: white; }
        .error-type.edge { background-color: #17a2b8; color: white; }
        .collapsible {
            cursor: pointer;
            padding: 10px;
            background-color: #e9ecef;
            border: none;
            text-align: left;
            outline: none;
            font-size: 16px;
            width: 100%;
            margin: 5px 0;
        }
        .collapsible:after {
            content: '\\002B';
            float: right;
            font-weight: bold;
        }
        .collapsible.active:after {
            content: "\\2212";
        }
        .content {
            padding: 0 18px;
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.2s ease-out;
            background-color: #f8f9fa;
        }
    </style>
</head>
<body>
<div class="container">
""")
    summary = validation_result["summary"]
    status_class = "pass" if summary["validation_status"] == "PASS" else "fail"
    html_parts.append(f"""
    <h1>835 CSV Validation Report</h1>
    <h2 class="{status_class}">Status: {summary['validation_status']}</h2>
    <p>Generated: {validation_result.get('validation_timestamp', '')}</p>
""")
    html_parts.append("""
    <h3>Validation Summary</h3>
    <table class="summary-table">
        <tr>
            <th>Metric</th>
            <th>Value</th>
        </tr>
""")
    summary_rows = [
        ("Total Segments", f"{summary['total_segments']:,}"),
        ("Total Fields", f"{summary['total_fields']:,}"),
        ("Fields Validated", f"{summary['fields_validated']:,}"),
        ("Calculations Checked", f"{summary['calculations_checked']:,}"),
        ("Errors Found", f"<span class='fail'>{summary['error_count']}</span>" if summary["error_count"] > 0 else "0"),
        (
            "Warnings",
            f"<span class='warning'>{summary['warning_count']}</span>" if summary["warning_count"] > 0 else "0",
        ),
    ]
    for label, value in summary_rows:
        html_parts.append(f"""
        <tr>
            <td>{label}</td>
            <td>{value}</td>
        </tr>
""")
    html_parts.append("    </table>")
    # Show data loading issues if any
    if validation_result.get("data_load_issues"):
        html_parts.append("""
    <h3 style="color: #856404;">Data Loading Issues</h3>
    <div style="background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
        <p style="margin-top: 0;"><strong>The following issues occurred while loading external data files:</strong></p>
""")
        for issue in validation_result["data_load_issues"]:
            severity = html.escape(issue.get("severity", "WARNING"))
            issue_type = html.escape(issue.get("type", "UNKNOWN"))
            message = html.escape(issue.get("message", "Unknown error"))
            html_parts.append(f"""
        <div style="margin: 10px 0; padding: 10px; background-color: #ffeaa7; border-radius: 3px;">
            <strong>[{severity}] {issue_type}</strong><br>
            <span style="font-family: monospace;">{message}</span>
        </div>
""")
        html_parts.append("    </div>")
    if validation_result.get("warnings"):
        html_parts.append("""
    <h3>Warnings</h3>
""")
        warnings_by_payer = defaultdict(list)
        for warning in validation_result["warnings"]:
            payer_info = warning.get("payer_info", {})
            payer_key = f"{payer_info.get('name', 'Unknown')} ({payer_info.get('state', 'N/A')})"
            warnings_by_payer[payer_key].append(warning)
        for payer_key in sorted(warnings_by_payer.keys()):
            payer_warnings = warnings_by_payer[payer_key]
            html_parts.append(f"""
        <div style="margin-bottom: 20px;">
            <h4>{html.escape(payer_key)} - {len(payer_warnings)} warning(s)</h4>
""")
            for _i, warning in enumerate(payer_warnings[:10], 1):
                location = warning.get("location") or ""
                claim_id = ""
                if location and "Claim ID:" in location:
                    claim_id = location.split("Claim ID:")[1].split("|")[0].strip()
                html_parts.append("""
            <div class="warning-box">
""")
                if claim_id:
                    html_parts.append(
                        f"                <strong style='color: #856404;'>Claim: {html.escape(claim_id)}</strong><br>"
                    )
                if warning.get("location"):
                    html_parts.append(f"                Location: {html.escape(warning['location'])}<br>")
                if warning.get("actual"):
                    html_parts.append(f"Details: {html.escape(str(warning['actual']))}<br>")
                html_parts.append("            </div>")
            if len(payer_warnings) > 10:
                html_parts.append(
                    f"            <p style='margin-left: 20px;'>... and {len(payer_warnings) - 10} more warnings for this payer</p>"
                )
            html_parts.append("        </div>")
        if len(validation_result["warnings"]) > 20:
            html_parts.append(f"        <p>... and {len(validation_result['warnings']) - 20} more warnings</p>")
    if validation_result.get("errors_by_type"):
        html_parts.append("""
    <h3>Errors by Type</h3>
""")
        for error_type, errors in validation_result["errors_by_type"].items():
            type_class = error_type.lower()
            html_parts.append(f"""
    <button class="collapsible">
        <span class="error-type {type_class}">{error_type}</span>
        {error_type} Errors ({len(errors)} found)
    </button>
    <div class="content">
""")
            for _i, error in enumerate(errors[:20], 1):
                html_parts.append("""
        <div class="error-box">
            <strong>
""")
                if error.get("location"):
                    html_parts.append(f"Location: {html.escape(error['location'])}<br>")
                if error.get("field"):
                    html_parts.append(f"Field: {html.escape(error['field'])}<br>")
                if error.get("expected") is not None and error.get("actual") is not None:
                    html_parts.append(f"Expected: <code>{html.escape(str(error['expected']))}</code>, ")
                    html_parts.append(f"Actual: <code>{html.escape(str(error['actual']))}</code><br>")
                html_parts.append("        </div>")
            if len(errors) > 20:
                html_parts.append(f"        <p>... and {len(errors) - 20} more {error_type} errors</p>")
            html_parts.append("    </div>")
    if validation_result.get("payers_missing_mileage_units"):
        html_parts.append("""
    <h3>Payers with Missing Mileage Unit Data</h3>
    <p style="background-color: #fff6d5; padding: 10px; border-left: 4px solid #ffc107;">
        The following payers sent mileage service lines (A0425/A0435/A0436) without ANY unit data
        in SVC05 or SVC07. This is a payer data quality issue.
    </p>
""")
        payer_data = validation_result.get("payers_missing_mileage_units", {})
        payer_list = sorted(payer_data.items(), key=lambda x: x[1], reverse=True)
        for payer, count in payer_list:
            payer_parts = payer.split("|")
            payer_name = payer_parts[0]
            payer_state = payer_parts[1] if len(payer_parts) > 1 else "N/A"
            html_parts.append(f"""
    <div class="warning-box">
        <strong>Payer:</strong> {html.escape(payer_name)}<br>
        <strong>State:</strong> {html.escape(payer_state)}<br>
        <strong>Missing Unit Count:</strong> {count} service line(s)
    </div>
""")
    # Priority RARC codes section - highlight payer-specific codes that need attention
    if validation_result.get("priority_rarc_codes"):
        html_parts.append("""
    <h3>Priority RARC Codes Detected</h3>
    <p style="background-color: #e7f3fe; padding: 10px; border-left: 4px solid #2196F3;">
        The following payer-specific RARC codes were detected. These codes may require special attention
        as they are priority codes for the identified payers.
    </p>
""")
        for payer_key, codes in validation_result["priority_rarc_codes"].items():
            if codes:  # Only show if there are priority codes
                html_parts.append(f"""
    <div style="background-color: #e7f3fe; border: 1px solid #2196F3; padding: 10px; margin: 10px 0; border-radius: 4px;">
        <strong>Payer:</strong> {html.escape(payer_key)}<br>
        <strong>Priority RARC Codes:</strong><br>
""")
                for code, count in sorted(codes.items()):
                    desc = dictionary.get_remark_code_description(code)
                    html_parts.append(
                        f"        • <code>{html.escape(code)}</code>: {html.escape(desc)} ({count} occurrence(s))<br>"
                    )
                html_parts.append("    </div>")
    if validation_result.get("missing_mappings"):
        html_parts.append("""
    <h3>Missing Dictionary Mappings</h3>
""")
        for payer_key, mappings in validation_result["missing_mappings"].items():
            payer_name, state = payer_key.split("|")
            html_parts.append(f"""
    <div class="warning-box">
        <strong>Payer:</strong> {html.escape(payer_name)}<br>
        <strong>State:</strong> {html.escape(state)}<br>
        <strong>Missing Codes:</strong><br>
""")
            by_field = defaultdict(set)
            for mapping in mappings:
                by_field[mapping["field"]].add(mapping["code"])
            for field, codes in by_field.items():
                unique_codes = sorted(codes)
                html_parts.append(f"        • {html.escape(field)}: ")
                html_parts.append(f"<code>{html.escape(', '.join(unique_codes[:10]))}</code>")
                if len(unique_codes) > 10:
                    html_parts.append(f" ... and {len(unique_codes) - 10} more")
                html_parts.append("<br>")
            html_parts.append("    </div>")
    if validation_result.get("sample_errors"):
        html_parts.append("""
    <h3>Sample Errors with Context</h3>
""")
        for i, sample in enumerate(validation_result["sample_errors"], 1):
            error = sample["error"]
            context = sample.get("context", {})
            html_parts.append(f"""
    <button class="collapsible">
        Example {i}: {error.get('type', 'UNKNOWN')} - {html.escape(error.get('message', 'Unknown error'))}
    </button>
    <div class="content">
        <div class="code-sample">
""")
            if error.get("location"):
                html_parts.append(f"Location: {html.escape(error['location'])}<br><br>")
            if error.get("expected") is not None and error.get("actual") is not None:
                html_parts.append(f"Expected: {html.escape(str(error['expected']))}<br>")
                html_parts.append(f"Actual: {html.escape(str(error['actual']))}<br><br>")
            # Show detailed EDI context if available
            if error.get("edi_context"):
                html_parts.append("<strong>Detailed Analysis:</strong><br>")
                html_parts.append(
                    '<pre style="background-color: #f8f9fa; padding: 10px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap;">'
                )
                for line in error["edi_context"]:
                    html_parts.append(html.escape(str(line)) + "\n")
                html_parts.append("</pre>")
            if context:
                html_parts.append("Context Data:<br>")
                for field, value in sorted(context.items()):
                    if value is not None and value != "":
                        if redact and any(s in field for s in ["Name", "SSN", "MemberID", "Address"]):
                            value = "[REDACTED]"
                        html_parts.append(f"{html.escape(field)}: {html.escape(str(value))}<br>")
            html_parts.append("""
        </div>
    </div>
""")
    # Payer Data Quality Issues section
    if validation_result.get("payer_data_quality_issues"):
        html_parts.append("""
    <h2>Payer Data Quality Issues</h2>
    <p>Issues detected that may require payer follow-up or dictionary updates:</p>
""")
        quality_issues = validation_result["payer_data_quality_issues"]
        # Missing CARC codes
        if quality_issues.get("missing_carc_codes"):
            html_parts.append("""
    <h3>Missing CARC Codes (Claim Adjustment Reason Codes)</h3>
    <table class="summary-table">
        <tr><th>Code</th><th>Payers Affected</th></tr>
""")
            carc_data = quality_issues["missing_carc_codes"]
            for code, payers in sorted(carc_data.items()):
                payer_list = list(payers.keys()) if isinstance(payers, dict) else list(payers)
                payer_display = ", ".join(payer_list[:5])
                if len(payer_list) > 5:
                    payer_display += f" ... and {len(payer_list) - 5} more"
                html_parts.append(
                    f"        <tr><td><code>{html.escape(str(code))}</code></td><td>{html.escape(payer_display)}</td></tr>\n"
                )
            html_parts.append("    </table>\n")
        # Missing RARC codes
        if quality_issues.get("missing_rarc_codes"):
            html_parts.append("""
    <h3>Missing RARC Codes (Remittance Advice Remark Codes)</h3>
    <table class="summary-table">
        <tr><th>Code</th><th>Payers Affected</th></tr>
""")
            rarc_data = quality_issues["missing_rarc_codes"]
            for code, payers in sorted(rarc_data.items()):
                payer_list = list(payers.keys()) if isinstance(payers, dict) else list(payers)
                payer_display = ", ".join(payer_list[:5])
                if len(payer_list) > 5:
                    payer_display += f" ... and {len(payer_list) - 5} more"
                html_parts.append(
                    f"        <tr><td><code>{html.escape(str(code))}</code></td><td>{html.escape(payer_display)}</td></tr>\n"
                )
            html_parts.append("    </table>\n")
        # Missing dictionary entries
        if quality_issues.get("missing_dictionary_entries"):
            html_parts.append("""
    <h3>Missing Dictionary Entries</h3>
    <table class="summary-table">
        <tr><th>Entry Type</th><th>Missing Values</th></tr>
""")
            dict_data = quality_issues["missing_dictionary_entries"]
            for entry_type, entries in sorted(dict_data.items()):
                entry_list = list(entries.keys()) if isinstance(entries, dict) else list(entries)
                entry_display = ", ".join(str(e) for e in entry_list[:10])
                if len(entry_list) > 10:
                    entry_display += f" ... and {len(entry_list) - 10} more"
                html_parts.append(
                    f"        <tr><td>{html.escape(str(entry_type))}</td><td><code>{html.escape(entry_display)}</code></td></tr>\n"
                )
            html_parts.append("    </table>\n")
        # Unrecognized date formats
        if quality_issues.get("unrecognized_date_formats"):
            html_parts.append("""
    <h3>Unrecognized Date Formats</h3>
    <table class="summary-table">
        <tr><th>Date Value</th><th>Occurrences</th></tr>
""")
            date_data = quality_issues["unrecognized_date_formats"]
            for date_val, count in sorted(date_data.items(), key=lambda x: x[1], reverse=True)[:10]:
                html_parts.append(
                    f"        <tr><td><code>{html.escape(str(date_val))}</code></td><td>{count}</td></tr>\n"
                )
            if len(date_data) > 10:
                html_parts.append(
                    f"        <tr><td colspan='2'><em>... and {len(date_data) - 10} more formats</em></td></tr>\n"
                )
            html_parts.append("    </table>\n")
        # Transaction balance skips
        if quality_issues.get("transaction_balance_skips"):
            html_parts.append("""
    <h3>Transaction Balance Skips</h3>
    <div class="warning-box">
        <strong>Transactions skipped during balance validation:</strong><br>
""")
            skip_data = quality_issues["transaction_balance_skips"]
            for reason, count in sorted(skip_data.items(), key=lambda x: x[1], reverse=True):
                html_parts.append(f"        • {html.escape(str(reason))}: {count} transaction(s)<br>\n")
            html_parts.append("    </div>\n")
        # Empty claims
        if quality_issues.get("empty_claims_count"):
            html_parts.append(f"""
    <div class="warning-box">
        <strong>Empty Claims:</strong> {quality_issues['empty_claims_count']} claim(s) found with no service lines
    </div>
""")
        # Duplicate claims
        if quality_issues.get("duplicate_claims"):
            html_parts.append("""
    <h3>Duplicate Claims Detected</h3>
    <table class="summary-table">
        <tr><th>Claim ID</th><th>Occurrences</th></tr>
""")
            dup_data = quality_issues["duplicate_claims"]
            for claim_id, count in sorted(dup_data.items(), key=lambda x: x[1], reverse=True)[:10]:
                html_parts.append(
                    f"        <tr><td><code>{html.escape(str(claim_id))}</code></td><td>{count}</td></tr>\n"
                )
            if len(dup_data) > 10:
                html_parts.append(
                    f"        <tr><td colspan='2'><em>... and {len(dup_data) - 10} more duplicates</em></td></tr>\n"
                )
            html_parts.append("    </table>\n")
        # Other issues not categorized above
        if quality_issues.get("other_issues"):
            html_parts.append("""
    <h3>Other Data Quality Issues</h3>
    <table class="summary-table">
        <tr><th>Issue</th><th>Occurrences</th></tr>
""")
            other_data = quality_issues["other_issues"]
            for issue_key, payers in sorted(other_data.items()):
                total = sum(payers.values()) if isinstance(payers, dict) else payers
                html_parts.append(f"        <tr><td>{html.escape(str(issue_key))}</td><td>{total}</td></tr>\n")
            html_parts.append("    </table>\n")
    html_parts.append("""
<script>
var coll = document.getElementsByClassName("collapsible");
var i;

for (i = 0; i < coll.length; i++) {
    coll[i].addEventListener("click", function() {
        this.classList.toggle("active");
        var content = this.nextElementSibling;
        if (content.style.maxHeight){
            content.style.maxHeight = null;
        } else {
            content.style.maxHeight = content.scrollHeight + "px";
        }
    });
}
</script>
</div>
</body>
</html>
""")
    return "".join(html_parts)


def validate_835_output(
    edi_segments,
    csv_rows: List[dict],
    element_delimiter: str = "*",
    output_file: str = None,
    output_format: str = "text",
    verbose: bool = False,
    debug: bool = False,
    status_callback=None,
    payer_keys: Dict = None,
) -> Dict:
    """Main validation entry point
    Args:
        edi_segments: Either List[str] (flat segments) or List[dict] with file context
                      Each dict has: {'file': filename, 'segments': [...], 'delimiter': '*'}
        csv_rows: List of CSV row dictionaries
        element_delimiter: EDI element delimiter (default '*')
        output_file: Path to save validation report
        output_format: Report format ('text' or 'html')
        verbose: Enable verbose output
        debug: Enable detailed debugging with X12 spec references
        status_callback: Optional callback for GUI status updates
        payer_keys: Optional dict mapping file index to payer key for payer-specific overrides
    """
    validator = ZeroFailValidator(debug=debug, payer_keys=payer_keys)

    # Handle file-aware segment data (new format) or flat list (legacy)
    if edi_segments and isinstance(edi_segments[0], dict) and "segments" in edi_segments[0]:
        # New format: list of {file, segments, delimiter} dicts
        all_segments = []
        for file_data in edi_segments:
            all_segments.extend(file_data["segments"])
        if debug:
            logger.debug(
                "Starting validation with %d files, %d total segments and %d CSV rows",
                len(edi_segments),
                len(all_segments),
                len(csv_rows),
            )
        validation_result = validator.validate_all_by_file(
            edi_segments, csv_rows, verbose=verbose, status_callback=status_callback
        )
    else:
        # Legacy format: flat list of segments
        if debug:
            logger.debug("Starting validation with %d EDI segments and %d CSV rows", len(edi_segments), len(csv_rows))
        validation_result = validator.validate_all(
            edi_segments, csv_rows, element_delimiter, verbose=verbose, status_callback=status_callback
        )
    if status_callback:
        status_callback("Generating validation reports...")
    if output_file or output_format:
        generate_validation_report(validation_result, output_format, output_file)
    return validation_result
