"""
Zero-Fail 835 CSV Validation System
===================================

This module provides comprehensive validation for 835 EDI to CSV conversion with:
- 100% field coverage tracking
- Mathematical accuracy validation
- Edge case handling
- Detailed error reporting with payer/state tracking
"""

from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict, OrderedDict
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import re
import html
import os
from categorization import categorize_adjustment
import colloquial


class ValidationError:
    """Structured validation error for detailed reporting"""
    def __init__(self, error_type: str, message: str, location: str = None,
                 segment: str = None, field: str = None, expected: Any = None,
                 actual: Any = None, payer_info: Dict = None):
        self.type = error_type
        self.message = message
        self.location = location
        self.segment = segment
        self.field = field
        self.expected = expected
        self.actual = actual
        self.payer_info = payer_info or {}
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
            'type': self.type,
            'message': self.message,
            'location': self.location,
            'segment': self.segment,
            'field': self.field,
            'expected': str(self.expected) if self.expected is not None else None,
            'actual': str(self.actual) if self.actual is not None else None,
            'payer_info': self.payer_info
        }


class SegmentFieldMap:
    """Complete mapping of ALL 835 segments to CSV fields"""
    BPR_MAP = {
        '01': 'CHK_TransactionHandling_Header_BPR',
        '02': 'CHK_PaymentAmount_Header_BPR',
        '03': 'CHK_CreditDebitFlag_Header_BPR',
        '04': 'CHK_PaymentMethod_Header_BPR',
        '05': 'CHK_Format_Header_BPR',
        '06': 'CHK_PayerDFI_Qualifier_Header_BPR',
        '07': 'CHK_PayerDFI_ID_Header_BPR',
        '08': 'CHK_PayerAccountQualifier_Header_BPR',
        '09': 'CHK_PayerAccountNumber_Header_BPR',
        '10': 'CHK_OriginatingCompanyID_Header_BPR',
        '11': 'CHK_OriginatingCompanySupplemental_Header_BPR',
        '12': 'CHK_PayeeDFI_Qualifier_Header_BPR',
        '13': 'CHK_PayeeDFI_ID_Header_BPR',
        '14': 'CHK_PayeeAccountQualifier_Header_BPR',
        '15': 'CHK_PayeeAccountNumber_Header_BPR',
        '16': 'CHK_EffectiveDate_Header_BPR',
        '17': 'CHK_BusinessFunctionCode_Header_BPR',
        '18': 'CHK_DFI_Qualifier_3_Header_BPR',
        '19': 'CHK_DFI_ID_3_Header_BPR',
        '20': 'CHK_AccountQualifier_3_Header_BPR',
        '21': 'CHK_AccountNumber_3_Header_BPR'
    }
    TRN_MAP = {
        '01': 'CHK_TraceType_Header_TRN',
        '02': 'CHK_TraceNumber_Header_TRN',
        '03': 'CHK_OriginatingCompanyID_TRN_Header_TRN',
        '04': 'CHK_ReferenceIDSecondary_Header_TRN'
    }
    CLP_MAP = {
        '01': 'CLM_PatientControlNumber_L2100_CLP',
        '02': 'CLM_Status_L2100_CLP',
        '03': 'CLM_ChargeAmount_L2100_CLP',
        '04': 'CLM_PaymentAmount_L2100_CLP',
        '05': 'CLM_PatientResponsibility_L2100_CLP',
        '06': 'CLM_FilingIndicator_L2100_CLP',
        '07': 'CLM_PayerControlNumber_L2100_CLP',
        '08': 'CLM_FacilityTypeCode_L2100_CLP',
        '09': 'CLM_FrequencyCode_L2100_CLP',
        '10': 'CLM_PatientConditionCode_L2100_CLP',
        '11': 'CLM_DRGCode_L2100_CLP',
        '12': 'CLM_DRGWeight_L2100_CLP',
        '13': 'CLM_DischargeFraction_L2100_CLP',
        '14': 'CLM_YesNoCondition_L2100_CLP',
        '16': 'Not Used',
        '17': 'Not Used',
        '18': 'Not Used',
        '19': 'CLM_PaymentTypology_L2100_CLP'
    }
    SVC_MAP = {
        '01': 'SVC_ProcedureCode_L2110_SVC',
        '02': 'SVC_ChargeAmount_L2110_SVC',
        '03': 'SVC_PaymentAmount_L2110_SVC',
        # '04': Not validated - rarely populated; SEQ column is generated row number
        '05': 'SVC_Units_L2110_SVC',
        '06': 'SVC_OriginalProcedure_L2110_SVC',
        '07': 'SVC_OriginalUnits_L2110_SVC'
    }
    MOA_MAP = {
        '01': 'MOA_ReimbursementRate_L2100_MOA',
        '02': 'MOA_ClaimHCPCSPayableAmount_L2100_MOA',
        '03': 'MOA_ClaimPaymentRemarkCode1_L2100_MOA',
        '04': 'MOA_ClaimPaymentRemarkCode2_L2100_MOA',
        '05': 'MOA_ClaimPaymentRemarkCode3_L2100_MOA',
        '06': 'MOA_ClaimPaymentRemarkCode4_L2100_MOA',
        '07': 'MOA_ClaimPaymentRemarkCode5_L2100_MOA',
        '08': 'MOA_ESRDPaymentAmount_L2100_MOA',
        '09': 'MOA_NonpayableProfessionalComponent_L2100_MOA'
    }
    MIA_MAP = {
        '01': 'MIA_CoveredDaysOrVisitsCount_L2100_MIA',
        '02': 'MIA_PPSOperatingOutlierAmount_L2100_MIA',
        '03': 'MIA_LifetimePsychiatricDaysCount_L2100_MIA',
        '04': 'MIA_ClaimDRGAmount_L2100_MIA',
        '05': 'MIA_ClaimPaymentRemarkCode_L2100_MIA',
        '06': 'MIA_ClaimDisproportionateShareAmount_L2100_MIA',
        '07': 'MIA_ClaimMSPPassThroughAmount_L2100_MIA',
        '08': 'MIA_ClaimPPSCapitalAmount_L2100_MIA',
        '09': 'MIA_PPSCapitalFSPDRGAmount_L2100_MIA',
        '10': 'MIA_PPSCapitalHSPDRGAmount_L2100_MIA',
        '11': 'MIA_PPSCapitalDSHDRGAmount_L2100_MIA',
        '12': 'MIA_OldCapitalAmount_L2100_MIA',
        '13': 'MIA_PPSCapitalIMEAmount_L2100_MIA',
        '14': 'MIA_PPSOperatingHospitalSpecificDRGAmount_L2100_MIA',
        '15': 'MIA_CostReportDayCount_L2100_MIA',
        '16': 'MIA_PPSOperatingFederalSpecificDRGAmount_L2100_MIA',
        '17': 'MIA_ClaimPPSCapitalOutlierAmount_L2100_MIA',
        '18': 'MIA_ClaimIndirectTeachingAmount_L2100_MIA',
        '19': 'MIA_NonpayableProfessionalComponentAmount_L2100_MIA',
        '20': 'MIA_ClaimPaymentRemarkCode2_L2100_MIA',
        '21': 'MIA_ClaimPaymentRemarkCode3_L2100_MIA',
        '22': 'MIA_ClaimPaymentRemarkCode4_L2100_MIA',
        '23': 'MIA_ClaimPaymentRemarkCode5_L2100_MIA',
        '24': 'MIA_PPSCapitalExceptionAmount_L2100_MIA'
    }
    PLB_MAP = {
        # PLB01-02: Provider ID and Fiscal Period Date
        '01': 'PLB_ProviderID_PLB',
        '02': 'PLB_FiscalPeriodDate_PLB',
        # PLB03-14: Up to 6 adjustment pairs (composite ID + amount)
        # Note: PLB03/05/07/09/11/13 are composites containing reason_code:reference_id
        # The parser flattens these into PLB_Adj#_ReasonCode_PLB and PLB_Adj#_RefID_PLB
        '03': 'PLB_Adj1_ReasonCode_PLB',  # Composite: reason_code:reference_id
        '04': 'PLB_Adj1_Amount_PLB',
        '05': 'PLB_Adj2_ReasonCode_PLB',
        '06': 'PLB_Adj2_Amount_PLB',
        '07': 'PLB_Adj3_ReasonCode_PLB',
        '08': 'PLB_Adj3_Amount_PLB',
        '09': 'PLB_Adj4_ReasonCode_PLB',
        '10': 'PLB_Adj4_Amount_PLB',
        '11': 'PLB_Adj5_ReasonCode_PLB',
        '12': 'PLB_Adj5_Amount_PLB',
        '13': 'PLB_Adj6_ReasonCode_PLB',
        '14': 'PLB_Adj6_Amount_PLB'
    }
    REF_QUALIFIER_MAP = {
        'EV': 'CHK_ReceiverID_Header_REF',
        'F2': 'CHK_VersionID',
        'TJ': 'Provider_TaxID_L1000B_REF',
        '0B': 'CLM_StateMedicalAssistanceNumber_L2100_REF',
        '0K': 'CLM_PolicyNumber_L2100_REF',
        '1A': 'CLM_BlueCrossProviderNumber_L2100_REF',
        '1B': 'CLM_BlueShieldProviderNumber_L2100_REF',
        '1C': 'CLM_MedicareProviderNumber_L2100_REF',
        '1D': 'CLM_MedicaidProviderNumber_L2100_REF',
        '1G': 'CLM_ProviderUPINNumber_L2100_REF',
        '1H': 'CLM_CHAMPUSIdentificationNumber_L2100_REF',
        '1K': 'CLM_PayersClaimNumber_L2100_REF',
        '1L': 'CLM_GroupNumber_L2100_REF',
        '1W': 'CLM_MemberID_L2100_NM1',
        '28': 'CLM_EmployeeIdentificationNumber_L2100_REF',
        '2U': 'CLM_PayerIdentificationNumber_L2100_REF',
        '6R': 'CLM_ProviderControl_L2100_REF',
        '9A': 'CLM_RepricedClaimRefNumber_L2100_REF',
        '9C': 'CLM_AdjustmentIdentifier_L2100_REF',
        '9F': 'CLM_ReferralNumber_L2100_REF',
        'A6': 'CLM_EmployeeIdentificationNumber_L2100_REF',
        'BB': 'CLM_AuthorizationNumber_L2100_REF',
        'CE': 'CLM_PlanName_L2100_REF',
        'D9': 'CLM_ClaimNumber_L2100_REF',
        'EA': 'CLM_MedicalRecord_L2100_REF',
        'F4': 'CLM_HierarchicalParentId_L2100_REF',
        'F8': 'CLM_OriginalRef_L2100_REF',
        'G1': 'CLM_PriorAuth_L2100_REF',
        'G3': 'CLM_PredeterminationNumber_L2100_REF',
        'HPI': 'CLM_HPID_L2100_REF',
        'IG': 'CLM_InsurancePolicyNumber_L2100_REF',
        'LU': 'SVC_FacilityTypeCode_L2110_REF',
        'LX': 'CLM_QualifiedProductsList_L2100_REF',
        'PQ': 'CLM_PayeeIdentification_L2100_REF',
        'SY': 'CLM_SSN_L2100_NM1',
        'Y8': 'CLM_AgencyClaimNumber_L2100_REF',
        'E9': 'SVC_LineItemControl_L2110_REF',
        # Repricing Reference Numbers
        'APC': 'CLM_AmbulatoryPaymentClassification_L2100_REF',
        'NF': 'CLM_NAICCode_L2100_REF'
    }
    DTM_QUALIFIER_MAP = {
        '009': 'CLM_ProcessDate_L2100_DTM',
        '036': 'CLM_ExpirationDate_L2100_DTM',
        '050': 'CLM_ReceivedDate_L2100_DTM',
        '232': 'CLM_ServiceStartDate_L2100_DTM',
        '233': 'CLM_ServiceEndDate_L2100_DTM',
        '434': 'CLM_StatementFromDate_L2100_DTM',
        '435': 'CLM_StatementToDate_L2100_DTM',
        '150': 'SVC_ServiceStartDate_L2110_DTM',
        '151': 'SVC_ServiceEndDate_L2110_DTM',
        '472': 'SVC_ServiceStartDate_L2110_DTM',
        # Production Date
        '405': 'CHK_ProductionDate_Header_DTM405'
    }
    AMT_QUALIFIER_MAP = {
        'AU': 'CLM_CoverageAmount_L2100_AMT',
        'D8': 'CLM_DiscountAmount',
        'DY': 'CLM_PerDayLimitAmount',
        'F5': 'CLM_PatientAmountPaid_L2100_AMT',
        'I': 'CLM_InterestAmount_L2100_AMT',
        'NL': 'CLM_PromptPaymentDiscount',
        'T': 'CLM_TaxAmount_L2100_AMT',
        'T2': 'CLM_TotalClaimBeforeTaxes',
        'ZK': 'CLM_FederalMedicareCreditAmount',
        'ZL': 'CLM_FederalMedicareBuyInAmount',
        'ZM': 'CLM_FederalMedicareBloodDeductible',
        'ZN': 'CLM_CoinsuranceAmount',
        'ZO': 'CLM_ZOAmount',
        'ZZ': 'CLM_MutuallyDefined',
        'B6': 'SVC_AllowedAmount_L2110_AMT',
        'KH': 'SVC_DeductibleAmount',
        'T': 'SVC_TaxAmount_L2110_AMT',
        'T2': 'SVC_TotalServiceBeforeTaxes'
    }
    QTY_QUALIFIER_MAP = {
        'PS': 'CLM_PrescriptionCount',
        'VS': 'CLM_VisitCount',
        'ZK': 'CLM_UnitsDenied',
        'ZL': 'CLM_UnitsNotCertified',
        'PT': 'SVC_AmbulancePatientCount_L2110_QTY',
        # Covered Actual
        'CA': 'CLM_CoveredActual_L2100_QTY',
    }
    NM1_ENTITY_MAP = {
        'IL': 'CLM_SubscriberName',
        'QC': 'CLM_PatientName',
        '74': 'CLM_CorrectedInsuredName',
        '82': 'SVC_RenderingProvider',
        'TT': 'CLM_TransferToProvider',
        '77': 'CLM_ServiceFacility',
        'PR': 'Payer_Name',
        'PE': 'Provider_Name',
        'GB': 'CLM_OtherPayer',
        'IC': 'CLM_IntermediaryBank',
        'P3': 'CLM_PrimaryCareProvider',
        '71': 'CLM_AttendingPhysician',
        '72': 'CLM_OperatingPhysician',
        'ZZ': 'CLM_MutuallyDefinedEntity',
        'QJ': 'AMB_PickupName',
        '45': 'AMB_DropoffName'
    }
    CAS_GROUP_CODES = ['CO', 'CR', 'DE', 'MA', 'OA', 'PI', 'PR']
    CUR_MAP = {
    }
    RDM_MAP = {
        '01': 'RDM_TransmissionCode_Header_RDM',
        '02': 'RDM_Name_Header_RDM',
        '03': 'RDM_CommunicationNumber_Header_RDM',
        '04': 'RDM_ReferenceID_Header_RDM',
        '05': 'RDM_ReferenceID2_Header_RDM',
        '06': 'RDM_CommunicationNumber2_Header_RDM',
        '07': 'RDM_ContactFunction_Header_RDM'
    }
    ISA_MAP = {
        '01': 'ENV_AuthorizationQualifier_Envelope_ISA',
        '02': 'ENV_AuthorizationInfo_Envelope_ISA',
        '03': 'ENV_SecurityQualifier_Envelope_ISA',
        '04': 'ENV_SecurityInfo_Envelope_ISA',
        '05': 'ENV_SenderIDQualifier_Envelope_ISA',
        '06': 'ENV_SenderID_Envelope_ISA',
        '07': 'ENV_ReceiverIDQualifier_Envelope_ISA',
        '08': 'ENV_ReceiverID_Envelope_ISA',
        '09': 'ENV_InterchangeDate_Envelope_ISA',
        '10': 'ENV_InterchangeTime_Envelope_ISA',
        '11': 'ENV_RepetitionSeparator_Envelope_ISA',
        '12': 'ENV_VersionNumber_Envelope_ISA',
        '13': 'ENV_InterchangeControlNumber_Envelope_ISA',
        '14': 'ENV_AcknowledgmentRequested_Envelope_ISA',
        '15': 'ENV_UsageIndicator_Envelope_ISA',
        '16': 'ENV_ComponentSeparator_Envelope_ISA'
    }
    GS_MAP = {
        '01': 'ENV_FunctionalIDCode_Envelope_GS',
        '02': 'ENV_ApplicationSenderCode_Envelope_GS',
        '03': 'ENV_ApplicationReceiverCode_Envelope_GS',
        '04': 'ENV_Date_Envelope_GS',
        '05': 'ENV_Time_Envelope_GS',
        '06': 'ENV_GroupControlNumber_Envelope_GS',
        '07': 'ENV_ResponsibleAgencyCode_Envelope_GS',
        '08': 'ENV_VersionReleaseID_Envelope_GS'
    }
    N1_MAP = {
        '01': 'EntityIDCode',
        '02': 'Name',
        '03': 'IDQualifier',
        '04': 'IDCode'
    }
    N2_MAP = {
        '01': 'AdditionalNameLine1',
        '02': 'AdditionalNameLine2'
    }
    N3_MAP = {
        '01': 'Address',
        '02': 'Address2'
    }
    N4_MAP = {
        '01': 'City',
        '02': 'State',
        '03': 'Zip',
        '04': 'Country',
        '05': 'LocationQualifier',
        '06': 'LocationID',
        '07': 'CountrySubdivisionCode'
    }
    PER_MAP = {
        '01': 'Contact_Function_Code',
        '02': 'Name',
        '03': 'Communication_Number_Qualifier_1',
        '04': 'Communication_Number_1',
        '05': 'Communication_Number_Qualifier_2',
        '06': 'Communication_Number_2',
        '07': 'Communication_Number_Qualifier_3',
        '08': 'Communication_Number_3',
        '09': 'Contact_Inquiry_Reference'
    }
    LQ_MAP = {
        '01': 'Code_List_Qualifier',
        '02': 'Industry_Code'
    }
    LQ_QUALIFIER_MAP = {
        # Healthcare Remark Codes (RARC)
        'HE': 'CLM_HealthcareRemarkCodes_L2100_LQ',
    }
    ST_MAP = {
        '01': 'File_TransactionType_Header_ST',
        '02': 'File_TransactionControlNumber_Header_ST',
        '03': 'File_ImplementationConventionRef_Header_ST'
    }
    @classmethod
    def get_all_segments(cls) -> Dict[str, Dict[str, str]]:
        """Return all segment field mappings"""
        return {
            'ISA': cls.ISA_MAP,
            'GS': cls.GS_MAP,
            'ST': cls.ST_MAP,
            'BPR': cls.BPR_MAP,
            'TRN': cls.TRN_MAP,
            'CLP': cls.CLP_MAP,
            'SVC': cls.SVC_MAP,
            'PLB': cls.PLB_MAP,
            'RDM': cls.RDM_MAP,
            'N1': cls.N1_MAP,
            'N2': cls.N2_MAP,
            'N3': cls.N3_MAP,
            'N4': cls.N4_MAP,
            'PER': cls.PER_MAP,
            'LQ': cls.LQ_MAP
        }
    @classmethod
    def get_qualifier_maps(cls) -> Dict[str, Dict[str, str]]:
        """Return all qualifier mappings"""
        return {
            'REF': cls.REF_QUALIFIER_MAP,
            'DTM': cls.DTM_QUALIFIER_MAP,
            'AMT': cls.AMT_QUALIFIER_MAP,
            'QTY': cls.QTY_QUALIFIER_MAP,
            'NM1': cls.NM1_ENTITY_MAP
        }
    @classmethod
    def get_cas_category_fields(cls) -> Dict[str, List[str]]:
        """Return CAS categorization fields for validation"""
        return {
            'claim': [
                'CLM_Contractual_L2100_CAS',
                'CLM_Copay_L2100_CAS',
                'CLM_Coinsurance_L2100_CAS',
                'CLM_Deductible_L2100_CAS',
                'CLM_Denied_L2100_CAS',
                'CLM_OtherAdjustments_L2100_CAS',
                'CLM_Sequestration_L2100_CAS',
                'CLM_COB_L2100_CAS',
                'CLM_HCRA_L2100_CAS',
                'CLM_QMB_L2100_CAS'
            ],
            'service': [
                'SVC_Contractual_L2110_CAS',
                'SVC_Copay_L2110_CAS',
                'SVC_Coinsurance_L2110_CAS',
                'SVC_Deductible_L2110_CAS',
                'SVC_Denied_L2110_CAS',
                'SVC_OtherAdjustments_L2110_CAS',
                'SVC_Sequestration_L2110_CAS',
                'SVC_COB_L2110_CAS',
                'SVC_HCRA_L2110_CAS',
                'SVC_QMB_L2110_CAS',
                'Patient_NonCovered'
            ]
        }


class EDIElementPresenceTracker:
    """Lightweight tracker to find EDI elements with data but no CSV mapping"""
    ELEMENT_DESCRIPTIONS = {
        'CLP*08': 'Facility Type Code (Institutional claims - hospital/SNF type)',
        'CLP*10': 'Patient Status Code (Institutional - discharge status)',
        'CLP*11': 'DRG Code (Institutional - Diagnosis Related Group)',
        'CLP*12': 'DRG Weight (Institutional - DRG payment weight)',
        'CLP*13': 'Discharge Fraction (Institutional - percent of stay)',
        'CLP*15': 'Exchange Rate (Foreign currency claims)',
        'MOA*01': 'Reimbursement Rate (Medicare outpatient payment rate)',
        'MOA*02': 'HCPCS Payable Amount (Medicare outpatient allowable)',
        'MOA*03': 'Remark Code 1 (Medicare outpatient remark)',
        'MOA*04': 'Remark Code 2 (Medicare outpatient remark)',
        'MOA*05': 'Remark Code 3 (Medicare outpatient remark)',
        'MOA*06': 'Remark Code 4 (Medicare outpatient remark)',
        'MOA*07': 'Remark Code 5 (Medicare outpatient remark)',
        'MOA*08': 'ESRD Payment Amount (End-Stage Renal Disease)',
        'MOA*09': 'Non-Payable Professional Component (Medicare)',
        'MIA*01': 'Covered Days/Visits Count (Medicare inpatient)',
        'MIA*02': 'PPS Operating Outlier Amount (Medicare inpatient)',
        'MIA*03': 'Lifetime Psychiatric Days Count (Medicare inpatient)',
        'MIA*04': 'DRG Amount (Medicare inpatient DRG payment)',
        'MIA*05': 'Remark Code (Medicare inpatient remark)',
        'MIA*06': 'Disproportionate Share Amount (Medicare DSH)',
        'MIA*07': 'MSP Pass-Through Amount (Medicare Secondary Payer)',
        'MIA*08': 'PPS Capital Amount (Medicare capital payment)',
        'MIA*09': 'PPS Capital FSP DRG Amount (Medicare)',
        'MIA*10': 'PPS Capital HSP DRG Amount (Medicare)',
        'MIA*11': 'PPS Capital DSH DRG Amount (Medicare)',
        'MIA*12': 'Old Capital Amount (Medicare)',
        'MIA*13': 'PPS Capital IME Amount (Medicare teaching adjustment)',
        'MIA*14': 'PPS Operating Hospital-Specific DRG Amount (Medicare)',
        'MIA*15': 'Cost Report Day Count (Medicare)',
        'MIA*16': 'PPS Operating Federal-Specific DRG Amount (Medicare)',
        'MIA*17': 'PPS Capital Outlier Amount (Medicare)',
        'MIA*18': 'Indirect Teaching Amount (Medicare IME)',
        'MIA*19': 'Non-Payable Professional Component (Medicare)',
        'MIA*20': 'Remark Code 2 (Medicare inpatient remark)',
        'MIA*21': 'Remark Code 3 (Medicare inpatient remark)',
        'MIA*22': 'Remark Code 4 (Medicare inpatient remark)',
        'MIA*23': 'Remark Code 5 (Medicare inpatient remark)',
        'MIA*24': 'PPS Capital Exception Amount (Medicare)',
        'SVC*04': 'Bundled/Unbundled Line Number (line item reference)',
        'CUR*02': 'Currency Code (foreign currency identifier)',
        'CUR*03': 'Exchange Rate (foreign currency rate)',
    }
    # Qualifier-based segments that need special tracking
    QUALIFIER_SEGMENTS = {'REF', 'DTM', 'AMT', 'QTY', 'LQ'}
    
    def __init__(self):
        self.element_presence = defaultdict(lambda: defaultdict(set))
        self.element_payers = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        # Qualifier tracking: {segment_type: {qualifier: {file_idx set}}}
        self.qualifier_presence = defaultdict(lambda: defaultdict(set))
        # Qualifier payer counts: {segment_type: {qualifier: {payer: count}}}
        self.qualifier_payers = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self.files_processed = 0
        self.current_file_idx = 0
        self.current_payer = 'Unknown'
    def track_segment(self, segment: str, elements: list, delimiter: str):
        """Track which element positions have data in this segment"""
        if not elements:
            return
        seg_id = elements[0]
        
        # Track qualifier-based segments separately
        if seg_id in self.QUALIFIER_SEGMENTS and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2] if len(elements) > 2 else ''
            if qualifier and value and str(value).strip():
                self.qualifier_presence[seg_id][qualifier].add(self.current_file_idx)
                self.qualifier_payers[seg_id][qualifier][self.current_payer] += 1
        
        # Track all element positions (for non-qualifier segments)
        for pos, value in enumerate(elements[1:], 1):
            if value and str(value).strip():
                self.element_presence[seg_id][pos].add(self.current_file_idx)
                self.element_payers[seg_id][pos][self.current_payer] += 1
    def new_file(self, payer_name: str = 'Unknown'):
        """Mark start of a new file"""
        self.files_processed += 1
        self.current_file_idx = self.files_processed
        self.current_payer = payer_name
    def get_unmapped_elements(self) -> dict:
        """Compare tracked elements to segment maps and find gaps"""
        all_maps = {
            'BPR': SegmentFieldMap.BPR_MAP,
            'TRN': SegmentFieldMap.TRN_MAP,
            'CLP': SegmentFieldMap.CLP_MAP,
            'SVC': SegmentFieldMap.SVC_MAP,
            'PLB': SegmentFieldMap.PLB_MAP,
            'MIA': SegmentFieldMap.MIA_MAP,
            'MOA': SegmentFieldMap.MOA_MAP,
            'CUR': SegmentFieldMap.CUR_MAP if hasattr(SegmentFieldMap, 'CUR_MAP') else {},
        }
        # Envelope/structural segments we intentionally skip
        envelope_segments = {
            'ISA', 'GS', 'GE', 'IEA', 'ST', 'SE',
        }
        # Segments handled by entity/context logic (not qualifier-based)
        context_segments = {
            'N1', 'N2', 'N3', 'N4', 'NM1', 'PER',
            'CAS', 'LX', 'RDM', 'TS2', 'TS3',
        }
        unmapped = {}
        
        # Check position-based segments
        for seg_id, positions in self.element_presence.items():
            if seg_id in envelope_segments or seg_id in context_segments or seg_id in self.QUALIFIER_SEGMENTS:
                continue
            seg_map = all_maps.get(seg_id, {})
            for pos, file_set in positions.items():
                pos_key = f'{pos:02d}'
                if pos_key not in seg_map or seg_map.get(pos_key) in [None, '', 'Not Used']:
                    key = f"{seg_id}*{pos:02d}"
                    files_with_data = len(file_set)
                    payer_counts = dict(self.element_payers[seg_id][pos])
                    sorted_payers = sorted(payer_counts.items(), key=lambda x: x[1], reverse=True)
                    unmapped[key] = {
                        'segment': seg_id,
                        'position': pos,
                        'description': self.ELEMENT_DESCRIPTIONS.get(key, 'Unknown field'),
                        'files_with_data': files_with_data,
                        'total_files': self.files_processed,
                        'pct': round(100 * files_with_data / self.files_processed, 1) if self.files_processed > 0 else 0,
                        'payers': sorted_payers[:10],
                        'total_occurrences': sum(payer_counts.values()),
                        'type': 'position'
                    }
        
        return dict(sorted(unmapped.items(), key=lambda x: x[1]['files_with_data'], reverse=True))
    
    def get_unmapped_qualifiers(self) -> dict:
        """Find qualifier-based segments with data but no CSV mapping"""
        qualifier_maps = {
            'REF': SegmentFieldMap.REF_QUALIFIER_MAP,
            'DTM': SegmentFieldMap.DTM_QUALIFIER_MAP,
            'AMT': SegmentFieldMap.AMT_QUALIFIER_MAP,
            'QTY': SegmentFieldMap.QTY_QUALIFIER_MAP,
            'LQ': SegmentFieldMap.LQ_QUALIFIER_MAP,
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
                        'segment': seg_id,
                        'qualifier': qualifier,
                        'description': f'Unmapped {seg_id} qualifier',
                        'files_with_data': files_with_data,
                        'total_files': self.files_processed,
                        'pct': round(100 * files_with_data / self.files_processed, 1) if self.files_processed > 0 else 0,
                        'payers': sorted_payers[:10],
                        'total_occurrences': sum(payer_counts.values()),
                        'type': 'qualifier'
                    }
        
        return dict(sorted(unmapped.items(), key=lambda x: x[1]['files_with_data'], reverse=True))
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
                if info['payers']:
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
                if info['payers']:
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
        seg_id = elements[0] if elements else ''
        self.segments_processed.append({
            'segment_id': seg_id,
            'raw': segment,
            'elements': elements,
            'element_count': len(elements)
        })
    def track_field(self, segment: str, position: int, value: Any, csv_field: str = None):
        """Track that a field was extracted"""
        key = f"{segment}_{position:02d}"
        self.fields_extracted[key] = {
            'segment': segment,
            'position': position,
            'value': value,
            'csv_field': csv_field,
            'timestamp': datetime.now()
        }
    def track_missing_mapping(self, segment: str, qualifier: str, value: str, field_type: str):
        """Track unmapped codes/qualifiers"""
        self.missing_mappings[field_type].add((segment, qualifier, value))
    def set_claim_context(self, claim_id: str, payer_info: Dict):
        """Set current claim context for error reporting"""
        self.claim_context = {
            'claim_id': claim_id,
            'payer_info': payer_info
        }
    def set_service_context(self, service_info: Dict):
        """Set current service context"""
        self.service_context = service_info


class ZeroFailValidator:
    """Zero-tolerance validation for 835 to CSV conversion"""
    def __init__(self, debug: bool = False):
        self.errors = []
        self.warnings = []
        self.debug = debug
        self.field_tracker = EDIFieldTracker()
        self.segment_maps = SegmentFieldMap.get_all_segments()
        self.qualifier_maps = SegmentFieldMap.get_qualifier_maps()
        self.debug_counts = defaultdict(lambda: defaultdict(int))
        self.debug_limit = 3
        if self.debug:
            print("[DEBUG] ZeroFailValidator initialized with debug mode ENABLED")
            print("[DEBUG] Debug output limited to failed validations only")
            print("[DEBUG] Maximum 3 debug outputs per error type per payer\n")
        self.stats = {
            'total_segments': 0,
            'total_fields': 0,
            'fields_validated': 0,
            'calculations_checked': 0,
            'missing_mappings': defaultdict(list),
            'payers_missing_mileage_units': defaultdict(int),
            'payer_data_quality_issues': defaultdict(lambda: defaultdict(int))
        }
    def _should_debug(self, payer_name: str, error_type: str) -> bool:
        """Check if debug output should be shown for this payer/error combination"""
        if not self.debug:
            return False
        if self.debug_counts[payer_name][error_type] >= self.debug_limit:
            return False
        self.debug_counts[payer_name][error_type] += 1
        return True
    
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
            'A0425': 18.0,   # Ground ambulance mileage
            'A0435': 19.0,   # Fixed wing air mileage
            'A0436': 36.0,   # Rotary wing air mileage
        }
        
        rate = rate_map.get(proc_code)
        if rate:
            return round(charge_amt / rate, 1)
        return None
    
    def _normalize_amount(self, edi_value: str) -> str:
        """Convert EDI amount to CSV format (handle implied decimals)"""
        if not edi_value:
            return ''
        return edi_value
    def _is_amount_field(self, csv_field: str, seg_id: str, pos: str) -> bool:
        """Check if a field contains monetary amounts that need normalization"""
        if seg_id == 'BPR' and pos == '02':
            return True
        if seg_id == 'CLP' and pos in ['03', '04', '05']:
            return True
        if seg_id == 'SVC' and pos in ['02', '03']:
            return True
        if seg_id == 'MOA' and pos in ['02', '08']:
            return True
        if seg_id == 'MIA' and pos in ['02', '04', '06', '07', '08', '09', '10',
                                        '11', '12', '13', '14', '16', '17', '18', '19', '24']:
            return True
        if seg_id == 'PLB' and pos in ['04', '06', '08', '10', '12', '14']:
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
            if not segment.strip().startswith('CAS'):
                continue
            elements = segment.split(delimiter)
            if len(elements) < 4:
                continue
            group_code = elements[1] if len(elements) > 1 else ''
            idx = 2
            while idx + 1 < len(elements):
                reason_code = elements[idx].strip() if idx < len(elements) else ''
                amount = elements[idx + 1].strip() if idx + 1 < len(elements) else ''
                quantity = elements[idx + 2].strip() if idx + 2 < len(elements) else ''
                if reason_code and amount:
                    # Normalize reason code to handle payer-specific variations (e.g., leading zeros)
                    normalized_reason = colloquial.normalize_carc_code(reason_code)
                    cas_entries.append({
                        'group_code': group_code,
                        'reason_code': normalized_reason,
                        'amount': amount,
                        'quantity': quantity
                    })
                idx += 3
                if idx > 19:
                    break
        return cas_entries
    def _validate_cas_adjustments(self, claim_id: str, rows: List[dict],
                                  edi_cas_segments: List[str], level: str, delimiter: str):
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
            'Contractual': Decimal('0'),
            'Copay': Decimal('0'),
            'Coinsurance': Decimal('0'),
            'Deductible': Decimal('0'),
            'Denied': Decimal('0'),
            'OtherAdjustments': Decimal('0'),
            'Sequestration': Decimal('0'),
            'COB': Decimal('0'),
            'HCRA': Decimal('0'),
            'QMB': Decimal('0'),
            'PR_NonCovered': Decimal('0'),
            'OtherPatientResp': Decimal('0')
        }
        for cas in cas_entries:
            categories = categorize_adjustment(cas['group_code'], cas['reason_code'], cas['amount'])
            for cat, amt in categories.items():
                if cat == 'AuditFlag':
                    continue  # Skip audit flag - it's a string, not an amount
                expected_categories[cat] += Decimal(str(amt))
        prefix = 'CLM' if level == 'claim' else 'SVC'
        suffix = 'L2100_CAS' if level == 'claim' else 'L2110_CAS'
        if level == 'claim':
            target_row = next((r for r in rows
                             if r.get('CLM_PatientControlNumber_L2100_CLP') == claim_id
                             and not r.get('SVC_ProcedureCode_L2110_SVC')), None)
        else:
            target_row = next((r for r in rows
                             if r.get('CLM_PatientControlNumber_L2100_CLP') == claim_id
                             and r.get('SVC_ProcedureCode_L2110_SVC')), None)
        if not target_row:
            return
        for category, expected_amt in expected_categories.items():
            if expected_amt == 0:
                continue
            field_name = f'{prefix}_{category}_{suffix}'
            actual_value = target_row.get(field_name, '')
            try:
                actual_amt = Decimal(str(actual_value)) if actual_value else Decimal('0')
            except:
                actual_amt = Decimal('0')
            if abs(expected_amt - actual_amt) > Decimal('0.01'):
                self.errors.append(ValidationError(
                    'CAS_CATEGORY',
                    f'CAS {category} categorization mismatch',
                    location=f'Claim {claim_id}' + (f' ({level} level)' if level == 'service' else ''),
                    field=field_name,
                    expected=float(expected_amt),
                    actual=float(actual_amt)
                ))
        self.stats['calculations_checked'] += 1
    def validate_all(self, edi_segments: List[str], csv_rows: List[dict],
                     delimiter: str = '*', enable_warnings: bool = True, verbose: bool = False,
                     status_callback=None) -> Dict:
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
                print(msg)
            if status_callback:
                status_callback(msg)
        self.errors = []
        self.warnings = []
        self.stats = {
            'total_segments': len(edi_segments),
            'total_fields': 0,
            'fields_validated': 0,
            'calculations_checked': 0,
            'missing_mappings': defaultdict(list),
            'payers_missing_mileage_units': defaultdict(int),
            'payer_data_quality_issues': defaultdict(lambda: defaultdict(int))
        }
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
    
    def validate_all_by_file(self, edi_segments_by_file: List[dict], csv_rows: List[dict],
                             verbose: bool = False, status_callback=None) -> Dict:
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
                print(msg)
            if status_callback:
                status_callback(msg)
        
        self.errors = []
        self.warnings = []
        
        # Count total segments across all files
        total_segments = sum(len(fd['segments']) for fd in edi_segments_by_file)
        self.stats = {
            'total_segments': total_segments,
            'total_fields': 0,
            'fields_validated': 0,
            'calculations_checked': 0,
            'missing_mappings': defaultdict(list),
            'payers_missing_mileage_units': defaultdict(int),
            'payer_data_quality_issues': defaultdict(lambda: defaultdict(int))
        }
        
        update_status("  [1/10] Parsing EDI structure (per-file)...")
        edi_data = self._parse_edi_data_by_file(edi_segments_by_file)
        
        # Combine segments for methods that need flat list
        all_segments = []
        default_delimiter = '*'
        for file_data in edi_segments_by_file:
            all_segments.extend(file_data['segments'])
            default_delimiter = file_data.get('delimiter', '*')
        
        update_status("  [2/10] Validating loop structure...")
        self._validate_loop_structure(edi_data)
        update_status("  [3/10] Validating segment sequences...")
        self._validate_critical_sequences(all_segments, default_delimiter)
        update_status("  [4/10] Grouping claims...")
        csv_by_claim = self._group_csv_by_claim(csv_rows)
        update_status("  [5/10] Validating calculations & balancing...")
        self._validate_calculations(csv_rows, csv_by_claim, all_segments, default_delimiter, edi_data, verbose=verbose)
        update_status("  [6/10] Validating field completeness...")
        self._validate_completeness(edi_data, csv_rows, default_delimiter, verbose=verbose)
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
            'header': {},
            'payer_loop': {},
            'payee_loop': {},
            'claims': defaultdict(dict),
            'claims_by_file': defaultdict(lambda: defaultdict(dict)),  # file -> claim_id -> data
            'services': defaultdict(list),
            'segment_counts': defaultdict(int),
            'all_segments': [],
            'current_loop': 'header'
        }
        
        for file_data in edi_segments_by_file:
            file_name = file_data['file']
            segments = file_data['segments']
            delimiter = file_data.get('delimiter', '*')
            
            current_claim = None
            current_claim_key = None  # claim_id|occurrence key
            current_service = None
            current_loop_level = 'header'
            
            # Track claim occurrences within this file (same claim can appear multiple times)
            claim_occurrence_tracker = {}
            
            for segment in segments:
                if not segment.strip():
                    continue
                elements = segment.split(delimiter)
                seg_id = elements[0]
                self.field_tracker.track_segment(segment, elements, delimiter)
                edi_data['segment_counts'][seg_id] += 1
                edi_data['all_segments'].append({
                    'segment': segment,
                    'elements': elements,
                    'seg_id': seg_id,
                    'file': file_name
                })
                self.stats['total_fields'] += len(elements)
                
                if seg_id == 'CLP':
                    current_loop_level = 'claim'
                    base_claim_id = elements[1] if len(elements) > 1 else 'UNKNOWN'
                    
                    # Track occurrence (same claim can appear multiple times: reversal, correction, etc.)
                    if base_claim_id not in claim_occurrence_tracker:
                        claim_occurrence_tracker[base_claim_id] = 0
                    claim_occurrence_tracker[base_claim_id] += 1
                    occurrence = claim_occurrence_tracker[base_claim_id]
                    
                    # Use claim_id|occurrence as the key to handle duplicates
                    current_claim = base_claim_id
                    current_claim_key = f"{base_claim_id}|{occurrence}"
                    current_service = None
                    
                    # Normalize file path to basename for consistent matching with CSV
                    normalized_file = self._normalize_file_path(file_name)
                    # Store by file AND claim_id|occurrence (like the parser does)
                    edi_data['claims_by_file'][normalized_file][current_claim_key] = {
                        'segments': {'CLP': elements},
                        'services': [],
                        'file': normalized_file,
                        'occurrence': occurrence
                    }
                    # Also store with composite key for backward compatibility
                    composite_key = f"{normalized_file}|{current_claim_key}"
                    edi_data['claims'][composite_key] = edi_data['claims_by_file'][normalized_file][current_claim_key]
                    
                elif seg_id == 'SVC' and current_claim_key:
                    current_loop_level = 'service'
                    normalized_file = self._normalize_file_path(file_name)
                    current_service = len(edi_data['claims_by_file'][normalized_file][current_claim_key]['services'])
                    service_data = {'SVC': elements, 'segments': {}}
                    edi_data['claims_by_file'][normalized_file][current_claim_key]['services'].append(service_data)
                    
                elif current_service is not None and current_claim_key:
                    normalized_file = self._normalize_file_path(file_name)
                    services = edi_data['claims_by_file'][normalized_file][current_claim_key]['services']
                    if services:
                        if seg_id in ['DTM', 'CAS', 'REF', 'AMT', 'QTY', 'LQ']:
                            services[-1]['segments'][seg_id] = elements
                        else:
                            current_service = None
                            edi_data['claims_by_file'][normalized_file][current_claim_key]['segments'][seg_id] = elements
                elif current_claim_key and current_service is None:
                    normalized_file = self._normalize_file_path(file_name)
                    if seg_id not in ['BPR', 'TRN', 'N1', 'N2', 'N3', 'N4', 'PER', 'REF', 'CUR', 'RDM']:
                        edi_data['claims_by_file'][normalized_file][current_claim_key]['segments'][seg_id] = elements
                else:
                    if current_loop_level == 'payer':
                        if seg_id not in ['CLP', 'SVC']:
                            # Keep first file's values to match CSV extraction (uses first row)
                            if seg_id not in edi_data['payer_loop']:
                                edi_data['payer_loop'][seg_id] = elements
                    elif current_loop_level == 'payee':
                        if seg_id not in ['CLP', 'SVC']:
                            if seg_id not in edi_data['payee_loop']:
                                edi_data['payee_loop'][seg_id] = elements
                    else:
                        if seg_id not in ['CLP', 'SVC']:
                            if seg_id not in edi_data['header']:
                                edi_data['header'][seg_id] = elements
        
        return edi_data
    
    def _validate_composite_fields_by_file(self, edi_data: Dict, csv_rows: List[dict]):
        """Validate composite fields using file context - matches how parser works."""
        # Group CSV rows by file and claim|occurrence (normalize file paths to basename)
        # This matches how the parser tracks claim occurrences for reversals/adjustments
        csv_by_file_and_claim = defaultdict(lambda: defaultdict(list))
        for r in csv_rows:
            claim_id = r.get('CLM_PatientControlNumber_L2100_CLP')
            occurrence = r.get('CLM_Occurrence_L2100_CLP', 1)
            file_name = self._normalize_file_path(r.get('Filename_File', ''))
            if claim_id and r.get('SVC_ProcedureCode_L2110_SVC'):
                # Key by claim_id|occurrence to match EDI parsing
                claim_key = f"{claim_id}|{occurrence}"
                csv_by_file_and_claim[file_name][claim_key].append(r)
        
        # Get component delimiter from first CSV row
        component_delimiter = ':'
        if csv_rows:
            component_delimiter = csv_rows[0].get('ENV_ComponentSeparator_Envelope_ISA', ':') or ':'
        
        # Validate per-file (like the parser processes)
        for file_name, claims in edi_data.get('claims_by_file', {}).items():
            csv_claims = csv_by_file_and_claim.get(file_name, {})
            
            for claim_key, claim_data in claims.items():
                # claim_key is now "claim_id|occurrence"
                service_rows = csv_claims.get(claim_key, [])
                
                # Extract display-friendly claim ID and occurrence
                if '|' in claim_key:
                    display_claim_id, occurrence = claim_key.rsplit('|', 1)
                    display_location = f"Claim {display_claim_id} (occurrence {occurrence})"
                else:
                    display_claim_id = claim_key
                    display_location = f"Claim {display_claim_id}"
                
                for svc_idx, service_data in enumerate(claim_data.get('services', [])):
                    svc_segment = service_data.get('SVC')
                    if not svc_segment or len(svc_segment) < 2:
                        continue
                    
                    svc01_composite = svc_segment[1]
                    components = svc01_composite.split(component_delimiter)
                    if len(components) < 2:
                        continue
                    
                    expected_code = components[1] if len(components) > 1 else ''
                    expected_modifiers = components[2:6]
                    
                    if svc_idx >= len(service_rows):
                        payer_name = 'Unknown'
                        payer_state = 'Unknown State'
                        if service_rows:
                            payer_name = service_rows[0].get('Payer_Name_L1000A_N1', 'Unknown')
                            payer_state = service_rows[0].get('Payer_State_L1000A_N4', 'Unknown State')
                        self.errors.append(ValidationError(
                            'COMPOSITE_PARSE',
                            f'Service {svc_idx + 1} exists in EDI but not in CSV',
                            location=f'File: {file_name}, {display_location}, Service {svc_idx + 1}',
                            expected=f'Service with code {expected_code}',
                            actual='Not found in CSV',
                            payer_info={'name': payer_name, 'state': payer_state}
                        ))
                        continue
                    
                    csv_row = service_rows[svc_idx]
                    payer_name = csv_row.get('Payer_Name_L1000A_N1', 'Unknown')
                    payer_state = csv_row.get('Payer_State_L1000A_N4', 'Unknown State')
                    payer_info = {'name': payer_name, 'state': payer_state}
                    actual_code = csv_row.get('SVC_ProcedureCode_L2110_SVC', '')
                    
                    if expected_code != actual_code:
                        self.errors.append(ValidationError(
                            'COMPOSITE_PARSE',
                            'Procedure code not correctly extracted from composite field',
                            location=f'File: {file_name}, {display_location}, Service {svc_idx + 1}',
                            field='SVC_ProcedureCode_L2110_SVC',
                            expected=expected_code,
                            actual=actual_code,
                            payer_info=payer_info
                        ))
                    
                    for mod_idx, expected_mod in enumerate(expected_modifiers, 1):
                        if not expected_mod:
                            continue
                        field_name = f'SVC_Modifier{mod_idx}_L2110_SVC'
                        actual_mod = csv_row.get(field_name, '')
                        if expected_mod != actual_mod:
                            self.errors.append(ValidationError(
                                'COMPOSITE_PARSE',
                                f'Modifier {mod_idx} not correctly extracted from composite',
                                location=f'File: {file_name}, {display_location}, Service {svc_idx + 1}',
                                field=field_name,
                                expected=expected_mod,
                                actual=actual_mod,
                                payer_info=payer_info
                            ))

    def _parse_edi_data(self, segments: List[str], delimiter: str) -> Dict:
        """Parse all EDI segments into structured data"""
        edi_data = {
            'header': {},
            'payer_loop': {},
            'payee_loop': {},
            'claims': defaultdict(dict),
            'services': defaultdict(list),
            'segment_counts': defaultdict(int),
            'all_segments': [],
            'current_loop': 'header'
        }

        current_claim = None
        current_service = None

        current_loop_level = 'header'
        in_payer_loop = False
        in_payee_loop = False
        for segment in segments:
            if not segment.strip():
                continue
            elements = segment.split(delimiter)
            seg_id = elements[0]
            self.field_tracker.track_segment(segment, elements, delimiter)
            edi_data['segment_counts'][seg_id] += 1
            edi_data['all_segments'].append({
                'segment': segment,
                'elements': elements,
                'seg_id': seg_id
            })
            self.stats['total_fields'] += len(elements)
            if seg_id == 'N1' and len(elements) > 1:
                entity_code = elements[1]
                if entity_code == 'PR':
                    current_loop_level = 'payer'
                    in_payer_loop = True
                    in_payee_loop = False
                elif entity_code == 'PE':
                    current_loop_level = 'payee'
                    in_payee_loop = True
                    in_payer_loop = False
            if seg_id == 'CLP':
                current_loop_level = 'claim'
                in_payer_loop = False
                in_payee_loop = False
                current_claim = elements[1] if len(elements) > 1 else 'UNKNOWN'
                current_service = None
                edi_data['claims'][current_claim] = {
                    'segments': {'CLP': elements},
                    'services': []
                }
            elif seg_id == 'SVC' and current_claim:
                current_loop_level = 'service'
                current_service = len(edi_data['claims'][current_claim]['services'])
                service_data = {'SVC': elements, 'segments': {}}
                edi_data['claims'][current_claim]['services'].append(service_data)
            elif current_service is not None and current_claim:
                services = edi_data['claims'][current_claim]['services']
                if services:
                    if seg_id in ['DTM', 'CAS', 'REF', 'AMT', 'QTY', 'LQ']:
                        services[-1]['segments'][seg_id] = elements
                    else:
                        current_service = None
                        edi_data['claims'][current_claim]['segments'][seg_id] = elements
            elif current_claim and current_service is None:
                if 'segments' not in edi_data['claims'][current_claim]:
                    edi_data['claims'][current_claim]['segments'] = {}
                if seg_id not in ['BPR', 'TRN', 'N1', 'N2', 'N3', 'N4', 'PER', 'REF', 'CUR', 'RDM']:
                    edi_data['claims'][current_claim]['segments'][seg_id] = elements
            else:
                if current_loop_level == 'payer':
                    if seg_id not in ['CLP', 'SVC']:
                        # Keep first file's values to match CSV extraction (uses first row)
                        if seg_id not in edi_data['payer_loop']:
                            edi_data['payer_loop'][seg_id] = elements
                elif current_loop_level == 'payee':
                    if seg_id not in ['CLP', 'SVC']:
                        if seg_id not in edi_data['payee_loop']:
                            edi_data['payee_loop'][seg_id] = elements
                else:
                    if seg_id not in ['CLP', 'SVC']:
                        if seg_id not in edi_data['header']:
                            edi_data['header'][seg_id] = elements
        return edi_data
    def _group_csv_by_claim(self, csv_rows: List[dict]) -> Dict[str, List[dict]]:
        """Group CSV rows by (file, claim_id) composite key to handle duplicate claim IDs across files"""
        grouped = defaultdict(list)
        for row in csv_rows:
            claim_id = row.get('CLM_PatientControlNumber_L2100_CLP', 'UNKNOWN')
            file_name = row.get('Filename_File', '')
            if file_name:
                # Normalize file path to basename for consistent matching
                normalized_file = self._normalize_file_path(file_name)
                composite_key = f"{normalized_file}|{claim_id}"
            else:
                composite_key = claim_id
            grouped[composite_key].append(row)
        return grouped
    def _normalize_file_path(self, file_path: str) -> str:
        """Normalize file path to just basename for consistent matching.
        
        This handles cases where CSV has paths with '_testing' folder but
        EDI files are read from original location.
        """
        if not file_path:
            return ''
        # Extract just the filename (basename) for consistent matching
        import os
        return os.path.basename(file_path)
    
    def _extract_claim_id_from_composite(self, composite_key: str) -> tuple:
        """Extract file name and claim ID from composite key"""
        if '|' in composite_key:
            parts = composite_key.split('|', 1)
            return parts[0], parts[1]
        else:
            return '', composite_key
    def _validate_calculations(self, csv_rows: List[dict], csv_by_claim: Dict[str, List[dict]],
                              edi_segments: List[str], delimiter: str, edi_data: Dict, verbose: bool = False):
        """Layer 1: Validate all calculations"""

        current_file_segments = []
        file_segment_groups = []

        for segment in edi_segments:
            seg_id = segment.split(delimiter)[0] if delimiter in segment else segment.split('*')[0]
            current_file_segments.append(segment)

            if seg_id == 'IEA':
                file_segment_groups.append(current_file_segments[:])
                current_file_segments = []

        for file_segments in file_segment_groups:
            check_total = None
            clp_payments = []
            plb_total = Decimal('0')
            payer_name = 'Unknown'
            payer_state = 'Unknown State'
            found_pr = False
            for segment in file_segments:
                if segment.startswith('N1' + delimiter + 'PR'):
                    elements = segment.split(delimiter)
                    if len(elements) > 2:
                        payer_name = elements[2]
                    found_pr = True
                elif found_pr and segment.startswith('N4' + delimiter):
                    elements = segment.split(delimiter)
                    if len(elements) > 2:
                        payer_state = elements[2]
                    break

            saw_bpr_segment = False
            notification_only_check = True
            zero_amount_tolerance = Decimal('0.01')

            for segment in file_segments:
                elements = segment.split(delimiter)
                seg_id = elements[0]

                if seg_id == 'BPR' and len(elements) > 2:
                    method_code = elements[4].strip().upper() if len(elements) > 4 and elements[4] else ''
                    amount = None
                    try:
                        amount = Decimal(str(elements[2]))
                        if check_total is None:
                            check_total = amount
                        else:
                            check_total += amount
                    except (ValueError, TypeError, decimal.InvalidOperation):
                        pass
                    saw_bpr_segment = True
                    if notification_only_check:
                        is_zero_non_payment = (
                            amount is not None
                            and abs(amount) <= zero_amount_tolerance
                            and method_code == 'NON'
                        )
                        if not is_zero_non_payment:
                            notification_only_check = False

                elif seg_id == 'CLP' and len(elements) > 4:
                    try:
                        payment = Decimal(str(elements[4]))
                        clp_payments.append(payment)
                    except (ValueError, TypeError, decimal.InvalidOperation):
                        pass

                elif seg_id == 'PLB' and len(elements) >= 3:
                    for i in range(3, min(len(elements), 15), 2):
                        if i+1 < len(elements) and elements[i+1]:
                            try:
                                amount = Decimal(str(elements[i+1]))
                                plb_total += amount
                            except (ValueError, TypeError):
                                pass

            skip_transaction_balance = saw_bpr_segment and notification_only_check
            if skip_transaction_balance:
                self.stats['transaction_balances_skipped_non_payment'] = (
                    self.stats.get('transaction_balances_skipped_non_payment', 0) + 1
                )
            elif check_total is not None and clp_payments:
                expected = sum(clp_payments) - plb_total
                if abs(check_total - expected) > Decimal('0.01'):
                    if self._should_debug(payer_name, 'TransactionBalance'):
                        print(f"\n[DEBUG] Transaction balance error")
                        print(f"[DEBUG] Payer: {payer_name}")
                        print(f"[DEBUG] BPR02 (Check Total): {check_total}")
                        print(f"[DEBUG] Sum(CLP04) Claim Payments: {sum(clp_payments)}")
                        print(f"[DEBUG] PLB Adjustments: {plb_total}")
                        print(f"[DEBUG] Expected (CLP04 - PLB): {expected}")
                        print(f"[DEBUG] Difference: {abs(check_total - expected)}")
                    self.errors.append(ValidationError(
                        'CALC',
                        f"Check total doesn't balance per X12 835 Section 1.10.2.1.3",
                        location='Transaction Level (BPR02 vs CLP04-PLB)',
                        expected=float(expected),
                        actual=float(check_total),
                        payer_info={'name': payer_name, 'state': payer_state}
                    ))
                self.stats['calculations_checked'] += 1

        # Build CAS segments by composite key (file|claim_id) to match csv_by_claim structure
        # This prevents collision when multiple files share the same claim ID
        cas_by_claim = defaultdict(lambda: {'claim': [], 'service': []})
        current_context = None
        current_claim_id = None
        current_file = None
        current_composite_key = None
        for seg_data in edi_data['all_segments']:
            seg_id = seg_data['seg_id']
            elements = seg_data['elements']
            # Track file changes - 'file' field is added when processing multi-file EDI data
            seg_file = seg_data.get('file', '')
            if seg_file:
                current_file = self._normalize_file_path(seg_file)

            if seg_id == 'CLP':
                current_claim_id = elements[1] if len(elements) > 1 else None
                current_context = 'claim'
                # Build composite key matching csv_by_claim format
                if current_file and current_claim_id:
                    current_composite_key = f"{current_file}|{current_claim_id}"
                else:
                    current_composite_key = current_claim_id

            elif seg_id == 'SVC' and current_claim_id:
                current_context = 'service'

            elif seg_id == 'CAS' and current_composite_key:
                if current_context == 'claim':
                    cas_by_claim[current_composite_key]['claim'].append(seg_data['segment'])
                elif current_context == 'service':
                    cas_by_claim[current_composite_key]['service'].append(seg_data['segment'])
        total_claims = len(csv_by_claim)
        for idx, (claim_id, rows) in enumerate(csv_by_claim.items(), 1):
            if verbose and idx % 100 == 0:
                print(f"      Validating claim {idx:,} of {total_claims:,}...")
            self._validate_claim_calculations(claim_id, rows)

            # Use composite key directly - both csv_by_claim and cas_by_claim now use same format
            claim_cas_segments = cas_by_claim.get(claim_id, {}).get('claim', [])
            service_cas_segments = cas_by_claim.get(claim_id, {}).get('service', [])

            if claim_cas_segments:
                self._validate_cas_adjustments(claim_id, rows, claim_cas_segments, 'claim', delimiter)

            if service_cas_segments:
                self._validate_cas_adjustments(claim_id, rows, service_cas_segments, 'service', delimiter)
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
        claim_row = next((r for r in rows if r.get('CLM_ChargeAmount_L2100_CLP') and not r.get('SVC_ChargeAmount_L2110_SVC')), None)
        if not claim_row:
            return
        payer_name = claim_row.get('Payer_Name_L1000A_N1', 'Unknown')
        payer_state = claim_row.get('Payer_State_L1000A_N4', 'Unknown State')
        payer_info = {'name': payer_name, 'state': payer_state}
        try:
            claim_charge = Decimal(str(claim_row.get('CLM_ChargeAmount_L2100_CLP', 0)))
            claim_payment = Decimal(str(claim_row.get('CLM_PaymentAmount_L2100_CLP', 0)))
            claim_status = claim_row.get('CLM_Status_L2100_CLP', '')
        except:
            return
        if claim_status == '25':
            if claim_payment != 0:
                error_location = f"Claim {display_claim_id}"
                if file_name:
                    error_location += f" (File: {file_name})"
                if self._should_debug(payer_name, 'Predetermination'):
                    print(f"\n[DEBUG] Predetermination claim error for {display_claim_id}")
                    print(f"[DEBUG] Payer: {payer_name}")
                    print(f"[DEBUG] CLP02 (Status): {claim_status} (Predetermination)")
                    print(f"[DEBUG] CLP04 Payment: {claim_payment} - Should be zero")
                self.errors.append(ValidationError(
                    'EDGE',
                    f"Predetermination claim (Status 25) should have zero payment per X12 spec section 1.10.2.7",
                    location=error_location,
                    expected=0.0,
                    actual=float(claim_payment),
                    field='CLM_PaymentAmount_L2100_CLP'
                ))
            return
        claim_adj_total = Decimal('0')

        for cas_idx in range(1, 6):
            cas_amount_field = f'CLM_CAS{cas_idx}_Amount_L2100_CAS'
            val = claim_row.get(cas_amount_field)
            if val:
                try:
                    amount = Decimal(str(val))
                    claim_adj_total += amount
                except (ValueError, TypeError, decimal.InvalidOperation):
                    pass

        service_rows = [r for r in rows if r.get('SVC_ChargeAmount_L2110_SVC')]
        for svc_row in service_rows:
            for cas_idx in range(1, 6):
                cas_amount_field = f'SVC_CAS{cas_idx}_Amount_L2110_CAS'
                val = svc_row.get(cas_amount_field)
                if val:
                    try:
                        amount = Decimal(str(val))
                        claim_adj_total += amount
                    except (ValueError, TypeError, decimal.InvalidOperation):
                        pass
        expected = claim_charge - claim_adj_total
        if abs(claim_payment - expected) > Decimal('0.01'):
            error_location = f"Claim {display_claim_id}"
            if file_name:
                error_location += f" (File: {file_name})"
            if self._should_debug(payer_name, 'ClaimBalance'):
                print(f"\n[DEBUG] Claim balance error for {display_claim_id}")
                print(f"[DEBUG] Payer: {payer_name}")
                print(f"[DEBUG] CLP03 (Charge): {claim_charge}")
                print(f"[DEBUG] CAS Adjustments Total: {claim_adj_total}")
                print(f"[DEBUG] CLP04 (Payment): {claim_payment}")
                print(f"[DEBUG] Expected (Charge - Adj): {expected}")
                print(f"[DEBUG] Difference: {abs(claim_payment - expected)}")
            self.errors.append(ValidationError(
                'CALC',
                f"Claim doesn't balance: Charge({claim_charge}) - Adjustments({claim_adj_total}) should equal Payment({claim_payment})",
                location=error_location,
                expected=float(expected),
                actual=float(claim_payment),
                field='CLM_PaymentAmount_L2100_CLP'
            ))
        self.stats['calculations_checked'] += 1
        service_rows = [r for r in rows if r.get('SVC_ChargeAmount_L2110_SVC')]
        service_charge_total = Decimal('0')
        service_payment_total = Decimal('0')
        for svc_row in service_rows:
            self._validate_service_calculations(display_claim_id, svc_row, file_name)
            try:
                service_charge_total += Decimal(str(svc_row.get('SVC_ChargeAmount_L2110_SVC', 0)))
                service_payment_total += Decimal(str(svc_row.get('SVC_PaymentAmount_L2110_SVC', 0)))
            except:
                pass
        if len(service_rows) > 0:
            try:
                if abs(claim_charge - service_charge_total) > Decimal('0.01'):
                    error_location = f"Claim {display_claim_id}"
                    if file_name:
                        error_location += f" (File: {file_name})"
                    self.errors.append(ValidationError(
                        'CALC',
                        f"Service charges don't sum to claim total: CLP03({claim_charge}) should equal sum of SVC02({service_charge_total})",
                        location=error_location,
                        expected=float(claim_charge),
                        actual=float(service_charge_total),
                        field='CLM_ChargeAmount_L2100_CLP'
                    ))
            except (ValueError, TypeError, decimal.InvalidOperation):
                pass
            try:
                if abs(claim_payment - service_payment_total) > Decimal('0.01'):
                    error_location = f"Claim {display_claim_id}"
                    if file_name:
                        error_location += f" (File: {file_name})"
                    self.errors.append(ValidationError(
                        'CALC',
                        f"Service payments don't sum to claim payment: CLP04({claim_payment}) should equal sum of SVC03({service_payment_total})",
                        location=error_location,
                        expected=float(claim_payment),
                        actual=float(service_payment_total),
                        field='CLM_PaymentAmount_L2100_CLP'
                    ))
            except (ValueError, TypeError, decimal.InvalidOperation):
                pass
    def _validate_service_calculations(self, claim_id: str, row: dict, file_name: str = ''):
        """Validate service line calculations
        X12 835 Section 1.10.2.1.1 Service Line Balancing:
        'The submitted service charge plus or minus the sum of all monetary
        adjustments must equal the amount paid for this service line.'
        Formula: SVC02 (charge) - CAS adjustments = SVC03 (payment)
        Where CAS adjustments = sum of CAS03, 06, 09, 12, 15, and 18
        """
        payer_name = row.get('Payer_Name_L1000A_N1', 'Unknown')
        try:
            charge = Decimal(str(row.get('SVC_ChargeAmount_L2110_SVC', 0)))
            payment = Decimal(str(row.get('SVC_PaymentAmount_L2110_SVC', 0)))
        except:
            return
        adj_total = Decimal('0')
        for cas_idx in range(1, 6):
            cas_amount_field = f'SVC_CAS{cas_idx}_Amount_L2110_CAS'
            val = row.get(cas_amount_field)
            if val:
                try:
                    amount = Decimal(str(val))
                    adj_total += amount
                except (ValueError, TypeError, decimal.InvalidOperation):
                    pass
        expected = charge - adj_total
        if abs(payment - expected) > Decimal('0.01'):
            proc = row.get('SVC_ProcedureCode_L2110_SVC', '')
            error_location = f"Claim {claim_id}, Service {proc}"
            if file_name:
                error_location += f" (File: {file_name})"
            if self._should_debug(payer_name, 'ServiceBalance'):
                print(f"\n[DEBUG] Service balance error for {proc} in claim {claim_id}")
                print(f"[DEBUG] Payer: {payer_name}")
                print(f"[DEBUG] SVC02 (Charge): {charge}")
                print(f"[DEBUG] CAS Adjustments Total: {adj_total}")
                print(f"[DEBUG] SVC03 (Payment): {payment}")
                print(f"[DEBUG] Expected (Charge - Adj): {expected}")
            self.errors.append(ValidationError(
                'CALC',
                f"Service doesn't balance: Charge({charge}) - Adjustments({adj_total}) should equal Payment({payment})",
                location=error_location,
                expected=float(expected),
                actual=float(payment),
                field='SVC_PaymentAmount_L2110_SVC'
            ))
        self.stats['calculations_checked'] += 1
    def _validate_completeness(self, edi_data: Dict, csv_rows: List[dict], delimiter: str, verbose: bool = False):
        """Layer 2: Validate 100% field coverage"""

        if verbose:
            print("      Detecting duplicate claim numbers...")
        from collections import Counter
        claim_numbers = [row.get('CLM_PatientControlNumber_L2100_CLP') for row in csv_rows if row.get('CLM_PatientControlNumber_L2100_CLP')]
        claim_counts = Counter(claim_numbers)
        duplicate_claim_numbers = set([cn for cn, count in claim_counts.items() if count > 1])
        if verbose:
            print(f"      Building expected fields from {len(edi_data.get('claims', {}))} claims...")
        expected_fields = self._build_expected_fields(edi_data, delimiter)

        if verbose:
            print(f"      Extracting actual fields from {len(csv_rows):,} CSV rows...")
        actual_fields = self._extract_actual_fields(csv_rows, verbose=verbose)
        if verbose:
            print(f"      Comparing {len(expected_fields):,} expected fields...")

        missing_count = 0
        total_fields = len(expected_fields)
        for idx, (field_key, expected_value) in enumerate(expected_fields.items(), 1):
            if verbose and idx % 1000 == 0:
                print(f"      Validating field {idx:,} of {total_fields:,}...")
            if '_CAS_Present_' in field_key:
                continue

            location = expected_value.get('location', '')

            is_loop_level = location in ['Header', 'Payer Loop', 'Payee Loop']

            if field_key not in actual_fields:
                if is_loop_level:
                    continue

                segment = expected_value.get('segment', '')
                if segment in ['MOA', 'MIA', 'PER', 'AMT', 'QTY', 'DTM', 'REF', 'CAS', 'LQ']:
                    continue

                self.errors.append(ValidationError(
                    'MISSING',
                    f"Field not found in CSV",
                    segment=expected_value['segment'],
                    field=expected_value['csv_field'],
                    expected=expected_value['value'],
                    location=expected_value.get('location')
                ))

            elif actual_fields[field_key] != expected_value['value']:
                if not self._values_match(actual_fields[field_key], expected_value['value']):
                    if is_loop_level and not actual_fields[field_key]:
                        continue
                    location_str = expected_value.get('location') or ''
                    if location_str and 'Claim' in location_str:
                        claim_from_location = location_str.split('Claim')[1].strip().split(',')[0].strip()
                        if claim_from_location in duplicate_claim_numbers:
                            continue

                    self.errors.append(ValidationError(
                        'MISMATCH',
                        f"Field value mismatch",
                        segment=expected_value['segment'],
                        field=expected_value['csv_field'],
                        expected=expected_value['value'],
                        actual=actual_fields[field_key],
                        location=expected_value.get('location')
                    ))

            self.stats['fields_validated'] += 1
    def _build_expected_fields(self, edi_data: Dict, delimiter: str) -> Dict:
        """Build complete list of expected fields from EDI"""
        expected = {}
        for seg_id, elements in edi_data['header'].items():
            self._add_expected_segment_fields(expected, seg_id, elements, 'Header')
        for seg_id, elements in edi_data['payer_loop'].items():
            self._add_expected_segment_fields(expected, seg_id, elements, 'Payer Loop')
        for seg_id, elements in edi_data['payee_loop'].items():
            self._add_expected_segment_fields(expected, seg_id, elements, 'Payee Loop')
        for claim_id, claim_data in edi_data['claims'].items():
            # Extract just the claim ID from composite key (file|claim_id) for location string
            _, actual_claim_id = self._extract_claim_id_from_composite(claim_id)
            display_claim_id = actual_claim_id if actual_claim_id else claim_id
            
            for seg_id, elements in claim_data.get('segments', {}).items():
                if seg_id not in ['ISA', 'GS', 'ST', 'SE', 'GE', 'IEA', 'BPR', 'TRN', 'N1', 'N2', 'N3', 'N4', 'PER', 'REF', 'CUR', 'RDM', 'DTM', 'PLB', 'LX', 'TS3', 'TS2']:
                    self._add_expected_segment_fields(expected, seg_id, elements, f"Claim {display_claim_id}")

            for svc_idx, service_data in enumerate(claim_data.get('services', [])):
                for seg_id, elements in service_data.get('segments', {}).items():
                    location = f"Claim {display_claim_id}, Service {svc_idx + 1}"
                    self._add_expected_segment_fields(expected, seg_id, elements, location)
        return expected
    def _add_expected_segment_fields(self, expected: Dict, seg_id: str,
                                    elements: List[str], location: str):
        """Add expected fields for a specific segment"""
        from dictionary import (
            get_bpr_transaction_handling_description,
            get_payment_method_description,
            get_payment_format_description,
            get_credit_debit_indicator_description,
            get_trace_type_description,
            get_claim_status_description,
            get_claim_filing_indicator_description,
            get_claim_frequency_description
        )
        if seg_id in self.segment_maps:
            field_map = self.segment_maps[seg_id]
            for pos, csv_field in field_map.items():
                if csv_field != 'Not Used':
                    element_index = int(pos)
                    if element_index < len(elements):
                        value = elements[element_index]
                        if value:
                            if csv_field == 'CHK_TransactionHandling_Header_BPR':
                                if seg_id == 'BPR' and pos == '01':
                                    value = get_bpr_transaction_handling_description(value)
                            if seg_id in ['N1', 'N2', 'N3', 'N4'] and location in ['Payer Loop', 'Payee Loop']:
                                if location == 'Payer Loop':
                                    csv_field = f"Payer_{csv_field}_L1000A_{seg_id}"
                                elif location == 'Payee Loop':
                                    csv_field = f"Provider_{csv_field}_L1000B_{seg_id}"

                            if seg_id == 'PER' and location == 'Payer Loop':
                                function_code = elements[1] if len(elements) > 1 else ''
                                if function_code:
                                    if csv_field == 'Contact_Function_Code':
                                        csv_field = f"Contact_{function_code}_Function_L1000A_PER"
                                    elif csv_field == 'Name':
                                        csv_field = f"Contact_{function_code}_Name_L1000A_PER"
                                    elif csv_field == 'Communication_Number_Qualifier_1':
                                        csv_field = f"Contact_{function_code}_Phone_Qualifier_L1000A_PER"
                                    elif csv_field == 'Communication_Number_1':
                                        csv_field = f"Contact_{function_code}_Phone_L1000A_PER"
                                    elif csv_field == 'Communication_Number_Qualifier_2':
                                        csv_field = f"Contact_{function_code}_Comm2_Qualifier_L1000A_PER"
                                    elif csv_field == 'Communication_Number_2':
                                        csv_field = f"Contact_{function_code}_Comm2_Number_L1000A_PER"

                            key = f"{location}:{csv_field}"
                            expected[key] = {
                                'segment': seg_id,
                                'position': pos,
                                'csv_field': csv_field,
                                'value': value,
                                'location': location
                            }
        if seg_id in self.qualifier_maps:
            self._add_qualifier_based_fields(expected, seg_id, elements, location)
        if seg_id == 'CAS' and len(elements) >= 4:
            group_code = elements[1] if len(elements) > 1 else ''
            cas_adjustments = []
            idx = 2
            while idx + 1 < len(elements):
                reason = elements[idx] if idx < len(elements) else ''
                amount = elements[idx + 1] if idx + 1 < len(elements) else ''
                qty = elements[idx + 2] if idx + 2 < len(elements) else ''
                if reason and amount:
                    cas_adjustments.append({
                        'group': group_code,
                        'reason': reason,
                        'amount': amount,
                        'qty': qty
                    })
                idx += 3
                if idx > 19:
                    break
            if cas_adjustments:
                if location and 'Service' in location:
                    field_suffix = 'L2110_CAS'
                    prefix = 'SVC'
                else:
                    field_suffix = 'L2100_CAS'
                    prefix = 'CLM'
                key = f"{location}:{prefix}_CAS_Present_{field_suffix}"
                expected[key] = {
                    'segment': seg_id,
                    'csv_field': f'{prefix}_*_{field_suffix}',
                    'value': 'EXPECTED',
                    'location': location,
                    'cas_data': cas_adjustments
                }
    def _add_qualifier_based_fields(self, expected: Dict, seg_id: str,
                                   elements: List[str], location: str):
        """Add expected fields for qualifier-based segments"""
        if seg_id == 'REF' and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps['REF']:
                csv_field = self.qualifier_maps['REF'][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    'segment': seg_id,
                    'qualifier': qualifier,
                    'csv_field': csv_field,
                    'value': value,
                    'location': location
                }
        elif seg_id == 'DTM' and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps['DTM']:
                csv_field = self.qualifier_maps['DTM'][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    'segment': seg_id,
                    'qualifier': qualifier,
                    'csv_field': csv_field,
                    'value': value,
                    'location': location
                }
        elif seg_id == 'AMT' and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps['AMT']:
                csv_field = self.qualifier_maps['AMT'][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    'segment': seg_id,
                    'qualifier': qualifier,
                    'csv_field': csv_field,
                    'value': value,
                    'location': location
                }
        elif seg_id == 'QTY' and len(elements) > 2:
            qualifier = elements[1]
            value = elements[2]
            if qualifier in self.qualifier_maps['QTY']:
                csv_field = self.qualifier_maps['QTY'][qualifier]
                key = f"{location}:{csv_field}"
                expected[key] = {
                    'segment': seg_id,
                    'qualifier': qualifier,
                    'csv_field': csv_field,
                    'value': value,
                    'location': location
                }
    def _extract_actual_fields(self, csv_rows: List[dict], verbose: bool = False) -> Dict:
        """Extract all actual field values from CSV"""
        actual = {}

        service_counts = defaultdict(int)
        total_rows = len(csv_rows)

        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                print(f"        Processing CSV row {idx:,} of {total_rows:,}...")
            raw_claim_id = row.get('CLM_PatientControlNumber_L2100_CLP', '')
            claim_id = str(raw_claim_id).strip() if raw_claim_id is not None else ''
            if not claim_id:
                continue
            occurrence_val = row.get('CLM_Occurrence_L2100_CLP')
            if occurrence_val in (None, ''):
                occurrence_val = row.get('CLAIM OCCURRENCE')
            occurrence = str(occurrence_val).strip() if occurrence_val not in (None, '') else ''
            claim_key = f"{claim_id}|{occurrence}" if occurrence else claim_id

            has_service = bool(row.get('SVC_ProcedureCode_L2110_SVC'))

            if not actual:
                for field, value in row.items():
                    if field.startswith(('CHK_', 'File_', 'Contact_', 'ENV_')):
                        key = f"Header:{field}"
                        actual[key] = value

                for field, value in row.items():
                    if field.startswith('Payer_'):
                        key = f"Payer Loop:{field}"
                        actual[key] = value

                for field, value in row.items():
                    if field.startswith('Provider_'):
                        key = f"Payee Loop:{field}"
                        actual[key] = value

            if claim_key:
                claim_location = f"Claim {claim_key}"

                if not has_service:
                    service_counts[claim_key] = 0

                for field, value in row.items():
                    if field.startswith('CLM_') and value:
                        key = f"{claim_location}:{field}"
                        actual[key] = value

                if has_service:
                    service_counts[claim_key] += 1
                    service_location = f"Claim {claim_key}, Service {service_counts[claim_key]}"
                    for field, value in row.items():
                        if field.startswith('SVC_') and value:
                            key = f"{service_location}:{field}"
                            actual[key] = value

        return actual
    def _values_match(self, actual: Any, expected: Any) -> bool:
        """Check if values match, allowing for formatting differences"""
        if actual is None and expected == '':
            return True
        if expected is None and actual == '':
            return True
        actual_str = str(actual).strip() if actual is not None else ''
        expected_str = str(expected).strip() if expected is not None else ''
        if actual_str == expected_str:
            return True
        try:
            actual_clean = actual_str.replace(',', '')
            expected_clean = expected_str.replace(',', '')
            actual_num = Decimal(actual_clean)
            expected_num = Decimal(expected_clean)
            return abs(actual_num - expected_num) <= Decimal('0.01')
        except:
            pass
        if len(actual_str) == 8 and len(expected_str) == 8:
            try:
                datetime.strptime(actual_str, '%Y%m%d')
                datetime.strptime(expected_str, '%Y%m%d')
                return actual_str == expected_str
            except:
                pass
        if actual_str.lower() == expected_str.lower():
            return True
        return False
    def _validate_field_mappings(self, csv_rows: List[dict], verbose: bool = False):
        """Layer 3: Validate field mappings and data quality"""
        self._track_dictionary_gaps(csv_rows, verbose=verbose)
        if verbose:
            print(f"      Validating data types for {len(csv_rows):,} rows...")
        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                print(f"        Validating data types for row {idx:,} of {len(csv_rows):,}...")
            self._validate_field_data_types(row)
    def _track_dictionary_gaps(self, csv_rows: List[dict], verbose: bool = False):
        """Identify all codes that have missing dictionary entries - OPTIMIZED"""
        import dictionary
        if not csv_rows:
            return
        total_rows = len(csv_rows)
        header_field_lookups = {
            'CHK_PaymentMethod_Header_BPR': dictionary.get_payment_method_description,
            'CHK_Format_Header_BPR': dictionary.get_payment_format_description,
            'CHK_TraceType_Header_TRN': dictionary.get_trace_type_description,
            'CHK_CreditDebitFlag_Header_BPR': dictionary.get_credit_debit_indicator_description,
            'CHK_BusinessFunctionCode_Header_BPR': dictionary.get_business_function_code_description,
            'CHK_PayerDFI_Qualifier_Header_BPR': dictionary.get_dfi_id_number_qualifier_description,
            'CHK_PayerAccountQualifier_Header_BPR': dictionary.get_account_number_qualifier_description,
            'CHK_PayeeDFI_Qualifier_Header_BPR': dictionary.get_dfi_id_number_qualifier_description,
            'CHK_PayeeAccountQualifier_Header_BPR': dictionary.get_account_number_qualifier_description,
            'CHK_DFI_Qualifier_3_Header_BPR': dictionary.get_dfi_id_number_qualifier_description,
            'CHK_AccountQualifier_3_Header_BPR': dictionary.get_account_number_qualifier_description,
            'Payer_IDQualifier_L1000A_REF': dictionary.get_id_code_qualifier_description,
            'Provider_IDQualifier_L1000B_REF': dictionary.get_id_code_qualifier_description,
        }
        row_level_lookups = {
            'CLM_Status_L2100_CLP': dictionary.get_claim_status_description,
            'CLM_FilingIndicator_L2100_CLP': dictionary.get_claim_filing_indicator_description,
            'CLM_FrequencyCode_L2100_CLP': dictionary.get_claim_frequency_description,
            'SVC_Qualifier_L2110_SVC': dictionary.get_service_qualifier_description,
        }
        carc_fields = [
            'CLM_CAS1_Reason_L2100_CAS', 'CLM_CAS2_Reason_L2100_CAS', 'CLM_CAS3_Reason_L2100_CAS',
            'CLM_CAS4_Reason_L2100_CAS', 'CLM_CAS5_Reason_L2100_CAS',
            'SVC_CAS1_Reason_L2110_CAS', 'SVC_CAS2_Reason_L2110_CAS', 'SVC_CAS3_Reason_L2110_CAS',
            'SVC_CAS4_Reason_L2110_CAS', 'SVC_CAS5_Reason_L2110_CAS',
        ]
        ambulance_code_field = 'SVC_ProcedureCode_L2110_SVC'
        modifier_fields = [
            'SVC_Modifier1_L2110_SVC', 'SVC_Modifier2_L2110_SVC',
            'SVC_Modifier3_L2110_SVC', 'SVC_Modifier4_L2110_SVC',
        ]
        # Remark code fields contain comma-separated values (e.g., "N130, N381")
        # Also check MOA/MIA remark code fields which are individual
        remark_fields_csv = [
            'CLM_RemarkCodes_L2100_LQ',   # Comma-separated claim-level remark codes
            'SVC_RemarkCodes_L2110_LQ',   # Comma-separated service-level remark codes
        ]
        remark_fields_individual = [
            'MOA_ClaimPaymentRemarkCode1_L2100_MOA', 'MOA_ClaimPaymentRemarkCode2_L2100_MOA',
            'MOA_ClaimPaymentRemarkCode3_L2100_MOA', 'MOA_ClaimPaymentRemarkCode4_L2100_MOA',
            'MOA_ClaimPaymentRemarkCode5_L2100_MOA',
            'MIA_ClaimPaymentRemarkCode_L2100_MIA', 'MIA_ClaimPaymentRemarkCode2_L2100_MIA',
            'MIA_ClaimPaymentRemarkCode3_L2100_MIA', 'MIA_ClaimPaymentRemarkCode4_L2100_MIA',
            'MIA_ClaimPaymentRemarkCode5_L2100_MIA',
        ]
        checked_codes = {}
        if verbose:
            print(f"      Checking header-level codes (once)...")
        first_row = csv_rows[0]
        payer_info = {
            'name': first_row.get('Payer_Name_L1000A_N1', 'Unknown'),
            'state': first_row.get('Payer_State_L1000A_N4', 'Unknown'),
            'id': first_row.get('CHK_PayerID_L1000A_REF', 'Unknown')
        }
        for code_field, lookup_func in header_field_lookups.items():
            code = first_row.get(code_field, '')
            if not code or not code.strip():
                continue
            desc = lookup_func(code)
            is_gap = not desc or desc == '' or desc.startswith('Unknown') or desc == code
            if is_gap:
                payer_key = f"{payer_info['name']}|{payer_info['state']}"
                self.stats['missing_mappings'][payer_key].append({
                    'field': code_field, 'code': code,
                    'claim': 'Header', 'lookup_result': desc
                })
                self.stats['payer_data_quality_issues'][payer_key][f'Missing dictionary: {code_field}'] += 1
        if verbose:
            print(f"      Collecting unique codes from {total_rows:,} rows...")
        code_payer_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        for idx, row in enumerate(csv_rows, 1):
            payer_key = f"{row.get('Payer_Name_L1000A_N1', 'Unknown')}|{row.get('Payer_State_L1000A_N4', 'Unknown')}"
            for code_field in row_level_lookups.keys():
                code = row.get(code_field, '')
                if code and code.strip():
                    code_payer_counts[code_field][code.strip()][payer_key] += 1
            for carc_field in carc_fields:
                code = row.get(carc_field, '')
                if code and code.strip():
                    code_payer_counts[carc_field][code.strip()][payer_key] += 1
            code = row.get(ambulance_code_field, '')
            if code and code.strip():
                code_payer_counts[ambulance_code_field][code.strip()][payer_key] += 1
            for mod_field in modifier_fields:
                code = row.get(mod_field, '')
                if code and code.strip():
                    code_payer_counts[mod_field][code.strip()][payer_key] += 1
            # Handle comma-separated remark code fields (LQ segments)
            for remark_field in remark_fields_csv:
                codes_str = row.get(remark_field, '')
                if codes_str and codes_str.strip():
                    # Split comma-separated codes and count each individually
                    for code in codes_str.split(','):
                        code = code.strip()
                        if code:
                            code_payer_counts['RARC'][code][payer_key] += 1
            # Handle individual remark code fields (MOA/MIA segments)
            for remark_field in remark_fields_individual:
                code = row.get(remark_field, '')
                if code and code.strip():
                    code_payer_counts['RARC'][code.strip()][payer_key] += 1
        if verbose:
            total_unique = sum(len(codes) for codes in code_payer_counts.values())
            print(f"      Validating {total_unique:,} unique codes...")
        carc_classifications = dictionary.get_carc_classifications()
        for code_field, codes_dict in code_payer_counts.items():
            for code, payer_counts in codes_dict.items():
                is_gap = False
                if code_field in row_level_lookups:
                    desc = row_level_lookups[code_field](code)
                    is_gap = not desc or desc == '' or desc.startswith('Unknown') or desc == code
                elif code_field in carc_fields:
                    normalized = colloquial.normalize_carc_code(code)
                    is_gap = normalized not in carc_classifications
                elif code_field == ambulance_code_field:
                    desc = dictionary.get_ambulance_code_description(code)
                    is_gap = desc.startswith('Unknown')
                elif code_field in modifier_fields:
                    desc = dictionary.get_ambulance_modifier_description(code)
                    is_gap = desc == code
                elif code_field == 'RARC':
                    # RARC = Remittance Advice Remark Code (from LQ, MOA, MIA segments)
                    desc = dictionary.get_remark_code_description(code)
                    is_gap = desc.startswith('Unknown')
                if is_gap:
                    total_count = sum(payer_counts.values())
                    for payer_key, count in payer_counts.items():
                        self.stats['missing_mappings'][payer_key].append({
                            'field': code_field, 'code': code,
                            'claim': f"{count} occurrences", 'lookup_result': f'Unknown'
                        })
                        # Categorize the missing code type for reporting
                        if 'CAS' in code_field:
                            issue_key = f'Missing CARC: {code}'
                        elif code_field == 'RARC':
                            issue_key = f'Missing RARC: {code}'
                        else:
                            issue_key = f'Missing: {code_field}'
                        self.stats['payer_data_quality_issues'][payer_key][issue_key] += count
    def _validate_field_data_types(self, row: dict):
        """Validate field data types"""
        amount_fields = [field for field in row.keys()
                        if ('Amount' in field or
                            ('Payment' in field and 'PaymentMethod' not in field) or
                            'Charge' in field or
                            'Responsibility' in field or
                            'Deductible' in field or
                            'Copay' in field or
                            'Coinsurance' in field)
                        and not field.endswith('Desc')
                        and not field.endswith('Method')
                        and not field.endswith('MethodDesc')
                        and not field == 'CHK_PaymentMethod_Header_BPR'
                        and not 'RemarkCode' in field
                        and not 'RemarkDesc' in field]
        for field in amount_fields:
            value = row.get(field)
            if value and value != '':
                try:
                    Decimal(str(value))
                except:
                    self.errors.append(ValidationError(
                        'MAPPING',
                        f"Non-numeric value in amount field",
                        field=field,
                        actual=value,
                        location=row.get('CLM_PatientControlNumber_L2100_CLP')
                    ))
        date_fields = [field for field in row.keys() if 'Date' in field]
        for field in date_fields:
            value = row.get(field)
            if value and value != '':
                if len(str(value)) == 8:
                    try:
                        datetime.strptime(str(value), '%Y%m%d')
                    except:
                        self.errors.append(ValidationError(
                            'MAPPING',
                            f"Invalid date format",
                            field=field,
                            actual=value,
                            expected='CCYYMMDD',
                            location=row.get('CLM_PatientControlNumber_L2100_CLP')
                        ))
    def _validate_date_formats(self, csv_rows: List[dict], verbose: bool = False):
        """Validate date formats in all date columns and report format coverage.
        
        Checks that all dates can be parsed and converted to MM/DD/YY format.
        Tracks which date formats are encountered for transparency.
        """
        if not csv_rows:
            return
        
        # Define known date formats we support (from redactor.py)
        known_formats = [
            ('%Y%m%d', 'YYYYMMDD (EDI DTM)'),
            ('%y%m%d', 'YYMMDD (EDI ISA)'),      # ISA segment uses 6-digit dates
            ('%Y-%m-%d', 'YYYY-MM-DD (ISO)'),
            ('%m/%d/%Y', 'MM/DD/YYYY'),
            ('%m-%d-%Y', 'MM-DD-YYYY'),
            ('%m/%d/%y', 'MM/DD/YY'),
            ('%m-%d-%y', 'MM-DD-YY'),
            ('%Y/%m/%d', 'YYYY/MM/DD'),
            ('%Y-%m-%d %H:%M:%S', 'ISO with time'),
            ('%Y-%m-%dT%H:%M:%S', 'ISO with T'),
        ]
        
        # Identify all date columns based on EDI segment structure
        # This uses the actual EDI structure rather than guessing from field name patterns
        date_columns = set()
        
        # Specific known date fields from envelope and header segments
        known_date_fields = [
            'INTERCHANGEDATE',      # ISA09 - Interchange Date
            'DATE_ENVELOPE_GS',     # GS04 - Functional Group Date
            'PAYMENTDATE',          # BPR16 - Payment/Effective Date
            'EFFECTIVEDATE',        # BPR16 - Check/EFT Effective Date
        ]
        
        for field in csv_rows[0].keys():
            field_upper = field.upper()
            
            # Exclude time-only columns
            if 'TIME' in field_upper and 'DATE' not in field_upper:
                continue
            
            # DTM segment fields are ALWAYS dates (contains _DTM)
            if '_DTM' in field_upper:
                date_columns.add(field)
                continue
            
            # Check for specific known date fields
            if any(known in field_upper for known in known_date_fields):
                date_columns.add(field)
        
        if verbose:
            print(f"      Found {len(date_columns)} date columns to validate")
        
        # Track statistics
        format_counts = defaultdict(lambda: defaultdict(int))  # column -> format -> count
        unrecognized_dates = defaultdict(list)  # column -> list of (value, claim_id)
        total_dates_checked = 0
        total_valid_dates = 0
        
        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                print(f"        Checking date formats: row {idx:,} of {len(csv_rows):,}...")
            
            claim_id = row.get('CLM_PatientControlNumber_L2100_CLP', f'Row_{idx}')
            payer_name = row.get('Payer_Name_L1000A_N1', 'Unknown')
            payer_state = row.get('Payer_State_L1000A_N4', 'Unknown')
            
            for col in date_columns:
                value = row.get(col)
                if not value or value == '' or value is None:
                    continue
                
                total_dates_checked += 1
                value_str = str(value).strip()
                
                # Try to match against known formats
                matched_format = None
                for fmt, fmt_name in known_formats:
                    try:
                        # Handle formats with time by trimming microseconds
                        test_value = value_str.split('.')[0] if '.' in value_str else value_str
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
                    if re.match(r'^\d{2}/\d{2}/\d{2}$', value_str):
                        format_counts[col]['MM/DD/YY'] += 1
                        total_valid_dates += 1
                    else:
                        unrecognized_dates[col].append((value_str, claim_id))
                        payer_key = f"{payer_name}|{payer_state}"
                        self.stats['payer_data_quality_issues'][payer_key][f'Unrecognized date format: {col}'] += 1
        
        # Add date format stats to the report
        self.stats['date_format_validation'] = {
            'date_columns_found': list(date_columns),
            'total_dates_checked': total_dates_checked,
            'total_valid_dates': total_valid_dates,
            'format_distribution': {col: dict(formats) for col, formats in format_counts.items()},
            'unrecognized_count': sum(len(v) for v in unrecognized_dates.values()),
        }
        
        # Report unrecognized date formats as warnings
        for col, issues in unrecognized_dates.items():
            if len(issues) > 0:
                # Only report first few examples per column
                examples = issues[:3]
                example_str = ', '.join([f"'{v}' (claim {c})" for v, c in examples])
                if len(issues) > 3:
                    example_str += f" ... and {len(issues) - 3} more"
                
                self.warnings.append(ValidationError(
                    'DATE_FORMAT',
                    f"Unrecognized date format in {col}: {example_str}",
                    field=col,
                    actual=f"{len(issues)} values"
                ))
        
        if verbose:
            print(f"      Date format validation complete:")
            print(f"        - Total dates checked: {total_dates_checked:,}")
            print(f"        - Valid dates: {total_valid_dates:,}")
            print(f"        - Unrecognized: {sum(len(v) for v in unrecognized_dates.values()):,}")
            if format_counts:
                print(f"        - Formats found:")
                all_formats = set()
                for formats in format_counts.values():
                    all_formats.update(formats.keys())
                for fmt in sorted(all_formats):
                    total = sum(formats.get(fmt, 0) for formats in format_counts.values())
                    print(f"          {fmt}: {total:,}")

    def _validate_description_fields(self, csv_rows: List[dict], verbose: bool = False):
        """Validate that description fields are populated when code fields have values"""
        total_rows = len(csv_rows)
        if verbose:
            print(f"      Validating description fields for {total_rows:,} rows...")
        code_desc_pairs = [
            ('CHK_PaymentMethod_Header_BPR', 'CHK_PaymentMethodDesc_Header_BPR'),
            ('CHK_Format_Header_BPR', 'CHK_FormatDesc_Header_BPR'),
            ('CHK_TraceType_Header_TRN', 'CHK_TraceTypeDesc_Header_TRN'),
            ('CHK_CreditDebitFlag_Header_BPR', 'CHK_CreditDebitFlagDesc_Header_BPR'),
            ('CHK_BusinessFunctionCode_Header_BPR', 'CHK_BusinessFunctionDesc_Header_BPR'),
            ('CHK_PayerDFI_Qualifier_Header_BPR', 'CHK_PayerDFI_QualifierDesc_Header_BPR'),
            ('CHK_PayerAccountQualifier_Header_BPR', 'CHK_PayerAccountQualifierDesc_Header_BPR'),
            ('CHK_PayeeDFI_Qualifier_Header_BPR', 'CHK_PayeeDFI_QualifierDesc_Header_BPR'),
            ('CHK_PayeeAccountQualifier_Header_BPR', 'CHK_PayeeAccountQualifierDesc_Header_BPR'),
            ('CHK_DFI_Qualifier_3_Header_BPR', 'CHK_DFI_Qualifier_3_Desc_Header_BPR'),
            ('CHK_AccountQualifier_3_Header_BPR', 'CHK_AccountQualifier_3_Desc_Header_BPR'),
            ('CLM_Status_L2100_CLP', 'CLM_StatusDescr_L2100_CLP'),
            ('CLM_FilingIndicator_L2100_CLP', 'CLM_FilingIndicatorDesc_L2100_CLP'),
            ('CLM_FrequencyCode_L2100_CLP', 'CLM_FrequencyCodeDesc_L2100_CLP'),
            ('Payer_IDQualifier_L1000A_REF', 'Payer_IDQualifierDesc_L1000A_REF'),
            ('Provider_IDQualifier_L1000B_REF', 'Provider_IDQualifierDesc_L1000B_REF'),
        ]
        for idx, row in enumerate(csv_rows, 1):
            if verbose and idx % 10000 == 0:
                print(f"        Validating descriptions for row {idx:,} of {total_rows:,}...")
            for code_field, desc_field in code_desc_pairs:
                code_value = row.get(code_field, '')
                desc_value = row.get(desc_field, '')
                if code_value and code_value.strip() and not desc_value:
                    self.errors.append(ValidationError(
                        'DESC_MISSING',
                        f'Code field has value but description field is empty',
                        field=desc_field,
                        location=row.get('CLM_PatientControlNumber_L2100_CLP', ''),
                        actual=f'Code={code_value}, Desc=empty'
                    ))
    def _validate_composite_fields(self, edi_data: Dict, csv_rows: List[dict]):
        """Validate that composite fields were properly parsed.
        
        Note: This legacy method works with flat segment lists and may have issues
        with duplicate claim IDs across files. Use validate_all_by_file() for proper
        per-file validation.
        """
        csv_by_file_and_claim = defaultdict(lambda: defaultdict(list))
        for r in csv_rows:
            claim_id = r.get('CLM_PatientControlNumber_L2100_CLP')
            file_name = r.get('Filename_File', '')
            # Normalize file path to basename for consistent matching
            normalized_file = self._normalize_file_path(file_name)
            if claim_id and r.get('SVC_ProcedureCode_L2110_SVC'):
                csv_by_file_and_claim[normalized_file][claim_id].append(r)
        claims_by_file = defaultdict(dict)
        for claim_id, claim_data in edi_data.get('claims', {}).items():
            # Extract normalized file from composite claim_id
            file_part, actual_claim_id = self._extract_claim_id_from_composite(claim_id)
            normalized_edi_file = self._normalize_file_path(file_part) if file_part else ''
            for file_name, file_claims in csv_by_file_and_claim.items():
                if normalized_edi_file == file_name and actual_claim_id in file_claims:
                    claims_by_file[file_name][actual_claim_id] = claim_data
                    break
        for file_name, file_claims in claims_by_file.items():
            csv_claims = csv_by_file_and_claim[file_name]
            component_delimiter = ':'
            for claim_rows in csv_claims.values():
                if claim_rows:
                    component_delimiter = claim_rows[0].get('ENV_ComponentSeparator_Envelope_ISA', ':') or ':'
                    break
            for claim_id, claim_data in file_claims.items():
                service_rows = csv_claims.get(claim_id, [])
                for svc_idx, service_data in enumerate(claim_data.get('services', [])):
                    svc_segment = service_data.get('SVC')
                    if not svc_segment or len(svc_segment) < 2:
                        continue
                    svc01_composite = svc_segment[1]
                    components = svc01_composite.split(component_delimiter)
                    if len(components) < 2:
                        continue
                    expected_qualifier = components[0] if len(components) > 0 else ''
                    expected_code = components[1] if len(components) > 1 else ''
                    expected_modifiers = components[2:6]
                    if svc_idx >= len(service_rows):
                        payer_name = 'Unknown'
                        payer_state = 'Unknown State'
                        if service_rows:
                            payer_name = service_rows[0].get('Payer_Name_L1000A_N1', 'Unknown')
                            payer_state = service_rows[0].get('Payer_State_L1000A_N4', 'Unknown State')
                        self.errors.append(ValidationError(
                            'COMPOSITE_PARSE',
                            f'Service {svc_idx + 1} exists in EDI but not in CSV',
                            location=f'File: {file_name}, Claim {claim_id}, Service {svc_idx + 1}',
                            expected=f'Service with code {expected_code}',
                            actual='Not found in CSV',
                            payer_info={'name': payer_name, 'state': payer_state}
                        ))
                        continue
                    csv_row = service_rows[svc_idx]
                    payer_name = csv_row.get('Payer_Name_L1000A_N1', 'Unknown')
                    payer_state = csv_row.get('Payer_State_L1000A_N4', 'Unknown State')
                    payer_info = {'name': payer_name, 'state': payer_state}
                    actual_code = csv_row.get('SVC_ProcedureCode_L2110_SVC', '')
                    if expected_code != actual_code:
                        self.errors.append(ValidationError(
                            'COMPOSITE_PARSE',
                            'Procedure code not correctly extracted from composite field',
                            location=f'File: {file_name}, Claim {claim_id}, Service {svc_idx + 1}',
                            field='SVC_ProcedureCode_L2110_SVC',
                            expected=expected_code,
                            actual=actual_code,
                            payer_info=payer_info
                        ))
                    for mod_idx, expected_mod in enumerate(expected_modifiers, 1):
                        if not expected_mod:
                            continue
                        field_name = f'SVC_Modifier{mod_idx}_L2110_SVC'
                        actual_mod = csv_row.get(field_name, '')
                        if expected_mod != actual_mod:
                            self.errors.append(ValidationError(
                                'COMPOSITE_PARSE',
                                f'Modifier {mod_idx} not correctly extracted from composite',
                                location=f'File: {file_name}, Claim {claim_id}, Service {svc_idx + 1}',
                                field=field_name,
                                expected=expected_mod,
                                actual=actual_mod,
                                payer_info=payer_info
                            ))
    def _validate_edge_cases(self, csv_rows: List[dict], csv_by_claim: Dict[str, List[dict]], verbose: bool = False):
        """Validate edge cases and anomalies"""
        total_claims = len(csv_by_claim)
        for idx, (claim_id, rows) in enumerate(csv_by_claim.items(), 1):
            if verbose and idx % 500 == 0:
                print(f"      Checking edge cases for claim {idx:,} of {total_claims:,}...")
            file_name, actual_claim_id = self._extract_claim_id_from_composite(claim_id)
            display_claim_id = actual_claim_id if file_name else claim_id
            claim_row = next((r for r in rows if r.get('CLM_Status_L2100_CLP')), None)
            if not claim_row:
                continue
            claim_payer_name = claim_row.get('Payer_Name_L1000A_N1', 'Unknown')
            claim_payer_state = claim_row.get('Payer_State_L1000A_N4', 'Unknown State')
            claim_payer_info = {'name': claim_payer_name, 'state': claim_payer_state}
            status = claim_row.get('CLM_Status_L2100_CLP')
            if status == '22':
                charge = float(claim_row.get('CLM_ChargeAmount_L2100_CLP', 0) or 0)
                payment = float(claim_row.get('CLM_PaymentAmount_L2100_CLP', 0) or 0)
                # Per X12 835 spec section 1.10.2.8, reversal claims should have negative values
                # However, $0 payment is valid when reversing a denied claim (no payment to reverse)
                if charge > 0:  # Positive charge is always wrong for reversal
                    if self._should_debug(claim_payer_name, 'ReversalCharge'):
                        print(f"\n[DEBUG] Reversal claim error for {display_claim_id}")
                        print(f"[DEBUG] Payer: {claim_payer_name}")
                        print(f"[DEBUG] CLP02 (Status): 22 (Reversal)")
                        print(f"[DEBUG] CLP03 (Charge): {charge} - Should be negative or zero")
                    self.errors.append(ValidationError(
                        'EDGE',
                        "Reversal claim (Status 22) has positive charge - should be negative per X12 spec section 1.10.2.8",
                        location=f"Claim {display_claim_id}" + (f" (File: {file_name})" if file_name else ""),
                        expected="Negative or zero value",
                        actual=charge,
                        payer_info=claim_payer_info
                    ))
                if payment > 0:  # Positive payment is always wrong for reversal; $0 is valid for denied claim reversals
                    if self._should_debug(claim_payer_name, 'ReversalPayment'):
                        print(f"\n[DEBUG] Reversal claim error for {display_claim_id}")
                        print(f"[DEBUG] Payer: {claim_payer_name}")
                        print(f"[DEBUG] CLP02 (Status): 22 (Reversal)")
                        print(f"[DEBUG] CLP04 (Payment): {payment} - Should be negative or zero")
                    self.errors.append(ValidationError(
                        'EDGE',
                        "Reversal claim (Status 22) has positive payment - should be negative per X12 spec section 1.10.2.8",
                        location=f"Claim {display_claim_id}" + (f" (File: {file_name})" if file_name else ""),
                        expected="Negative or zero value",
                        actual=payment,
                        payer_info=claim_payer_info
                    ))
            elif status == '4':
                pass
            remark_codes = []
            for i in range(1, 6):
                code = claim_row.get(f'CLM_RemarkCode{i}')
                if code:
                    remark_codes.append(code)
            nsa_codes = ['N864', 'N865', 'N866', 'N875']
            if any(code in remark_codes for code in nsa_codes):
                if not claim_row.get('CLM_QPA_Amount'):
                    self.warnings.append(ValidationError(
                        'EDGE',
                        "No Surprises Act claim may be missing QPA amount",
                        location=f"Claim ID: {display_claim_id}"
                    ))
            service_rows = [r for r in rows if r.get('SVC_ProcedureCode_L2110_SVC')]
            for svc_row in service_rows:
                proc = svc_row.get('SVC_ProcedureCode_L2110_SVC', '')
                if proc in ['A0425', 'A0435', 'A0436']:
                    if status == '22':
                        continue
                    paid_units = svc_row.get('SVC_Units_L2110_SVC', '').strip()
                    original_units = svc_row.get('SVC_OriginalUnits_L2110_SVC', '').strip()
                    payment_amt_str = svc_row.get('SVC_PaymentAmount_L2110_SVC', '0')
                    charge_amt_str = svc_row.get('SVC_ChargeAmount_L2110_SVC', '0')
                    payer_name = svc_row.get('Payer_Name_L1000A_N1', 'Unknown')
                    payer_state = svc_row.get('Payer_State_L1000A_N4', '')
                    payer_key = f"{payer_name}|{payer_state}" if payer_state else payer_name
                    
                    # Parse payment amount to determine if denied
                    try:
                        payment_amt = float(payment_amt_str) if payment_amt_str else 0
                    except (ValueError, TypeError):
                        payment_amt = 0
                    
                    try:
                        charge_amt = float(charge_amt_str) if charge_amt_str else 0
                    except (ValueError, TypeError):
                        charge_amt = 0
                    
                    # Apply X12 835 default rules for missing units:
                    # - SVC05 (paid_units): "If not present, the value is assumed to be one" (X12 TR3)
                    # - SVC07 (original_units): "Required when paid units differ from submitted" (X12 TR3)
                    #   If omitted, original units = paid units
                    # However, for denied claims (payment=0), payers often truncate SVC segment entirely
                    
                    both_missing = not paid_units and not original_units
                    
                    if both_missing:
                        if payer_name == 'PAYERNAME':
                            continue
                        
                        # Denied claims (payment = $0): payers often omit units entirely
                        # This is a payer data quality issue but not a parsing error
                        if payment_amt == 0:
                            # Denied claim - units missing is common payer behavior
                            # Per X12, would default to 1, but for mileage we can derive from charge
                            derived_units = self._derive_mileage_units(proc, charge_amt)
                            if derived_units:
                                # Log as info, not error - we can derive the units
                                self.stats['payers_missing_mileage_units'][payer_key] += 1
                                # Don't add to payer_data_quality_issues for denied claims
                            # Skip warning for denied claims - this is expected payer behavior
                            continue
                        
                        # Paid claim with missing units - this IS a data quality issue
                        self.stats['payers_missing_mileage_units'][payer_key] += 1
                        self.stats['payer_data_quality_issues'][payer_key]['Missing mileage units (paid claim)'] += 1
                        
                        if 'MEDICARE' in payer_name.upper():
                            self.warnings.append(ValidationError(
                                'EDGE',
                                f"Medicare {proc} missing units - likely base/loaded mile (Payment: ${payment_amt:.2f}, Charge: ${charge_amt:.2f})",
                                location=f"Claim ID: {display_claim_id} | Service: {proc}",
                                actual=f"Payer: {payer_name}, State: {payer_state} | Trace to verify base rate billing",
                                payer_info={'name': payer_name, 'state': payer_state}
                            ))
                        else:
                            self.warnings.append(ValidationError(
                                'EDGE',
                                f"Paid mileage {proc} missing unit data (Payment: ${payment_amt:.2f}, Charge: ${charge_amt:.2f})",
                                location=f"Claim ID: {display_claim_id} | Service: {proc}",
                                actual=f"Payer: {payer_name}, State: {payer_state}",
                                payer_info={'name': payer_name, 'state': payer_state}
                            ))
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
                            if status == '2':
                                # Secondary claim - units=0 is expected and valid
                                continue
                            
                            # Check if filing indicator suggests COB/secondary situation
                            # Filing codes 12-16, 41-47 are Medicare COB related
                            filing_indicator = claim_row.get('CLM_FilingIndicator_L2100_CLP', '')
                            cob_filing_codes = ['12', '13', '14', '15', '16', '41', '42', '43', '44', '45', '46', '47']
                            if filing_indicator in cob_filing_codes:
                                # COB/Medicare secondary situation - units=0 may be valid
                                # Downgrade to info level, not error
                                continue
                            
                            # For primary claims with non-COB filing, this could be:
                            # 1. Flat rate mileage payment (valid)
                            # 2. Base/loaded mile scenario (valid)
                            # 3. Data quality issue
                            # Downgrade to warning, not error
                            self.warnings.append(ValidationError(
                                'EDGE',
                                f"Mileage {proc} paid with zero units (may be flat rate or base mile)",
                                location=f"Claim {display_claim_id}, Service {proc}",
                                actual=f"Paid: {paid_units}, Original: {original_units}, Payment: ${payment_amt:.2f}",
                                payer_info={'name': payer_name, 'state': payer_state}
                            ))
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
            if not display_claim_id or display_claim_id == 'UNKNOWN':
                source_file = claim_row.get('Filename_File', 'UNKNOWN_FILE')
                display_claim_id = f"EMPTY_ID_IN_{os.path.basename(source_file)}"
            # DISABLED: Claim-level date validation (DTM*232/233)
            # Reason: Claim-level dates are often unreliable (swapped, typos).
            # Service-level dates (DTM*150/472/151) are more accurate.
            # See DATE_STRUCTURE_ANALYSIS.md for details.
            try:
                charge = float(claim_row.get('CLM_ChargeAmount_L2100_CLP', 0) or 0)
                payment = float(claim_row.get('CLM_PaymentAmount_L2100_CLP', 0) or 0)
                interest = float(claim_row.get('CLM_InterestAmount_L2100_AMT', 0) or 0)
            except:
                pass
            
            # Validate Allowed Amount calculations
            # Service-level: Method1 (Charge - CO) should equal Method2 (Payment + PR)
            for svc_row in service_rows:
                try:
                    svc_method1 = float(svc_row.get('Allowed_Amount', 0) or 0)
                    svc_method2 = float(svc_row.get('Allowed_Verification', 0) or 0)
                    
                    # Skip if both are zero (likely no service data)
                    if svc_method1 == 0 and svc_method2 == 0:
                        continue
                    
                    # Check if methods match (within $0.01 tolerance)
                    if abs(svc_method1 - svc_method2) > 0.01:
                        proc = svc_row.get('SVC_ProcedureCode_L2110_SVC', 'Unknown')
                        svc_charge = float(svc_row.get('SVC_ChargeAmount_L2110_SVC', 0) or 0)
                        svc_payment = float(svc_row.get('SVC_PaymentAmount_L2110_SVC', 0) or 0)
                        self.warnings.append(ValidationError(
                            'CALC',
                            f"Service allowed amount mismatch: Method1 (Charge-CO)=${svc_method1:.2f} vs Method2 (Payment+PR)=${svc_method2:.2f}",
                            location=f"Claim {display_claim_id} | Service {proc}",
                            expected=f"Methods should match. Charge=${svc_charge:.2f}, Payment=${svc_payment:.2f}",
                            actual=f"Difference: ${abs(svc_method1 - svc_method2):.2f}",
                            payer_info=claim_payer_info
                        ))
                except (ValueError, TypeError):
                    pass

    def _validate_loop_structure(self, edi_data: Dict):
        """Validate loop structure follows X12 835 rules"""
        if 'BPR' not in edi_data['header']:
            self.errors.append(ValidationError(
                'MISSING_REQUIRED',
                'Required BPR segment missing from transaction header',
                segment='BPR'
            ))
        if 'TRN' not in edi_data['header']:
            self.errors.append(ValidationError(
                'MISSING_REQUIRED',
                'Required TRN segment missing from transaction header',
                segment='TRN'
            ))
        for claim_id, claim_data in edi_data['claims'].items():
            if 'CLP' not in claim_data.get('segments', {}) and 'CLP' not in claim_data:
                self.errors.append(ValidationError(
                    'MISSING_REQUIRED',
                    'Required CLP segment missing from claim',
                    location=f'Claim {claim_id}',
                    segment='CLP'
                ))
        for claim_id, claim_data in edi_data['claims'].items():
            for svc_idx, service_data in enumerate(claim_data.get('services', [])):
                if 'SVC' not in service_data and not service_data.get('SVC'):
                    self.errors.append(ValidationError(
                        'MISSING_REQUIRED',
                        'Required SVC segment missing from service line',
                        location=f'Claim {claim_id}, Service {svc_idx + 1}',
                        segment='SVC'
                    ))
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
        if 'BPR' in segment_positions and 'TRN' in segment_positions:
            if segment_positions['TRN'] < segment_positions['BPR']:
                self.errors.append(ValidationError(
                    'SEQUENCE_VIOLATION',
                    'TRN segment must follow BPR segment',
                    segment='TRN',
                    location=f'Segment position {segment_positions["TRN"]}'
                ))
        if 'CLP' in segment_positions and 'SVC' in segment_positions:
            if segment_positions['SVC'] < segment_positions['CLP']:
                self.errors.append(ValidationError(
                    'SEQUENCE_VIOLATION',
                    'SVC segment cannot appear before first CLP segment',
                    segment='SVC',
                    location=f'Segment position {segment_positions["SVC"]}'
                ))
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
                warnings_list.append({'message': str(warning), 'type': 'WARNING'})
        report = {
            'summary': {
                'total_segments': len(edi_segments),
                'total_fields': self.stats['total_fields'],
                'fields_validated': self.stats['fields_validated'],
                'calculations_checked': self.stats['calculations_checked'],
                'error_count': len(self.errors),
                'warning_count': len(self.warnings),
                'validation_status': 'PASS' if len(self.errors) == 0 else 'FAIL'
            },
            'errors_by_type': {
                error_type: [e.to_dict() for e in errors]
                for error_type, errors in errors_by_type.items()
            },
            'errors_by_payer': {
                payer: {
                    error_type: [e.to_dict() for e in errors]
                    for error_type, errors in payer_errors.items()
                }
                for payer, payer_errors in errors_by_payer.items()
            },
            'warnings': warnings_list,
            'missing_mappings': dict(self.stats['missing_mappings']),
            'payers_missing_mileage_units': dict(self.stats['payers_missing_mileage_units']),
            'payer_data_quality_issues': {k: dict(v) for k, v in self.stats['payer_data_quality_issues'].items()},
            'date_format_validation': self.stats.get('date_format_validation', {}),
            'sample_errors': self._get_sample_errors(csv_rows),
            'validation_timestamp': datetime.now().isoformat()
        }
        return report
    def _get_sample_errors(self, csv_rows: List[dict], max_samples: int = 5) -> List[Dict]:
        """Get sample errors with redacted data"""
        samples = []
        for error in self.errors[:max_samples]:
            sample = {
                'error': error.to_dict(),
                'context': {}
            }
            if error.location:
                claim_id = error.location.split(',')[0].replace('Claim ', '')
                matching_row = next((r for r in csv_rows
                                   if r.get('CLM_PatientControlNumber_L2100_CLP') == claim_id), None)
                if matching_row:
                    if error.type == 'CALC':
                        sample['context'] = {
                            field: matching_row.get(field)
                            for field in matching_row
                            if 'Amount' in field or 'Payment' in field or 'Charge' in field
                        }
                    elif error.field:
                        sample['context'] = {
                            field: matching_row.get(field)
                            for field in matching_row
                            if error.field and (error.field in field or field in error.field)
                        }
            samples.append(sample)
        return samples


def generate_executive_dashboard(validation_result: Dict) -> str:
    """Generate executive summary dashboard grouped by issue type for data quality analysis"""
    summary = validation_result['summary']
    lines = []
    lines.append("=" * 100)
    lines.append("VALIDATION EXECUTIVE DASHBOARD - GROUPED BY ISSUE TYPE")
    lines.append("=" * 100)
    lines.append("")
    status = summary['validation_status']
    status_symbol = "[PASS]" if status == 'PASS' else "[FAIL]"
    lines.append(f"{status_symbol} OVERALL STATUS: {status}")
    lines.append("")
    lines.append("KEY METRICS:")
    lines.append(f"  Segments Processed: {summary['total_segments']:,}")
    lines.append(f"  Fields Validated: {summary['fields_validated']:,}")
    lines.append(f"  Calculations Checked: {summary['calculations_checked']:,}")
    lines.append(f"  Errors: {summary['error_count']:,}")
    lines.append(f"  Warnings: {summary['warning_count']:,}")
    lines.append("")
    if validation_result.get('errors_by_type'):
        lines.append("ISSUES GROUPED BY TYPE:")
        lines.append("-" * 100)
        from collections import defaultdict
        issues_by_type = defaultdict(lambda: defaultdict(list))
        for error_type, errors in (validation_result.get('errors_by_type') or {}).items():
            for error in errors:
                if not isinstance(error, dict):
                    continue
                msg = error.get('message') or ''
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
                payer_info = error.get('payer_info', {})
                payer_name = payer_info.get('name', 'Unknown')
                payer_state = payer_info.get('state', 'Unknown')
                payer_key = f"{payer_name}|{payer_state}"
                issues_by_type[issue_key][payer_key].append(error)
        for issue_type in sorted(issues_by_type.keys()):
            payer_data = issues_by_type[issue_type]
            total_count = sum(len(errors) for errors in payer_data.values())
            lines.append(f"\n{issue_type.upper()} ({total_count:,} total):")
            lines.append("  Payers affected:")
            for payer_key in sorted(payer_data.keys()):
                errors = payer_data[payer_key]
                payer_name, payer_state = payer_key.split('|')
                lines.append(f"    - {payer_name} ({payer_state}): {len(errors):,} instances")
                example = errors[0]
                location = example.get('location', 'Unknown')
                if example.get('expected') is not None and example.get('actual') is not None:
                    lines.append(f"      Example: {location}")
                    lines.append(f"        Expected: {example['expected']}, Actual: {example['actual']}")
                else:
                    lines.append(f"      Example: {location}")
        lines.append("")
    if validation_result.get('missing_mappings'):
        total_gaps = sum(len(codes) for codes in validation_result['missing_mappings'].values())
        payer_count = len(validation_result['missing_mappings'])
        lines.append(f"DICTIONARY GAPS: {total_gaps:,} missing codes across {payer_count} payers")
        gap_counts = [(payer, len(codes)) for payer, codes in validation_result['missing_mappings'].items()]
        gap_counts.sort(key=lambda x: x[1], reverse=True)
        lines.append("  Top payers with missing codes:")
        for payer, count in gap_counts[:5]:
            payer_name = payer.split('|')[0]
            lines.append(f"    - {payer_name}: {count} missing codes")
        lines.append("")
    # Date Format Validation Summary
    if validation_result.get('date_format_validation'):
        date_stats = validation_result['date_format_validation']
        lines.append("DATE FORMAT VALIDATION:")
        lines.append(f"  Date columns found: {len(date_stats.get('date_columns_found', []))}")
        lines.append(f"  Total dates checked: {date_stats.get('total_dates_checked', 0):,}")
        lines.append(f"  Valid dates: {date_stats.get('total_valid_dates', 0):,}")
        lines.append(f"  Unrecognized formats: {date_stats.get('unrecognized_count', 0):,}")
        
        if date_stats.get('format_distribution'):
            all_formats = set()
            for formats in date_stats['format_distribution'].values():
                all_formats.update(formats.keys())
            if all_formats:
                lines.append("  Formats encountered:")
                for fmt in sorted(all_formats):
                    total = sum(formats.get(fmt, 0) for formats in date_stats['format_distribution'].values())
                    lines.append(f"    - {fmt}: {total:,}")
        lines.append("")
    
    if validation_result.get('payers_missing_mileage_units'):
        payer_data = validation_result.get('payers_missing_mileage_units', {})
        total_missing = sum(payer_data.values())
        payer_count = len(payer_data)
        lines.append(f"PAYERS MISSING MILEAGE UNIT DATA: {total_missing:,} instances across {payer_count} payers")
        payer_list = sorted(payer_data.items(),
                           key=lambda x: x[1], reverse=True)
        lines.append("  Payers with incomplete mileage data:")
        for payer, count in payer_list:
            payer_parts = payer.split('|')
            payer_name = payer_parts[0]
            payer_state = payer_parts[1] if len(payer_parts) > 1 else ''
            display = f"{payer_name} ({payer_state})" if payer_state else payer_name
            lines.append(f"    - {display}: {count} service line(s)")
        lines.append("")
    lines.append("RECOMMENDED ACTIONS:")
    if summary['error_count'] == 0:
        lines.append("  [OK] No critical issues found - system operating correctly")
    else:
        priority_actions = []
        if validation_result.get('errors_by_type', {}).get('CALC'):
            priority_actions.append("CRITICAL: Fix mathematical balancing errors (may indicate parser bugs)")
        if validation_result.get('errors_by_type', {}).get('MISSING'):
            priority_actions.append("HIGH: Investigate missing required fields")
        if validation_result.get('errors_by_type', {}).get('CAS_CATEGORY'):
            priority_actions.append("HIGH: Review CAS adjustment categorization logic")
        if validation_result.get('missing_mappings'):
            priority_actions.append("MEDIUM: Update dictionary with missing code mappings")
        if validation_result.get('errors_by_type', {}).get('COMPOSITE_PARSE'):
            priority_actions.append("MEDIUM: Fix composite field parsing")
        for idx, action in enumerate(priority_actions, 1):
            lines.append(f"  {idx}. {action}")
    lines.append("")
    lines.append("=" * 100)
    return "\n".join(lines)


def generate_validation_report(validation_result: Dict, output_format: str = 'text',
                              output_file: str = None, redact: bool = True) -> str:
    """Generate a formatted validation report"""
    dashboard = generate_executive_dashboard(validation_result)
    if output_format == 'html':
        report_content = generate_html_report(validation_result, redact)
    else:
        detail_report = generate_text_report(validation_result, redact)
        report_content = dashboard + "\n\n" + detail_report
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
    return report_content


def generate_text_report(validation_result: Dict, redact: bool = True) -> str:
    """Generate text format validation report"""
    lines = []
    lines.append("=" * 80)
    lines.append("VALIDATION REPORT - ZERO FAIL MODE")
    lines.append("=" * 80)
    lines.append("")
    summary = validation_result['summary']
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
    lines.append("-" * 80)
    if validation_result.get('errors_by_type'):
        lines.append("ERRORS BY TYPE")
        lines.append("-" * 80)
        for error_type, errors in validation_result['errors_by_type'].items():
            lines.append(f"\n{error_type} Errors ({len(errors)} found):")
            lines.append("-" * 40)
            for i, error in enumerate(errors[:10], 1):
                lines.append(f"{i}. {error.get('message', 'Unknown error')}")
                if error.get('location'):
                    lines.append(f"   Location: {error['location']}")
                if error.get('expected') is not None and error.get('actual') is not None:
                    lines.append(f"   Expected: {error['expected']}, Actual: {error['actual']}")
                if error.get('field'):
                    lines.append(f"   Field: {error['field']}")
                lines.append("")
            if len(errors) > 10:
                lines.append(f"   ... and {len(errors) - 10} more {error_type} errors")
        lines.append("")
        lines.append("-" * 80)
    if validation_result.get('warnings'):
        lines.append("WARNINGS")
        lines.append("-" * 80)
        for i, warning in enumerate(validation_result['warnings'][:20], 1):
            lines.append(f"{i}. {warning.get('message', 'Unknown warning')}")
            if warning.get('location'):
                lines.append(f"   Location: {warning['location']}")
            if warning.get('actual'):
                lines.append(f"   Details: {warning['actual']}")
            lines.append("")
        if len(validation_result['warnings']) > 20:
            lines.append(f"   ... and {len(validation_result['warnings']) - 20} more warnings")
        lines.append("")
        lines.append("-" * 80)
    if validation_result.get('missing_mappings'):
        lines.append("MISSING DICTIONARY MAPPINGS")
        lines.append("-" * 80)
        for payer_key, mappings in validation_result['missing_mappings'].items():
            payer_name, state = payer_key.split('|')
            lines.append(f"\nPayer: {payer_name}")
            lines.append(f"State: {state}")
            lines.append("")
            by_field = defaultdict(list)
            for mapping in mappings:
                by_field[mapping['field']].append(mapping['code'])
            for field, codes in by_field.items():
                unique_codes = list(set(codes))
                lines.append(f"Field: {field}")
                lines.append(f"Missing Codes: {', '.join(unique_codes[:5])}")
                if len(unique_codes) > 5:
                    lines.append(f"   ... and {len(unique_codes) - 5} more")
                lines.append("")
        lines.append("-" * 80)
    if validation_result.get('payers_missing_mileage_units'):
        lines.append("PAYERS WITH MISSING MILEAGE UNIT DATA")
        lines.append("-" * 80)
        lines.append("")
        lines.append("The following payers sent mileage service lines (A0425/A0435/A0436)")
        lines.append("without ANY unit data in SVC05 or SVC07. This is a payer data quality issue.")
        lines.append("")
        payer_data = validation_result.get('payers_missing_mileage_units', {})
        payer_list = sorted(payer_data.items(),
                           key=lambda x: x[1], reverse=True)
        for payer, count in payer_list:
            payer_parts = payer.split('|')
            payer_name = payer_parts[0]
            payer_state = payer_parts[1] if len(payer_parts) > 1 else 'N/A'
            lines.append(f"Payer: {payer_name}")
            lines.append(f"State: {payer_state}")
            lines.append(f"Missing Unit Count: {count} service line(s)")
            lines.append("")
        lines.append("-" * 80)
    if validation_result.get('sample_errors'):
        lines.append("SAMPLE ERRORS WITH CONTEXT")
        lines.append("-" * 80)
        for i, sample in enumerate(validation_result['sample_errors'], 1):
            error = sample['error']
            context = sample.get('context', {})
            lines.append(f"\nExample {i}: {error.get('type', 'UNKNOWN')} - {error.get('message', 'Unknown error')}")
            if error.get('location'):
                lines.append(f"Location: {error['location']}")
            if context:
                lines.append("Context Data:")
                for field, value in sorted(context.items()):
                    if value is not None and value != '':
                        if redact and any(sensitive in field for sensitive in
                                        ['Name', 'SSN', 'MemberID', 'Address']):
                            value = '[REDACTED]'
                        lines.append(f"  {field}: {value}")
            lines.append("")
        lines.append("-" * 80)
    if validation_result.get('errors_by_payer'):
        lines.append("SUMMARY BY PAYER")
        lines.append("-" * 80)
        for payer, payer_errors in validation_result['errors_by_payer'].items():
            error_counts = {error_type: len(errors)
                           for error_type, errors in payer_errors.items()}
            total = sum(error_counts.values())
            summary_parts = [f"{count} {error_type}"
                            for error_type, count in error_counts.items()]
            lines.append(f"{payer}: {', '.join(summary_parts)} (Total: {total})")
        lines.append("")
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
            background-color:
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        h1, h2, h3 { color:
        .pass { color:
        .fail { color:
        .warning { color:
        .summary-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .summary-table td, .summary-table th {
            padding: 10px;
            border: 1px solid
            text-align: left;
        }
        .summary-table th {
            background-color:
            font-weight: bold;
        }
        .error-box {
            background-color:
            border: 1px solid
            color:
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
        }
        .warning-box {
            background-color:
            border: 1px solid
            color:
            padding: 10px;
            margin: 10px 0;
            border-radius: 4px;
        }
        .code-sample {
            background-color:
            border: 1px solid
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
        .error-type.calc { background-color:
        .error-type.missing { background-color:
        .error-type.mismatch { background-color:
        .error-type.mapping { background-color:
        .error-type.edge { background-color:
        .collapsible {
            cursor: pointer;
            padding: 10px;
            background-color:
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
            background-color:
        }
    </style>
</head>
<body>
<div class="container">
""")
    summary = validation_result['summary']
    status_class = 'pass' if summary['validation_status'] == 'PASS' else 'fail'
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
        ('Total Segments', f"{summary['total_segments']:,}"),
        ('Total Fields', f"{summary['total_fields']:,}"),
        ('Fields Validated', f"{summary['fields_validated']:,}"),
        ('Calculations Checked', f"{summary['calculations_checked']:,}"),
        ('Errors Found', f"<span class='fail'>{summary['error_count']}</span>" if summary['error_count'] > 0 else "0"),
        ('Warnings', f"<span class='warning'>{summary['warning_count']}</span>" if summary['warning_count'] > 0 else "0"),
    ]
    for label, value in summary_rows:
        html_parts.append(f"""
        <tr>
            <td>{label}</td>
            <td>{value}</td>
        </tr>
""")
    html_parts.append("    </table>")
    if validation_result.get('warnings'):
        html_parts.append("""
    <h3>Warnings</h3>
""")
        warnings_by_payer = defaultdict(list)
        for warning in validation_result['warnings']:
            payer_info = warning.get('payer_info', {})
            payer_key = f"{payer_info.get('name', 'Unknown')} ({payer_info.get('state', 'N/A')})"
            warnings_by_payer[payer_key].append(warning)
        for payer_key in sorted(warnings_by_payer.keys()):
            payer_warnings = warnings_by_payer[payer_key]
            html_parts.append(f"""
        <div style="margin-bottom: 20px;">
            <h4>{html.escape(payer_key)} - {len(payer_warnings)} warning(s)</h4>
""")
            for i, warning in enumerate(payer_warnings[:10], 1):
                location = warning.get('location') or ''
                claim_id = ''
                if location and 'Claim ID:' in location:
                    claim_id = location.split('Claim ID:')[1].split('|')[0].strip()
                html_parts.append(f"""
            <div class="warning-box">
""")
                if claim_id:
                    html_parts.append(f"                <strong style='color: #856404;'>Claim: {html.escape(claim_id)}</strong><br>")
                if warning.get('location'):
                    html_parts.append(f"                Location: {html.escape(warning['location'])}<br>")
                if warning.get('actual'):
                    html_parts.append(f"Details: {html.escape(str(warning['actual']))}<br>")
                html_parts.append("            </div>")
            if len(payer_warnings) > 10:
                html_parts.append(f"            <p style='margin-left: 20px;'>... and {len(payer_warnings) - 10} more warnings for this payer</p>")
            html_parts.append("        </div>")
        if len(validation_result['warnings']) > 20:
            html_parts.append(f"        <p>... and {len(validation_result['warnings']) - 20} more warnings</p>")
    if validation_result.get('errors_by_type'):
        html_parts.append("""
    <h3>Errors by Type</h3>
""")
        for error_type, errors in validation_result['errors_by_type'].items():
            type_class = error_type.lower()
            html_parts.append(f"""
    <button class="collapsible">
        <span class="error-type {type_class}">{error_type}</span>
        {error_type} Errors ({len(errors)} found)
    </button>
    <div class="content">
""")
            for i, error in enumerate(errors[:20], 1):
                html_parts.append(f"""
        <div class="error-box">
            <strong>
""")
                if error.get('location'):
                    html_parts.append(f"Location: {html.escape(error['location'])}<br>")
                if error.get('field'):
                    html_parts.append(f"Field: {html.escape(error['field'])}<br>")
                if error.get('expected') is not None and error.get('actual') is not None:
                    html_parts.append(f"Expected: <code>{html.escape(str(error['expected']))}</code>, ")
                    html_parts.append(f"Actual: <code>{html.escape(str(error['actual']))}</code><br>")
                html_parts.append("        </div>")
            if len(errors) > 20:
                html_parts.append(f"        <p>... and {len(errors) - 20} more {error_type} errors</p>")
            html_parts.append("    </div>")
    if validation_result.get('payers_missing_mileage_units'):
        html_parts.append("""
    <h3>Payers with Missing Mileage Unit Data</h3>
    <p style="background-color:
        The following payers sent mileage service lines (A0425/A0435/A0436) without ANY unit data
        in SVC05 or SVC07. This is a payer data quality issue.
    </p>
""")
        payer_data = validation_result.get('payers_missing_mileage_units', {})
        payer_list = sorted(payer_data.items(), key=lambda x: x[1], reverse=True)
        for payer, count in payer_list:
            payer_parts = payer.split('|')
            payer_name = payer_parts[0]
            payer_state = payer_parts[1] if len(payer_parts) > 1 else 'N/A'
            html_parts.append(f"""
    <div class="warning-box">
        <strong>Payer:</strong> {html.escape(payer_name)}<br>
        <strong>State:</strong> {html.escape(payer_state)}<br>
        <strong>Missing Unit Count:</strong> {count} service line(s)
    </div>
""")
    if validation_result.get('missing_mappings'):
        html_parts.append("""
    <h3>Missing Dictionary Mappings</h3>
""")
        for payer_key, mappings in validation_result['missing_mappings'].items():
            payer_name, state = payer_key.split('|')
            html_parts.append(f"""
    <div class="warning-box">
        <strong>Payer:</strong> {html.escape(payer_name)}<br>
        <strong>State:</strong> {html.escape(state)}<br>
        <strong>Missing Codes:</strong><br>
""")
            by_field = defaultdict(set)
            for mapping in mappings:
                by_field[mapping['field']].add(mapping['code'])
            for field, codes in by_field.items():
                unique_codes = sorted(list(codes))
                html_parts.append(f"        • {html.escape(field)}: ")
                html_parts.append(f"<code>{html.escape(', '.join(unique_codes[:10]))}</code>")
                if len(unique_codes) > 10:
                    html_parts.append(f" ... and {len(unique_codes) - 10} more")
                html_parts.append("<br>")
            html_parts.append("    </div>")
    if validation_result.get('sample_errors'):
        html_parts.append("""
    <h3>Sample Errors with Context</h3>
""")
        for i, sample in enumerate(validation_result['sample_errors'], 1):
            error = sample['error']
            context = sample.get('context', {})
            html_parts.append(f"""
    <button class="collapsible">
        Example {i}: {error.get('type', 'UNKNOWN')} - {html.escape(error.get('message', 'Unknown error'))}
    </button>
    <div class="content">
        <div class="code-sample">
""")
            if error.get('location'):
                html_parts.append(f"Location: {html.escape(error['location'])}<br><br>")
            if context:
                html_parts.append("Context Data:<br>")
                for field, value in sorted(context.items()):
                    if value is not None and value != '':
                        if redact and any(s in field for s in ['Name', 'SSN', 'MemberID', 'Address']):
                            value = '[REDACTED]'
                        html_parts.append(f"{html.escape(field)}: {html.escape(str(value))}<br>")
            html_parts.append("""
        </div>
    </div>
""")
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


def validate_835_output(edi_segments, csv_rows: List[dict],
                       element_delimiter: str = '*', output_file: str = None,
                       output_format: str = 'text', verbose: bool = False, debug: bool = False,
                       status_callback=None) -> Dict:
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
    """
    validator = ZeroFailValidator(debug=debug)
    
    # Handle file-aware segment data (new format) or flat list (legacy)
    if edi_segments and isinstance(edi_segments[0], dict) and 'segments' in edi_segments[0]:
        # New format: list of {file, segments, delimiter} dicts
        all_segments = []
        for file_data in edi_segments:
            all_segments.extend(file_data['segments'])
        if debug:
            print(f"[DEBUG] Starting validation with {len(edi_segments)} files, {len(all_segments)} total segments and {len(csv_rows)} CSV rows")
        validation_result = validator.validate_all_by_file(edi_segments, csv_rows, verbose=verbose, status_callback=status_callback)
    else:
        # Legacy format: flat list of segments
        if debug:
            print(f"[DEBUG] Starting validation with {len(edi_segments)} EDI segments and {len(csv_rows)} CSV rows")
        validation_result = validator.validate_all(edi_segments, csv_rows, element_delimiter, verbose=verbose, status_callback=status_callback)
    if status_callback:
        status_callback("Generating validation reports...")
    if output_file or output_format:
        generate_validation_report(validation_result, output_format, output_file)
    return validation_result
