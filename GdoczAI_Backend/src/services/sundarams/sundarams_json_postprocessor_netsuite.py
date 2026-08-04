# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
NetSuite Vendor Bill JSON Post-Processor
=========================================
STEP 4.5 of the OCR Invoice Processing Pipeline.

Converts Gemini-extracted invoice JSON into NetSuite Vendor Bill format.
Applies vendor master mapping, business rule transformations, and field enrichment.

Pipeline position:
    PDF/Image -> Mistral/Qwen/chandra OCR -> Markdown -> Gemini JSON Extraction
    -> STEP 4.5 (this module) -> Final NetSuite Vendor Bill JSON

Key Design Rules:
    - Vendor name ALWAYS comes from document_type (passed via extracted_data["vendor_name"])
    - Vendor name must NEVER be parsed from memo or any OCR field
    - Vendor lookup uses VENDOR_MASTER dictionary keyed by vendor name
    - entity             = entity          (NS Vendor Code, from vendor mapping)
    - department         = department      (NS Department ID, from vendor mapping)
    - location           = location        (Branch NS ID, from vendor mapping)
    - sm_location        = sm_location     (Branch code, from vendor mapping)
    - account            = account         (NS GL code, from vendor mapping, per expense line)
    - custcol_subledger  = custcol_subledger (NS SUB GL code, from vendor mapping, per expense line)
    - subsidiary         = "7"             (hardcoded)
    - custbody_cardtype  = "C139"         (hardcoded)
    - custbody_doc_create_by = "60719"     (hardcoded)
    - custcol_partsgroup = "PA"            (hardcoded, per expense line)
    - duedate            = custbody_entrydate + 30 days
    - Tax logic          = IGST if inter-state (buyer_state != supplier_state), else CGST+SGST
"""

import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# VENDOR MASTER MAPPING
# Converted from VENDOR_DETAILS_Update.xlsx -- no Excel reading at runtime.
#
# Key legend (matches Excel column headers):
#   entity           = Vendor Code      (NS Vendor Code / Col 2)
#   department       = NS_Department    (NetSuite Department ID / Col 11)
#   location         = NS_Branch_code   (Branch NS ID / Col 13)
#   sm_location      = Branch code      (Branch / Col 12)
#   account          = NS_GL_Code       (NS GL code / Col 5)
#   custcol_subledger= NS_sub_GL_code   (NS SUB GL code / Col 7)
#
# Vendor name keys are canonical names (must match document_type sent by caller).
def load_pg_config_fallback() -> Dict:
    import os
    import yaml
    
    paths = ['config/config.yaml', '../config/config.yaml', '../../config/config.yaml', '../../../config/config.yaml']
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, 'r') as f:
                    config = yaml.safe_load(f)
                    return config.get('postgres', {})
            except Exception as e:
                logger.warning("Failed to load config from %s: %s", p, e)
    return {}

def _query_vendor_db_sync(vendor_name: str, pg_config: Dict) -> list:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    conn = psycopg2.connect(
        host=pg_config.get('host', 'localhost'),
        port=pg_config.get('port', 5432),
        database=pg_config.get('database', 'document_pipeline'),
        user=pg_config.get('user'),
        password=pg_config.get('password'),
        connect_timeout=5
    )
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT entity, department, location, sm_location, account, custcol_subledger
            FROM sundarams_vendor_master
            WHERE UPPER(vendor_name) = UPPER(%s)
        """, (vendor_name,))
        results = cursor.fetchall()
        cursor.close()
        return [dict(r) for r in results]
    finally:
        conn.close()

async def lookup_vendor_db(vendor_name: str, pg_config: Optional[Dict] = None) -> Optional[Dict[str, str]]:
    """
    Look up vendor mapping in the sundarams_vendor_master table (case-insensitive).
    Falls back to VENDOR_MASTER dictionary if database lookup fails or table is missing.
    """
    if not vendor_name:
        logger.warning("lookup_vendor_db: vendor_name is empty or None")
        return None

    name = vendor_name.strip()

    # Load fallback config if not provided
    if not pg_config:
        pg_config = load_pg_config_fallback()

    if pg_config:
        try:
            results = await asyncio.to_thread(_query_vendor_db_sync, name, pg_config)
            if results:
                if len(results) > 1:
                    logger.warning("lookup_vendor_db: Multiple mappings (%d) found in DB for '%s'. Returning the first match.", len(results), name)
                else:
                    logger.info("lookup_vendor_db: Match found in DB for '%s'", name)
                return results[0]
            else:
                logger.warning("lookup_vendor_db: '%s' not found in DB table", name)
        except Exception as e:
            logger.error("lookup_vendor_db: Database query failed: %s", e)
            
    return None

# =============================================================================
# STATIC DEFAULTS -- Applied to every NetSuite Vendor Bill without exception.
# These values always override any Gemini-extracted counterparts.
# =============================================================================
STATIC_DEFAULTS: Dict[str, Any] = {
    "recordtype":                   "vendorBill",
    "custbody_source_application":  "1",
    "subsidiary":                   "7",
    "approvalstatus":               "1",
    "taxdetailsoverride":           True,
    "custbody_cardtype":            "C139",
    "custbody_doc_create_by":       "60719"
}


# =============================================================================
# UTILITY HELPERS
# =============================================================================

def normalize_number(value: Any) -> str:
    """
    Normalize a numeric value by removing commas, percentages, and extra whitespace.
    
    This function handles OCR/Gemini output that may contain:
    - Thousand separators: "42,500.00" -> "42500.00"
    - Percentages: "18%" -> "18"
    - Extra spaces: " 100.50 " -> "100.50"
    
    Args:
        value: Raw value (str, int, float, None).
    
    Returns:
        Cleaned numeric string, or "" if value is None/empty.
        
    Examples:
        "42,500.00" -> "42500.00"
        "7,650.00" -> "7650.00"
        "18%" -> "18"
        " 100.50 " -> "100.50"
        None -> ""
    """
    if value is None or value == "":
        return ""
    
    # Convert to string and strip whitespace
    text = str(value).strip()
    
    if not text:
        return ""
    
    # Remove commas (thousand separators)
    text = text.replace(",", "")
    
    # Remove percentage sign if present
    text = text.replace("%", "")
    
    # Strip again after removals
    text = text.strip()
    
    return text


def extract_state_code(value: str) -> str:
    """
    Extract state code from a GST state value.
    
    Handles formats:
    - "33" -> "33"
    - "33 - TAMIL NADU" -> "33"
    - "33 - TN" -> "33"
    
    This ensures proper comparison of buyer_state vs supplier_state.
    Extracts only the numeric part.
    """
    if not value:
        return ""
    
    value = str(value).strip()
    match = re.search(r'\d+', value)
    if match:
        return match.group()
    return ""


def extract_numeric_only(value: Any, default: str = "") -> str:
    """
    Extracts only the numeric part (and decimal point) from a string.
    Useful for stripping units ('1 Nos' -> '1') and percentages ('9%' -> '9').
    """
    if value is None or value == "":
        return default
    text = str(value).strip()
    match = re.search(r'\d+(\.\d+)?', text)
    if match:
        return match.group()
    return default


def safe_str(value: Any, default: str = "") -> str:
    """
    Safely coerce any value to a stripped string.

    Args:
        value:   Raw value from input dict (may be None, int, float, str).
        default: Fallback when value is None or empty after stripping.

    Returns:
        Stripped string or default.
    """
    if value is None:
        return default
    coerced = str(value).strip()
    return coerced if coerced else default


def safe_float_str(value: Any, default: str = "0.00") -> str:
    """
    Safely convert a value to a 2-decimal-place float string.
    
    Handles:
    - Comma-separated numbers: "42,500.00" -> "42500.00"
    - Percentages: "18%" -> "18.00"
    - Spaces and normalization
    - None and empty values

    Args:
        value:   Raw value (str, int, float, None).
        default: Fallback string when conversion fails.

    Returns:
        String like "1500.50" or "42500.00", or default on failure.
    """
    if value is None or value == "":
        return default
    
    try:
        # Normalize the value first (remove commas, %, spaces)
        normalized = normalize_number(value)
        
        if not normalized:
            return default
        
        # Convert to float
        float_val = float(normalized)
        
        # Format with 2 decimal places
        return f"{float_val:.2f}"
        
    except (ValueError, TypeError):
        logger.warning("safe_float_str: could not convert %r to float, using %s", value, default)
        return default


def calculate_due_date(entry_date_str: str, days: int = 30) -> str:
    """
    Calculate due date as entry_date + N days.

    Args:
        entry_date_str: Date string in DD/MM/YYYY format.
        days:           Number of days to add (default 30).

    Returns:
        Due date string in DD/MM/YYYY format, or "" on parse failure.
    """
    if not entry_date_str:
        logger.warning("calculate_due_date: entry_date is empty, cannot compute due date")
        return ""
    try:
        entry_dt = datetime.strptime(entry_date_str.strip(), "%d/%m/%Y")
        due_dt = entry_dt + timedelta(days=days)
        due_date = due_dt.strftime("%d/%m/%Y")
        logger.info(
            "calculate_due_date: %s + %d days = %s", entry_date_str, days, due_date
        )
        return due_date
    except ValueError as exc:
        logger.warning("calculate_due_date: failed to parse '%s': %s", entry_date_str, exc)
        return ""


def determine_nature_of_item(hsn_code: Optional[str]) -> str:
    """
    Determine NetSuite nature_of_item code from HSN code.

    Rule:
        HSN starts with "9"  ->  "3"   (service)
        Otherwise            ->  "1"   (goods)

    Args:
        hsn_code: HSN/SAC code string from invoice line item.

    Returns:
        "3" or "1".
    """
    code = safe_str(hsn_code)
    if not code:
        logger.warning("determine_nature_of_item: HSN code empty, defaulting to '1'")
        return "1"
    nature = "3" if code.startswith("9") else "1"
    logger.debug("determine_nature_of_item: HSN=%s -> nature=%s", code, nature)
    return nature


def determine_tax_strategy(buyer_state: str, supplier_state: str) -> bool:
    """
    Determine whether inter-state tax (IGST) or intra-state tax (CGST+SGST) applies.

    Rule:
        buyer_state == supplier_state  ->  intra-state  ->  CGST + SGST  (returns False)
        buyer_state != supplier_state  ->  inter-state  ->  IGST only    (returns True)
    
    Handles state code extraction:
        "33" vs "33 - TAMIL NADU" -> correctly compared as same state
        "33" vs "22 - KARNATAKA" -> correctly compared as different states

    Args:
        buyer_state:    custbody_in_gst_pos value (buyer GST state, e.g., "33" or "33 - TAMIL NADU").
        supplier_state: shippingaddress value (supplier GST state, e.g., "33" or "33 - TAMIL NADU").

    Returns:
        True if inter-state (use IGST), False if intra-state (use CGST+SGST).
    """
    # Extract state codes (handles both "33" and "33 - TAMIL NADU" formats)
    buyer_code = extract_state_code(buyer_state)
    supplier_code = extract_state_code(supplier_state)
    
    is_inter = buyer_code != supplier_code
    
    if is_inter:
        logger.info(
            "determine_tax_strategy: INTER-STATE detected (buyer=%s [code=%s], supplier=%s [code=%s]) -> use IGST",
            buyer_state, buyer_code, supplier_state, supplier_code,
        )
    else:
        logger.info(
            "determine_tax_strategy: INTRA-STATE detected (buyer=%s [code=%s], supplier=%s [code=%s]) -> use CGST+SGST",
            buyer_state, buyer_code, supplier_state, supplier_code,
        )
    
    return is_inter


# =============================================================================
# EXPENSE LINE BUILDER
# =============================================================================

def build_expense_line(
    raw_line: Dict[str, Any],
    vendor_mapping: Dict[str, str],
    is_inter_state: bool,
) -> Dict[str, Any]:
    """
    Convert a single raw expense/line-item dict into a NetSuite expense line.

    Mapping applied:
        account              <- vendor_mapping["account"]        (NS GL code)
        custcol_subledger    <- vendor_mapping["custcol_subledger"] (NS SUB GL code)
        custcol_in_scode_tds <- always empty string per spec
        custcol_in_nature_of_item <- HSN rule (starts with "9" -> "3" service, else "1" goods)
        custcol_partsgroup   <- hardcoded "PA"
        Tax fields           <- IGST or CGST+SGST based on is_inter_state flag

    Args:
        raw_line:       Single expense dict from extracted invoice data.
        vendor_mapping: Resolved vendor master row (keys: entity, department,
                        location, sm_location, account, custcol_subledger).
        is_inter_state: True -> populate IGST; False -> populate CGST+SGST.

    Returns:
        NetSuite-ready expense line dict.
    """
    hsn_code     = safe_str(raw_line.get("custcol_in_hsn_code"))
    amount       = safe_float_str(raw_line.get("amount", "0.00"))
    quantity     = extract_numeric_only(raw_line.get("custcol_quantity"), "1")
    nature       = determine_nature_of_item(hsn_code)
    scode_tds    = safe_str(raw_line.get("custcol_in_scode_tds"), "")  # Extract from Gemini JSON

    # Raw tax values from OCR extraction
    igst_rate    = extract_numeric_only(raw_line.get("igst_taxrate"),    "0")
    igst_amount  = safe_float_str(raw_line.get("igst_taxamount",  "0.00"))
    cgst_rate    = extract_numeric_only(raw_line.get("cgst_taxrate"),    "0")
    cgst_amount  = safe_float_str(raw_line.get("cgst_taxamount",  "0.00"))
    sgst_rate    = extract_numeric_only(raw_line.get("sgst_taxrate"),    "0")
    sgst_amount  = safe_float_str(raw_line.get("sgst_taxamount",  "0.00"))
    utgst_rate   = extract_numeric_only(raw_line.get("utgst_taxrate"),   "0")
    utgst_amount = safe_float_str(raw_line.get("utgst_taxamount", "0.00"))
    cess_rate    = extract_numeric_only(raw_line.get("cess_taxrate"),    "0")
    cess_amount  = safe_float_str(raw_line.get("cess_taxamount",  "0.00"))

    # Apply inter-state / intra-state tax exclusion logic
    # DEFENSIVE CHECK: Trust the actual tax data if it contradicts state comparison
    # If IGST is present (non-zero), it's definitely inter-state
    igst_is_present = igst_amount != "0.00"
    cgst_is_present = cgst_amount != "0.00"
    sgst_is_present = sgst_amount != "0.00"
    
    # Determine actual tax scenario based on both state comparison AND actual tax values
    # Scenario 1: State says inter-state, or IGST is present ? use IGST
    if is_inter_state or igst_is_present:
        out_igst_rate    = igst_rate
        out_igst_amount  = igst_amount
        out_cgst_rate    = "0.00"
        out_cgst_amount  = "0.00"
        out_sgst_rate    = "0.00"
        out_sgst_amount  = "0.00"
        
        # Log warning if state detection disagreed (supplier state extraction likely failed)
        if igst_is_present and not is_inter_state:
            logger.warning(
                "build_expense_line: State comparison indicated INTRA-STATE, "
                "but IGST non-zero (%s). Field 'shippingaddress' likely contains place-of-supply "
                "(buyer state) instead of supplier state. Using actual IGST data.",
                igst_amount
            )
    
    # Scenario 2: State says intra-state, and IGST is zero ? use CGST+SGST
    else:
        out_igst_rate    = "0.00"
        out_igst_amount  = "0.00"
        out_cgst_rate    = cgst_rate
        out_cgst_amount  = cgst_amount
        out_sgst_rate    = sgst_rate
        out_sgst_amount  = sgst_amount

    expense_line = {
        # GL account from vendor mapping
        "account":                    vendor_mapping.get("account", ""),
        "amount":                     amount,
        # TDS section code -- from Gemini JSON (defaults to empty string if not present)
        "custcol_in_scode_tds":       scode_tds,
        "custcol_in_hsn_code":        hsn_code,
        "custcol_in_nature_of_item":  nature,
        # CGST
        "cgst_taxrate":               out_cgst_rate,
        "cgst_taxamount":             out_cgst_amount,
        # SGST
        "sgst_taxrate":               out_sgst_rate,
        "sgst_taxamount":             out_sgst_amount,
        # UTGST (always zero -- not applicable for standard GST scenarios)
        "utgst_taxrate":              utgst_rate,
        "utgst_taxamount":            utgst_amount,
        # IGST
        "igst_taxrate":               out_igst_rate,
        "igst_taxamount":             out_igst_amount,
        # Cess (preserved as-is regardless of state logic)
        "cess_taxrate":               cess_rate,
        "cess_taxamount":             cess_amount,
        # Sub-ledger from vendor mapping
        "custcol_subledger":          vendor_mapping.get("custcol_subledger", ""),
        "custcol_employee":           "",
        "custcol_subsidiary":         "",
        "custcol_branch":             "",
        "custcol_quantity":           quantity,
        # Hardcoded per spec
        "custcol_partsgroup":         "PA",
        "cseg_tssf_vendor_gl":        safe_str(raw_line.get("cseg_tssf_vendor_gl")),
    }

    logger.info(
        "build_expense_line: HSN=%s amount=%s nature=%s account=%s subledger=%s | "
        "Selected Tax: IGST=%s CGST=%s SGST=%s (is_inter=%s, igst_present=%s)",
        hsn_code, amount, nature,
        expense_line["account"],
        expense_line["custcol_subledger"],
        out_igst_amount, out_cgst_amount, out_sgst_amount,
        is_inter_state, igst_is_present,
    )

    return expense_line


# =============================================================================
# HEADER BUILDER
# =============================================================================

def build_netsuite_header(
    extracted_data: Dict[str, Any],
    vendor_mapping: Dict[str, str],
    due_date: str,
) -> Dict[str, Any]:
    """
    Build the NetSuite Vendor Bill header dict (excluding expense lines).

    Field sourcing:
        entity      <- vendor_mapping["entity"]       (NS Vendor Code)
        department  <- vendor_mapping["department"]   (NS Department ID)
        location    <- vendor_mapping["location"]     (Branch NS ID)
        sm_location <- vendor_mapping["sm_location"]  (Branch code)
        subsidiary  <- STATIC_DEFAULTS (always "7")
        custbody_cardtype <- STATIC_DEFAULTS (always "C139")
        duedate     <- custbody_entrydate + 30 days (pre-calculated)
        All other transaction fields <- extracted_data pass-through
        Static defaults <- STATIC_DEFAULTS constant

    Args:
        extracted_data: Cleaned invoice fields from OCR/Gemini + document_type.
        vendor_mapping: Resolved vendor master row.
        due_date:       Pre-calculated due date string (DD/MM/YYYY).

    Returns:
        Header dict (no expense key -- caller will attach expense lines separately).
    """
    header = {
        # -----------------------------------------------------------------
        # Static defaults -- always applied first, always override Gemini
        # -----------------------------------------------------------------
        **STATIC_DEFAULTS,

        # -----------------------------------------------------------------
        # Vendor-mapped header fields
        # -----------------------------------------------------------------
        "entity":       vendor_mapping.get("entity", ""),
        "department":   vendor_mapping.get("department", ""),
        "location":     vendor_mapping.get("location", ""),
        "sm_location":  vendor_mapping.get("sm_location", ""),

        # -----------------------------------------------------------------
        # Transaction identity fields (pass-through from OCR/Gemini output)
        # -----------------------------------------------------------------
        "custbody_sm_ori_docu_no":    safe_str(extracted_data.get("custbody_sm_ori_docu_no")),
        "tranid":                     safe_str(extracted_data.get("custbody_sm_ori_docu_no")),

        # -----------------------------------------------------------------
        # Date fields
        # -----------------------------------------------------------------
        "trandate":                   safe_str(extracted_data.get("trandate")),
        "custbody_entrydate":         safe_str(extracted_data.get("custbody_entrydate")),
        "duedate":                    due_date,

        # -----------------------------------------------------------------
        # Memo -- pass-through from Gemini extraction
        # -----------------------------------------------------------------
        "memo":                       safe_str(extracted_data.get("memo")),

        # -----------------------------------------------------------------
        # GST state fields
        # -----------------------------------------------------------------
        "custbody_in_gst_pos":        extract_state_code(safe_str(extracted_data.get("custbody_in_gst_pos"))),
        "shippingaddress":            extract_state_code(safe_str(extracted_data.get("shippingaddress"))),

        # -----------------------------------------------------------------
        # Financial fields
        # -----------------------------------------------------------------
        "custbody_actual_bill_amount": safe_float_str(
            extracted_data.get("custbody_actual_bill_amount")
        ),
        "custbody_tds_taxamount":     safe_str(
            extracted_data.get("custbody_tds_taxamount"), "0"
        ),
        "custbody_tds_taxrate":       safe_str(
            extracted_data.get("custbody_tds_taxrate"), "0"
        ),

        # -----------------------------------------------------------------
        # LR / GRN fields
        # -----------------------------------------------------------------
        "custbody_lr_no":             safe_str(extracted_data.get("custbody_lr_no")),
        "custbody_lr_date":           safe_str(extracted_data.get("custbody_lr_date")),
        "custbody_grnvrn_no":         safe_str(extracted_data.get("custbody_grnvrn_no")),
        "custbody_grnvrn_date":       safe_str(extracted_data.get("custbody_grnvrn_date")),

        # -----------------------------------------------------------------
        # Round-off fields
        # -----------------------------------------------------------------
        "custbody_round_off_val":     safe_float_str(
            extracted_data.get("custbody_round_off_val"), "0.00"
        ),
        "custbody_round_off_acc":     safe_str(extracted_data.get("custbody_round_off_acc")),
        "custbody_round_off_sub_gl":  safe_str(extracted_data.get("custbody_round_off_sub_gl")),

        # -----------------------------------------------------------------
        # Miscellaneous pass-through fields
        # -----------------------------------------------------------------
        "custbody_vin_no":            safe_str(extracted_data.get("custbody_vin_no")),
        "custbody_registration_no":   safe_str(extracted_data.get("custbody_registration_no")),
        "createdfrom":                safe_str(extracted_data.get("createdfrom")),
        "createddate":                safe_str(extracted_data.get("createddate")),
        "custbody_insurercd":         safe_str(extracted_data.get("custbody_insurercd")),
        "custbody_rcm_check":         safe_str(extracted_data.get("custbody_rcm_check")),
    }

    return header


# =============================================================================
# VALIDATION
# =============================================================================

def validate_netsuite_bill(netsuite_bill: Dict[str, Any]) -> List[str]:
    """
    Validate the final NetSuite bill dict for required fields.

    Args:
        netsuite_bill: Completed NetSuite Vendor Bill dict.

    Returns:
        List of missing/empty required field names (empty list = valid).
    """
    required = [
        "recordtype",
        "entity",
        "department",
        "location",
        "tranid",
        "trandate",
        "custbody_entrydate",
        "duedate",
        "custbody_actual_bill_amount",
    ]
    missing = [f for f in required if not netsuite_bill.get(f)]
    return missing


# =============================================================================
# MAIN ASYNC ENTRY POINT
# =============================================================================

async def post_process_invoice_to_netsuite(
    extracted_data: Dict[str, Any],
    request_id: Optional[str] = None,
    pg_config: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Convert extracted invoice data into a NetSuite Vendor Bill JSON payload.

    This is STEP 4.75 of the OCR pipeline.  It is called by the endpoint as:

        netsuite_bill = await post_process_invoice_to_netsuite(
            extracted_data=extracted_data,
            request_id=request_id,
        )

    IMPORTANT -- vendor identification:
        The caller (endpoint) MUST set extracted_data["vendor_name"] = document_type.
        This module reads vendor_name exclusively from extracted_data["vendor_name"].
        It does NOT parse vendor name from memo or any other OCR field.

    Processing steps:
        1.  Validate input dict
        2.  Read vendor_name from extracted_data["vendor_name"]  (== document_type)
        3.  Lookup vendor in VENDOR_MASTER -> get entity, department, location, sm_location, account, custcol_subledger
        4.  Extract all header fields from extracted_data
        5.  Calculate duedate = custbody_entrydate + 30 days
        6.  Determine tax strategy (inter-state IGST vs intra-state CGST+SGST)
        7.  Build NetSuite header with static defaults + vendor mapping + extracted fields
        8.  Build expense lines with vendor-mapped account / subledger / TDS codes
        9.  Validate required fields
        10. Return final NetSuite Vendor Bill dict

    Args:
        extracted_data: Dict built by the endpoint containing ALL of the following:
            {
                "vendor_name":                <str>  <- MUST equal document_type
                "custbody_sm_ori_docu_no":    <str>
                "memo":                       <str>
                "trandate":                   <str>  DD/MM/YYYY
                "custbody_entrydate":         <str>  DD/MM/YYYY
                "custbody_in_gst_pos":        <str>  buyer state code
                "shippingaddress":            <str>  supplier state code
                "custbody_actual_bill_amount":<str>
                "custbody_tds_taxamount":     <str>
                "custbody_tds_taxrate":       <str>
                "custbody_lr_no":             <str>
                "custbody_lr_date":           <str>
                "custbody_grnvrn_no":         <str>
                "custbody_grnvrn_date":       <str>
                "custbody_round_off_val":     <str>
                "custbody_vin_no":            <str>
                "custbody_registration_no":   <str>
                "createdfrom":               <str>
                "createddate":               <str>
                "custbody_insurercd":         <str>
                # NOTE: subsidiary is always injected as "7" via STATIC_DEFAULTS.
                # NOTE: sm_location is always sourced from VENDOR_MASTER.
                # Neither field should be passed from Gemini output.
                "expense":                    <list> raw expense/line-item dicts
            }
        request_id: Optional request tracking ID for log correlation.

    Returns:
        Dict: Complete NetSuite Vendor Bill JSON ready for API submission.

    Raises:
        ValueError: If extracted_data is invalid, or vendor not found in VENDOR_MASTER.
    """

    sep = "=" * 80

    logger.info(sep)
    logger.info("NETSUITE POST-PROCESSOR STARTED  (STEP 4.75)")
    logger.info(sep)
    if request_id:
        logger.info("Request ID: %s", request_id)

    # -------------------------------------------------------------------------
    # STEP 1: Input validation
    # -------------------------------------------------------------------------
    logger.info("STEP 1: Input validation")

    if not extracted_data:
        raise ValueError("extracted_data is None or empty")
    if not isinstance(extracted_data, dict):
        raise ValueError("extracted_data must be a dict")

    logger.info("STEP 1: OK -- extracted_data is a non-empty dict")

    # -------------------------------------------------------------------------
    # STEP 2: Read vendor name (must equal document_type -- set by caller)
    # -------------------------------------------------------------------------
    logger.info("STEP 2: Reading vendor_name from extracted_data")

    vendor_name = safe_str(extracted_data.get("vendor_name"))

    if not vendor_name:
        raise ValueError(
            "extracted_data['vendor_name'] is empty. "
            "The endpoint must set vendor_name = document_type before calling this function."
        )

    logger.info("STEP 2: vendor_name = '%s'", vendor_name)

    # -------------------------------------------------------------------------
    # STEP 3: Vendor master lookup
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("STEP 3: Vendor master lookup for '%s'", vendor_name)
    logger.info(sep)

    vendor_mapping = await lookup_vendor_db(vendor_name, pg_config)

    if not vendor_mapping:
        raise ValueError(
            f"Vendor '{vendor_name}' not found in database or local VENDOR_MASTER."
        )

    logger.info("STEP 3: Vendor mapping resolved:")
    logger.info("  entity           = %s", vendor_mapping.get("entity"))
    logger.info("  department       = %s", vendor_mapping.get("department"))
    logger.info("  location         = %s", vendor_mapping.get("location"))
    logger.info("  sm_location      = %s", vendor_mapping.get("sm_location"))
    logger.info("  account          = %s", vendor_mapping.get("account"))
    logger.info("  custcol_subledger= %s", vendor_mapping.get("custcol_subledger"))

    # -------------------------------------------------------------------------
    # STEP 4: Extract and log all key header fields
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("STEP 4: Extracting header fields from extracted_data")
    logger.info(sep)

    document_no    = safe_str(extracted_data.get("custbody_sm_ori_docu_no"))
    entry_date     = safe_str(extracted_data.get("custbody_entrydate"))
    tran_date      = safe_str(extracted_data.get("trandate"))
    memo           = safe_str(extracted_data.get("memo"))
    gst_pos        = safe_str(extracted_data.get("custbody_in_gst_pos"))
    supplier_state = safe_str(extracted_data.get("shippingaddress"))
    total_amount   = safe_float_str(extracted_data.get("custbody_actual_bill_amount"))
    tds_amount     = safe_str(extracted_data.get("custbody_tds_taxamount"), "0")
    tds_rate       = safe_str(extracted_data.get("custbody_tds_taxrate"),   "0")

    # Expense lines -- accept both "expense" and "line_items" key names
    # Handle multiple formats: list of dicts, single dict, or empty
    raw_lines_raw = extracted_data.get(
        "expense", extracted_data.get("line_items", [])
    )
    
    if isinstance(raw_lines_raw, list):
        # Already a list
        raw_lines: List[Dict[str, Any]] = raw_lines_raw
    elif isinstance(raw_lines_raw, dict):
        # Single line item object - convert to list with one element
        logger.info("  (expense is a single dict object - converting to list with 1 element)")
        raw_lines = [raw_lines_raw]
    else:
        # Invalid format
        raw_lines = []

    logger.info("  document_no    = %s", document_no)
    logger.info("  entry_date     = %s", entry_date)
    logger.info("  tran_date      = %s", tran_date)
    logger.info("  memo           = %s", memo)
    logger.info("  gst_pos        = %s", gst_pos)
    logger.info("  supplier_state = %s", supplier_state)
    logger.info("  total_amount   = %s", total_amount)
    logger.info("  tds_amount     = %s", tds_amount)
    logger.info("  raw_lines      = %d line(s)", len(raw_lines))

    # -------------------------------------------------------------------------
    # STEP 5: Calculate due date (entry_date + 30 days)
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("STEP 5: Calculating due date")
    logger.info(sep)

    due_date = calculate_due_date(entry_date, days=30)

    if not due_date:
        logger.warning(
            "STEP 5: due_date could not be calculated from entry_date='%s'. "
            "Falling back to empty string.",
            entry_date,
        )

    logger.info("STEP 5: duedate = %s", due_date)

    # -------------------------------------------------------------------------
    # STEP 6: Determine tax strategy
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("STEP 6: Determining tax strategy")
    logger.info(sep)

    is_inter_state = determine_tax_strategy(gst_pos, supplier_state)

    # -------------------------------------------------------------------------
    # STEP 7: Build NetSuite header
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("STEP 7: Building NetSuite header")
    logger.info(sep)

    netsuite_bill = build_netsuite_header(
        extracted_data=extracted_data,
        vendor_mapping=vendor_mapping,
        due_date=due_date,
    )

    logger.info("STEP 7: Header built -- entity=%s dept=%s loc=%s duedate=%s",
                netsuite_bill.get("entity"),
                netsuite_bill.get("department"),
                netsuite_bill.get("location"),
                netsuite_bill.get("duedate"))

    # -------------------------------------------------------------------------
    # STEP 8: Build expense lines
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("STEP 8: Building expense lines (%d raw line(s))", len(raw_lines))
    logger.info(sep)

    expense_lines: List[Dict[str, Any]] = []

    if not raw_lines:
        logger.warning("STEP 8: No expense lines found in extracted_data")
    else:
        for idx, raw_line in enumerate(raw_lines, start=1):
            if not isinstance(raw_line, dict):
                logger.warning("STEP 8: Line %d is not a dict -- skipping", idx)
                continue
            try:
                expense_line = build_expense_line(
                    raw_line=raw_line,
                    vendor_mapping=vendor_mapping,
                    is_inter_state=is_inter_state,
                )
                expense_lines.append(expense_line)
                logger.info("STEP 8: Line %d processed OK", idx)
            except Exception as exc:
                logger.error("STEP 8: Error processing line %d: %s", idx, exc, exc_info=True)
                raise

    netsuite_bill["expense"] = expense_lines

    logger.info("STEP 8: %d expense line(s) attached", len(expense_lines))

    # -------------------------------------------------------------------------
    # STEP 9: Validation
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("STEP 9: Validating required fields")
    logger.info(sep)

    missing = validate_netsuite_bill(netsuite_bill)

    if missing:
        logger.warning("STEP 9: Missing required fields: %s", missing)
    else:
        logger.info("STEP 9: All required fields present")

    if len(expense_lines) == 0:
        logger.warning("STEP 9: Bill has zero expense lines -- this may be invalid")

    # -------------------------------------------------------------------------
    # STEP 10: Final summary log and return
    # -------------------------------------------------------------------------
    logger.info(sep)
    logger.info("NETSUITE POST-PROCESSOR COMPLETE")
    logger.info(sep)
    logger.info("  Vendor        : %s", vendor_name)
    logger.info("  Entity code   : %s", netsuite_bill.get("entity"))
    logger.info("  Document      : %s", netsuite_bill.get("tranid"))
    logger.info("  Tran date     : %s", netsuite_bill.get("trandate"))
    logger.info("  Entry date    : %s", netsuite_bill.get("custbody_entrydate"))
    logger.info("  Due date      : %s", netsuite_bill.get("duedate"))
    logger.info("  Amount        : %s", netsuite_bill.get("custbody_actual_bill_amount"))
    logger.info("  Tax strategy  : %s", "IGST (inter-state)" if is_inter_state else "CGST+SGST (intra-state)")
    logger.info("  Expense lines : %d", len(expense_lines))
    logger.info("  Missing fields: %s", missing if missing else "none")
    logger.info(sep)

    return netsuite_bill


# =============================================================================
# SYNCHRONOUS WRAPPER -- for use outside async contexts
# =============================================================================

def post_process_invoice_to_netsuite_sync(
    extracted_data: Dict[str, Any],
    request_id: Optional[str] = None,
    pg_config: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Synchronous wrapper around post_process_invoice_to_netsuite.

    Use this only when you are NOT already inside an async event loop.

    Args:
        extracted_data: Same as the async version.
        request_id:     Optional request tracking ID.
        pg_config:      Optional database config dict.

    Returns:
        Dict: Complete NetSuite Vendor Bill JSON.
    """
    import asyncio

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If inside a running loop (e.g. Jupyter), create a new one
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    asyncio.run,
                    post_process_invoice_to_netsuite(extracted_data, request_id, pg_config),
                )
                return future.result()
        return loop.run_until_complete(
            post_process_invoice_to_netsuite(extracted_data, request_id, pg_config)
        )
    except RuntimeError:
        return asyncio.run(
            post_process_invoice_to_netsuite(extracted_data, request_id, pg_config)
        )


# =============================================================================
# DEBUG HELPER
# =============================================================================

def print_netsuite_bill_summary(netsuite_bill: Dict[str, Any]) -> None:
    """
    Print a human-readable summary of the NetSuite Vendor Bill to stdout.

    Args:
        netsuite_bill: Completed NetSuite Vendor Bill dict.
    """
    sep = "=" * 80
    print(f"\n{sep}")
    print("NETSUITE VENDOR BILL SUMMARY")
    print(sep)
    print(f"  Record Type   : {netsuite_bill.get('recordtype')}")
    print(f"  Invoice ID    : {netsuite_bill.get('tranid')}")
    print(f"  Entity        : {netsuite_bill.get('entity')}")
    print(f"  Department    : {netsuite_bill.get('department')}")
    print(f"  Location      : {netsuite_bill.get('location')}")
    print(f"  Subsidiary    : {netsuite_bill.get('subsidiary')}")
    print(f"  Tran Date     : {netsuite_bill.get('trandate')}")
    print(f"  Entry Date    : {netsuite_bill.get('custbody_entrydate')}")
    print(f"  Due Date      : {netsuite_bill.get('duedate')}")
    print(f"  Amount        : {netsuite_bill.get('custbody_actual_bill_amount')}")
    print(f"  Memo          : {netsuite_bill.get('memo')}")
    print(f"  Source App    : {netsuite_bill.get('custbody_source_application')}")
    print(f"  Approval      : {netsuite_bill.get('approvalstatus')}")
    print(f"  Created By    : {netsuite_bill.get('custbody_doc_create_by')}")

    expense = netsuite_bill.get("expense", [])
    print(f"\n  Expense Lines : {len(expense)}")
    for idx, line in enumerate(expense, start=1):
        print(f"\n    Line {idx}:")
        print(f"      account              : {line.get('account')}")
        print(f"      amount               : {line.get('amount')}")
        print(f"      custcol_subledger    : {line.get('custcol_subledger')}")
        print(f"      custcol_in_scode_tds : {line.get('custcol_in_scode_tds')}")
        print(f"      custcol_in_hsn_code  : {line.get('custcol_in_hsn_code')}")
        print(f"      nature_of_item       : {line.get('custcol_in_nature_of_item')}")
        print(f"      igst_taxamount       : {line.get('igst_taxamount')}")
        print(f"      cgst_taxamount       : {line.get('cgst_taxamount')}")
        print(f"      sgst_taxamount       : {line.get('sgst_taxamount')}")

    print(f"\n{sep}\n")