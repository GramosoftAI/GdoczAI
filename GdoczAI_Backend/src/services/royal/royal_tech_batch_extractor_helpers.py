# -*- coding: utf-8 -*-
"""
royal_tech_batch_extractor_helpers.py ? Module-level helpers for STEP 5.

Contains (imported by royal_tech_batch_extractor.py):
  ? Schema field lists  (_HEADER_GEMINI_FIELDS, _ITEM_GEMINI_FIELDS)
    ? populated dynamically from the DB schema passed per-request
  ? Prompt template constants and builders
  ? Gemini HTTP caller (_call_gemini)
  ? JSON repair (_extract_json via RobustJSONParser)
  ? Schema injection helpers (null/default fields)
  ? Numeric coercion (_coerce_numeric_fields)
  ? Cross-validation (_cross_validate)

All functions are pure / stateless ? they read cfg but hold no instance state.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

import requests

from src.services.royal.royal_tech_config import cfg
from src.services.royal.royal_tech_batch_manager import BatchSlice
from src.services.royal.royal_tech_identifier_extractor import IdentifierRecord
from src.services.royal.royal_tech_json_parser import RobustJSONParser as _RobustJSONParser

logger = logging.getLogger(__name__)


# ============================================================================
# System instruction (used by token-tracker monkey-patch in processor)
# ============================================================================

_SYSTEM_INSTRUCTION = """\
You are a precise structured-data extraction engine for commercial invoices.
You output ONLY valid JSON. No prose. No explanation. No markdown fences.\
"""


# ============================================================================
# Default schema field lists
# These are the FALLBACK field definitions used when the DB schema does not
# provide explicit header_fields / item_fields overrides.
# royal_tech_batch_extractor.py replaces these at runtime with DB-loaded values.
# ============================================================================

DEFAULT_HEADER_GEMINI_FIELDS: list[tuple[str, str]] = [
    ("BuyerAdd1",          "Bill-to party address line 1 (below buyer name, capital letters only)"),
    ("BuyerAdd2",          "Bill-to party address line 2"),
    ("BuyerAdd3",          "Bill-to party address line 3"),
    ("BuyerName",          "Bill-to buyer name ? capital letters only"),
    ("Client",             "Exporter name"),
    ("ConsigneeAdd1",      "Consignee / Ship-to party address line 1"),
    ("ConsigneeAdd2",      "Consignee / Ship-to party address line 2"),
    ("ConsigneeAdd3",      "Consignee / Ship-to party address line 3"),
    ("ConsigneeAdd4",      "Consignee / Ship-to party address line 4, or null if absent"),
    ("ConsigneeName",      "Consignee / Ship-to party name"),
    ("CountryOfDischarge", "Country of Final Destination, e.g. USA"),
    ("Currency",           "Currency code from Rate/Amount column bracket, e.g. USD"),
    ("ExporterName",       "Exporter name from Exporter field"),
    ("InvoiceDate",        "Invoice date from top header"),
    ("InvoiceNo",          "Invoice number from top header"),
    ("InvoiceValue",       "Total invoice value from footer"),
    ("PaymentPeriod",      "Numeric days only from payment terms, e.g. 60"),
    ("PortOfDischarge",    "Port of Discharge from top header"),
    ("Terms",              "Terms of Delivery from top header"),
    ("TotalCarton",        "Total number of pallets from footer ? numeric only"),
    ("TotalGrossWeight",   "Total gross weight from footer"),
    ("TotalNetWeight",     "Total net weight from footer"),
]

DEFAULT_ITEM_GEMINI_FIELDS: list[tuple[str, str]] = [
    ("Amount",        "Amount column value for this line item ? numeric"),
    ("HSNCode",       "HSN code for this material_id from examination report / HSN table"),
    ("IGSTAmount",    "IGST Amount for this material_id from Annexure table"),
    ("IGSTRate",      "IGST Rate for this material_id from Annexure table"),
    ("ItemDesc",      "Description of Goods for this line item from the invoice table (the text below the part number / material_id, e.g. 'Hydraulic Trailer Brake Kit, FIK, TRAILE'). Never return null."),
    ("ItemQTYCode",   "Unit of measure string from Qty column, e.g. PC, NOS, KG ? not numeric"),
    ("Itemslno",      "Sequential line item number ? integer"),
    ("NetWeight",     "Net weight for this material_id from footer/packing table; sum if multiple rows share the same material_id"),
    ("Quantity",      "Quantity for this line item from Qty column ? numeric string"),
    ("Rate",          "Unit rate/price for this line item ? numeric"),
    ("TaxableAmount", "Assessable value for this material_id from Annexure table"),
]


# ============================================================================
# Prompt template
# ============================================================================

_BATCH_PROMPT_TEMPLATE = """\
You are extracting structured data from a multi-page commercial invoice (converted to Markdown).
Page boundaries are marked:  ---PAGE <number>---

?????????????????????????????????????????????????????
BATCH SCOPE  (Batch {batch_number} of {total_batches})
?????????????????????????????????????????????????????
You must extract data for EXACTLY the following {batch_size} line item(s).
Identifier format:  [serial] <composite_identifier>

{identifier_block}

CRITICAL SCOPING RULES:
  ? Extract line items ONLY for the identifiers listed above.
  ? Do NOT extract any item not listed.
  ? Do NOT duplicate items.
  ? If a field value is not explicitly present in the document, return null.
  ? Do NOT infer, guess, or hallucinate any value.

?????????????????????????????????????????????????????
CROSS-PAGE LINKING RULES
?????????????????????????????????????????????????????
This is a cross-page invoice. For each line item search the ENTIRE document:
  ? ItemDesc       ? description text printed below the part number in the main invoice table (e.g. "Hydraulic Trailer Brake Kit"). Always populate; never null.
  ? HSNCode        ? may be in an Examination Report or HSN Summary table on any page
  ? IGSTAmount     ? from the Annexure table, matched by material_id
  ? IGSTRate       ? from the Annexure table, matched by material_id
  ? TaxableAmount  ? from Annexure Assessable Value column, matched by material_id
  ? NetWeight      ? from footer or packing list; SUM all weights if material_id appears multiple times

Match cross-page data by material_id / part number exactly. Do NOT match by description or row position.

?????????????????????????????????????????????????????
HEADER EXTRACTION RULES
?????????????????????????????????????????????????????
{header_instruction}

?????????????????????????????????????????????????????
FIELD-LEVEL RULES
?????????????????????????????????????????????????????
Header fields to extract:
{header_field_spec}

Line item fields to extract per item:
{item_field_spec}

General rules:
  ? String values: return as plain string, no surrounding quotes in value.
  ? Numeric fields (Amount, Rate, Quantity, Itemslno, NetWeight, TaxableAmount,
    IGSTAmount, IGSTRate): return as JSON number, not a string.
    Strip currency symbols (? $ ? ?) and commas before returning.
  ? PaymentPeriod: extract only the numeric day count (e.g. "60" from "60 Days Net").
  ? ItemQTYCode: unit string only (PC, NOS, KG, etc.) ? NOT the numeric quantity.
  ? TotalCarton: numeric digits only, no text.
  ? If a value is not found: return JSON null (not the string "null", not "").
  ? Do NOT add fields not listed in the schema.

?????????????????????????????????????????????????????
REQUIRED OUTPUT FORMAT ? STRICT JSON
?????????????????????????????????????????????????????
Return ONLY this JSON object. No markdown fences. No commentary.

{{
  "header": {{
{header_json_skeleton}
  }},
  "container_details": {{
{container_json_skeleton}
  }},
  "line_items": [
    {{
{item_json_skeleton}
    }}
  ]
}}

One object in "line_items" per identifier listed in BATCH SCOPE above.
Preserve the same order as the identifier list.

?????????????????????????????????????????????????????
FULL DOCUMENT MARKDOWN FOLLOWS
?????????????????????????????????????????????????????
{full_markdown}
"""

_HEADER_INSTRUCTION_BATCH0 = """\
Extract all header fields listed below from the document.
Header data is typically on page 1 or the first invoice page.\
"""

_HEADER_INSTRUCTION_SUBSEQUENT = """\
For this batch, extract header fields anyway ? they will be discarded in favour
of Batch 1's header during merging, but must still be structurally present.\
"""

# Always-null container field names (Document 2 schema)
_CONTAINER_ALWAYS_NULL_FIELDS: tuple[str, ...] = (
    "ContainerNo", "ContainerSealDate", "ContainerSealNo",
    "ContainerSize", "Containerslno",
)


# ============================================================================
# Prompt builders
# ============================================================================

def build_header_field_spec(
    header_fields: list[tuple[str, str]],
) -> str:
    return "\n".join(f"  {name}: {desc}" for name, desc in header_fields)


def build_item_field_spec(
    item_fields: list[tuple[str, str]],
) -> str:
    return "\n".join(f"  {name}: {desc}" for name, desc in item_fields)


def build_header_json_skeleton(
    header_fields: list[tuple[str, str]],
) -> str:
    return ",\n".join(f'    "{name}": null' for name, _ in header_fields)


def build_item_json_skeleton(
    item_fields: list[tuple[str, str]],
) -> str:
    return ",\n".join(f'      "{name}": null' for name, _ in item_fields)


def build_container_json_skeleton() -> str:
    return ",\n".join(f'    "{f}": null' for f in _CONTAINER_ALWAYS_NULL_FIELDS)


def build_identifier_block(prompt_lines: list[str]) -> str:
    return "\n".join(prompt_lines)


def build_full_prompt(
    batch: BatchSlice,
    full_markdown: str,
    total_batches: int,
    header_fields: list[tuple[str, str]],
    item_fields: list[tuple[str, str]],
) -> str:
    """Build the complete Gemini extraction prompt for one BatchSlice."""
    header_instruction = (
        _HEADER_INSTRUCTION_BATCH0
        if batch.index == 0
        else _HEADER_INSTRUCTION_SUBSEQUENT
    )
    return _BATCH_PROMPT_TEMPLATE.format(
        batch_number=batch.batch_number,
        total_batches=total_batches,
        batch_size=batch.size,
        identifier_block=build_identifier_block(batch.prompt_lines),
        header_instruction=header_instruction,
        header_field_spec=build_header_field_spec(header_fields),
        item_field_spec=build_item_field_spec(item_fields),
        header_json_skeleton=build_header_json_skeleton(header_fields),
        container_json_skeleton=build_container_json_skeleton(),
        item_json_skeleton=build_item_json_skeleton(item_fields),
        full_markdown=full_markdown,
    )


# ============================================================================
# Gemini HTTP caller
# ============================================================================

def _build_gemini_url() -> str:
    gcfg = cfg.gemini
    return f"{gcfg.api_base_url}/{gcfg.model}:generateContent?key={gcfg.api_key}"


def _call_gemini(prompt: str, max_output_tokens: int) -> Optional[str]:
    """Send prompt to Gemini and return the text response, or None on failure."""
    gcfg = cfg.gemini
    url  = _build_gemini_url()

    payload = {
        "system_instruction": {"parts": [{"text": _SYSTEM_INSTRUCTION}]},
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature":      gcfg.temperature,
            "topP":             gcfg.top_p,
            "topK":             gcfg.top_k,
            "maxOutputTokens":  max_output_tokens,
            "responseMimeType": "application/json",
        },
    }
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            url, headers=headers, json=payload, timeout=gcfg.timeout
        )
    except requests.exceptions.Timeout:
        logger.error("BatchExtractor: Gemini timeout after %ds", gcfg.timeout)
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("BatchExtractor: Gemini request failed ? %s", exc)
        return None

    if response.status_code != 200:
        logger.error(
            "BatchExtractor: Gemini HTTP %d ? %s",
            response.status_code, response.text[:400],
        )
        return None

    try:
        data = response.json()
        text = (
            data
            .get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        return text.strip() if text else None
    except (KeyError, IndexError, ValueError) as exc:
        logger.error("BatchExtractor: Failed to parse Gemini response ? %s", exc)
        return None


# ============================================================================
# JSON repair ? delegates to RobustJSONParser (5-strategy parser)
# ============================================================================

def extract_json(raw: str) -> Optional[dict]:
    """Parse (possibly malformed) Gemini JSON response into a dict."""
    if not raw:
        return None
    result = _RobustJSONParser.clean_and_parse(raw)
    if isinstance(result, dict) and result.get("status") == "error":
        logger.warning(
            "BatchExtractor: Cannot parse JSON from response: %s", raw[:300]
        )
        return None
    return _RobustJSONParser.recursive_clean_values(result)


# ============================================================================
# Schema injection helpers
# ============================================================================

def inject_null_and_default_header_fields(
    header: dict,
    schema: dict,
) -> dict:
    """
    Merge always-null and always-default header fields into the Gemini-returned
    header dict using the dynamic DB schema.

    Falls back to cfg.schema (ExtractionSchemaConfig) when schema keys are absent.
    """
    result = dict(header)

    always_null = schema.get(
        "header_always_null_fields",
        list(cfg.schema.header_always_null_fields) if hasattr(cfg, "schema") else [],
    )
    always_default = schema.get(
        "header_default_fields",
        dict(cfg.schema.header_default_fields) if hasattr(cfg, "schema") else {},
    )

    for fname in always_null:
        result[fname] = None
    for fname, fval in (always_default.items() if isinstance(always_default, dict) else []):
        result[fname] = fval

    return result


def inject_null_item_fields(item: dict, schema: dict) -> dict:
    """Inject always-null item fields using the dynamic DB schema."""
    result = dict(item)
    always_null = schema.get(
        "items_always_null_fields",
        list(cfg.schema.items_always_null_fields) if hasattr(cfg, "schema") else [],
    )
    for fname in always_null:
        result[fname] = None
    return result


def build_null_container() -> dict:
    """Build the all-null ShipmentContainerDetails structure."""
    return {f: None for f in _CONTAINER_ALWAYS_NULL_FIELDS}


# ============================================================================
# Numeric coercion
# ============================================================================

_CURRENCY_STRIP_RE = re.compile(r"[?$??,\s]")

_NUMERIC_ITEM_FIELDS: frozenset[str] = frozenset({
    "Amount", "Rate", "Itemslno", "NetWeight",
    "TaxableAmount", "IGSTAmount", "IGSTRate",
})


def coerce_numeric_fields(item: dict) -> dict:
    """
    Ensure all numeric item fields are JSON numbers, not strings.

    Strips currency symbols / commas then converts to int or float.
    Leaves the value unchanged if conversion fails.
    """
    result = dict(item)
    for fname in _NUMERIC_ITEM_FIELDS:
        val = result.get(fname)
        if val is None or isinstance(val, (int, float)):
            continue
        try:
            cleaned = _CURRENCY_STRIP_RE.sub("", str(val))
            result[fname] = int(cleaned) if "." not in cleaned else float(cleaned)
        except (ValueError, TypeError):
            logger.warning(
                "BatchExtractor: Could not coerce %r ? numeric for field %s ? left as-is",
                val, fname,
            )
    return result


# ============================================================================
# Cross-validation helpers
# ============================================================================

def cross_validate(
    line_items: list[dict],
    batch: BatchSlice,
) -> tuple[list[str], list[str]]:
    """
    Return (missing_serials, extra_serials).

    missing_serials : batch serials that produced no line item.
    extra_serials   : line items returned that don't map to any batch serial.

    Uses position as the primary matching key ? Gemini is instructed to return
    items in the same order as the identifier list.
    """
    expected_serials = set(batch.serials)
    matched_serials: set[str] = set()
    extra: list[str] = []

    for idx, _ in enumerate(line_items):
        if idx < len(batch.records):
            matched_serials.add(batch.records[idx].serial)
        else:
            extra.append(f"extra_item_{idx}")

    missing = sorted(expected_serials - matched_serials)
    return missing, extra


def resolve_schema_fields(
    schema: dict,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Extract header_fields and item_fields from the dynamic DB schema dict.

    The schema_json stored in document_schemas is expected to contain:
      {
        "header_fields": [{"name": "...", "description": "..."}, ...],
        "item_fields":   [{"name": "...", "description": "..."}, ...]
      }

    Falls back to DEFAULT_HEADER_GEMINI_FIELDS / DEFAULT_ITEM_GEMINI_FIELDS
    if the schema does not provide them in the expected format.

    Returns
    -------
    (header_fields, item_fields) : tuple of list[tuple[str, str]]
    """
    raw_header = schema.get("header_fields", [])
    raw_items  = schema.get("item_fields",   [])

    def _to_tuples(raw: Any) -> list[tuple[str, str]]:
        if isinstance(raw, list) and raw:
            first = raw[0]
            if isinstance(first, dict):
                return [(f["name"], f.get("description", "")) for f in raw if "name" in f]
            if isinstance(first, (list, tuple)) and len(first) >= 2:
                return [(str(f[0]), str(f[1])) for f in raw]
        return []

    header_fields = _to_tuples(raw_header) or DEFAULT_HEADER_GEMINI_FIELDS
    item_fields   = _to_tuples(raw_items)  or DEFAULT_ITEM_GEMINI_FIELDS

    return header_fields, item_fields