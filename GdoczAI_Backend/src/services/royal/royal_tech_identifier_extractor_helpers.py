# -*- coding: utf-8 -*-
"""
royal_tech_identifier_extractor_helpers.py � Stateless helpers for STEP 3.

Contains (imported by royal_tech_identifier_extractor.py):
  � _SYSTEM_INSTRUCTION, _PAGE_EXTRACT_PROMPT  � prompt constants
  � _call_gemini                               � Gemini HTTP caller
  � _extract_json                              � 3-stage JSON repair
  � _sanitise_number, _is_valid_number         � numeric normalisation
  � _sanitise_value_string                     � canonical grammar enforcer
  � _parse_record_components                   � value ? component tuple

All functions are pure / stateless.  Config is read from royal_tech_config.cfg.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

import requests

from src.services.royal.royal_tech_config import cfg

logger = logging.getLogger(__name__)


# ============================================================================
# Prompt constants
# ============================================================================

_SYSTEM_INSTRUCTION = """\
You are a precise invoice line-item analyser.
You output ONLY valid JSON. No prose. No explanation. No markdown fences.\
"""

_PAGE_EXTRACT_PROMPT = """\
You are analysing ONE page of a multi-page commercial invoice (already converted to Markdown).

?????????????????????????????????????????????????????
TASK
?????????????????????????????????????????????????????
Extract ONLY the rows from the MAIN invoice line-item table on this page.
Produce one composite identifier string per row.

?????????????????????????????????????????????????????
WHAT IS THE MAIN LINE-ITEM TABLE
?????????????????????????????????????????????????????
The MAIN line-item table:
  � Contains repeating rows � one row per actual good sold or shipped.
  � Has a material_id / part number / catalogue number per row.
  � Has a numeric Qty per row AND a numeric Amount/Total per row.
  � Is the core billing table.

?????????????????????????????????????????????????????
WHAT TO IGNORE (DO NOT EXTRACT FROM THESE)
?????????????????????????????????????????????????????
  � Annexure / tax computation tables
  � Packing list tables
  � HSN summary tables
  � IGST / SGST / CGST summary tables
  � Container summary tables
  � Totals rows (sub-total, grand total, net total)
  � Header rows, blank rows, continuation headers

?????????????????????????????????????????????????????
IDENTIFIER CONSTRUCTION RULES
?????????????????????????????????????????????????????
For each main line-item row, build ONE identifier string using EXACTLY one
of these two grammar patterns (choose whichever applies):

  PATTERN A (preferred) � use when the row has a material_id / part number:
    "material id <ID> have qty <NUMBER> and amount <NUMBER>"

  PATTERN B � use ONLY when material_id is absent:
    "description <TEXT> have qty <NUMBER> and amount <NUMBER>"

Strict value rules:
  � <ID>     � raw material_id / part number exactly as printed. No spaces inside.
  � <TEXT>   � description text, trimmed, no newlines.
  � <NUMBER> � numeric digits only. No currency symbols (? $ � �). No commas.
               No units (PC, NOS, KG). Decimal point allowed.
  � qty must come from the Qty / Quantity column of THAT row only.
  � amount must come from the Amount / Total column of THAT row only.
  � Do NOT use rate/unit-price as amount.
  � Do NOT use total-row amounts.

?????????????????????????????????????????????????????
OUTPUT FORMAT � STRICT JSON
?????????????????????????????????????????????????????
Return ONLY this JSON structure. No markdown fences. No commentary.

{{
  "line_items_count_this_page": <integer>,
  "items": [
    {{
      "serial": "{serial_placeholder}",
      "type": "material_id",
      "value": "material id SU26573 have qty 2 and amount 4.38"
    }},
    {{
      "serial": "{serial_next_placeholder}",
      "type": "description",
      "value": "description STEEL BOLT M6x20 have qty 100 and amount 350.00"
    }}
  ]
}}

serial format: exactly {serial_zero_pad} digits, zero-padded, starting at {serial_start}.

If this page has NO main line-item rows, return:
{{
  "line_items_count_this_page": 0,
  "items": []
}}

?????????????????????????????????????????????????????
PAGE {page_num} MARKDOWN FOLLOWS
?????????????????????????????????????????????????????
{page_markdown}
"""


# ============================================================================
# Gemini HTTP caller
# ============================================================================

def _build_gemini_url() -> str:
    gcfg = cfg.gemini
    return f"{gcfg.api_base_url}/{gcfg.model}:generateContent?key={gcfg.api_key}"


def _call_gemini(prompt: str, max_output_tokens: int) -> Optional[str]:
    """POST prompt to Gemini 2.5 Flash.  Returns raw text or None on failure."""
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
        logger.error(
            "IdentifierExtractor: Gemini request timed out after %ds", gcfg.timeout
        )
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("IdentifierExtractor: Gemini request failed � %s", exc)
        return None

    if response.status_code != 200:
        logger.error(
            "IdentifierExtractor: Gemini HTTP %d � %s",
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
        logger.error(
            "IdentifierExtractor: Failed to parse Gemini response � %s", exc
        )
        return None


# ============================================================================
# JSON repair  (3-stage: direct ? strip fences ? regex search)
# ============================================================================

def _extract_json(raw: str) -> Optional[dict]:
    """Three-stage JSON extraction with progressive fallback."""
    if not raw:
        return None

    # Stage 1 � direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Stage 2 � strip markdown fences
    stripped = (
        re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE)
        .strip()
        .rstrip("`")
        .strip()
    )
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # Stage 3 � find first balanced {...}
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    logger.warning("IdentifierExtractor: Cannot parse JSON: %s", raw[:300])
    return None


# ============================================================================
# Canonical grammar regex patterns
# ============================================================================

_PATTERN_MATERIAL = re.compile(
    r"^material id\s+(.+?)\s+have qty\s+([\d.]+)\s+and amount\s+([\d.]+)$",
    re.IGNORECASE,
)
_PATTERN_DESCRIPTION = re.compile(
    r"^description\s+(.+?)\s+have qty\s+([\d.]+)\s+and amount\s+([\d.]+)$",
    re.IGNORECASE,
)

_CURRENCY_STRIP = re.compile(r"[?$��,\s]")
_NON_NUMERIC    = re.compile(r"[^\d.]")


# ============================================================================
# Numeric normalisation helpers
# ============================================================================

def _sanitise_number(raw: str) -> str:
    """
    Strip currency symbols, commas, and spaces from a number string.
    Keeps only digits and a single decimal point.
    Returns "0" for empty / unparseable input.
    """
    cleaned = _CURRENCY_STRIP.sub("", str(raw))
    cleaned = _NON_NUMERIC.sub("", cleaned)
    # Collapse multiple dots � keep only the last decimal point
    parts = cleaned.split(".")
    if len(parts) > 2:
        cleaned = parts[0] + "." + "".join(parts[1:])
    return cleaned if cleaned else "0"


def _is_valid_number(s: str) -> bool:
    """Return True if s is a non-empty, parseable, non-negative number."""
    try:
        return float(s) >= 0
    except (ValueError, TypeError):
        return False


# ============================================================================
# Value string sanitiser � canonical grammar enforcer
# ============================================================================

def _sanitise_value_string(raw_value: str) -> Optional[str]:
    """
    Attempt to normalise Gemini's raw value string into the canonical grammar.

    Strategy
    --------
    1. Try PATTERN A (material_id) � clean numbers and recompose.
    2. Try PATTERN B (description) � clean numbers and recompose.
    3. Fuzzy recovery � locate qty/amount/material_id tokens anywhere in string.
    4. Return None if none of the above produces a valid canonical string.
    """
    if not raw_value or not isinstance(raw_value, str):
        return None

    v = raw_value.strip()

    # Strategy 1 � PATTERN A
    m = _PATTERN_MATERIAL.match(v)
    if m:
        mid = m.group(1).strip()
        qty = _sanitise_number(m.group(2))
        amt = _sanitise_number(m.group(3))
        if _is_valid_number(qty) and _is_valid_number(amt) and mid:
            return f"material id {mid} have qty {qty} and amount {amt}"

    # Strategy 2 � PATTERN B
    m = _PATTERN_DESCRIPTION.match(v)
    if m:
        desc = m.group(1).strip()
        qty  = _sanitise_number(m.group(2))
        amt  = _sanitise_number(m.group(3))
        if _is_valid_number(qty) and _is_valid_number(amt) and desc:
            return f"description {desc} have qty {qty} and amount {amt}"

    # Strategy 3 � fuzzy token search
    qty_match  = re.search(r"qty\s*([\d,?.$��]+)",           v, re.IGNORECASE)
    amt_match  = re.search(r"amount\s*([\d,?.$��]+)",        v, re.IGNORECASE)
    mid_match  = re.search(r"material id\s+(\S+)",            v, re.IGNORECASE)
    desc_match = re.search(r"description\s+(.+?)\s+(?:have|qty)", v, re.IGNORECASE)

    if qty_match and amt_match:
        qty = _sanitise_number(qty_match.group(1))
        amt = _sanitise_number(amt_match.group(1))
        if _is_valid_number(qty) and _is_valid_number(amt):
            if mid_match:
                return (
                    f"material id {mid_match.group(1).strip()} "
                    f"have qty {qty} and amount {amt}"
                )
            if desc_match:
                return (
                    f"description {desc_match.group(1).strip()} "
                    f"have qty {qty} and amount {amt}"
                )

    logger.warning(
        "IdentifierExtractor: Could not sanitise value string: %r", v[:120]
    )
    return None


# ============================================================================
# Record component parser
# ============================================================================

def _parse_record_components(
    canonical_value: str,
) -> tuple[str, Optional[str], Optional[str], str, str]:
    """
    Parse a validated canonical value string and return:
    (identifier_type, raw_material_id, raw_description, raw_qty, raw_amount)

    Reads identifier_type strings from cfg.identifier_extractor to stay
    consistent with the config-driven naming convention.
    """
    icfg = cfg.identifier_extractor

    m = _PATTERN_MATERIAL.match(canonical_value)
    if m:
        return (
            icfg.identifier_type_material,
            m.group(1).strip(),
            None,
            m.group(2),
            m.group(3),
        )

    m = _PATTERN_DESCRIPTION.match(canonical_value)
    if m:
        return (
            icfg.identifier_type_description,
            None,
            m.group(1).strip(),
            m.group(2),
            m.group(3),
        )

    # Unreachable if _sanitise_value_string was called first
    return "unknown", None, None, "0", "0"


# ============================================================================
# Prompt builder
# ============================================================================

def build_page_extract_prompt(
    page_markdown: str,
    page_num: int,
    serial_start: int,
) -> str:
    """
    Render _PAGE_EXTRACT_PROMPT for one page.

    Uses serial_start=1 placeholder when called from the parallel phase
    (real serials are assigned in Phase 2 after all pages complete).
    """
    pad = cfg.identifier_extractor.serial_zero_pad
    return _PAGE_EXTRACT_PROMPT.format(
        serial_placeholder=str(serial_start).zfill(pad),
        serial_next_placeholder=str(serial_start + 1).zfill(pad),
        serial_zero_pad=pad,
        serial_start=serial_start,
        page_num=page_num,
        page_markdown=page_markdown,
    )