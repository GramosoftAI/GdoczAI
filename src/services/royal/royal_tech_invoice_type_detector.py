# -*- coding: utf-8 -*-
"""
royal_tech_invoice_type_detector.py  -  STEP 2: Invoice-type classification.

Classifies a commercial invoice as:
    "NORMAL_INVOICE"       -  The invoice is self-contained per page.
                              Every LINE-ITEM page has the full exporter
                              header, column headers, AND every data row
                              contains Qty + Rate + Amount on that same page.
                              Each page can be processed independently.
                              Examples: John Deere (9 pages, full header each),
                              Beckman Coulter (26 pages, full header each).

    "CROSS_PAGE_INVOICE"   -  Item rows, amounts, or the header are SPLIT
                              across pages.  The most common pattern:
                              * Invoice page 1 has header + items with Rate+Amount
                              * Packing list / continuation pages 2+ have NO
                                exporter block, NO Rate, NO Amount columns  -
                                they just show description + qty + weights.
                              OR: item serial numbers continue unbroken across
                              pages with no column-header repeat.
                              Examples: Surya Roshni (inv page 1 + packing pg 2),
                              Havells (inv pg 1 + 3 packing pages with no header),
                              Brakes India JABLONEC (inv pages 1-2 + packing 3-4),
                              MAF Clothing (inv page 1 + packing page 2),
                              New Century Sofa (inv page 1 + packing page 2).

DETECTION APPROACH
------------------
Two-stage:
    Stage 1  -  Heuristic (no Gemini call). Uses four complementary signals:
                  S1: page count and INVOICE-ONLY content
                  S2: RATE/AMOUNT tokens present on EVERY non-cover page
                  S3: full exporter block repeated on EVERY later page
                  S4: table column-header line repeated on EVERY later page

                The heuristic is CONSERVATIVE:
                  -> NORMAL only when ALL strong NORMAL signals agree
                  -> CROSS only when strong CROSS signals are present
                  -> None (inconclusive) -> fall through to Gemini

    Stage 2  -  Gemini classification on the full markdown.
                Called only when Stage 1 is inconclusive.

Public API
----------
    detector = InvoiceTypeDetector()
    invoice_type = detector.detect(full_markdown)
    # Returns: "NORMAL_INVOICE" | "CROSS_PAGE_INVOICE"
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Optional

import requests

from src.services.royal.royal_tech_config import cfg

logger = logging.getLogger(__name__)

# ============================================================================
# Gemini prompt constants
# ============================================================================

_SYSTEM_INSTRUCTION = """\
You are a precise document-structure classifier for commercial invoices.
You output ONLY valid JSON. No prose. No explanation. No markdown fences.\
"""

_TYPE_DETECT_PROMPT = """\
You are analysing a commercial invoice converted to Markdown.
Page boundaries are marked with:  ---PAGE <number>---

The FULL document is shown below (all pages).

YOUR ONLY TASK: classify this invoice as exactly one of:
  "NORMAL_INVOICE"
  "CROSS_PAGE_INVOICE"

===========================================================
DEFINITION: NORMAL_INVOICE
===========================================================
ALL of the following must be true for the LINE-ITEM pages:

1. FULL HEADER ON EVERY LINE-ITEM PAGE
   Every page that shows invoice items contains:
     - Exporter name and address
     - Invoice No  (same value)
     - Invoice Date
     - Consignee / Buyer
     - Port of Loading / Port of Discharge

2. EVERY ITEM ROW IS COMPLETE ON ITS PAGE
   Each row on each page has ALL of:
     - Description of Goods
     - Quantity + UOM
     - Unit Rate  (USD / EUR / GBP / INR)
     - Line Amount / Total

3. COLUMN HEADERS REPEAT ON EVERY LINE-ITEM PAGE
   "Description", "Qty", "Rate", "Amount" (or equivalents) appear
   at the top of the item table on EACH page.

4. PAGES ARE INDEPENDENTLY PROCESSABLE
   Any single line-item page can be extracted in isolation with
   complete item data (no need to reference another page).

NORMAL_INVOICE real-world examples in this dataset:
  * John Deere PR2523300129 (11 pages + annexures):
    Pages 1-9 are line-item pages. EVERY page has full John Deere
    header (Exporter, Invoice No, Date, Consignee, Port), column
    headers (Part No, Qty, Rate USD, Amount USD), and 3-4 complete
    item rows with Rate and Amount on the same page.
  * Brakes India BQ5250000324:
    2 invoice pages, each has full Brakes India header, and each
    item row has Qty + Rate (EUR) + Amount (EUR) on the same page.
  * Havells 5539800033 COMMERCIAL INVOICE page 1:
    Single invoice page with full header + all items with Rate+Amount.
    (The Packing List pages that follow are NOT invoice pages.)

===========================================================
DEFINITION: CROSS_PAGE_INVOICE
===========================================================
ANY of the following is true:

1. PACKING LIST / CONTINUATION PAGES LACK RATE AND AMOUNT
   The document has pages after page 1 (or the invoice section)
   where item rows appear WITHOUT a Unit Rate column and WITHOUT
   a Line Amount column. These pages only show Description + Qty
   + Net Weight + Gross Weight. This is the most common pattern.

   Examples in this dataset:
   - Surya Roshni 628: Page 1 = invoice with Rate+Amount.
     Page 2 = packing list with Qty+Weight only, no Rate/Amount.
   - Havells 5539800033: Invoice page 1 has Rate+Amount.
     Packing list pages 2-4 have no Rate column at all.
   - Brakes India BQ5250001017 (JABLONEC): Invoice pages 1-2 have
     items with Rate+Amount. Packing list pages 3-4 have only
     Qty + Net Wt + Gross Wt, no Rate/Amount.
   - MAF Clothing MAF2425/BE1456: Page 1 = commercial invoice with
     Unit Price + Amount. Page 2 = packing list with carton/size
     breakdown, no pricing at all.
   - New Century Sofa 9810001377: Page 1 = invoice with U/Price+Amount.
     Page 2 = packing list with Net Wt + Gross Wt only.
   - TRIO TREND (67/TTE/25-26): Page 1 = invoice with Rate+Amount.
     Page 2 = packing list with carton details, no Rate/Amount.

2. ITEM ROWS SPAN PAGES (header absent on later pages)
   Serial numbers continue across pages without column header repeat.
   Page 2+ starts directly with numbered item rows, no exporter block.

3. HEADER ABSENT ON PAGES 2+
   Later pages show only a minimal repeat (invoice no only, or nothing),
   not the full exporter + port + terms block.

===========================================================
CRITICAL RULE: PACKING LIST PAGES
===========================================================
When a document has an invoice page AND a separate packing list page:
  - If the packing list page has NO Rate column and NO Amount column
    -> classify as CROSS_PAGE_INVOICE
  - Only classify as NORMAL_INVOICE if EVERY page (including packing
    list pages) has full header + Rate + Amount on each item row.

The presence of the invoice number on a packing list page does NOT
make it a NORMAL_INVOICE page. The packing list page must also have
Rate and Amount for the invoice to be NORMAL.

===========================================================
DECISION RULES (apply in order)
===========================================================
A. Only ONE line-item page (all others are clearly annexures with
   no item rows at all)?
   -> NORMAL_INVOICE immediately.

B. Does ANY page after page 1 contain item-like rows (descriptions
   + quantities) but NO Unit Rate column and NO Amount column?
   YES -> CROSS_PAGE_INVOICE.

C. Does every line-item page have: exporter block + Rate + Amount
   on every data row?
   YES -> NORMAL_INVOICE.
   NO  -> CROSS_PAGE_INVOICE.

D. Uncertain? -> CROSS_PAGE_INVOICE (safer default).

===========================================================
COMMON MISTAKES TO AVOID
===========================================================
DO NOT classify as NORMAL_INVOICE just because the invoice number
  appears on every page. A packing list page will often repeat the
  invoice number but still lacks Rate/Amount -> CROSS_PAGE_INVOICE.

DO NOT classify as NORMAL_INVOICE because the document is multi-page.
  John Deere with 9 invoice pages (each self-contained) = NORMAL.
  Havells with 1 invoice page + 3 packing-list pages = CROSS_PAGE.

DO NOT classify as CROSS_PAGE_INVOICE just because there are many
  pages. John Deere has 11 pages and is still NORMAL_INVOICE.

DO NOT be confused by annexures (Annexure I, Examination Report).
  These are administrative, not line-item pages. Only count pages
  that actually show invoice line items.

===========================================================
OUTPUT FORMAT  -  STRICT JSON, NOTHING ELSE
===========================================================
Return ONLY this JSON object. No prose. No markdown fences.

{{
  "invoice_type": "<NORMAL_INVOICE or CROSS_PAGE_INVOICE>",
  "reason": "<one concise sentence citing the key evidence>"
}}

===========================================================
DOCUMENT MARKDOWN FOLLOWS
===========================================================
{full_markdown}
"""


# ============================================================================
# Private helpers
# ============================================================================

def _build_gemini_url() -> str:
    gcfg = cfg.gemini
    return (
        f"{gcfg.api_base_url}/{gcfg.model}"
        f":generateContent?key={gcfg.api_key}"
    )


def _call_gemini(prompt: str, max_output_tokens: int) -> Optional[str]:
    """POST to Gemini and return raw text. Returns None on any failure."""
    gcfg    = cfg.gemini
    url     = _build_gemini_url()
    payload = {
        "system_instruction": {"parts": [{"text": _SYSTEM_INSTRUCTION}]},
        "contents":           [{"role": "user", "parts": [{"text": prompt}]}],
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
        resp = requests.post(url, headers=headers, json=payload,
                             timeout=gcfg.timeout)
    except requests.exceptions.Timeout:
        logger.error("InvoiceTypeDetector: Gemini timed out after %ds",
                     gcfg.timeout)
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("InvoiceTypeDetector: Gemini request failed  -  %s", exc)
        return None

    if resp.status_code != 200:
        logger.error("InvoiceTypeDetector: Gemini HTTP %d  -  %s",
                     resp.status_code, resp.text[:400])
        return None

    try:
        data = resp.json()
        text = (
            data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
        )
        return text.strip() if text else None
    except (KeyError, IndexError, ValueError) as exc:
        logger.error("InvoiceTypeDetector: response parse failed  -  %s", exc)
        return None


def _extract_json(raw: str) -> Optional[dict]:
    """Best-effort JSON extraction from a Gemini text response."""
    if not raw:
        return None
    for candidate in (
        raw,
        re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE).strip().rstrip("`"),
    ):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    logger.warning("InvoiceTypeDetector: could not parse JSON from: %s",
                   raw[:300])
    return None


def _parse_page_texts(full_markdown: str) -> dict[int, str]:
    """
    Parse full_markdown into {page_num: page_text} dict.
    Handles ---PAGE N--- separators produced by royal_tech_ocr_service.
    """
    raw_segs = re.split(r"---PAGE\s+(\d+)---", full_markdown,
                        flags=re.IGNORECASE)
    page_texts: dict[int, str] = {}
    i = 0
    while i < len(raw_segs) - 1:
        try:
            pnum = int(raw_segs[i + 1]) if i + 1 < len(raw_segs) else None
            ptxt = raw_segs[i + 2]      if i + 2 < len(raw_segs) else ""
        except (ValueError, IndexError):
            i += 1
            continue
        if pnum is not None:
            page_texts[pnum] = ptxt
        i += 2
    return page_texts


# ---------------------------------------------------------------------------
# Signal helpers
# ---------------------------------------------------------------------------

def _has_rate_and_amount(text: str) -> bool:
    """
    Return True if the page contains BOTH a pricing column (rate / unit price /
    unit cost / price per) AND an amount/total column.

    These tokens are present on genuine invoice line-item pages where each row
    is fully priced.  They are ABSENT on packing-list pages which only carry
    description + qty + weight.

    Uses a presence-of-token check: we do NOT require numbers to follow
    immediately, because OCR sometimes puts the column header far from the
    data column.
    """
    t = text.lower()

    # Rate / unit-price tokens
    rate_tokens = [
        r"\brate\b",
        r"\bunit\s*price\b",
        r"\bu/?price\b",
        r"\bunit\s*cost\b",
        r"\bprice\s*per\b",
        r"\bper\s*(?:pcs?|pc|unit|mtr|kg|nos?)\b",
        r"\bin\s*usd\b",
        r"\bin\s*eur\b",
        r"\bin\s*gbp\b",
        r"\bin\s*inr\b",
        r"\busd\b",
        r"\beur\b",
        r"\bgbp\b",
    ]
    # Amount / total tokens
    amount_tokens = [
        r"\bamount\b",
        r"\btotal\s*amount\b",
        r"\bline\s*total\b",
        r"\bfob\s*value\b",
        r"\binvoice\s*value\b",
        r"\bext(?:ended)?\s*price\b",
        r"\bvalue\b",
    ]

    rate_found   = any(re.search(p, t) for p in rate_tokens)
    amount_found = any(re.search(p, t) for p in amount_tokens)

    return rate_found and amount_found


def _has_full_exporter_block(text: str) -> bool:
    """
    Return True if the page contains enough header tokens to constitute a
    full exporter / invoice header block.

    We require at least 3 of these 5 signals to be present:
      1. Exporter name / address label
      2. Invoice number label
      3. Invoice date label
      4. Port of Loading / Port of Discharge
      5. Consignee or Buyer or Ship-to / Bill-to
    """
    t = text.lower()
    signals = [
        bool(re.search(r"\bexporter\b", t)),
        bool(re.search(r"\binvoice\s*no\b|\binv\.?\s*no\b|\binvoice\s*number\b", t)),
        bool(re.search(r"\binvoice\s*date\b|\bdt\.?\b|\bdate\s*:", t)),
        bool(re.search(r"\bport\s*of\s*loading\b|\bport\s*of\s*discharge\b", t)),
        bool(re.search(r"\bconsignee\b|\bbuyer\b|\bship\s*to\b|\bbill\s*to\b", t)),
    ]
    return sum(signals) >= 3


def _has_column_headers(text: str) -> bool:
    """
    Return True if the page contains a table column-header row typical of
    invoice line-item tables.

    Looks for combinations of: description / goods / part no, qty / quantity,
    rate / price, amount / total.
    """
    t = text.lower()
    desc   = bool(re.search(
        r"\bdescription\b|\bgoods\b|\bpart\s*no\b|\bitem\b|\bmaterial\b", t))
    qty    = bool(re.search(r"\bqty\b|\bquantity\b|\bunits\b", t))
    rate   = bool(re.search(r"\brate\b|\bprice\b|\bcost\b|\busd\b|\beur\b|\bgbp\b", t))
    amount = bool(re.search(r"\bamount\b|\btotal\b|\bvalue\b", t))
    return desc and qty and (rate or amount)


def _is_packing_list_page(text: str) -> bool:
    """
    Return True if the page is clearly a packing list / weight sheet with
    no pricing.  Key markers: weight columns present but Rate/Amount absent.
    """
    t = text.lower()
    weight_signal = bool(
        re.search(r"\bnet\s*w(?:eig)?h?t\b|\bgross\s*w(?:eig)?h?t\b"
                  r"|\bnet\s*wt\b|\bgross\s*wt\b", t)
    )
    packing_signal = bool(
        re.search(r"\bpacking\s*list\b|\bpack(?:ing)?\s*details\b"
                  r"|\bcarton\b|\bcrt\s*no\b|\bcase\s*no\b"
                  r"|\bpallets?\b|\bbundles?\b", t)
    )
    no_rate = not _has_rate_and_amount(text)
    return (weight_signal or packing_signal) and no_rate


def _heuristic(full_markdown: str) -> Optional[str]:
    """
    Rule-based pre-check on the raw full markdown (no Gemini call).
    Returns "NORMAL_INVOICE", "CROSS_PAGE_INVOICE", or None (inconclusive).

    REVISED RULES (applied in order):

    R1.  Single-page document               -> NORMAL_INVOICE
    R2.  ALL later pages are packing-list   -> CROSS_PAGE_INVOICE
         pages (have weight/carton markers
         but NO Rate+Amount)
    R3.  EVERY page (all pages) has         -> NORMAL_INVOICE
         Rate+Amount AND full exporter
         block AND column headers
    R4.  Invoice number repeats BUT any     -> CROSS_PAGE_INVOICE
         later page lacks Rate+Amount
    R5.  None of the above                  -> None (inconclusive -> Gemini)
    """
    page_texts = _parse_page_texts(full_markdown)

    if not page_texts:
        return None

    all_pages   = sorted(page_texts.keys())
    n_pages     = len(all_pages)

    # R1  -  single page document
    if n_pages <= 1:
        logger.info(
            "InvoiceTypeDetector [heuristic]: single page -> NORMAL_INVOICE"
        )
        return "NORMAL_INVOICE"

    later_pages  = all_pages[1:]
    first_text   = page_texts.get(all_pages[0], "")

    # Compute per-page signals
    pg_rate_amount  = {p: _has_rate_and_amount(page_texts[p])  for p in all_pages}
    pg_exporter     = {p: _has_full_exporter_block(page_texts[p]) for p in all_pages}
    pg_col_hdrs     = {p: _has_column_headers(page_texts[p])   for p in all_pages}
    pg_is_packing   = {p: _is_packing_list_page(page_texts[p]) for p in all_pages}

    logger.debug(
        "InvoiceTypeDetector [heuristic]: pages=%s rate_amount=%s "
        "exporter=%s col_hdrs=%s is_packing=%s",
        all_pages, pg_rate_amount, pg_exporter, pg_col_hdrs, pg_is_packing,
    )

    # R2  -  ALL later pages are packing-list/weight pages (no Rate+Amount)
    #        even if inv-no repeats on them
    all_later_packing = all(
        pg_is_packing.get(p, False) or not pg_rate_amount.get(p, False)
        for p in later_pages
    )
    first_has_rate = pg_rate_amount.get(all_pages[0], False)

    if first_has_rate and all_later_packing:
        logger.info(
            "InvoiceTypeDetector [heuristic]: page 1 has Rate+Amount but "
            "all later pages are packing/weight only -> CROSS_PAGE_INVOICE"
        )
        return "CROSS_PAGE_INVOICE"

    # R3  -  ALL pages have Rate+Amount + exporter block + column headers
    #        Strong NORMAL_INVOICE signal
    all_complete = all(
        pg_rate_amount.get(p, False)
        and pg_exporter.get(p, False)
        and pg_col_hdrs.get(p, False)
        for p in all_pages
    )
    if all_complete:
        logger.info(
            "InvoiceTypeDetector [heuristic]: all %d pages have "
            "Rate+Amount + exporter + col-hdrs -> NORMAL_INVOICE", n_pages
        )
        return "NORMAL_INVOICE"

    # R4  -  Invoice number repeats on later pages but any later page
    #        lacks Rate+Amount  -> CROSS_PAGE_INVOICE
    inv_patterns = [
        re.compile(r"(?:invoice\s*no[.:]?\s*)([A-Z0-9/\-]{4,40})",
                   re.IGNORECASE),
        re.compile(r"(?:inv\.?\s*no[.:]?\s*)([A-Z0-9/\-]{4,40})",
                   re.IGNORECASE),
        re.compile(r"(?:invoice\s*number[.:]?\s*)([A-Z0-9/\-]{4,40})",
                   re.IGNORECASE),
    ]
    inv_no: Optional[str] = None
    for pat in inv_patterns:
        m = pat.search(first_text)
        if m:
            inv_no = m.group(1).strip()
            break

    inv_repeats_on_later = (
        inv_no is not None and
        any(inv_no.lower() in page_texts.get(p, "").lower()
            for p in later_pages)
    )

    if inv_repeats_on_later:
        # Invoice no repeats  -  but are later pages missing Rate+Amount?
        later_missing_rate = [
            p for p in later_pages
            if not pg_rate_amount.get(p, False)
        ]
        if later_missing_rate:
            logger.info(
                "InvoiceTypeDetector [heuristic]: inv_no repeats but "
                "page(s) %s lack Rate+Amount -> CROSS_PAGE_INVOICE",
                later_missing_rate,
            )
            return "CROSS_PAGE_INVOICE"

        # Invoice no repeats AND all later pages have Rate+Amount
        # Check if later pages also have exporter block (strong NORMAL signal)
        later_has_exporter = [
            p for p in later_pages if pg_exporter.get(p, False)
        ]
        if len(later_has_exporter) == len(later_pages):
            logger.info(
                "InvoiceTypeDetector [heuristic]: inv_no repeats + "
                "all later pages have Rate+Amount + exporter -> "
                "NORMAL_INVOICE"
            )
            return "NORMAL_INVOICE"

    # R5  -  inconclusive  -  send to Gemini
    logger.info("InvoiceTypeDetector [heuristic]: inconclusive -> Gemini")
    return None


# ============================================================================
# InvoiceTypeDetector
# ============================================================================

class InvoiceTypeDetector:
    """
    STEP 2  -  Classifies a commercial invoice as NORMAL_INVOICE or
    CROSS_PAGE_INVOICE directly from the raw OCR full_markdown.

    Called by RoyalInvoicePipeline BEFORE any line-item page detection.
    The result determines which downstream path the processor takes:
      NORMAL_INVOICE     -> NI-3 extraction (all pages passed directly)
      CROSS_PAGE_INVOICE -> Step 2b page detection -> Steps 3-8

    Usage
    -----
        detector = InvoiceTypeDetector()
        invoice_type = detector.detect(full_markdown)
        # -> "NORMAL_INVOICE" | "CROSS_PAGE_INVOICE"
    """

    _FALLBACK = "CROSS_PAGE_INVOICE"   # safer default on any failure

    def __init__(self) -> None:
        self._gcfg    = cfg.gemini
        self._max_tok = cfg.page_detector.max_output_tokens
        logger.info(
            "InvoiceTypeDetector initialised (model=%s)", self._gcfg.model
        )

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def detect(self, full_markdown: str) -> str:
        """
        Classify the invoice from the raw OCR markdown.

        Parameters
        ----------
        full_markdown : str
            Complete OCR markdown with ---PAGE N--- separators.
            All pages are included; no prior filtering is applied.

        Returns
        -------
        str
            "NORMAL_INVOICE" or "CROSS_PAGE_INVOICE".
            Defaults to "CROSS_PAGE_INVOICE" on any failure.
        """
        if not full_markdown or not full_markdown.strip():
            logger.warning(
                "InvoiceTypeDetector.detect: empty markdown  -  "
                "defaulting to %s", self._FALLBACK,
            )
            return self._FALLBACK

        logger.info("InvoiceTypeDetector.detect: classifying document...")

        # Stage 1  -  heuristic (no Gemini call)
        result = _heuristic(full_markdown)
        if result is not None:
            logger.info(
                "InvoiceTypeDetector.detect: heuristic -> %s", result
            )
            return result

        # Stage 2  -  Gemini
        logger.info(
            "InvoiceTypeDetector.detect: heuristic inconclusive  -  "
            "calling Gemini"
        )
        return self._gemini_classify(full_markdown)

    # ------------------------------------------------------------------
    # Private  -  Gemini classification
    # ------------------------------------------------------------------

    def _gemini_classify(self, full_markdown: str) -> str:
        """Call Gemini with the full document markdown."""
        prompt   = _TYPE_DETECT_PROMPT.format(full_markdown=full_markdown)
        raw_text = self._call_with_retry(prompt)

        if raw_text is None:
            logger.error(
                "InvoiceTypeDetector: Gemini call failed  -  "
                "defaulting to %s", self._FALLBACK,
            )
            return self._FALLBACK

        parsed = _extract_json(raw_text)
        if parsed is None:
            logger.error(
                "InvoiceTypeDetector: JSON parse failed  -  "
                "defaulting to %s", self._FALLBACK,
            )
            return self._FALLBACK

        raw_type = parsed.get("invoice_type", "").strip().upper()
        reason   = parsed.get("reason", "")

        if raw_type == "NORMAL_INVOICE":
            logger.info(
                "InvoiceTypeDetector: Gemini -> NORMAL_INVOICE | %s", reason
            )
            return "NORMAL_INVOICE"

        if raw_type == "CROSS_PAGE_INVOICE":
            logger.info(
                "InvoiceTypeDetector: Gemini -> CROSS_PAGE_INVOICE | %s", reason
            )
            return "CROSS_PAGE_INVOICE"

        logger.warning(
            "InvoiceTypeDetector: unexpected invoice_type %r from Gemini  -  "
            "defaulting to %s", raw_type, self._FALLBACK,
        )
        return self._FALLBACK

    def _call_with_retry(self, prompt: str) -> Optional[str]:
        """Call Gemini with exponential back-off retry."""
        gcfg = self._gcfg
        for attempt in range(gcfg.max_retries):
            logger.info(
                "InvoiceTypeDetector: Gemini attempt %d/%d",
                attempt + 1, gcfg.max_retries,
            )
            result = _call_gemini(prompt, self._max_tok)
            if result is not None:
                return result
            if attempt < gcfg.max_retries - 1:
                wait = gcfg.retry_backoff_base ** attempt
                logger.warning(
                    "InvoiceTypeDetector: attempt %d failed  -  retry in %.1fs",
                    attempt + 1, wait,
                )
                time.sleep(wait)

        logger.error(
            "InvoiceTypeDetector: all %d Gemini attempts exhausted",
            gcfg.max_retries,
        )
        return None