# -*- coding: utf-8 -*-
#!/usr/bin/env python3

"""
Document Segmentation Analyzer for OCR Server.

Provides Gemini 2.5 Flash-based document segmentation in two modes:

  AUTO mode
  ---------
  Sends the full page-wise markdown to Gemini 2.5 Flash and asks it to
  detect logical document boundaries automatically (by reading titles,
  headers, layout patterns, etc.).

  GUIDED mode
  -----------
  User supplies a list of keywords and an optional plain-text description.
  Gemini uses those as strict guidance to assign each page to a named
  segment. The same keyword can produce multiple non-contiguous segments
  (e.g. "line items" appears on pages 1-2 and again on pages 4-5).

Design principles
-----------------
- DocumentSegmentAnalyzer is a STANDALONE class -- it does NOT inherit
  from GeminiJSONGenerator or GeminiHeavyMethods. It owns its own Gemini
  model instance and follows the same initialisation pattern as
  GeminiJSONGenerator.__init__() so the codebase stays consistent.
- generate_segmentation_json() always uses gemini-2.5-flash explicitly
  at temperature 0.1 with max_tokens=8192 -- same safety-settings pattern
  as _call_gemini_api_async() in GeminiHeavyMethods.
- JSON parsing delegates to RobustJSONParser (5-strategy repair chain)
  so malformed Gemini output is handled gracefully.
- All public surface is async.
"""

import asyncio
import json
import logging
import os
import yaml

from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.services.ocr_pipeline.ocr_server_json_parser import RobustJSONParser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Gemini model constants (same naming convention as ocr_server_gemini.py)
# ---------------------------------------------------------------------------
_SEGMENTATION_MODEL = "gemini-2.5-flash"
_SEGMENTATION_TEMPERATURE = 0.1
_SEGMENTATION_MAX_TOKENS = 8192
_SEGMENTATION_MAX_RETRIES = 3
_SEGMENTATION_RETRY_DELAY = 2


# ===========================================================================
# DOCUMENT SEGMENT ANALYZER
# ===========================================================================
class DocumentSegmentAnalyzer:
    """
    Gemini 2.5 Flash-backed document segmentation engine.

    Usage
    -----
    analyzer = DocumentSegmentAnalyzer(config)
    result   = await analyzer.segment_document(
                   page_map=page_map,
                   mode="auto",          # or "guided"
                   keywords=None,
                   description=None,
                   request_id=request_id,
               )
    # result -> {"segments": [...], "metadata": {...}}
    """

    def __init__(self, config):
        """
        Parameters
        ----------
        config : Config
            The global Config object from ocr_server_config.py.
            Used to read the Gemini API key from config.yaml
            (same path GeminiJSONGenerator uses).
        """
        self.config = config

        # -----------------------------------------------------------------
        # Load raw YAML so we can pull the gemini api_key the same way
        # GeminiJSONGenerator._load_config() does.
        # -----------------------------------------------------------------
        raw_cfg = self._load_yaml_config()

        self.api_key = (
            raw_cfg.get("gemini", {}).get("api_key")
            or os.getenv("GEMINI_API_KEY")
        )

        # Segmentation always uses fixed model / hyper-params
        self.model_name   = _SEGMENTATION_MODEL
        self.temperature  = _SEGMENTATION_TEMPERATURE
        self.max_tokens   = _SEGMENTATION_MAX_TOKENS
        self.max_retries  = _SEGMENTATION_MAX_RETRIES
        self.retry_delay  = _SEGMENTATION_RETRY_DELAY

        # Token counters (reset per segment_document call)
        self.total_prompt_tokens   = 0
        self.total_response_tokens = 0
        self.total_tokens_used     = 0

        # -----------------------------------------------------------------
        # Initialise Gemini -- identical try/except structure to
        # GeminiJSONGenerator.__init__()
        # -----------------------------------------------------------------
        self.enabled = False
        self.model   = None
        self.genai   = None
        self.HarmCategory        = None
        self.HarmBlockThreshold  = None

        logger.info("=" * 80)
        logger.info("[SEGMENT-ANALYZER] Initialising DocumentSegmentAnalyzer")
        logger.info("=" * 80)

        if self.api_key:
            try:
                from google import genai
                from google.genai import types

                logger.info("[SEGMENT-ANALYZER] Configuring Gemini API...")
                self.client = genai.Client(api_key=self.api_key)
                self.model = self.model_name
                self.genai = genai
                self.HarmCategory = types.HarmCategory
                self.HarmBlockThreshold = types.HarmBlockThreshold
                self.enabled            = True

                logger.info("[SEGMENT-ANALYZER] Gemini initialised successfully")
                logger.info("[SEGMENT-ANALYZER]   Model      : %s", self.model_name)
                logger.info("[SEGMENT-ANALYZER]   Temperature: %s", self.temperature)
                logger.info("[SEGMENT-ANALYZER]   Max Tokens : %s", self.max_tokens)
                logger.info("=" * 80)

            except ImportError as exc:
                logger.error(
                    "[SEGMENT-ANALYZER] ImportError -- google-genai not installed: %s", exc
                )
            except Exception as exc:
                logger.error(
                    "[SEGMENT-ANALYZER] Gemini init failed: %s", exc
                )
        else:
            logger.warning(
                "[SEGMENT-ANALYZER] No Gemini API key found -- segmentation disabled"
            )
            logger.warning(
                "[SEGMENT-ANALYZER] Add gemini.api_key to config.yaml "
                "or set GEMINI_API_KEY env var"
            )

    # =======================================================================
    # PUBLIC -- main entry point
    # =======================================================================
    async def segment_document(
        self,
        page_map: Dict[int, str],
        mode: str,
        segments: Optional[List[Dict]] = None,
        request_id: Optional[str] = None,
    ) -> Dict:
        """
        Segment a document represented as a page_map.

        Parameters
        ----------
        page_map    : {page_number (int) -> page_content (str)}
                      1-based page numbers, produced by _parse_page_map()
                      in ocr_server_segment_endpoint.py.
        mode        : "auto" | "guided"
        segments    : list of segment dicts, each with 'name' and 'description'
                      (required for guided mode)
        request_id  : tracing ID for logs

        Returns
        -------
        dict with keys:
            "segments" : list of segment dicts
            "metadata" : dict with segmentation_method, strategy, total_pages
        """
        rid = request_id or "unknown"

        self._reset_token_counters()

        logger.info("=" * 80)
        logger.info(
            "[SEGMENT-ANALYZER] segment_document called | mode=%s | pages=%d | request_id=%s",
            mode, len(page_map), rid,
        )
        logger.info("=" * 80)

        if not self.enabled:
            logger.error(
                "[SEGMENT-ANALYZER] Gemini not enabled -- cannot segment | request_id=%s", rid
            )
            return self._error_result(
                "Gemini is not enabled. Configure a valid API key.", page_map, mode
            )

        if not page_map:
            logger.error(
                "[SEGMENT-ANALYZER] Empty page_map received | request_id=%s", rid
            )
            return self._error_result("No page content to segment.", page_map, mode)

        # ------------------------------------------------------------------
        # Build prompt
        # ------------------------------------------------------------------
        total_pages = len(page_map)

        if mode == "auto":
            prompt = self._build_auto_prompt(page_map, total_pages)
        else:  # guided
            prompt = self._build_guided_prompt(
                page_map, total_pages, segments or []
            )

        logger.info(
            "[SEGMENT-ANALYZER] Prompt built: %d chars | request_id=%s",
            len(prompt), rid,
        )

        # ------------------------------------------------------------------
        # Call Gemini 2.5 Flash
        # ------------------------------------------------------------------
        raw_response, prompt_tokens, response_tokens = (
            await self.generate_segmentation_json(prompt, request_id=rid)
        )

        if not raw_response:
            logger.error(
                "[SEGMENT-ANALYZER] No response from Gemini | request_id=%s", rid
            )
            return self._error_result(
                "Gemini returned no response after all retries.", page_map, mode
            )

        logger.info(
            "[SEGMENT-ANALYZER] Raw Gemini response: %d chars | request_id=%s",
            len(raw_response), rid,
        )
        logger.debug("[SEGMENT-ANALYZER] Raw response text:\n%s", raw_response[:2000])

        # ------------------------------------------------------------------
        # Parse + repair JSON
        # ------------------------------------------------------------------
        clean_text = self._extract_json_from_response(raw_response)
        parsed     = RobustJSONParser.clean_and_parse(clean_text)

        logger.info(
            "[SEGMENT-ANALYZER] JSON parsed: %d top-level keys | request_id=%s",
            len(parsed), rid,
        )

        # ------------------------------------------------------------------
        # Validate and normalise segments
        # ------------------------------------------------------------------
        segments = self._normalise_segments(parsed, total_pages, mode, rid)

        # ------------------------------------------------------------------
        # Build metadata
        # ------------------------------------------------------------------
        if mode == "auto":
            strategy   = "AutoDocSegmentationStrategy"
        else:
            strategy   = "GuidedSegmentationStrategy"

        metadata = {
            "total_pages":          total_pages,
            "strategy":             strategy
        }

        logger.info("=" * 80)
        logger.info(
            "[SEGMENT-ANALYZER] Segmentation complete | segments=%d | request_id=%s",
            len(segments), rid,
        )
        for i, seg in enumerate(segments, 1):
            logger.info(
                "[SEGMENT-ANALYZER]   Segment %d: name='%s' | pages=%s | confidence=%s",
                i, seg.get("name"), seg.get("pages"), seg.get("confidence"),
            )
        logger.info("=" * 80)

        return {"segments": segments, "metadata": metadata}

    # =======================================================================
    # PUBLIC -- Gemini API call (new method, separate from GeminiHeavyMethods)
    # =======================================================================
    async def generate_segmentation_json(
        self,
        prompt: str,
        request_id: Optional[str] = None,
    ) -> Tuple[Optional[str], int, int]:
        """
        Send *prompt* to Gemini 2.5 Flash and return the raw response text.

        Always uses:
            model       = gemini-2.5-flash
            temperature = 0.1
            max_tokens  = 8192

        Safety settings and retry logic mirror _call_gemini_api_async()
        in GeminiHeavyMethods so behaviour is consistent across the codebase.

        """
        rid = request_id or "unknown"

        if not self.client:
            logger.error(
                "[SEGMENT-ANALYZER] Gemini client not initialised | request_id=%s", rid
            )
            return None, 0, 0

        from google.genai import types

        config = types.GenerateContentConfig(
            max_output_tokens=self.max_tokens,
            temperature=self.temperature,
            top_p=0.8,
            top_k=40,
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.BLOCK_ONLY_HIGH
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.BLOCK_ONLY_HIGH
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.BLOCK_ONLY_HIGH
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.BLOCK_ONLY_HIGH
                ),
            ]
        )

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    "[SEGMENT-ANALYZER] Gemini API call attempt %d/%d | request_id=%s",
                    attempt, self.max_retries, rid,
                )

                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.client.models.generate_content(
                        model=self.model_name,
                        contents=prompt,
                        config=config
                    ),
                )

                if not response.text:
                    logger.warning(
                        "[SEGMENT-ANALYZER] Empty response on attempt %d | request_id=%s",
                        attempt, rid,
                    )
                    if attempt < self.max_retries:
                        await asyncio.sleep(self.retry_delay)
                    continue

                # ------- token accounting -------
                prompt_tokens   = 0
                response_tokens = 0
                try:
                    if hasattr(response, "usage_metadata") and response.usage_metadata:
                        prompt_tokens   = getattr(response.usage_metadata, 'prompt_token_count', 0) or 0
                        response_tokens = getattr(response.usage_metadata, 'response_token_count', None) or getattr(response.usage_metadata, 'candidates_token_count', 0) or 0
                        logger.info(
                            "[SEGMENT-ANALYZER] Tokens -- prompt: %d | response: %d | request_id=%s",
                            prompt_tokens, response_tokens, rid,
                        )
                    else:
                        prompt_tokens   = int(len(prompt.split()) * 1.3)
                        response_tokens = int(len(response.text.split()) * 1.3)
                        logger.warning(
                            "[SEGMENT-ANALYZER] usage_metadata unavailable -- estimated tokens | request_id=%s",
                            rid,
                        )
                except Exception as token_err:
                    logger.warning(
                        "[SEGMENT-ANALYZER] Could not extract token usage: %s | request_id=%s",
                        token_err, rid,
                    )
                    prompt_tokens   = int(len(prompt.split()) * 1.3)
                    response_tokens = int(len(response.text.split()) * 1.3)

                self.total_prompt_tokens   += int(prompt_tokens)
                self.total_response_tokens += int(response_tokens)
                self.total_tokens_used      = (
                    self.total_prompt_tokens + self.total_response_tokens
                )

                logger.info(
                    "[SEGMENT-ANALYZER] Gemini call succeeded on attempt %d | request_id=%s",
                    attempt, rid,
                )
                return response.text, int(prompt_tokens), int(response_tokens)

            except Exception as exc:
                logger.error(
                    "[SEGMENT-ANALYZER] Gemini API error on attempt %d: %s | request_id=%s",
                    attempt, exc, rid,
                )
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_delay)

        logger.error(
            "[SEGMENT-ANALYZER] All %d Gemini attempts failed | request_id=%s",
            self.max_retries, rid,
        )
        return None, 0, 0

    # =======================================================================
    # PROMPT BUILDERS
    # =======================================================================
    def _build_auto_prompt(self, page_map: Dict[int, str], total_pages: int) -> str:
        """
        Build the AUTO segmentation prompt.

        Key design: the prompt must make absolutely clear that:
        - Multiple consecutive pages that belong to the same logical document
          MUST be grouped into ONE segment with multiple page numbers.
        - One segment = one entry in the JSON, NOT one entry per page.
        - A concrete worked example is included to prevent Gemini from
          defaulting to one-segment-per-page output.
        """
        page_sections = self._format_pages_for_prompt(page_map)

        prompt = f"""You are a document segmentation expert. A scanned PDF has been converted to text page by page. Your job is to group those pages into logical segments (sub-documents).

CRITICAL UNDERSTANDING -- READ THIS FIRST:
A single logical document (e.g. an Export Invoice) often spans MULTIPLE pages.
You MUST group all pages of the same document into ONE segment entry.
DO NOT create one segment per page. That is WRONG.

CORRECT EXAMPLE for a 7-page PDF containing 4 sub-documents:
  Page 1 = Export Invoice page 1 of 3
  Page 2 = Export Invoice page 2 of 3
  Page 3 = Examination Report
  Page 4 = (empty / blank)
  Page 5 = Annexure I page 1 of 2
  Page 6 = Annexure I page 2 of 2
  Page 7 = Packing List

CORRECT output (4 segments, NOT 7):
{{
  "segments": [
    {{"name": "Export Invoice",       "pages": [1, 2],    "confidence": "high"}},
    {{"name": "Examination Report",   "pages": [3, 4],    "confidence": "high"}},
    {{"name": "Annexure I",           "pages": [5, 6],    "confidence": "high"}},
    {{"name": "Packing List",         "pages": [7],       "confidence": "high"}}
  ]
}}

WRONG output (never do this -- one segment per page is always wrong):
{{
  "segments": [
    {{"name": "Export Invoice page 1", "pages": [1], "confidence": "high"}},
    {{"name": "Export Invoice page 2", "pages": [2], "confidence": "high"}},
    ...
  ]
}}

HOW TO DETECT BOUNDARIES:
- A new logical document starts when you see a NEW title / document type header
  (e.g. "EXPORT INVOICE", "PACKING LIST", "EXAMINATION REPORT", "ANNEXURE").
- Pages that say "Page 2 of 3", "Page 2 OF 3", "Continued", or repeat the same
  header as the previous page belong to the SAME segment -- do NOT split them.
- Empty or blank pages with no meaningful content should be absorbed into the
  segment that immediately precedes or follows them.
- Footer/header repetition, invoice number repetition, and "page X of Y"
  indicators all confirm pages belong together.

DOCUMENT CONTENT (Total pages: {total_pages})
============================================================
{page_sections}
============================================================

TASK: Identify the logical sub-documents. Group all pages of the same sub-document into one segment.

STRICT RULES:
1. Every page from 1 to {total_pages} MUST appear in exactly one segment.
2. NO page may be duplicated across segments.
3. NO page may be missing.
4. Multiple pages of the same logical document MUST share ONE segment entry.
5. Segment names must be meaningful document-type names (e.g. "Export Invoice",
   "Packing List", "Examination Report", "Annexure I") -- NOT "Page 1", "Section 1".
6. confidence: "high" when boundary is clear, "medium" when inferred, "low" when guessed.
7. Output ONLY valid JSON. No markdown fences, no explanation, no preamble.

OUTPUT FORMAT:
{{
  "segments": [
    {{
      "name": "<logical document name>",
      "pages": [<all page numbers belonging to this document>],
      "confidence": "<low|medium|high>"
    }}
  ],
  "metadata": {{
    "total_pages": {total_pages},
    "segmentation_method": "document_boundary",
    "strategy": "AutoDocSegmentationStrategy"
  }}
}}

Output ONLY the JSON object. Nothing before it, nothing after it."""

        return prompt

    def _build_guided_prompt(
        self,
        page_map: Dict[int, str],
        total_pages: int,
        segments: List[Dict],
    ) -> str:
        """
        Build the GUIDED segmentation prompt using structured segment definitions.

        Each segment definition has:
            - name        : the label Gemini must use for that segment
            - description : semantic hint describing what content belongs there

        Key design: same multi-page grouping instruction as AUTO, plus:
        - Segment names are used strictly from the provided definitions
        - Descriptions guide Gemini via semantic understanding, not keyword matching
        - Concrete example shows multi-page grouping with named segments
        """
        page_sections = self._format_pages_for_prompt(page_map)

        # Build numbered segment definition block
        segment_lines: List[str] = []
        for i, seg in enumerate(segments, 1):
            name = str(seg.get("name") or "").strip()
            desc = str(seg.get("description") or "").strip()
            segment_lines.append(f"{i}. {name}")
            segment_lines.append(f"   Description: {desc}")
            segment_lines.append("")
        segment_definitions = "\n".join(segment_lines).rstrip()

        # Build example using first two segment names for illustration
        example_names = [str(s.get("name", f"Segment {i+1}")).strip() for i, s in enumerate(segments)]
        ex_name_1 = example_names[0] if len(example_names) > 0 else "Section A"
        ex_name_2 = example_names[1] if len(example_names) > 1 else "Section B"

        prompt = f"""You are a document segmentation expert. A scanned PDF has been converted to text page by page. Your job is to assign each page to a named segment using the structured segment definitions provided below.

CRITICAL UNDERSTANDING -- READ THIS FIRST:
A single logical document often spans MULTIPLE pages.
You MUST group all pages of the same document into ONE segment entry.
DO NOT create one segment per page. That is WRONG.

CORRECT EXAMPLE for a 5-page PDF using two segment definitions ("{ex_name_1}" and "{ex_name_2}"):
  Page 1 -> "{ex_name_1}"  (matches description of segment 1)
  Page 2 -> "{ex_name_1}"  (same document continues -- same segment)
  Page 3 -> "{ex_name_2}"  (matches description of segment 2)
  Page 4 -> "{ex_name_2}"  (same document -- same segment)
  Page 5 -> "{ex_name_2}"  (continuation)

CORRECT output (2 segments, NOT 5):
{{
  "segments": [
    {{"name": "{ex_name_1}", "pages": [1, 2],    "confidence": "high"}},
    {{"name": "{ex_name_2}", "pages": [3, 4, 5], "confidence": "high"}}
  ]
}}

WRONG output (never do this -- one segment per page is always wrong):
{{
  "segments": [
    {{"name": "{ex_name_1}", "pages": [1], "confidence": "high"}},
    {{"name": "{ex_name_1}", "pages": [2], "confidence": "high"}},
    ...
  ]
}}

HOW TO DETECT PAGE GROUPING:
- Pages saying "Page 2 of 3", "Page 2 OF 3", "Continued", or repeating the same
  header belong to the SAME segment -- always merge them.
- Empty or near-empty pages: absorb into the nearest named segment.
- If the SAME segment fits non-contiguous page ranges, emit TWO separate segment
  entries for that segment name.

DOCUMENT CONTENT (Total pages: {total_pages})
============================================================
{page_sections}
============================================================

USE THE FOLLOWING SEGMENT DEFINITIONS (use ONLY these as segment names):
{segment_definitions}

Rules:
- Assign each page to exactly ONE segment based on the description above
- Do NOT skip pages
- Do NOT duplicate pages
- Use semantic understanding (not exact keyword match)
- Segment names MUST come from the definitions above -- do NOT invent new names
- Consecutive pages belonging to the same logical document MUST share ONE segment entry
- If a page does not clearly match any segment, assign it to the closest one
- confidence: "high" when match is clear, "medium" when inferred, "low" when guessed
- Output ONLY valid JSON. No markdown fences, no explanation, no preamble

OUTPUT FORMAT:
{{
  "segments": [
    {{
      "name": "<segment name from definitions above>",
      "pages": [<all page numbers for this segment>],
      "confidence": "<low|medium|high>"
    }}
  ],
  "metadata": {{
    "total_pages": {total_pages},
    "segmentation_method": "user_guided",
    "strategy": "GuidedSegmentationStrategy"
  }}
}}

Output ONLY the JSON object. Nothing before it, nothing after it."""

        return prompt

    # =======================================================================
    # INTERNAL HELPERS
    # =======================================================================
    def _format_pages_for_prompt(self, page_map: Dict[int, str]) -> str:
        """
        Convert the page_map into a human-readable string for the prompt.

        Each page is presented as:
            <---- Page N ---->
            <content>

        Empty pages are flagged explicitly so Gemini knows the page exists
        even if it has no text.
        """
        parts: List[str] = []

        for page_num in sorted(page_map.keys()):
            content = (page_map[page_num] or "").strip()
            parts.append(f"<---- Page {page_num} ---->")
            if content:
                # Truncate very long pages to avoid hitting token limits while
                # still giving Gemini enough context to make a decision.
                if len(content) > 3000:
                    parts.append(content[:3000])
                    parts.append(
                        f"... [Page {page_num} truncated -- {len(content)} chars total]"
                    )
                else:
                    parts.append(content)
            else:
                parts.append("[empty page -- no text extracted]")
            parts.append("")  # blank separator line

        return "\n".join(parts).strip()

    def _extract_json_from_response(self, response_text: str) -> str:
        """
        Strip markdown fences and extract the outermost JSON object.
        Identical logic to GeminiJSONGenerator._extract_json_from_response().
        """
        text = response_text.strip()

        if text.startswith("```json"):
            text = text[7:].lstrip()
        elif text.startswith("```"):
            text = text[3:].lstrip()

        if text.endswith("```"):
            text = text[:-3].rstrip()

        first_brace = text.find("{")
        last_brace  = text.rfind("}")

        if first_brace != -1 and last_brace != -1 and first_brace < last_brace:
            text = text[first_brace : last_brace + 1]

        return text

    def _normalise_segments(
        self,
        parsed: Dict,
        total_pages: int,
        mode: str,
        request_id: str,
    ) -> List[Dict]:
        """
        Extract, validate, and clean the segments list from the parsed dict.

        Steps
        -----
        1. Locate the segments list in whatever shape Gemini returned it.
        2. Coerce each segment's pages to sorted unique ints; drop out-of-range.
        3. Drop segments with no valid pages after coercion.
        4. Coerce confidence to low / medium / high.
        5. MERGE CONSECUTIVE segments that share the same name into one entry.
           This is the universal safety net for Gemini still emitting one
           entry per page despite prompt instructions. Consecutive same-name
           entries are always wrong and always collapsed here.
           Non-consecutive same-name entries are intentionally preserved
           (guided mode allows "line items" to appear twice for non-adjacent
           page ranges).
        6. Fall back to a single catch-all segment if nothing is usable.
        """
        rid = request_id

        # ------------------------------------------------------------------ #
        # Step 1: locate the segments list                                    #
        # ------------------------------------------------------------------ #
        raw_segments = None

        if isinstance(parsed, dict):
            raw_segments = parsed.get("segments")
        elif isinstance(parsed, list):
            raw_segments = parsed

        if not raw_segments or not isinstance(raw_segments, list):
            logger.warning(
                "[SEGMENT-ANALYZER] No 'segments' key in parsed response -- "
                "falling back to single catch-all segment | request_id=%s",
                rid,
            )
            return self._fallback_single_segment(total_pages, mode)

        valid_page_range = set(range(1, total_pages + 1))
        normalised: List[Dict] = []

        # ------------------------------------------------------------------ #
        # Steps 2-4: coerce each raw segment                                 #
        # ------------------------------------------------------------------ #
        for idx, seg in enumerate(raw_segments):
            if not isinstance(seg, dict):
                logger.warning(
                    "[SEGMENT-ANALYZER] Segment %d is not a dict -- skipping | request_id=%s",
                    idx + 1, rid,
                )
                continue

            # Name
            name = str(seg.get("name") or "").strip()
            if not name:
                name = f"Segment {idx + 1}"
                logger.warning(
                    "[SEGMENT-ANALYZER] Segment %d missing name -- using '%s' | request_id=%s",
                    idx + 1, name, rid,
                )

            # Pages
            raw_pages = seg.get("pages") or []
            if not isinstance(raw_pages, list):
                raw_pages = [raw_pages]

            coerced_pages: List[int] = []
            for p in raw_pages:
                try:
                    pi = int(p)
                    if pi in valid_page_range:
                        coerced_pages.append(pi)
                    else:
                        logger.warning(
                            "[SEGMENT-ANALYZER] Page %d out of range (1-%d) in "
                            "segment '%s' -- dropped | request_id=%s",
                            pi, total_pages, name, rid,
                        )
                except (TypeError, ValueError):
                    logger.warning(
                        "[SEGMENT-ANALYZER] Non-integer page value '%s' in "
                        "segment '%s' -- skipped | request_id=%s",
                        p, name, rid,
                    )

            coerced_pages = sorted(set(coerced_pages))

            if not coerced_pages:
                logger.warning(
                    "[SEGMENT-ANALYZER] Segment '%s' has no valid pages after "
                    "normalisation -- skipping | request_id=%s",
                    name, rid,
                )
                continue

            # Confidence
            confidence = str(seg.get("confidence") or "medium").strip().lower()
            if confidence not in ("low", "medium", "high"):
                logger.warning(
                    "[SEGMENT-ANALYZER] Invalid confidence '%s' in segment "
                    "'%s' -- defaulting to 'medium' | request_id=%s",
                    confidence, name, rid,
                )
                confidence = "medium"

            normalised.append(
                {
                    "name":       name,
                    "pages":      coerced_pages,
                    "confidence": confidence,
                }
            )

        if not normalised:
            logger.warning(
                "[SEGMENT-ANALYZER] All segments were invalid after normalisation -- "
                "falling back to single catch-all segment | request_id=%s",
                rid,
            )
            return self._fallback_single_segment(total_pages, mode)

        # ------------------------------------------------------------------ #
        # Step 5: merge CONSECUTIVE segments that share the same name.       #
        #                                                                     #
        # This collapses the common Gemini failure mode of emitting one      #
        # entry per page, e.g.:                                               #
        #   {"name":"Export Invoice","pages":[1],"confidence":"high"}        #
        #   {"name":"Export Invoice","pages":[2],"confidence":"high"}        #
        # into the correct single entry:                                      #
        #   {"name":"Export Invoice","pages":[1,2],"confidence":"high"}      #
        #                                                                     #
        # Non-consecutive same-name segments are deliberately NOT merged --  #
        # guided mode allows a keyword like "line items" to appear twice     #
        # for genuinely separate non-adjacent page ranges.                   #
        # ------------------------------------------------------------------ #
        confidence_rank = {"low": 0, "medium": 1, "high": 2}
        merged: List[Dict] = []

        for seg in normalised:
            if (
                merged
                and merged[-1]["name"].lower() == seg["name"].lower()
            ):
                # Consecutive same-name entry -- absorb into previous
                combined_pages = sorted(set(merged[-1]["pages"] + seg["pages"]))
                prev_rank = confidence_rank.get(merged[-1]["confidence"], 1)
                curr_rank = confidence_rank.get(seg["confidence"], 1)
                best_confidence = (
                    merged[-1]["confidence"] if prev_rank >= curr_rank
                    else seg["confidence"]
                )
                merged[-1]["pages"]      = combined_pages
                merged[-1]["confidence"] = best_confidence
                logger.info(
                    "[SEGMENT-ANALYZER] Merged consecutive '%s' entries into "
                    "pages=%s | request_id=%s",
                    seg["name"], combined_pages, rid,
                )
            else:
                merged.append(
                    {
                        "name":       seg["name"],
                        "pages":      list(seg["pages"]),
                        "confidence": seg["confidence"],
                    }
                )

        logger.info(
            "[SEGMENT-ANALYZER] Normalisation complete: %d raw -> %d merged "
            "segments | request_id=%s",
            len(normalised), len(merged), rid,
        )

        return merged

    def _fallback_single_segment(self, total_pages: int, mode: str) -> List[Dict]:
        """
        Return a single catch-all segment that covers all pages.
        Used when Gemini returns unusable output.
        """
        label = "auto_full_document" if mode == "auto" else "guided_full_document"
        return [
            {
                "name":       label,
                "pages":      list(range(1, total_pages + 1)),
                "confidence": "low",
            }
        ]

    def _error_result(
        self, message: str, page_map: Dict[int, str], mode: str
    ) -> Dict:
        """
        Return a structured error response that still satisfies the
        endpoint's expected output shape.
        """
        total_pages = len(page_map)

        if mode == "auto":
            strategy   = "AutoDocSegmentationStrategy"
        else:
            strategy   = "GuidedSegmentationStrategy"

        return {
            "segments": self._fallback_single_segment(total_pages, mode),
            "metadata": {
                "total_pages":         total_pages,
                "strategy":            strategy,
                "error":               message,
            },
        }

    def _reset_token_counters(self):
        """Reset per-request token counters."""
        self.total_prompt_tokens   = 0
        self.total_response_tokens = 0
        self.total_tokens_used     = 0

    @staticmethod
    def _load_yaml_config() -> Dict:
        """
        Load config.yaml -- same path and pattern as
        GeminiJSONGenerator._load_config().
        """
        try:
            with open("config/config.yaml", "r") as fh:
                cfg = yaml.safe_load(fh)
                return cfg if cfg else {}
        except Exception:
            return {}