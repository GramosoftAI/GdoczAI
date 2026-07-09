# -*- coding: utf-8 -*-
"""
royal_tech_json_parser.py - Ultra-robust JSON parser for the Royal Tech
Invoice Extraction pipeline.

Direct refactor of mineru_ocr_server_json_parser.py:
  * Module renamed; class RobustJSONParser preserved exactly (same public API)
  * All 5 repair strategies preserved verbatim -- their logic must never change
    silently as batch_extractor depends on them for LLM output recovery
  * Emoji log characters replaced with plain-text equivalents for clean logs

Public API (unchanged)
----------
    from royal_tech_json_parser import RobustJSONParser

    parsed = RobustJSONParser.clean_and_parse(raw_text)   -> dict
    clean  = RobustJSONParser.recursive_clean_values(obj) -> dict | list | str | any
"""

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================================================
# RobustJSONParser
# ============================================================================

class RobustJSONParser:
    """
    Ultra-robust JSON parser with 5 progressive repair strategies.

    Handles malformed JSON output from LLMs by applying strategies in order:
    1. Normal   - direct json.loads
    2. Basic    - strip control chars + markdown fences, extract {...} block
    3. Advanced - fix escape sequences, newlines-in-strings, trailing commas
    4. Aggressive - rebuild structure: unquoted keys, single?double quotes,
                    balance braces/brackets
    5. Partial  - regex-extract whatever key-value pairs can be salvaged

    All methods are static; the class has no instance state.
    """

    # ------------------------------------------------------------------
    # Primary entry point
    # ------------------------------------------------------------------

    @staticmethod
    def clean_and_parse(json_text: str) -> dict:
        """
        Try all 5 repair strategies in order and return the first success.

        Parameters
        ----------
        json_text : str
            Raw text from an LLM response (possibly malformed JSON).

        Returns
        -------
        dict
            Parsed dictionary on success, or an error-sentinel dict:
            {"status": "error", "message": "...", "raw_content": "..."}
        """
        # Strategy 1 - direct parse
        try:
            parsed = json.loads(json_text)
            logger.info("OK Strategy 1: normal JSON parsing succeeded")
            return parsed
        except json.JSONDecodeError as exc:
            logger.warning("Strategy 1 failed: %s", str(exc)[:100])

        # Strategy 2 - basic cleaning
        try:
            cleaned = RobustJSONParser._basic_clean(json_text)
            parsed  = json.loads(cleaned)
            logger.info("OK Strategy 2: basic cleaning succeeded")
            return parsed
        except json.JSONDecodeError as exc:
            logger.warning("Strategy 2 failed: %s", str(exc)[:100])

        # Strategy 3 - advanced repair
        try:
            repaired = RobustJSONParser._advanced_repair(json_text)
            parsed   = json.loads(repaired)
            logger.info("OK Strategy 3: advanced repair succeeded")
            return parsed
        except json.JSONDecodeError as exc:
            logger.warning("Strategy 3 failed: %s", str(exc)[:100])

        # Strategy 4 - aggressive repair
        try:
            aggressive = RobustJSONParser._aggressive_repair(json_text)
            parsed     = json.loads(aggressive)
            logger.info("OK Strategy 4: aggressive repair succeeded")
            return parsed
        except json.JSONDecodeError as exc:
            logger.warning("Strategy 4 failed: %s", str(exc)[:100])

        # Strategy 5 - extract valid portions
        try:
            partial = RobustJSONParser._extract_valid_json_portions(json_text)
            if partial:
                logger.info(
                    "OK Strategy 5: partial extraction succeeded (%d field(s))",
                    len(partial),
                )
                return partial
        except Exception as exc:
            logger.error("Strategy 5 failed: %s", exc)

        # All strategies failed
        logger.error("FAIL All 5 repair strategies failed for input (first 200 chars): %s",
                     json_text[:200])
        return {
            "status":      "error",
            "message":     "JSON parsing failed after all repair attempts",
            "raw_content": json_text[:500],
        }

    # ------------------------------------------------------------------
    # Strategy 2 - basic cleaning
    # ------------------------------------------------------------------

    @staticmethod
    def _basic_clean(text: str) -> str:
        """
        Remove control characters, strip markdown fences, and extract
        the outermost {...} block.
        """
        # Remove NULL and other control characters (keep \\n \\r \\t)
        cleaned = "".join(
            ch for ch in text if ord(ch) >= 32 or ch in "\n\r\t"
        )

        # Strip markdown code fences
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$",          "", cleaned)

        # Extract from first { to last }
        first = cleaned.find("{")
        last  = cleaned.rfind("}")
        if first != -1 and last != -1:
            cleaned = cleaned[first : last + 1]

        return cleaned

    # ------------------------------------------------------------------
    # Strategy 3 - advanced repair
    # ------------------------------------------------------------------

    @staticmethod
    def _advanced_repair(text: str) -> str:
        """
        Walk the string character-by-character to fix:
        * Invalid escape sequences inside strings
        * Raw newline/CR/tab characters inside JSON strings
        * Trailing commas before } or ]
        * Multiple consecutive commas
        * Missing commas between adjacent string tokens or objects/arrays
        """
        result: list[str] = []
        in_string   = False
        escape_next = False
        i = 0

        while i < len(text):
            ch = text[i]

            # -- Handle pending escape --
            if escape_next:
                if ch not in '"\\/bfnrtu':
                    # Invalid escape -- drop the backslash, keep the char
                    result.append(ch)
                else:
                    result.append("\\")
                    result.append(ch)
                escape_next = False
                i += 1
                continue

            # -- Backslash --
            if ch == "\\":
                if in_string:
                    escape_next = True
                # Outside a string: skip stray backslash
                i += 1
                continue

            # -- Quote: toggle string mode --
            if ch == '"':
                in_string = not in_string
                result.append(ch)
                i += 1
                continue

            # -- Characters inside a string --
            if in_string:
                if ch == "\n":
                    result.append("\\n")
                elif ch == "\r":
                    result.append("\\r")
                elif ch == "\t":
                    result.append("\\t")
                elif ord(ch) < 32:
                    pass  # skip other control chars
                else:
                    result.append(ch)
                i += 1
                continue

            result.append(ch)
            i += 1

        repaired = "".join(result)

        # Fix trailing commas before closing bracket/brace
        repaired = re.sub(r",(\s*[}\]])", r"\1", repaired)
        # Collapse multiple commas
        repaired = re.sub(r",\s*,+", ",", repaired)
        # Fix missing commas between adjacent string tokens
        repaired = re.sub(r'"\s+"', '", "', repaired)
        # Fix missing commas between adjacent objects/arrays
        repaired = re.sub(r"}\s*{", "}, {", repaired)
        repaired = re.sub(r"]\s*\[", "], [", repaired)

        return repaired

    # ------------------------------------------------------------------
    # Strategy 4 - aggressive repair
    # ------------------------------------------------------------------

    @staticmethod
    def _aggressive_repair(text: str) -> str:
        """
        Apply advanced repair then additionally:
        * Quote unquoted keys   { key: ... } ? { "key": ... }
        * Replace single quotes with double quotes outside strings
        * Balance unclosed braces and brackets
        * Strip trailing commas before closing delimiters
        """
        text = RobustJSONParser._advanced_repair(text)

        # Quote unquoted keys
        text = re.sub(r'\{\s*(\w+)\s*:',  r'{"\\1":', text)
        text = re.sub(r',\s*(\w+)\s*:',   r', "\\1":', text)

        # Replace single quotes with double quotes (outside already-quoted strings)
        parts: list[str] = []
        in_string = False
        for ch in text:
            if ch == '"':
                in_string = not in_string
                parts.append(ch)
            elif ch == "'" and not in_string:
                parts.append('"')
            else:
                parts.append(ch)
        text = "".join(parts)

        # Balance unclosed braces
        open_b  = text.count("{")
        close_b = text.count("}")
        if open_b > close_b:
            text += "}" * (open_b - close_b)

        # Balance unclosed brackets
        open_br  = text.count("[")
        close_br = text.count("]")
        if open_br > close_br:
            text += "]" * (open_br - close_br)

        # Final trailing-comma sweep
        text = re.sub(r",(\s*[}\]])", r"\1", text)

        return text

    # ------------------------------------------------------------------
    # Strategy 5 - extract valid portions
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_valid_json_portions(text: str) -> Optional[dict]:
        """
        Last-resort: scan for "key": value patterns and build a dict from
        whatever can be cleanly parsed.  Returns None if nothing is found.
        """
        result: dict = {}

        # Pattern: "key": <value up to the next delimiter>
        pattern = r'"([^"]+)"\s*:\s*([^,}\]]+(?:[,}\]])?)'
        for match in re.finditer(pattern, text):
            key       = match.group(1)
            value_str = match.group(2).rstrip(",}]").strip()

            # Try to parse the value as JSON first
            try:
                result[key] = json.loads(value_str)
                continue
            except (json.JSONDecodeError, ValueError):
                pass

            # Quoted string
            if value_str.startswith('"') and value_str.endswith('"'):
                result[key] = value_str[1:-1]
                continue

            # Number
            if value_str.replace(".", "", 1).replace("-", "", 1).isdigit():
                try:
                    result[key] = (
                        float(value_str) if "." in value_str else int(value_str)
                    )
                    continue
                except ValueError:
                    pass

            # Boolean
            if value_str.lower() in ("true", "false"):
                result[key] = value_str.lower() == "true"
                continue

            # Null
            if value_str.lower() == "null":
                result[key] = None
                continue

            # Fallback: keep as raw string
            result[key] = value_str

        if result:
            logger.info(
                "Extracted %d key-value pair(s) from malformed JSON", len(result)
            )
            return result

        return None

    # ------------------------------------------------------------------
    # Post-parse value cleaner
    # ------------------------------------------------------------------

    @staticmethod
    def recursive_clean_values(obj):
        """
        Recursively walk a parsed JSON object and strip control characters
        from every string value.

        Parameters
        ----------
        obj : dict | list | str | any
            The parsed JSON structure (or a fragment of it).

        Returns
        -------
        The same structure with all string values cleaned.
        """
        if isinstance(obj, dict):
            return {
                key: RobustJSONParser.recursive_clean_values(val)
                for key, val in obj.items()
            }
        if isinstance(obj, list):
            return [RobustJSONParser.recursive_clean_values(item) for item in obj]
        if isinstance(obj, str):
            return "".join(
                ch for ch in obj if ord(ch) >= 32 or ch in "\n\r\t"
            )
        return obj