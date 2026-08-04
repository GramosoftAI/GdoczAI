# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Gemini JSON generation integration for OCR Server.
Exclusively uses Gemini 2.5 Flash for optimal context limits.
Split architecture to keep line count per file low.
"""

import json
import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

# Import JSON parser & utilities
from src.services.sundarams.sundarams_ocr_server_json_parser import RobustJSONParser
from src.services.sundarams.sundarams_ocr_server_post_processor import GenericPostProcessor
from src.services.sundarams.sundarams_ocr_server_manual_splitter import process_oversized_chunks

# Import heavy methods mixin
from src.services.sundarams.sundarams_ocr_server_gemini2 import GeminiHeavyMethods

# ============================================================================
# GEMINI MODEL NAME CONSTANTS
# ============================================================================
GEMINI_MODEL_2_5 = "gemini-2.5-flash"


def select_full_document_model(markdown_length: int) -> str:
    """Retained for backward compatibility. Always returns Gemini 2.5 Flash."""
    return GEMINI_MODEL_2_5


# ============================================================================
# GEMINI JSON GENERATOR CLASS (LIGHTWEIGHT + HEAVY METHODS INHERITED)
# ============================================================================
class GeminiJSONGenerator(GeminiHeavyMethods):
    """Generates JSON from markdown using Gemini API (exclusively gemini-2.5-flash) with support for manual splitting."""

    def __init__(self, config, chunker, gemini_available):
        # Load configuration from config.yaml
        cfg = self._load_config()
        self.config = config
        self.chunker = chunker
        self.gemini_available = gemini_available

        # Try to get API key from config.yaml first, then fall back to environment variable
        self.api_key = cfg.get('gemini', {}).get('api_key') or os.getenv('GEMINI_API_KEY')
        self.model_name = cfg.get('gemini', {}).get('model', GEMINI_MODEL_2_5)
        self.timeout = cfg.get('gemini', {}).get('timeout_seconds', 90)
        self.max_retries = cfg.get('gemini', {}).get('max_retries', 3)
        self.retry_delay = cfg.get('gemini', {}).get('retry_delay_seconds', 2)
        self.temperature = cfg.get('gemini', {}).get('temperature', 0.1)
        self.max_tokens = cfg.get('gemini', {}).get('max_tokens', 65536)
        self.universal_chunk_prompt = self._load_universal_chunk_prompt()
        self.universal_single_prompt = self._load_universal_single_prompt()
        self.post_processor = GenericPostProcessor()

        # Import langchain splitters if available
        self.langchain_available = False
        try:
            from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
            self.MarkdownHeaderTextSplitter = MarkdownHeaderTextSplitter
            self.RecursiveCharacterTextSplitter = RecursiveCharacterTextSplitter
            self.langchain_available = True
        except ImportError:
            logger.warning("[WARN] Langchain not available - will use Unstructured chunker only")

        # Import Unstructured chunker
        self.unstructured_available = False
        try:
            from unstructured.partition.auto import partition
            from unstructured.chunking.title import chunk_by_title
            self.unstructured_partition = partition
            self.unstructured_chunk_by_title = chunk_by_title
            self.unstructured_available = True
        except ImportError:
            logger.warning("[WARN] Unstructured not available - install with: pip install unstructured")

        self.total_prompt_tokens = 0
        self.total_response_tokens = 0
        self.total_tokens_used = 0

        # Initialize Gemini client
        if self.api_key and gemini_available:
            try:
                from google import genai
                from google.genai import types

                self.client = genai.Client(api_key=self.api_key)
                self.model = self.model_name
                self.genai = genai
                self.types = types
                self.HarmCategory = types.HarmCategory
                self.HarmBlockThreshold = types.HarmBlockThreshold
                self.enabled = True
            except ImportError as e:
                self.enabled = False
                self.model = None
                self.client = None
                logger.error("=" * 80)
                logger.error("[ERR] GEMINI INITIALIZATION FAILED - ImportError")
                logger.error(f"   Error: {e}")
                logger.error("   Install: pip install google-genai")
                logger.error("=" * 80)
            except Exception as e:
                self.enabled = False
                self.model = None
                self.client = None
                logger.error("=" * 80)
                logger.error("[ERR] GEMINI INITIALIZATION FAILED")
                logger.error(f"   Error: {e}")
                logger.error("=" * 80)
        else:
            self.enabled = False
            self.model = None
            self.client = None
            logger.warning("=" * 80)
            logger.warning("[WARN] GEMINI JSON GENERATION DISABLED")

            if not self.api_key:
                logger.warning("   Reason: API key not found")
            if not gemini_available:
                logger.warning("   Reason: google-genai library not available")
            logger.warning("=" * 80)

    def switch_model(self, model_name: str) -> bool:
        """Retained for backward compatibility. Model remains gemini-2.5-flash."""
        logger.info(f"Using exclusive gemini-2.5-flash model (switching request to {model_name} ignored)")
        return True

    def apply_routing(self, markdown_length: int) -> str:
        """Retained for backward compatibility. Always returns Gemini 2.5 Flash."""
        return GEMINI_MODEL_2_5

    def _load_config(self) -> Dict:
        """Load configuration from config.yaml"""
        try:
            with open('config/config.yaml', 'r') as f:
                cfg = yaml.safe_load(f)
                return cfg if cfg else {}
        except Exception:
            return {}

    def reset_token_counters(self):
        """Reset token counters for new request"""
        self.total_prompt_tokens = 0
        self.total_response_tokens = 0
        self.total_tokens_used = 0
        logger.info(" Gemini token counters reset")

    def _load_universal_single_prompt(self):
        prompt_path = Path("prompts/UNIVERSAL_SINGLE_PROMPT.txt")
        try:
            with open(prompt_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Failed to load universal single-document prompt: {e}")
            return None

    def _load_universal_chunk_prompt(self):
        prompt_path = Path("prompts/UNIVERSAL_CHUNK_PROMPT.txt")
        try:
            with open(prompt_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Failed to load universal chunk prompt: {e}")
            return None

    def _create_chunk_extraction_prompt(
        self,
        markdown_content,
        document_type,
        chunk_num=1,
        total_chunks=1,
        schema_json=None
    ):
        # [OK] PATH A: Schema exists -> Use schema-based extraction
        if schema_json:
            # Flatten schema if it has "fields" list format
            if isinstance(schema_json, dict) and "fields" in schema_json and isinstance(schema_json["fields"], list):
                flat_schema = {}
                for field in schema_json["fields"]:
                    if isinstance(field, dict) and "field_name" in field:
                        if "properties" in field and isinstance(field["properties"], list):
                            nested_schema = {}
                            for prop in field["properties"]:
                                if isinstance(prop, dict) and "field_name" in prop:
                                    nested_schema[prop["field_name"]] = prop.get("description") or prop.get("value") or ""
                            # Format nested properties as an array of objects to indicate multiple line items
                            flat_schema[field["field_name"]] = [nested_schema]
                        else:
                            flat_schema[field["field_name"]] = field.get("description") or field.get("value") or ""
                # Preserve any other root-level keys (like expense/line_items)
                for k, v in schema_json.items():
                    if k != "fields":
                        flat_schema[k] = v
                schema_json = flat_schema

            logger.info(f"[OK] Using schema-based extraction for {document_type}")
            prompt = (
                f"Extract structured data from this markdown chunk "
                f"({chunk_num}/{total_chunks}).\n\n"
                f"Document Type: {document_type}\n\n"
                f"Use this JSON schema as the expected output structure:\n"
                f"{json.dumps(schema_json, indent=2)}\n\n"
                f"Markdown Content:\n{markdown_content}\n\n"
                f"CRITICAL: Return ONLY valid JSON matching the exact structure of the provided JSON schema. "
                f"If the schema contains nested arrays or objects (e.g. line items or expenses), you MUST output them strictly as arrays/objects. "
                f"IMPORTANT: Map all line items, products, and services found in the invoice tables into the appropriate array (e.g., 'expense').\n"
                f"Do not flatten nested structures if they are defined as arrays in the schema.\n"
                f"For amount-related fields, DO NOT include commas (e.g., output '47000' instead of '47,000').\n"
                f"DO NOT return null values; if a value is missing, use an empty string \"\" for strings and an empty array [] for arrays."
            )
            return prompt

        # [OK] PATH B: No schema -> Use universal prompt
        else:
            logger.info(f"[WARN] No schema - using universal prompt for {document_type}")

            if not self.universal_chunk_prompt:
                logger.warning("Universal chunk prompt missing, using fallback.")
                return f"Extract JSON:\n{markdown_content}"

            prompt = str(self.universal_chunk_prompt)
            prompt = prompt.replace("{chunk_num}", str(chunk_num))
            prompt = prompt.replace("{total_chunks}", str(total_chunks))
            prompt = prompt.replace("<<DOCUMENT_TYPE>>", document_type)
            prompt = prompt.replace("<<MARKDOWN_CONTENT>>", markdown_content)
            return prompt

    def _create_single_document_extraction_prompt(
        self,
        markdown_content: str,
        document_type: str,
        schema_json=None
    ) -> str:
        if schema_json:
            # Flatten schema if it has "fields" list format
            if isinstance(schema_json, dict) and "fields" in schema_json and isinstance(schema_json["fields"], list):
                flat_schema = {}
                for field in schema_json["fields"]:
                    if isinstance(field, dict) and "field_name" in field:
                        if "properties" in field and isinstance(field["properties"], list):
                            nested_schema = {}
                            for prop in field["properties"]:
                                if isinstance(prop, dict) and "field_name" in prop:
                                    nested_schema[prop["field_name"]] = prop.get("description") or prop.get("value") or ""
                            # Format nested properties as an array of objects to indicate multiple line items
                            flat_schema[field["field_name"]] = [nested_schema]
                        else:
                            flat_schema[field["field_name"]] = field.get("description") or field.get("value") or ""
                # Preserve any other root-level keys (like expense/line_items)
                for k, v in schema_json.items():
                    if k != "fields":
                        flat_schema[k] = v
                schema_json = flat_schema

            prompt = (
                f"Extract structured data from the provided markdown document.\n\n"
                f"Document Type: {document_type}\n\n"
                f"Use this JSON schema as the expected output structure:\n"
                f"{json.dumps(schema_json, indent=2)}\n\n"
                f"Markdown Content:\n{markdown_content}\n\n"
                """==============================================================================
STRICT OUTPUT RULES (NON-NEGOTIABLE)
==============================================================================

Return ONLY a single valid JSON object.

The JSON structure MUST match the provided schema EXACTLY.

DO NOT:
- flatten nested objects
- flatten arrays
- move child fields to the root level
- rename keys
- remove keys
- add extra keys
- convert objects into strings
- convert arrays into strings
- include commas in amount-related fields (e.g., output "47000" instead of "47,000")
- return null values (use "" for missing strings, and [] for missing arrays)

If a field is defined as an OBJECT, it MUST remain an OBJECT.

If a field is defined as an ARRAY OF OBJECTS, it MUST remain an ARRAY containing one or more OBJECTS.

For example, if the schema contains:

"expense": [
  {
    "amount": "",
    "custcol_in_scode_tds": "",
    "custcol_in_hsn_code": ""
  }
]

then your output MUST look like:

"expense": [
  {
    "amount": "...",
    "custcol_in_scode_tds": "...",
    "custcol_in_hsn_code": "..."
  }
]

It MUST NEVER become:

"expense":"{...}"

It MUST NEVER become:

"amount":"...",
"custcol_in_scode_tds":"...",
"custcol_in_hsn_code":"..."

These fields MUST exist ONLY inside the expense object.

Every nested property belongs ONLY inside its parent object.

IMPORTANT: The "expense" array (or any array of objects in the schema) must contain ALL line items, products, or services found in the document's tables, even if they are labeled as "Item Description", "Particulars", "Services", etc. Do not leave it empty if there are items in the invoice.

If you cannot determine a value for a string field, return an empty string ("").
If there are no items for an array field (like expense), return an empty array ([]).

Return ONLY JSON.
No markdown.
No explanation.
No comments.
No surrounding text.

ANY OUTPUT THAT DOES NOT EXACTLY MATCH THE PROVIDED JSON STRUCTURE IS INVALID."""
            )
            return prompt

        else:
            if not self.universal_single_prompt:
                logger.warning("Universal SINGLE prompt missing, using fallback.")
                return f"Extract JSON:\n{markdown_content}"

            prompt = str(self.universal_single_prompt)
            prompt = prompt.replace("<<DOCUMENT_TYPE>>", document_type)
            prompt = prompt.replace("<<MARKDOWN_CONTENT>>", markdown_content)
            return prompt

    def _extract_json_from_response(self, response_text: str) -> str:
        """Extract JSON content from response, handling markdown fences and extra text."""
        response_text = response_text.strip()

        # Remove markdown code fences
        if response_text.startswith('```json'):
            response_text = response_text[7:].lstrip()
        elif response_text.startswith('```'):
            response_text = response_text[3:].lstrip()

        if response_text.endswith('```'):
            response_text = response_text[:-3].rstrip()

        # Find JSON structure (first { to last })
        first_brace = response_text.find('{')
        last_brace = response_text.rfind('}')

        if first_brace != -1 and last_brace != -1 and first_brace < last_brace:
            response_text = response_text[first_brace:last_brace + 1]

        return response_text