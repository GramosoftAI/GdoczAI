# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Gemini JSON generation integration for OCR Server.

Routing logic (select_full_document_model):
- 0 - 20,000 chars      -> Qwen2.5-7B  (DeepInfra)
- 20,001 - 30,000 chars -> Gemini 2.0 Flash
- > 30,000 chars        -> Gemini 2.5 Flash

Chunk-level rule:
- ALL chunks always use Qwen2.5-7B regardless of full-document model
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

# Import JSON parser
from src.services.ocr_pipeline.ocr_server_json_parser import RobustJSONParser
from src.services.ocr_pipeline.ocr_server_post_processor import GenericPostProcessor

# Import heavy methods mixin
from src.services.ocr_pipeline.ocr_server_gemini2 import GeminiHeavyMethods


# ============================================================================
# MODEL ROUTING FUNCTION
# ============================================================================

def select_full_document_model(markdown_length: int) -> str:
    
    logger.info("=" * 80)
    logger.info(" MODEL ROUTING - FULL DOCUMENT")
    logger.info(f" Markdown length: {markdown_length} characters")

    if markdown_length <= 20000:
        selected = "qwen"
        reason = "<= 20,000 chars -> Qwen2.5-7B (DeepInfra)"
    elif markdown_length <= 30000:
        selected = "gemini-2.0"
        reason = "20,001-30,000 chars -> Gemini 2.0 Flash"
    else:
        selected = "gemini-2.5"
        reason = "> 30,000 chars -> Gemini 2.5 Flash"

    logger.info(f" Selected model : {selected}")
    logger.info(f" Reason         : {reason}")
    logger.info("=" * 80)

    return selected


# ============================================================================
# GEMINI MODEL NAME CONSTANTS
# ============================================================================

GEMINI_MODEL_2_0 = "gemini-2.0-flash"
GEMINI_MODEL_2_5 = "gemini-2.5-flash"


# ============================================================================
# GEMINI JSON GENERATOR CLASS (LIGHTWEIGHT + HEAVY METHODS INHERITED)
# ============================================================================
class GeminiJSONGenerator(GeminiHeavyMethods):
    """Generates JSON from markdown using Gemini API with support for manual splitting."""

    def __init__(self, config, chunker, gemini_available):
        # Load configuration from config.yaml
        cfg = self._load_config()
        self.config = config
        self.chunker = chunker
        self.gemini_available = gemini_available

        # Try to get API key from config.yaml first, then fall back to environment variable
        self.api_key = cfg.get('gemini', {}).get('api_key') or os.getenv('GEMINI_API_KEY')
        self.model_name = cfg.get('gemini', {}).get('model', GEMINI_MODEL_2_0)
        self.timeout = cfg.get('gemini', {}).get('timeout_seconds', 60)
        self.max_retries = cfg.get('gemini', {}).get('max_retries', 3)
        self.retry_delay = cfg.get('gemini', {}).get('retry_delay_seconds', 2)
        self.temperature = cfg.get('gemini', {}).get('temperature', 0.1)
        self.max_tokens = cfg.get('gemini', {}).get('max_tokens', 8192)
        self.universal_chunk_prompt = self._load_universal_chunk_prompt()
        self.universal_single_prompt = self._load_universal_single_prompt()
        self.post_processor = GenericPostProcessor()
        logger.info("[OK] Generic post-processor initialized for ALL document types")

        #  Import langchain splitters if available
        self.langchain_available = False
        try:
            from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
            self.MarkdownHeaderTextSplitter = MarkdownHeaderTextSplitter
            self.RecursiveCharacterTextSplitter = RecursiveCharacterTextSplitter
            self.langchain_available = True
            logger.info("[OK] Langchain text splitters loaded for custom splitting")
        except ImportError:
            logger.warning("[WARN] Langchain not available - will use Unstructured chunker only")

        #  Import Unstructured chunker
        self.unstructured_available = False
        try:
            from unstructured.partition.auto import partition
            from unstructured.chunking.title import chunk_by_title
            self.unstructured_partition = partition
            self.unstructured_chunk_by_title = chunk_by_title
            self.unstructured_available = True
            logger.info("[OK] Unstructured chunker loaded for semantic chunking")
        except ImportError:
            logger.warning("[WARN] Unstructured not available - install with: pip install unstructured")

        self.total_prompt_tokens = 0
        self.total_response_tokens = 0
        self.total_tokens_used = 0

        # ========================================================================
        #  ENHANCED GEMINI INITIALIZATION WITH DETAILED DEBUGGING
        # ========================================================================

        logger.info("=" * 80)
        logger.info("Gemini API configured")
        logger.info("=" * 80)

        # Initialize Gemini
        if self.api_key and gemini_available:
            try:
                import google.generativeai as genai
                from google.generativeai.types import HarmCategory, HarmBlockThreshold

                logger.info("Configuring Gemini API...")
                genai.configure(api_key=self.api_key)

                logger.info(f" Creating GenerativeModel: {self.model_name}")
                self.model = genai.GenerativeModel(self.model_name)

                self.genai = genai
                self.HarmCategory = HarmCategory
                self.HarmBlockThreshold = HarmBlockThreshold
                self.enabled = True

                logger.info("=" * 80)
                logger.info("[OK] GEMINI API INITIALIZED SUCCESSFULLY")
                logger.info(f"   Model      : {self.model_name}")
                logger.info(f"   Temperature: {self.temperature}")
                logger.info(f"   Max Tokens : {self.max_tokens}")
                logger.info("=" * 80)

            except ImportError as e:
                self.enabled = False
                self.model = None
                logger.error("=" * 80)
                logger.error("[ERR] GEMINI INITIALIZATION FAILED - ImportError")
                logger.error(f"   Error: {e}")
                logger.error("   Install: pip install google-generativeai")
                logger.error("=" * 80)

            except Exception as e:
                self.enabled = False
                self.model = None
                logger.error("=" * 80)
                logger.error("[ERR] GEMINI INITIALIZATION FAILED")
                logger.error(f"   Error: {e}")
                logger.error("=" * 80)
        else:
            self.enabled = False
            self.model = None

            logger.warning("=" * 80)
            logger.warning("[WARN] GEMINI JSON GENERATION DISABLED")

            if not self.api_key:
                logger.warning("   Reason: API key not found")
                logger.warning("   Solutions:")
                logger.warning("   1. Add to config.yaml:")
                logger.warning("      gemini:")
                logger.warning("        api_key: 'your-api-key-here'")
                logger.warning("   2. Or set environment variable:")
                logger.warning("      export GEMINI_API_KEY='your-api-key-here'")

            if not gemini_available:
                logger.warning("   Reason: google-generativeai library not available")
                logger.warning("   Solution: pip install google-generativeai")

            logger.warning("=" * 80)

    # =========================================================================
    # SWITCH ACTIVE GEMINI MODEL AT RUNTIME
    # =========================================================================
    def switch_model(self, model_name: str) -> bool:
        
        if not self.enabled:
            logger.warning("[WARN] Cannot switch model - Gemini not enabled")
            return False

        if self.model_name == model_name:
            logger.info(f"[OK] Gemini model already set to: {model_name} (no switch needed)")
            return True

        try:
            logger.info(f" Switching Gemini model: {self.model_name} -> {model_name}")
            self.model = self.genai.GenerativeModel(model_name)
            self.model_name = model_name
            logger.info(f"[OK] Gemini model switched to: {self.model_name}")
            return True
        except Exception as e:
            logger.error(f"[ERR] Failed to switch Gemini model to {model_name}: {e}")
            return False

    # =========================================================================
    # APPLY ROUTING DECISION TO THIS GENERATOR
    # =========================================================================
    def apply_routing(self, markdown_length: int) -> str:
        
        model_choice = select_full_document_model(markdown_length)

        if model_choice == "gemini-2.0":
            self.switch_model(GEMINI_MODEL_2_0)
        elif model_choice == "gemini-2.5":
            self.switch_model(GEMINI_MODEL_2_5)
        # "qwen" -> caller will route to QwenJSONGenerator, no action needed here

        return model_choice

    # =========================================================================
    # CONFIG + PROMPT HELPERS
    # =========================================================================
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

    # =========================================================================
    # PROMPT BUILDERS (used by GeminiHeavyMethods via inheritance)
    # =========================================================================
    def _create_chunk_extraction_prompt(
        self,
        markdown_content,
        document_type,
        chunk_num=1,
        total_chunks=1,
        schema_json=None
    ):
        import json

        # [OK] PATH A: Schema exists -> Use schema-based extraction
        if schema_json:
            logger.info(f"[OK] Using schema-based extraction for {document_type}")
            prompt = (
                f"Extract structured data from this markdown chunk "
                f"({chunk_num}/{total_chunks}).\n\n"
                f"Document Type: {document_type}\n\n"
                f"Use this JSON schema as the structure:\n"
                f"{json.dumps(schema_json, indent=2)}\n\n"
                f"Markdown Content:\n{markdown_content}\n\n"
                f"Return ONLY valid JSON matching the schema structure."
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
        import json

        if schema_json:
            return f"Extract data matching schema...\n{json.dumps(schema_json)}"

        else:
            if not self.universal_single_prompt:
                logger.warning("Universal SINGLE prompt missing, using fallback.")
                return f"Extract JSON:\n{markdown_content}"

            prompt = str(self.universal_single_prompt)
            prompt = prompt.replace("<<DOCUMENT_TYPE>>", document_type)
            prompt = prompt.replace("<<MARKDOWN_CONTENT>>", markdown_content)
            return prompt

    # =========================================================================
    # JSON RESPONSE CLEANER (used by GeminiHeavyMethods via inheritance)
    # =========================================================================
    def _extract_json_from_response(self, response_text: str) -> str:
        """
        Extract JSON content from response, handling markdown fences and extra text.
        """
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