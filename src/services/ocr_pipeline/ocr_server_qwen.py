# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Qwen2.5-7B JSON generation integration via DeepInfra OpenAI-compatible API.
Used for:
- Full document extraction when markdown length is 020,000 characters
- ALL chunk-level extractions regardless of full-document model
"""
import asyncio
import json
import logging
import yaml
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

from src.services.ocr_pipeline.ocr_server_json_parser import RobustJSONParser
from src.services.ocr_pipeline.ocr_server_post_processor import GenericPostProcessor

# ============================================================================
# QWEN JSON GENERATOR CLASS
# ============================================================================
class QwenJSONGenerator:

    DEEPINFRA_API_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
    def __init__(self, config):
        self.config = config
        cfg = self._load_config()
        #  DeepInfra / Qwen settings from config.yaml 
        qwen_cfg = cfg.get('qwen', {})
        deepinfra_cfg = cfg.get('deepinfra', {})

        self.api_key = (qwen_cfg.get('api_key') or deepinfra_cfg.get('api_key') or getattr(config, 'deepinfra_api_key', None))
        self.model_name  = qwen_cfg.get('model',       'Qwen/Qwen2.5-7B-Instruct')
        self.timeout     = qwen_cfg.get('timeout',      120)
        self.temperature = qwen_cfg.get('temperature',  0.1)
        self.max_tokens  = qwen_cfg.get('max_tokens',   8192)
        self.max_retries      = qwen_cfg.get('max_retries',  3)
        self.retry_delay      = qwen_cfg.get('retry_delay_seconds', 2)
        self.max_json_retries = qwen_cfg.get('max_json_retries', 2)  # extra retries on malformed JSON
        self.total_prompt_tokens   = 0
        self.total_response_tokens = 0
        self.total_tokens_used     = 0
        self.post_processor = GenericPostProcessor() 
        self.universal_single_prompt = self._load_prompt("prompts/UNIVERSAL_SINGLE_PROMPT.txt")
        self.universal_chunk_prompt  = self._load_prompt("prompts/UNIVERSAL_CHUNK_PROMPT.txt")
        self.enabled = bool(self.api_key) 
        self._httpx_available = False
        try:
            import httpx  # noqa: F401
            self._httpx_available = True
        except ImportError:
            logger.warning(" httpx not installed  falling back to requests in executor")
        logger.info("=" * 80)
        if self.enabled:
            logger.info(" QWEN2.5-7B INITIALIZED (DeepInfra)")
            logger.info(f"   Model    : {self.model_name}")
            logger.info(f"   Timeout  : {self.timeout}s")
            logger.info(f"   Temp     : {self.temperature}")
            logger.info(f"   MaxTokens: {self.max_tokens}")
        else:
            logger.warning("  QWEN2.5-7B DISABLED  no DeepInfra API key found")
            logger.warning("   Set 'qwen.api_key' or 'deepinfra.api_key' in config.yaml")
        logger.info("=" * 80)

    # =========================================================================
    # PUBLIC: RESET COUNTERS
    # =========================================================================
    def reset_token_counters(self):
        self.total_prompt_tokens   = 0
        self.total_response_tokens = 0
        self.total_tokens_used     = 0
        logger.info(" Qwen token counters reset")

    # =========================================================================
    # PUBLIC: FULL DOCUMENT EXTRACTION
    # =========================================================================
    async def generate_json_from_markdown(
        self,
        markdown: str,
        document_type: str,
        schema_json: Optional[Dict] = None
    ) -> Tuple[Dict, int, int, int]:
 
        if not self.enabled:
            logger.error(" Qwen not enabled  cannot generate JSON")
            return {"status": "error", "message": "Qwen not enabled"}, 0, 0, 0

        markdown_length = len(markdown)
        logger.info("=" * 80)
        logger.info(" Qwen2.5-7B  FULL DOCUMENT EXTRACTION")
        logger.info(f" Markdown length  : {markdown_length} characters")
        logger.info(f" Selected model   : Qwen2.5-7B (full document  20,000 chars)")
        logger.info(f" Document type    : {document_type}")
        logger.info("=" * 80)

        prompt = self._build_single_prompt(markdown, document_type, schema_json)
        response_text, prompt_tokens, response_tokens = await self._call_qwen_api_async(prompt)

        if not response_text:
            return {"status": "error", "message": "No response from Qwen"}, 0, 0, 0

        json_result = self._validate_json_response(response_text)

        # -- JSON-repair retry loop ------------------------------------------
        json_attempt = 0
        while json_result.get("status") == "error" and json_attempt < self.max_json_retries:
            json_attempt += 1
            logger.warning(
                f" Malformed JSON on full-doc extraction "
                f"(json_retry {json_attempt}/{self.max_json_retries}) - retrying with repair prompt"
            )
            repair_prompt = self._build_json_repair_prompt(
                bad_output=response_text,
                original_source=markdown,
                context_hint="full document"
            )
            response_text, p2, r2 = await self._call_qwen_api_async(repair_prompt)
            prompt_tokens   += p2
            response_tokens += r2
            if not response_text:
                logger.error(" No response from Qwen on JSON repair attempt")
                break
            json_result = self._validate_json_response(response_text)

        total = prompt_tokens + response_tokens
        logger.info(f" Qwen full-doc extraction done | tokens: {prompt_tokens}+{response_tokens}={total}")
        return json_result, prompt_tokens, response_tokens, total

    # =========================================================================
    # PUBLIC: CHUNK EXTRACTION
    # =========================================================================
    async def generate_json_from_chunk(
        self,
        chunk_text: str,
        document_type: str,
        chunk_num: int = 1,
        total_chunks: int = 1,
        schema_json: Optional[Dict] = None
    ) -> Tuple[Dict, int, int]:

        if not self.enabled:
            logger.error(" Qwen not enabled  cannot extract chunk JSON")
            return {"status": "error", "message": "Qwen not enabled"}, 0, 0

        chunk_size = len(chunk_text)
        logger.info(f" Chunk size       : {chunk_size} characters")
        logger.info(f" Model            : Qwen2.5-7B (chunk-level)")
        logger.info(f" Chunk            : {chunk_num}/{total_chunks}")

        prompt = self._build_chunk_prompt(
            chunk_text, document_type, chunk_num, total_chunks, schema_json
        )
        response_text, prompt_tokens, response_tokens = await self._call_qwen_api_async(prompt)

        if not response_text:
            return {"status": "error", "message": "No response from Qwen for chunk"}, 0, 0

        json_result = self._validate_json_response(response_text)

        # -- JSON-repair retry loop ------------------------------------------
        json_attempt = 0
        while json_result.get("status") == "error" and json_attempt < self.max_json_retries:
            json_attempt += 1
            logger.warning(
                f" Malformed JSON on chunk {chunk_num}/{total_chunks} "
                f"(json_retry {json_attempt}/{self.max_json_retries}) - retrying with repair prompt"
            )
            repair_prompt = self._build_json_repair_prompt(
                bad_output=response_text,
                original_source=chunk_text,
                context_hint=f"chunk {chunk_num}/{total_chunks} of a {document_type} document"
            )
            response_text, p2, r2 = await self._call_qwen_api_async(repair_prompt)
            prompt_tokens   += p2
            response_tokens += r2
            if not response_text:
                logger.error(f" No response from Qwen on JSON repair attempt for chunk {chunk_num}")
                break
            json_result = self._validate_json_response(response_text)
        # -------------------------------------------------------------------
        logger.info(f" Qwen chunk {chunk_num}/{total_chunks} done | tokens: {prompt_tokens}+{response_tokens}")

        return json_result, prompt_tokens, response_tokens

    # =========================================================================
    # PUBLIC: PARALLEL CHUNK PROCESSING (convenience wrapper)
    # =========================================================================
    async def process_chunks_parallel(
        self,
        chunks: list,
        document_type: str,
        schema_json: Optional[Dict] = None
    ) -> Tuple[list, int, int]:
 
        total_chunks = len(chunks)
        logger.info("=" * 80)
        logger.info(f" Qwen2.5-7B  PARALLEL CHUNK PROCESSING")
        logger.info(f" Total chunks  : {total_chunks}")
        logger.info(f" Model         : Qwen2.5-7B (chunk-level, always)")
        logger.info("=" * 80)

        tasks = []
        for i, chunk in enumerate(chunks, 1):
            chunk_text = chunk.get('text', '')
            tasks.append(
                self.generate_json_from_chunk(
                    chunk_text=chunk_text,
                    document_type=document_type,
                    chunk_num=i,
                    total_chunks=total_chunks,
                    schema_json=schema_json
                )
            )

        responses = await asyncio.gather(*tasks)

        chunk_results = []
        total_prompt   = 0
        total_response = 0

        for i, (json_result, p_tokens, r_tokens) in enumerate(responses, 1):
            total_prompt   += p_tokens
            total_response += r_tokens

            is_error = (
                json_result is None
                or not json_result
                or json_result.get("status") == "error"
            )
            if not is_error:
                json_result.pop('chunk_metadata', None)
                chunk_results.append(json_result)
                logger.info(f" Chunk {i}/{total_chunks} extracted successfully")

                # Log chunk JSON summary
                logger.info("=" * 60)
                logger.info(f" CHUNK {i}/{total_chunks} JSON RESULT:")
                chunk_json_str = json.dumps(json_result, indent=2, ensure_ascii=False)
                logger.info(f" Size: {len(chunk_json_str)} chars")
                if len(chunk_json_str) > 1000:
                    logger.info(chunk_json_str[:1000] + "... (truncated)")
                else:
                    logger.info(chunk_json_str)
                logger.info("=" * 60)
            else:
                logger.error(f" Failed to get valid JSON for chunk {i}")

        logger.info(f" Qwen parallel chunk processing done: {len(chunk_results)}/{total_chunks} chunks OK")
        logger.info(f" Total tokens  Prompt: {total_prompt}, Response: {total_response}")

        # Accumulate into instance counters
        self.total_prompt_tokens   += total_prompt
        self.total_response_tokens += total_response
        self.total_tokens_used      = self.total_prompt_tokens + self.total_response_tokens

        return chunk_results, total_prompt, total_response
    # =========================================================================
    # INTERNAL: PROMPT BUILDERS
    # =========================================================================
    def _build_json_repair_prompt(
        self,
        bad_output: str,
        original_source: str,
        context_hint: str = ""
    ) -> str:

        hint = f" ({context_hint})" if context_hint else ""

        return (
            f"Your previous extraction attempt{hint} produced malformed output that cannot be used.\n\n"
            f"YOUR BROKEN OUTPUT WAS:\n"
            f"---\n"
            f"{bad_output[:1500]}\n"
            f"---\n\n"
            f"THE PROBLEM: You serialised nested arrays/objects as escaped strings. Examples:\n"
            f'  BAD:  \"Header\": \"[\"                   <- string, must be a real array\n'
            f'  BAD:  \"tables\": \"[{{\"table_name\"...\"  <- string, must be a real array\n'
            f'  BAD:  \"InvoiceSummary\": \"{{...\"         <- string, must be a real object\n'
            f"\n"
            f"CORRECT OUTPUT SHAPE:\n"
            f'  \"Header\": []                                        <- real empty array\n'
            f'  \"tables\": [{{\"table_name\": \"LABOUR\", \"headers\": [...], \"rows\": [...]}}]\n'
            f'  \"InvoiceSummary\": {{\"Labour\": null, \"Parts\": null}}   <- real object\n'
            f"\n"
            f"NOW RE-EXTRACT from the ORIGINAL SOURCE below. Do NOT try to fix the broken output above.\n"
            f"Extract everything fresh from this source:\n\n"
            f"--- ORIGINAL SOURCE START ---\n"
            f"{original_source}\n"
            f"--- ORIGINAL SOURCE END ---\n\n"
            f"OUTPUT RULES (all mandatory):\n"
            f"1. Start with {{ and end with }} - single top-level JSON object only.\n"
            f"2. NEVER return {{}} - always populate every key you find.\n"
            f"3. Arrays must be real arrays [...], objects must be real objects {{...}}.\n"
            f"4. No markdown fences, no explanation, no text outside the JSON.\n"
            f"5. All brackets/braces must be properly closed.\n"
            f"6. No trailing commas.\n"
            f"7. Use JSON null (not the string \"null\") for empty/missing values.\n"
            f"8. Must be parseable by Python json.loads() with zero preprocessing.\n\n"
            f"Output the JSON object now. Start with {{"
        )

    def _build_single_prompt(
        self,
        markdown: str,
        document_type: str,
        schema_json: Optional[Dict] = None
    ) -> str:
        """Build extraction prompt for full single document."""
        if schema_json:
            return (
                f"Extract structured data from this document matching the schema.\n\n"
                f"Document Type: {document_type}\n\n"
                f"Schema:\n{json.dumps(schema_json, indent=2)}\n\n"
                f"Markdown Content:\n{markdown}\n\n"
                f"Return ONLY valid JSON matching the schema structure. "
                f"Do not include any explanation or markdown fences."
            )

        if self.universal_single_prompt:
            prompt = str(self.universal_single_prompt)
            prompt = prompt.replace("<<DOCUMENT_TYPE>>", document_type)
            prompt = prompt.replace("<<MARKDOWN_CONTENT>>", markdown)
            return prompt

        # Fallback
        logger.warning(" Universal single prompt missing  using inline fallback")
        return (
            f"You are a document data extractor.\n"
            f"Document Type: {document_type}\n\n"
            f"Extract ALL key information from the following markdown and return "
            f"a well-structured JSON object. Return ONLY valid JSON, no explanation.\n\n"
            f"Markdown:\n{markdown}"
        )

    def _build_chunk_prompt(
        self,
        chunk_text: str,
        document_type: str,
        chunk_num: int,
        total_chunks: int,
        schema_json: Optional[Dict] = None
    ) -> str:

        MANDATORY_OUTPUT_RULES = (
            f"MANDATORY OUTPUT RULES - YOU MUST FOLLOW ALL OF THESE:\n"
            f"1. Start your response with {{ and end with }} - NEVER return {{}}\n"
            f"2. NEVER return an empty object. ALWAYS populate every key you find.\n"
            f"3. For ANY HTML <table> found: extract it as:\n"
            f"   {{\"table_name\": \"<section heading above table>\", \"headers\": [\"col1\", \"col2\", ...], \"rows\": [{{\"col1\": val, ...}}]}}\n"
            f"   If the table has no data rows, still return the headers and rows as empty array [].\n"
            f"4. For key:value text lines: extract each as a field in the JSON object.\n"
            f"5. For summary/totals sections: extract each row as a key-value pair.\n"
            f"6. Use null (JSON null, not string) for fields present in the document but with no value.\n"
            f"7. NEVER use markdown fences, NEVER add explanation, NEVER truncate.\n"
            f"8. ALL arrays and braces must be properly closed.\n"
        )

        if schema_json:
            return (
                f"You are a data extraction engine processing chunk {chunk_num}/{total_chunks} of a {document_type} document.\n"
                f"Your ONLY job is to read the markdown below and output a JSON object containing ALL data found.\n\n"
                f"{MANDATORY_OUTPUT_RULES}\n"
                f"Schema to follow:\n{json.dumps(schema_json, indent=2)}\n\n"
                f"--- MARKDOWN CHUNK START ---\n{chunk_text}\n--- MARKDOWN CHUNK END ---\n\n"
                f"Output the JSON object now. Start with {{"
            )

        if self.universal_chunk_prompt:
            prompt = str(self.universal_chunk_prompt)
            prompt = prompt.replace("{chunk_num}",       str(chunk_num))
            prompt = prompt.replace("{total_chunks}",    str(total_chunks))
            prompt = prompt.replace("<<DOCUMENT_TYPE>>", document_type)
            prompt = prompt.replace("<<MARKDOWN_CONTENT>>", chunk_text)
            return prompt

        logger.warning(" Universal chunk prompt missing - using inline fallback")
        return (
            f"You are a data extraction engine processing chunk {chunk_num}/{total_chunks} of a {document_type} document.\n"
            f"Your ONLY job is to read the markdown below and output a JSON object containing ALL data found.\n\n"
            f"{MANDATORY_OUTPUT_RULES}\n"
            f"EXAMPLE - if your chunk contains:\n"
            f"  PARTS\n"
            f"  <table><tr><th>Part #</th><th>Qty</th></tr></table>\n"
            f"You MUST output:\n"
            f"  {{\"Header\": [], \"tables\": [{{\"table_name\": \"PARTS\", \"headers\": [\"Part #\", \"Qty\"], \"rows\": []}}], \"InvoiceSummary\": {{}}}}\n\n"
            f"EXAMPLE - if your chunk contains an Invoice Summary table:\n"
            f"  Labour: 0, Parts: 0, CGST: 0, Net Invoice Value: 0\n"
            f"You MUST output:\n"
            f"  {{\"Header\": [], \"tables\": [], \"InvoiceSummary\": {{\"Labour\": \"0\", \"Parts\": \"0\", \"CGST\": \"0\", \"Net_Invoice_Value\": \"0\"}}}}\n\n"
            f"--- MARKDOWN CHUNK START ---\n{chunk_text}\n--- MARKDOWN CHUNK END ---\n\n"
            f"Output the JSON object now. Start with {{"
        )
    # =========================================================================
    # INTERNAL: ASYNC API CALL
    # =========================================================================
    async def _call_qwen_api_async(self, prompt: str) -> Tuple[Optional[str], int, int]:

        if not self.api_key:
            logger.error(" No DeepInfra API key  Qwen call aborted")
            return None, 0, 0

        payload = {
            "model": self.model_name,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a precise document data extraction assistant. "
                        "You MUST always return a single valid JSON object starting with { and ending with }. "
                        "NEVER return an empty object {}. ALWAYS extract and structure ALL content present. "
                        "Even if a table has no data rows, return its headers and an empty rows array. "
                        "Even if values are missing, return the keys with null values. "
                        "NEVER start your response with [ or any character other than {. "
                        "NEVER include markdown code fences, explanations, or any text outside the JSON object. "
                        "NEVER truncate the JSON. Ensure all brackets and braces are properly closed. "
                        "The response must be parseable by Python json.loads() without any preprocessing."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": self.temperature,
            "max_tokens":  self.max_tokens,
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type":  "application/json",
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f" Calling Qwen2.5-7B API (attempt {attempt}/{self.max_retries})")

                if self._httpx_available:
                    response_text, p_tokens, r_tokens = await self._call_with_httpx(
                        headers, payload
                    )
                else:
                    response_text, p_tokens, r_tokens = await self._call_with_requests(
                        headers, payload
                    )

                if response_text is not None:
                    self.total_prompt_tokens   += p_tokens
                    self.total_response_tokens += r_tokens
                    self.total_tokens_used      = (
                        self.total_prompt_tokens + self.total_response_tokens
                    )
                    logger.info(
                        f" Qwen tokens  Prompt: {p_tokens}, "
                        f"Response: {r_tokens}, "
                        f"Running Total: {self.total_tokens_used}"
                    )
                    return response_text, p_tokens, r_tokens

                logger.warning(f" Empty response on attempt {attempt}")

            except Exception as e:
                logger.error(f" Qwen API call failed on attempt {attempt}: {e}")

            if attempt < self.max_retries:
                logger.info(f" Retrying in {self.retry_delay}s...")
                await asyncio.sleep(self.retry_delay)

        logger.error(f" Qwen API failed after {self.max_retries} attempts")
        return None, 0, 0

    async def _call_with_httpx(
        self,
        headers: dict,
        payload: dict
    ) -> Tuple[Optional[str], int, int]:
        """Async HTTP call using httpx."""
        import httpx
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                self.DEEPINFRA_API_URL,
                headers=headers,
                json=payload
            )
            resp.raise_for_status()
            data = resp.json()
            return self._parse_api_response(data)

    async def _call_with_requests(
        self,
        headers: dict,
        payload: dict
    ) -> Tuple[Optional[str], int, int]:
        """Sync HTTP call via requests, run in executor to avoid blocking."""
        import requests
        loop = asyncio.get_event_loop()

        def _sync_call():
            resp = requests.post(
                self.DEEPINFRA_API_URL,
                headers=headers,
                json=payload,
                timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json()

        data = await loop.run_in_executor(None, _sync_call)
        return self._parse_api_response(data)

    def _parse_api_response(
        self,
        data: dict
    ) -> Tuple[Optional[str], int, int]:

        choices = data.get("choices", [])
        if not choices:
            logger.error(" No choices in Qwen API response")
            return None, 0, 0

        content = choices[0].get("message", {}).get("content", "")
        if not content:
            logger.warning(" Empty content in Qwen API response")
            return None, 0, 0
        usage         = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens",     0)
        resp_tokens   = usage.get("completion_tokens", 0)

        if not prompt_tokens and not resp_tokens:
            logger.warning(" Token usage not reported by API  estimating")
            prompt_tokens = int(len(content.split()) * 1.3)
            resp_tokens   = int(len(content.split()) * 1.3)

        return content, int(prompt_tokens), int(resp_tokens)

    # =========================================================================
    # INTERNAL: JSON VALIDATION
    # =========================================================================
    def _validate_json_response(self, response_text: str) -> Dict:

        try:
            json_text   = self._extract_json_from_response(response_text)
            parsed_json = RobustJSONParser.clean_and_parse(json_text)

            if not isinstance(parsed_json, dict):
                logger.error(" Qwen response is not a JSON object")
                return {
                    "status":      "error",
                    "message":     "Response is not a JSON object",
                    "raw_content": response_text[:500]
                }

            if not parsed_json:
                logger.warning(" Qwen returned empty JSON object {} - prompt ignored, treating as error")
                return {
                    "status":  "error",
                    "message": "Qwen returned empty object {} despite strict prompt"
                }

            if parsed_json.get("status") == "error":
                return parsed_json

            stringified_issue = self._detect_stringified_json_values(parsed_json)
            if stringified_issue:
                logger.warning(
                    f" Qwen returned stringified nested JSON - field '{stringified_issue}' "
                    f"is a string that looks like a JSON array/object. Triggering repair retry."
                )
                return {
                    "status":  "error",
                    "message": f"Stringified JSON value detected in field '{stringified_issue}'",
                    "raw_content": response_text[:500]
                }

            parsed_json = RobustJSONParser.recursive_clean_values(parsed_json)
            logger.info(" Qwen JSON validated and cleaned")
            return parsed_json

        except Exception as e:
            logger.error(f" Qwen JSON validation error: {e}")
            return {
                "status":      "error",
                "message":     f"Validation error: {str(e)}",
                "raw_content": response_text[:500] if isinstance(response_text, str) else str(response_text)[:500]
            }

    _STRUCTURAL_KEYS = {
        "Header", "tables", "headers", "rows",
        "InvoiceSummary", "CompanyDetails", "CustomerDetails", "VehicleDetails",
    }

    def _detect_stringified_json_values(self, parsed: dict) -> Optional[str]:

        def _is_stringified(value) -> bool:
            if not isinstance(value, str):
                return False
            s = value.strip()
            return s.startswith('[') or s.startswith('{')

        for key, val in parsed.items():
            if _is_stringified(val):
                return key
            if isinstance(val, dict):
                for sub_key, sub_val in val.items():
                    if _is_stringified(sub_val):
                        return f"{key}.{sub_key}"
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        for sub_key, sub_val in item.items():
                            if _is_stringified(sub_val):
                                return f"{key}[].{sub_key}"
        return None

    def _extract_json_from_response(self, response_text: str) -> str:
        """Strip markdown fences and extract the JSON object/array."""
        response_text = response_text.strip()

        if response_text.startswith('```json'):
            response_text = response_text[7:].lstrip()
        elif response_text.startswith('```'):
            response_text = response_text[3:].lstrip()

        if response_text.endswith('```'):
            response_text = response_text[:-3].rstrip()

        first_brace  = response_text.find('{')
        first_bracket = response_text.find('[')
        last_brace   = response_text.rfind('}')
        last_bracket  = response_text.rfind(']')

        if first_brace != -1 and last_brace != -1 and first_brace < last_brace:
            if first_bracket != -1 and first_bracket < first_brace:
                response_text = response_text[first_brace:last_brace + 1]
            else:
                response_text = response_text[first_brace:last_brace + 1]
        elif first_bracket != -1 and last_bracket != -1 and first_bracket < last_bracket:
            logger.warning(" Response is a JSON array, not object - wrapping in {data: [...]}")
            array_text = response_text[first_bracket:last_bracket + 1]
            response_text = '{"data": ' + array_text + '}'
        return response_text

    # =========================================================================
    # INTERNAL: HELPERS
    # =========================================================================
    def _load_config(self) -> Dict:
        """Load raw YAML config."""
        try:
            with open('config/config.yaml', 'r') as f:
                cfg = yaml.safe_load(f)
                return cfg if cfg else {}
        except Exception as e:
            logger.warning(f" Could not load config.yaml: {e}")
            return {}

    def _load_prompt(self, path: str) -> Optional[str]:
        """Load a prompt template file."""
        try:
            with open(Path(path), 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.warning(f" Could not load prompt '{path}': {e}")
            return None