# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import asyncio
import json
import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

from src.services.ocr_pipeline.ocr_server_json_parser import RobustJSONParser
from src.services.ocr_pipeline.ocr_server_post_processor import GenericPostProcessor
from src.services.ocr_pipeline.ocr_server_manual_splitter import process_oversized_chunks


class GeminiHeavyMethods:
    """Contains heavy processing methods for GeminiJSONGenerator."""
    # ------------------------------------------------------------------ #
    # LANGCHAIN CHUNKING WITH MANUAL SPLITTING                            #
    # ------------------------------------------------------------------ #
    def _chunk_with_langchain_markdown(self, markdown_content: str, langchain_keys: List[str]) -> List:
        if not self.langchain_available:
            logger.error("? Langchain not available but needed for custom splitting")
            raise ImportError("langchain-text-splitters required for custom section splitting")

        try:
            logger.info(f"?? Using LangChain splitter with {len(langchain_keys)} section markers...")
            logger.info(f"?? Section markers: {', '.join(langchain_keys)}")

            marker_positions = []
            for marker in langchain_keys:
                pos = markdown_content.upper().find(marker.upper())
                if pos != -1:
                    marker_positions.append((pos, marker))

            marker_positions.sort(key=lambda x: x[0])
            logger.info(f"?? Found {len(marker_positions)} sections in document")

            if len(marker_positions) == 0:
                logger.warning("?? No section markers found - treating as single chunk")
                return [{
                    'text': markdown_content,
                    'token_count': len(markdown_content) // 4,
                    'metadata': {'section': 'full_document'}
                }]

            chunks_by_section = []

            if marker_positions[0][0] > 0:
                header_chunk = markdown_content[:marker_positions[0][0]].strip()
                if header_chunk:
                    chunks_by_section.append({
                        'text': header_chunk,
                        'token_count': len(header_chunk) // 4,
                        'metadata': {'section': 'header'}
                    })
                    logger.info(f"  ?? Header section: {len(header_chunk)} chars")

            for i in range(len(marker_positions)):
                start_pos = marker_positions[i][0]
                section_name = marker_positions[i][1]
                end_pos = marker_positions[i + 1][0] if i < len(marker_positions) - 1 else len(markdown_content)
                section_content = markdown_content[start_pos:end_pos].strip()

                if section_content:
                    chunks_by_section.append({
                        'text': section_content,
                        'token_count': len(section_content) // 4,
                        'metadata': {'section': section_name.lower()}
                    })
                    logger.info(f"  ?? {section_name} section: {len(section_content)} chars")

            final_chunks = []
            chunk_size = self.config.langchain_chunk_size
            chunk_overlap = self.config.langchain_chunk_overlap

            text_splitter = self.RecursiveCharacterTextSplitter(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separators=["</table>", "\n\n", "\n", " "],
                length_function=len,
            )

            for chunk_data in chunks_by_section:
                content = chunk_data['text']
                metadata = chunk_data['metadata']
                if len(content) <= chunk_size:
                    final_chunks.append(chunk_data)
                else:
                    logger.info(f"  ?? Splitting oversized {metadata['section']} section ({len(content)} chars)")
                    sub_docs = text_splitter.split_text(content)
                    for idx, sub_content in enumerate(sub_docs, 1):
                        final_chunks.append({
                            'text': sub_content,
                            'token_count': len(sub_content) // 4,
                            'metadata': {
                                'section': f"{metadata['section']}_part{idx}",
                                'parent_section': metadata['section']
                            }
                        })

            logger.info(f"? LangChain chunking complete: {len(final_chunks)} chunks")
            for i, chunk in enumerate(final_chunks, 1):
                section = chunk['metadata'].get('section', 'unknown')
                logger.info(f"   Chunk {i}/{len(final_chunks)}: {section} - {len(chunk['text'])} chars (~{chunk['token_count']} tokens)")

            logger.info("=" * 80)
            logger.info("?? STEP 3: APPLYING MANUAL SPLITTING (LANGCHAIN CHUNKS ONLY)")
            logger.info("=" * 80)
            final_chunks = process_oversized_chunks(final_chunks, self.config)
            logger.info(f"? Final chunk count after manual splitting: {len(final_chunks)}")
            logger.info("=" * 80)

            return final_chunks

        except Exception as e:
            logger.error(f"? LangChain chunking failed: {e}", exc_info=True)
            raise

    # ------------------------------------------------------------------ #
    # UNSTRUCTURED SEMANTIC CHUNKING                                      #
    # ------------------------------------------------------------------ #
    def _chunk_with_unstructured(self, markdown_content: str) -> List:
        if not self.unstructured_available:
            logger.error("? Unstructured not available but needed for semantic chunking")
            raise ImportError("unstructured library required for semantic chunking")

        try:
            logger.info(f"?? Using Unstructured semantic chunker...")
            logger.info(f"?? Markdown size: {len(markdown_content)} characters")

            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False, encoding='utf-8') as tmp_file:
                tmp_file.write(markdown_content)
                tmp_path = tmp_file.name

            try:
                elements = self.unstructured_partition(filename=tmp_path, strategy="fast", include_page_breaks=False)
                logger.info(f"? Found {len(elements)} elements")

                chunks = self.unstructured_chunk_by_title(
                    elements,
                    max_characters=self.config.chunk_size,
                    combine_text_under_n_chars=1000,
                    new_after_n_chars=self.config.chunk_size - 1000,
                )
                logger.info(f"? Created {len(chunks)} semantic chunks")

                formatted_chunks = []
                for idx, chunk in enumerate(chunks, 1):
                    chunk_text = str(chunk)
                    formatted_chunks.append({
                        'text': chunk_text,
                        'token_count': len(chunk_text) // 4,
                        'metadata': {'chunk_type': 'unstructured_semantic', 'chunk_index': idx}
                    })

                logger.info(f"?? Unstructured chunking complete: {len(formatted_chunks)} chunks")
                for i, chunk in enumerate(formatted_chunks, 1):
                    logger.info(f"   Chunk {i}/{len(formatted_chunks)}: {len(chunk['text'])} chars (~{chunk['token_count']} tokens)")

                logger.info("=" * 80)
                logger.info("?? MANUAL SPLITTING: NOT APPLIED (Unstructured chunks)")
                logger.info("?? Reason: Manual splitting is ONLY for LangChain chunks")
                logger.info("=" * 80)

                return formatted_chunks
            finally:
                import os
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        except Exception as e:
            logger.error(f"? Unstructured chunking failed: {e}", exc_info=True)
            raise

    # ------------------------------------------------------------------ #
    # JSON VALIDATION                                                      #
    # ------------------------------------------------------------------ #
    def _validate_json_response(self, response_text: str) -> Dict:
        try:
            json_text = self._extract_json_from_response(response_text)
            parsed_json = RobustJSONParser.clean_and_parse(json_text)

            if not isinstance(parsed_json, dict):
                logger.error("Response is not a valid JSON object")
                return {"status": "error", "message": "Response is not a JSON object", "raw_content": response_text[:500]}

            if parsed_json.get("status") == "error":
                return parsed_json

            parsed_json = RobustJSONParser.recursive_clean_values(parsed_json)
            logger.info("? Valid JSON received and cleaned")
            return parsed_json

        except Exception as e:
            logger.error(f"Error validating JSON response: {e}")
            return {
                "status": "error",
                "message": f"Validation error: {str(e)}",
                "raw_content": response_text[:500] if isinstance(response_text, str) else str(response_text)[:500]
            }

    # ------------------------------------------------------------------ #
    # GEMINI API ASYNC CALL                                               #
    # ------------------------------------------------------------------ #
    async def _call_gemini_api_async(self, prompt: str) -> tuple:
        if not self.model:
            logger.error("Gemini model not initialized")
            return None, 0, 0

        for attempt in range(self.max_retries):
            try:
                logger.info(f"Calling Gemini API (attempt {attempt + 1}/{self.max_retries})")

                generation_config = self.genai.types.GenerationConfig(
                    max_output_tokens=self.max_tokens,
                    temperature=self.temperature,
                    top_p=0.8,
                    top_k=40
                )

                safety_settings = {
                    self.HarmCategory.HARM_CATEGORY_HATE_SPEECH: self.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                    self.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: self.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                    self.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: self.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                    self.HarmCategory.HARM_CATEGORY_HARASSMENT: self.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                }

                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.model.generate_content(prompt, generation_config=generation_config, safety_settings=safety_settings)
                )

                if response.text:
                    prompt_tokens = 0
                    response_tokens = 0
                    try:
                        if hasattr(response, 'usage_metadata'):
                            prompt_tokens = response.usage_metadata.prompt_token_count
                            response_tokens = response.usage_metadata.candidates_token_count
                            logger.info(f"?? Gemini tokens - Prompt: {prompt_tokens}, Response: {response_tokens}")
                        else:
                            logger.warning("?? Token metadata not available, using estimation")
                            prompt_tokens = int(len(prompt.split()) * 1.3)
                            response_tokens = int(len(response.text.split()) * 1.3)
                    except Exception as e:
                        logger.warning(f"Could not extract token usage: {e}, using estimation")
                        prompt_tokens = int(len(prompt.split()) * 1.3)
                        response_tokens = int(len(response.text.split()) * 1.3)

                    self.total_prompt_tokens += int(prompt_tokens)
                    self.total_response_tokens += int(response_tokens)
                    self.total_tokens_used = self.total_prompt_tokens + self.total_response_tokens
                    logger.info(f"?? Running total - Prompt: {self.total_prompt_tokens}, Response: {self.total_response_tokens}, Total: {self.total_tokens_used}")
                    return response.text, int(prompt_tokens), int(response_tokens)
                else:
                    logger.warning("Empty response from Gemini API")

            except Exception as e:
                logger.error(f"Gemini API call failed on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)

        logger.error("Failed to get response from Gemini after all retries")
        return None, 0, 0

    # ------------------------------------------------------------------ #
    # QWEN GENERATOR ACCESSOR                                             #
    # ------------------------------------------------------------------ #
    def _get_qwen_generator(self):
        """Lazily retrieve QwenJSONGenerator singleton from app module."""
        try:
            from src.services.ocr_pipeline.ocr_server_app import qwen_generator
            if qwen_generator and qwen_generator.enabled:
                return qwen_generator
            logger.warning("?? qwen_generator not available or not enabled in app module")
            return None
        except ImportError as e:
            logger.error(f"? Could not import qwen_generator from app: {e}")
            return None

    # ------------------------------------------------------------------ #
    # SWITCH GEMINI MODEL AT RUNTIME                                      #
    # ------------------------------------------------------------------ #
    def _apply_gemini_model(self, model_name: str, max_tokens: int):
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(model_name)
            self.model_name = model_name
            self.max_tokens = max_tokens
            logger.info(f"? Gemini model switched to: {model_name} | max_tokens={max_tokens:,}")
        except Exception as e:
            logger.error(f"? Failed to switch Gemini model to {model_name}: {e}")

    # ------------------------------------------------------------------ #
    # GEMINI FALLBACK CHUNK PROCESSING                                    #
    # ------------------------------------------------------------------ #
    async def _process_chunks_with_gemini(self, chunks: List, chunk_count: int,
                                          document_type: str, schema_json: Optional[Dict]) -> List:
        """Fallback: process all chunks with Gemini when Qwen is unavailable."""
        logger.warning("?? Using Gemini for chunk processing (Qwen fallback)")

        tasks = []
        for i, chunk in enumerate(chunks, 1):
            chunk_text = chunk.get('text', '')
            chunk_tokens = chunk.get('token_count', 0)
            logger.info(f"Preparing chunk {i}/{chunk_count} (~{chunk_tokens} tokens, {len(chunk_text)} chars)...")
            prompt = self._create_chunk_extraction_prompt(
                chunk_text, document_type, chunk_num=i, total_chunks=chunk_count, schema_json=schema_json
            )
            tasks.append(self._call_gemini_api_async(prompt))

        logger.info(f"?? Executing {len(tasks)} Gemini API calls concurrently...")
        responses = await asyncio.gather(*tasks)

        chunk_results = []
        for i, (response_text, prompt_tokens, response_tokens) in enumerate(responses, 1):
            if not response_text:
                logger.error(f"Failed to get response for chunk {i}")
                continue
            chunk_json = self._validate_json_response(response_text)
            if chunk_json and chunk_json.get("status") != "error":
                chunk_json.pop('chunk_metadata', None)
                chunk_results.append(chunk_json)
                logger.info(f"? Chunk {i} extracted successfully (Gemini fallback)")
            else:
                logger.error(f"? Failed to validate JSON for chunk {i}")

        return chunk_results

    # ------------------------------------------------------------------ #
    # LOGGING HELPERS                                                     #
    # ------------------------------------------------------------------ #
    def _log_full_doc_json(self, json_result: Dict, label: str = "FULL DOCUMENT"):
        logger.info("=" * 80)
        logger.info(f"?? {label} JSON RESULT:")
        logger.info("=" * 80)
        json_str = json.dumps(json_result, indent=2, ensure_ascii=False)
        logger.info(f"?? Full JSON size: {len(json_str)} chars")
        logger.info("=" * 80)
        if len(json_str) > 2000:
            logger.info(json_str[:2000])
            logger.info(f"... (truncated, full size: {len(json_str)} chars)")
        else:
            logger.info(json_str)
        logger.info("=" * 80)

    def _log_single_chunk_json(self, json_result: Dict):
        logger.info("=" * 80)
        logger.info("?? SINGLE CHUNK JSON RESULT:")
        logger.info("=" * 80)
        single_json_str = json.dumps(json_result, indent=2, ensure_ascii=False)
        logger.info(f"?? Full JSON size: {len(single_json_str)} chars")
        logger.info("=" * 80)
        logger.info(single_json_str)
        logger.info("=" * 80)

    # ------------------------------------------------------------------ #
    # MAIN PIPELINE                                                       #
    # ------------------------------------------------------------------ #
    async def generate_json_from_markdown_async(
        self,
        markdown_content: str,
        document_type: str,
        doc_config: Dict,
        original_file_bytes: Optional[bytes] = None,
        original_filename: Optional[str] = None,
        gemini_model: Optional[str] = None,
        disable_unstructured_chunking: bool = False
    ) -> tuple:
   
        self.reset_token_counters()

        from src.services.ocr_pipeline.ocr_server_gemini import select_full_document_model

        # Caller-forced Gemini model override
        if gemini_model:
            logger.info("=" * 80)
            logger.info(f"?? CALLER-FORCED GEMINI MODEL: {gemini_model}")
            logger.info("=" * 80)
            try:
                import google.generativeai as genai
                genai.configure(api_key=self.api_key)
                if 'gemini-2.5' in gemini_model or 'gemini-2-5' in gemini_model:
                    self.max_tokens = 65536
                    logger.info("?? Gemini 2.5 Flash - Max output tokens: 65,536")
                else:
                    self.max_tokens = 8192
                    logger.info("?? Gemini 2.0 Flash - Max output tokens: 8,192")
                self.model = genai.GenerativeModel(gemini_model)
                self.model_name = gemini_model
                logger.info(f"? Successfully switched to model: {gemini_model}")
                logger.info(f"   ?? Temperature: {self.temperature}")
                logger.info(f"   ?? Max Output Tokens: {self.max_tokens:,}")
                logger.info(f"   ?? Timeout: {self.timeout}s")
            except Exception as e:
                logger.error(f"? Failed to switch model to {gemini_model}: {e}")
                logger.warning(f"?? Falling back to default model: {self.model_name}")
        else:
            logger.info(f"?? Using default Gemini model: {self.model_name}")

        if not self.enabled:
            logger.warning("Gemini is not enabled, returning empty JSON structure")
            return {"status": "disabled", "message": "JSON generation not available"}, 0, 0, 0

        try:
            content_length = len(markdown_content)
            logger.info(f"Processing markdown content: {content_length} characters")
            logger.info(f"?? Document Type: {document_type}")

            langchain_keys = doc_config.get('langchain_keys', [])
            schema_json = doc_config.get('schema_json')
            has_langchain_keys = doc_config.get('has_langchain_keys', False)
            config_status = doc_config.get('status', 'unknown')

            if schema_json:
                logger.info(f"?? Schema available with {len(schema_json)} fields (used for extraction guidance only)")
            else:
                logger.info(f"?? No schema - using dynamic extraction")

            # ============================================================
            # STEP 1: INTELLIGENT CHUNKING
            # ============================================================
            if self.config.chunking_enabled:
                logger.info(f"?? Chunking enabled - starting intelligent splitting...")
                logger.info(f"?? Markdown size: {len(markdown_content)} characters")

                chunks = None
                chunk_count = 0

                if has_langchain_keys and len(langchain_keys) > 0:
                    logger.info("=" * 80)
                    logger.info("?? CONFIGURED DOCUMENT TYPE - Using LangChain with database keys")
                    logger.info("?? MANUAL SPLITTING: WILL BE APPLIED to oversized chunks")
                    logger.info("=" * 80)
                    try:
                        chunks = self._chunk_with_langchain_markdown(markdown_content, langchain_keys)
                        chunk_count = len(chunks)
                        logger.info(f"? ?? ? LANGCHAIN + MANUAL SPLITTING SUCCESSFUL: {chunk_count} chunks")
                    except Exception as e:
                        logger.error(f"? LangChain chunking FAILED: {e}", exc_info=True)
                        return {"status": "error", "message": f"LangChain chunking failed: {str(e)}"}, 0, 0, 0

                else:
                    if disable_unstructured_chunking:
                        logger.info("=" * 80)
                        logger.info("?? OLMOCR MODE - NO LANGCHAIN KEYS - CHUNKING DISABLED")
                        logger.info("?? Will send full markdown to LLM without chunking")
                        logger.info("=" * 80)
                        chunks = None
                        chunk_count = 0
                    else:
                        logger.info("=" * 80)
                        logger.info("?? UNKNOWN DOCUMENT TYPE - Using Unstructured Semantic Chunker")
                        logger.info("?? MANUAL SPLITTING: NOT APPLIED (Unstructured chunks)")
                        logger.info("=" * 80)
                        if not self.unstructured_available:
                            logger.error("? Unstructured chunker not available")
                            return {"status": "error", "message": "Unstructured chunker not installed"}, 0, 0, 0
                        try:
                            chunks = self._chunk_with_unstructured(markdown_content)
                            chunk_count = len(chunks)
                            logger.info(f"? ?? ? UNSTRUCTURED CHUNKING SUCCESSFUL: {chunk_count} chunks")
                        except Exception as e:
                            logger.error(f"? Unstructured chunking FAILED: {e}", exc_info=True)
                            return {"status": "error", "message": f"Unstructured chunking failed: {str(e)}"}, 0, 0, 0

                # ============================================================
                # NO-CHUNKING CASE - full-doc routing
                # ============================================================
                if chunks is None or chunk_count == 0:
                    logger.info("=" * 80)
                    logger.info("?? NO CHUNKING MODE - Processing full markdown as single document")
                    logger.info(f"?? Markdown size: {content_length} characters")
                    logger.info("=" * 80)

                    model_choice = select_full_document_model(content_length)

                    if model_choice == "qwen":
                        qwen = self._get_qwen_generator()
                        if qwen:
                            json_result, p_tokens, r_tokens, total = await qwen.generate_json_from_markdown(
                                markdown=markdown_content, document_type=document_type, schema_json=schema_json
                            )
                            self._log_full_doc_json(json_result, "NO-CHUNK / QWEN")
                            logger.info("? Full document extraction complete (no chunking, Qwen)")
                            return json_result, p_tokens, r_tokens, total
                        else:
                            logger.error("? Qwen not available for no-chunk routing, falling back to Gemini")
                            model_choice = "gemini-2.0"

                    if model_choice == "gemini-2.0":
                        self._apply_gemini_model("gemini-2.0-flash", max_tokens=8192)
                    elif model_choice == "gemini-2.5":
                        self._apply_gemini_model("gemini-2.5-flash", max_tokens=65536)

                    logger.info(f"?? Selected Gemini model: {self.model_name}")
                    prompt = self._create_single_document_extraction_prompt(markdown_content, document_type, schema_json)
                    response_text, prompt_tokens, response_tokens = await self._call_gemini_api_async(prompt)

                    if not response_text:
                        return {"status": "error", "message": "No response from Gemini"}, 0, 0, 0

                    json_result = self._validate_json_response(response_text)
                    self._log_full_doc_json(json_result, "NO-CHUNK / GEMINI")
                    logger.info("? Full document extraction complete (no chunking, Gemini)")
                    return json_result, self.total_prompt_tokens, self.total_response_tokens, self.total_tokens_used

                #  Log chunk breakdown 
                logger.info(f"?? Chunk breakdown:")
                for i, chunk in enumerate(chunks, 1):
                    chunk_text = chunk.get('text', '')
                    chunk_tokens = chunk.get('token_count', 0)
                    metadata = chunk.get('metadata', {})
                    manually_split = metadata.get('manually_split', False)
                    split_indicator = " ??" if manually_split else ""
                    logger.info(f"   Chunk {i}/{chunk_count}: {len(chunk_text)} chars (~{chunk_tokens} tokens){split_indicator}")

                #  Log chunk markdown contents 
                logger.info("=" * 80)
                logger.info("?? CHUNK MARKDOWN CONTENTS:")
                logger.info("=" * 80)
                for i, chunk in enumerate(chunks, 1):
                    chunk_text = chunk.get('text', '')
                    metadata = chunk.get('metadata', {})
                    manually_split = metadata.get('manually_split', False)
                    split_tag = " [MANUALLY SPLIT]" if manually_split else ""
                    logger.info(f"\n{'='*80}")
                    logger.info(f"?? CHUNK {i}/{chunk_count} MARKDOWN ({len(chunk_text)} chars){split_tag}:")
                    logger.info(f"{'='*80}")
                    logger.info(chunk_text)
                    logger.info(f"{'='*80}\n")

                logger.info("=" * 80)
                logger.info(f"? All {chunk_count} chunk markdown contents logged above")
                logger.info("=" * 80)

                # ============================================================
                # SINGLE CHUNK - full-doc routing
                # ============================================================
                if chunk_count == 1:
                    logger.info("?? Processing as single document (1 chunk)...")
                    chunk_text = chunks[0].get('text', '')
                    single_length = len(chunk_text)
                    model_choice = select_full_document_model(single_length)

                    if model_choice == "qwen":
                        qwen = self._get_qwen_generator()
                        if qwen:
                            json_result, p_tokens, r_tokens, total = await qwen.generate_json_from_markdown(
                                markdown=chunk_text, document_type=document_type, schema_json=schema_json
                            )
                            self._log_single_chunk_json(json_result)
                            logger.info("? Single chunk extraction complete (Qwen, no merge needed)")
                            return json_result, p_tokens, r_tokens, total
                        else:
                            logger.error("? Qwen not available for single-chunk routing, falling back to Gemini")
                            model_choice = "gemini-2.0"

                    if model_choice == "gemini-2.0":
                        self._apply_gemini_model("gemini-2.0-flash", max_tokens=8192)
                    elif model_choice == "gemini-2.5":
                        self._apply_gemini_model("gemini-2.5-flash", max_tokens=65536)

                    prompt = self._create_single_document_extraction_prompt(chunk_text, document_type, schema_json)
                    response_text, prompt_tokens, response_tokens = await self._call_gemini_api_async(prompt)

                    if not response_text:
                        return {"status": "error", "message": "No response from Gemini"}, 0, 0, 0

                    json_result = self._validate_json_response(response_text)
                    self._log_single_chunk_json(json_result)
                    logger.info("? Single chunk extraction complete (Gemini, no merge needed)")
                    return json_result, self.total_prompt_tokens, self.total_response_tokens, self.total_tokens_used

                # ============================================================
                # MULTIPLE CHUNKS: ALWAYS QWEN
                # ============================================================
                else:
                    logger.info("=" * 80)
                    logger.info(f"?? STEP 1: Processing {chunk_count} chunks in PARALLEL")
                    logger.info(f"?? Model: Qwen2.5-7B (chunk-level, always)")
                    logger.info("=" * 80)

                    qwen = self._get_qwen_generator()

                    if qwen:
                        chunk_results, p_tokens, r_tokens = await qwen.process_chunks_parallel(
                            chunks=chunks, document_type=document_type, schema_json=schema_json
                        )
                        self.total_prompt_tokens += p_tokens
                        self.total_response_tokens += r_tokens
                        self.total_tokens_used = self.total_prompt_tokens + self.total_response_tokens
                    else:
                        logger.warning("?? Qwen not available for chunk processing - falling back to Gemini")
                        chunk_results = await self._process_chunks_with_gemini(
                            chunks, chunk_count, document_type, schema_json
                        )

                    if not chunk_results:
                        logger.error("No valid chunks were processed")
                        return (
                            {"status": "error", "message": "No valid chunks processed"},
                            self.total_prompt_tokens, self.total_response_tokens, self.total_tokens_used
                        )

                    logger.info(f"? STEP 1 Complete: {len(chunk_results)} chunks extracted")

                    logger.info("=" * 80)
                    logger.info(f"?? SUMMARY: All {len(chunk_results)} Chunk JSONs Ready for Merge")
                    logger.info("=" * 80)
                    for idx, chunk_result in enumerate(chunk_results, 1):
                        fields = list(chunk_result.keys()) if isinstance(chunk_result, dict) else []
                        logger.info(f"  Chunk {idx}: {len(fields)} fields - {', '.join(fields[:5])}{' ...' if len(fields) > 5 else ''}")
                    logger.info("=" * 80)

                    # STEP 2: UNIFIED POST-PROCESSING MERGE
                    logger.info("=" * 80)
                    logger.info("? STEP 2: UNIFIED POST-PROCESSING MERGE")
                    logger.info("=" * 80)
                    logger.info(f"?? Using rule-based post-processor for {len(chunk_results)} chunks...")
                    logger.info(f"? No LLM merge - deterministic processing")

                    section_keys = []
                    if has_langchain_keys:
                        section_keys = langchain_keys
                        logger.info(f"?? Using LangChain keys as section markers: {', '.join(section_keys)}")
                    else:
                        logger.info(f"?? No section keys (Unstructured chunking)")

                    final_result = self.post_processor.process_chunks(chunk_results, section_keys)

                    logger.info("=" * 80)
                    logger.info("? POST-PROCESSING COMPLETE")
                    logger.info("=" * 80)
                    logger.info(f"?? Token Savings: ~2000-4000 tokens (no LLM merge API call)")
                    logger.info(f"? Processing Time: Instant (no API latency)")
                    logger.info(f"?? Deterministic: Same input = Same output")

                    logger.info("=" * 80)
                    logger.info("?? FINAL MERGED JSON:")
                    logger.info("=" * 80)
                    final_json_str = json.dumps(final_result, indent=2, ensure_ascii=False)
                    if len(final_json_str) > 2000:
                        logger.info(final_json_str[:2000])
                        logger.info(f"... (truncated, full size: {len(final_json_str)} chars)")
                    else:
                        logger.info(final_json_str)
                    logger.info("=" * 80)

                    logger.info("? STEP 2 Complete: Final JSON merged via post-processing")
                    return final_result, self.total_prompt_tokens, self.total_response_tokens, self.total_tokens_used

            else:
                # Chunking disabled - full-doc routing
                logger.info("?? Processing as single document (chunking disabled)...")
                model_choice = select_full_document_model(len(markdown_content))

                if model_choice == "qwen":
                    qwen = self._get_qwen_generator()
                    if qwen:
                        json_result, p_tokens, r_tokens, total = await qwen.generate_json_from_markdown(
                            markdown=markdown_content, document_type=document_type, schema_json=schema_json
                        )
                        logger.info("? Single document extraction complete (chunking disabled, Qwen)")
                        return json_result, p_tokens, r_tokens, total
                    else:
                        logger.error("? Qwen not available (chunking-disabled path), falling back to Gemini")
                        model_choice = "gemini-2.0"

                if model_choice == "gemini-2.0":
                    self._apply_gemini_model("gemini-2.0-flash", max_tokens=8192)
                elif model_choice == "gemini-2.5":
                    self._apply_gemini_model("gemini-2.5-flash", max_tokens=65536)

                prompt = self._create_single_document_extraction_prompt(markdown_content, document_type, schema_json)
                response_text, prompt_tokens, response_tokens = await self._call_gemini_api_async(prompt)

                if not response_text:
                    return {"status": "error", "message": "No response from Gemini"}, 0, 0, 0

                json_result = self._validate_json_response(response_text)
                logger.info("? Single document extraction complete (chunking disabled, Gemini)")
                return json_result, self.total_prompt_tokens, self.total_response_tokens, self.total_tokens_used

        except Exception as e:
            logger.error(f"Error generating JSON from Gemini: {e}", exc_info=True)
            return (
                {"status": "error", "message": str(e)},
                self.total_prompt_tokens, self.total_response_tokens, self.total_tokens_used
            )