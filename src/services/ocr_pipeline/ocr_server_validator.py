# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import re
import logging
from typing import List, Tuple, Dict, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# GENERIC MARKDOWN VALIDATOR CLASS
# ============================================================================
class MarkdownValidator:

    def __init__(self, required_keywords: List[str]):

        self.required_keywords = required_keywords
        self.enabled = len(required_keywords) > 0
        
        logger.info("?? Markdown Validator initialized")
        logger.info(f"? Validation enabled: {self.enabled}")
        logger.info(f"?? Required keywords: {len(self.required_keywords)}")
        logger.info(f"?? CASE-SENSITIVE matching enabled")
        if self.required_keywords:
            logger.info(f"?? Keywords: {', '.join(self.required_keywords[:5])}{'...' if len(self.required_keywords) > 5 else ''}")
    
    def validate_markdown(self, markdown_content: str) -> Tuple[bool, List[str]]:

        if not self.enabled or len(self.required_keywords) == 0:
            logger.info("?? Validation is disabled (no keywords configured)")
            return True, []
        
        logger.info("=" * 80)
        logger.info("?? MARKDOWN VALIDATION: Starting keyword check (CASE-SENSITIVE)...")
        logger.info("=" * 80)
        logger.info(f"?? Markdown length: {len(markdown_content)} characters")
        logger.info(f"?? Checking for {len(self.required_keywords)} required keywords")
        logger.info(f"?? Case-sensitive mode: Exact case match required")
        
        # Perform keyword search
        found_keywords = []
        missing_keywords = []
        keyword_details = []
        
        for keyword in self.required_keywords:
            result = self._search_keyword(markdown_content, keyword)
            
            if result['found']:
                found_keywords.append(keyword)
                keyword_details.append({
                    'keyword': keyword,
                    'status': '?',
                    'count': result['count'],
                    'positions': result['positions'][:3]
                })
                logger.info(f"  ? '{keyword}' - FOUND ({result['count']} occurrences, exact case)")
            else:
                missing_keywords.append(keyword)
                keyword_details.append({
                    'keyword': keyword,
                    'status': '?',
                    'count': 0,
                    'positions': []
                })
                logger.warning(f"  ? '{keyword}' - MISSING (exact case not found)")
        
        # Log summary
        logger.info("=" * 80)
        logger.info("?? VALIDATION SUMMARY:")
        logger.info("=" * 80)
        logger.info(f"? Found: {len(found_keywords)}/{len(self.required_keywords)}")
        logger.info(f"? Missing: {len(missing_keywords)}/{len(self.required_keywords)}")
        
        if missing_keywords:
            logger.warning(f"?? Missing keywords (exact case): {', '.join(missing_keywords)}")
        
        is_valid = len(missing_keywords) == 0
        
        if is_valid:
            logger.info("=" * 80)
            logger.info("? MARKDOWN VALIDATION: PASSED")
            logger.info("=" * 80)
            logger.info("?? All required keywords found - OCR extraction is complete")
            logger.info("?? Proceeding with chunking and JSON extraction")
        else:
            logger.error("=" * 80)
            logger.error("? MARKDOWN VALIDATION: FAILED")
            logger.error("=" * 80)
            logger.error(f"?? OCR failed to extract complete document")
            logger.error(f"? Missing {len(missing_keywords)} required keywords (exact case)")
            logger.error(f"?? Missing: {', '.join(missing_keywords)}")
            logger.error("?? Will trigger fallback to OLMOCR...")
        logger.info("=" * 80)
        self._log_detailed_report(keyword_details, markdown_content)
        return is_valid, missing_keywords
    
    def _search_keyword(self, markdown_content: str, keyword: str) -> Dict:

        # ? CASE-SENSITIVE search - removed re.IGNORECASE flag
        pattern = re.compile(re.escape(keyword))
        matches = list(pattern.finditer(markdown_content))
        
        found = len(matches) > 0
        count = len(matches)
        positions = [match.start() for match in matches]
        
        # Extract context around first match (if found)
        context = []
        if found and len(matches) > 0:
            for match in matches[:3]:  # Get context for first 3 matches
                start_pos = max(0, match.start() - 50)
                end_pos = min(len(markdown_content), match.end() + 50)
                context_text = markdown_content[start_pos:end_pos].replace('\n', ' ')
                context.append(context_text)
        
        return {
            'found': found,
            'count': count,
            'positions': positions,
            'context': context
        }
    
    def _log_detailed_report(self, keyword_details: List[Dict], markdown_content: str):
        logger.info("=" * 80)
        logger.info("?? DETAILED KEYWORD REPORT (CASE-SENSITIVE):")
        logger.info("=" * 80)
        
        for detail in keyword_details:
            keyword = detail['keyword']
            status = detail['status']
            count = detail['count']
            positions = detail['positions']
            
            logger.info(f"{status} Keyword: '{keyword}' (exact case required)")
            logger.info(f"   Count: {count}")
            
            if count > 0:
                logger.info(f"   Positions: {positions}")
                if positions:
                    first_pos = positions[0]
                    start = max(0, first_pos - 100)
                    end = min(len(markdown_content), first_pos + 100)
                    context = markdown_content[start:end].replace('\n', ' ')
                    logger.info(f"   Context: ...{context}...")
            
            logger.info("")
        
        logger.info("=" * 80)
    
    def get_validation_report(self, markdown_content: str) -> Dict:

        is_valid, missing_keywords = self.validate_markdown(markdown_content)
        
        found_count = len(self.required_keywords) - len(missing_keywords)
        
        report = {
            'validation_enabled': self.enabled,
            'validation_passed': is_valid,
            'case_sensitive': True,  # ? Added to indicate case-sensitive matching
            'total_keywords_required': len(self.required_keywords),
            'keywords_found': found_count,
            'keywords_missing': len(missing_keywords),
            'missing_keyword_list': missing_keywords,
            'completion_percentage': round((found_count / len(self.required_keywords)) * 100, 2) if len(self.required_keywords) > 0 else 100,
            'markdown_length': len(markdown_content),
            'timestamp': self._get_timestamp()
        }
        
        return report
    
    def _get_timestamp(self) -> str:
        """Get current timestamp for reporting"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def check_partial_keywords(self, markdown_content: str, min_match_percentage: float = 0.7) -> bool:

        found_count = 0
        
        for keyword in self.required_keywords:
            result = self._search_keyword(markdown_content, keyword)
            if result['found']:
                found_count += 1
        
        match_percentage = found_count / len(self.required_keywords) if len(self.required_keywords) > 0 else 1.0
        
        logger.info(f"?? Partial keyword match (case-sensitive): {found_count}/{len(self.required_keywords)} ({match_percentage:.1%})")
        
        return match_percentage >= min_match_percentage
    
    def validate_with_fuzzy_matching(self, markdown_content: str, similarity_threshold: float = 0.8) -> Tuple[bool, List[str]]:

        try:
            from fuzzywuzzy import fuzz
            has_fuzzy = True
        except ImportError:
            logger.warning("?? fuzzywuzzy not installed, falling back to exact matching")
            has_fuzzy = False
            return self.validate_markdown(markdown_content)
        
        if not has_fuzzy:
            return self.validate_markdown(markdown_content)
        
        logger.info("?? Using fuzzy keyword matching (case-sensitive base)...")
        
        found_keywords = []
        missing_keywords = []
        
        words = re.findall(r'\b\w+\b', markdown_content)
        
        for keyword in self.required_keywords:
            if keyword in markdown_content:
                found_keywords.append(keyword)
                logger.info(f"  ? '{keyword}' - EXACT MATCH (case-sensitive)")
                continue
            
            best_match_score = 0
            best_match_text = None
            
            keyword_word_count = len(keyword.split())
            
            for i in range(len(words) - keyword_word_count + 1):
                window = ' '.join(words[i:i + keyword_word_count])
                score = fuzz.ratio(keyword, window) / 100.0
                
                if score > best_match_score:
                    best_match_score = score
                    best_match_text = window
            
            if best_match_score >= similarity_threshold:
                found_keywords.append(keyword)
                logger.info(f"  ? '{keyword}' - FUZZY MATCH (score: {best_match_score:.2f}, matched: '{best_match_text}')")
            else:
                missing_keywords.append(keyword)
                logger.warning(f"  ? '{keyword}' - NOT FOUND (best match: '{best_match_text}', score: {best_match_score:.2f})")
        
        is_valid = len(missing_keywords) == 0
        
        logger.info(f"?? Fuzzy validation result: {len(found_keywords)}/{len(self.required_keywords)} keywords found")
        
        return is_valid, missing_keywords
    
    def validate_with_alternatives(self, markdown_content: str, keyword_alternatives: Optional[Dict[str, List[str]]] = None) -> Tuple[bool, List[str]]:

        if keyword_alternatives is None:
            keyword_alternatives = {}
        
        logger.info("?? Validating with keyword alternatives (case-sensitive)...")
        
        found_keywords = []
        missing_keywords = []
        
        for keyword in self.required_keywords:
            # Check main keyword (case-sensitive)
            result = self._search_keyword(markdown_content, keyword)
            
            if result['found']:
                found_keywords.append(keyword)
                logger.info(f"  ? '{keyword}' - FOUND (exact case)")
                continue
            
            # Check alternatives (case-sensitive)
            alternatives = keyword_alternatives.get(keyword, [])
            found_alternative = False
            
            for alt in alternatives:
                alt_result = self._search_keyword(markdown_content, alt)
                if alt_result['found']:
                    found_keywords.append(keyword)
                    logger.info(f"  ? '{keyword}' - FOUND via alternative '{alt}' (exact case)")
                    found_alternative = True
                    break
            
            if not found_alternative:
                missing_keywords.append(keyword)
                logger.warning(f"  ? '{keyword}' - NOT FOUND (checked {len(alternatives)} alternatives, exact case)")
        
        is_valid = len(missing_keywords) == 0
        
        logger.info(f"?? Alternative validation result: {len(found_keywords)}/{len(self.required_keywords)} keywords found")
        
        return is_valid, missing_keywords


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def quick_validate_markdown(markdown_content: str, required_keywords: List[str]) -> bool:

    for keyword in required_keywords:
        # ? CASE-SENSITIVE: Removed .lower() conversion
        if keyword not in markdown_content:
            return False
    return True


def count_keyword_occurrences(markdown_content: str, keywords: List[str]) -> Dict[str, int]:

    counts = {}
    
    for keyword in keywords:
        # ? CASE-SENSITIVE: Removed re.IGNORECASE flag
        pattern = re.compile(re.escape(keyword))
        matches = pattern.findall(markdown_content)
        counts[keyword] = len(matches)
    
    return counts


def extract_keyword_context(markdown_content: str, keyword: str, context_chars: int = 100) -> List[str]:

    contexts = []
    # ? CASE-SENSITIVE: Removed re.IGNORECASE flag
    pattern = re.compile(re.escape(keyword))
    
    for match in pattern.finditer(markdown_content):
        start = max(0, match.start() - context_chars)
        end = min(len(markdown_content), match.end() + context_chars)
        context = markdown_content[start:end].replace('\n', ' ')
        contexts.append(context)
    
    return contexts

def create_validator_from_config(doc_config: Dict) -> Optional[MarkdownValidator]:

    conditional_keys = doc_config.get('conditional_keys', [])
    if not conditional_keys or len(conditional_keys) == 0:
        logger.info("?? No conditional keys configured - skipping validation")
        return None    
    return MarkdownValidator(conditional_keys)

def should_run_validation(doc_config: Dict) -> bool:

    has_keys = doc_config.get('has_conditional_keys', False)
    keys = doc_config.get('conditional_keys', [])
    return has_keys and len(keys) > 0

