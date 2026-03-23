# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Ultra-robust JSON parser for OLMOCR OCR Server.

Handles malformed JSON from LLM responses using:
- Multiple repair strategies (5 total)
- Character-by-character reconstruction
- Bracket and quote fixing
- Trailing comma removal
"""

import re
import json
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# ULTRA-ROBUST JSON PARSER CLASS
# ============================================================================
class RobustJSONParser:

    @staticmethod
    def clean_and_parse(json_text: str) -> dict:

        # Strategy 1: Try normal parsing
        try:
            parsed = json.loads(json_text)
            logger.info("? Strategy 1: Normal JSON parsing succeeded")
            return parsed
        except json.JSONDecodeError as e:
            logger.warning(f"?? Strategy 1 failed: {str(e)[:100]}")
        
        # Strategy 2: Basic cleaning + parse
        try:
            cleaned = RobustJSONParser._basic_clean(json_text)
            parsed = json.loads(cleaned)
            logger.info("? Strategy 2: Basic cleaning succeeded")
            return parsed
        except json.JSONDecodeError as e:
            logger.warning(f"?? Strategy 2 failed: {str(e)[:100]}")
        
        # Strategy 3: Advanced repair
        try:
            repaired = RobustJSONParser._advanced_repair(json_text)
            parsed = json.loads(repaired)
            logger.info("? Strategy 3: Advanced repair succeeded")
            return parsed
        except json.JSONDecodeError as e:
            logger.warning(f"?? Strategy 3 failed: {str(e)[:100]}")
        
        # Strategy 4: Aggressive repair
        try:
            aggressive = RobustJSONParser._aggressive_repair(json_text)
            parsed = json.loads(aggressive)
            logger.info("? Strategy 4: Aggressive repair succeeded")
            return parsed
        except json.JSONDecodeError as e:
            logger.warning(f"?? Strategy 4 failed: {str(e)[:100]}")
        
        # Strategy 5: Last resort - extract valid portions
        try:
            partial = RobustJSONParser._extract_valid_json_portions(json_text)
            if partial:
                logger.info(f"? Strategy 5: Partial extraction succeeded ({len(partial)} fields)")
                return partial
        except Exception as e:
            logger.error(f"? Strategy 5 failed: {e}")
        
        # All strategies failed
        logger.error("? All 5 repair strategies failed")
        return {
            "status": "error",
            "message": "JSON parsing failed after all repair attempts",
            "raw_content": json_text[:500]
        }
    
    @staticmethod
    def _basic_clean(text: str) -> str:
        """Basic cleaning - remove control chars, fix common issues"""
        # Remove NULL and control characters (except \n, \r, \t)
        cleaned = ''.join(
            char for char in text 
            if ord(char) >= 32 or char in '\n\r\t'
        )
        
        # Remove markdown fences
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        
        # Extract JSON structure
        first = cleaned.find('{')
        last = cleaned.rfind('}')
        if first != -1 and last != -1:
            cleaned = cleaned[first:last+1]
        
        return cleaned
    
    @staticmethod
    def _advanced_repair(text: str) -> str:
        """Advanced repair - fix escape sequences, newlines in strings"""
        result = []
        in_string = False
        escape_next = False
        i = 0
        
        while i < len(text):
            char = text[i]
            
            # Handle escape sequences
            if escape_next:
                # Fix invalid escapes
                if char not in '"\\\/bfnrtu':
                    # Invalid escape - remove the backslash
                    result.append(char)
                else:
                    result.append('\\')
                    result.append(char)
                escape_next = False
                i += 1
                continue
            
            # Check for backslash
            if char == '\\':
                if in_string:
                    escape_next = True
                    i += 1
                    continue
                else:
                    # Backslash outside string - skip it
                    i += 1
                    continue
            
            # Toggle string state
            if char == '"':
                in_string = not in_string
                result.append(char)
                i += 1
                continue
            
            # Handle problematic characters inside strings
            if in_string:
                # Convert newlines to escaped newlines
                if char == '\n':
                    result.append('\\n')
                    i += 1
                    continue
                
                # Convert carriage returns
                if char == '\r':
                    result.append('\\r')
                    i += 1
                    continue
                
                # Convert tabs
                if char == '\t':
                    result.append('\\t')
                    i += 1
                    continue
                
                # Skip other control characters
                if ord(char) < 32:
                    i += 1
                    continue
            
            result.append(char)
            i += 1
        
        repaired = ''.join(result)
        
        # Fix trailing commas
        repaired = re.sub(r',(\s*[}\]])', r'\1', repaired)
        
        # Fix multiple consecutive commas
        repaired = re.sub(r',\s*,+', ',', repaired)
        
        # Fix missing commas between array/object elements
        repaired = re.sub(r'"\s+"', '", "', repaired)
        repaired = re.sub(r'}\s*{', '}, {', repaired)
        repaired = re.sub(r']\s*\[', '], [', repaired)
        
        return repaired
    
    @staticmethod
    def _aggressive_repair(text: str) -> str:
        """Aggressive repair - rebuild JSON structure character by character"""
        # Start with advanced repair
        text = RobustJSONParser._advanced_repair(text)
        
        # Additional aggressive fixes
        
        # Fix unquoted keys (common LLM mistake)
        text = re.sub(r'{\s*(\w+)\s*:', r'{"\1":', text)
        text = re.sub(r',\s*(\w+)\s*:', r', "\1":', text)
        
        # Fix single quotes (should be double quotes)
        # Be careful - only outside of already-quoted strings
        parts = []
        in_string = False
        for char in text:
            if char == '"':
                in_string = not in_string
                parts.append(char)
            elif char == "'" and not in_string:
                parts.append('"')
            else:
                parts.append(char)
        text = ''.join(parts)
        
        # Ensure proper closing braces
        open_braces = text.count('{')
        close_braces = text.count('}')
        if open_braces > close_braces:
            text += '}' * (open_braces - close_braces)
        
        open_brackets = text.count('[')
        close_brackets = text.count(']')
        if open_brackets > close_brackets:
            text += ']' * (open_brackets - close_brackets)
        
        # Remove trailing commas before closing
        text = re.sub(r',(\s*[}\]])', r'\1', text)
        
        return text
    
    @staticmethod
    def _extract_valid_json_portions(text: str) -> Optional[dict]:
 
        result = {}
        
        # Try to extract key-value pairs
        # Pattern: "key": value
        pattern = r'"([^"]+)"\s*:\s*([^,}\]]+(?:[,}\]])?)'
        matches = re.finditer(pattern, text)
        
        for match in matches:
            key = match.group(1)
            value_str = match.group(2).rstrip(',}]').strip()
            
            # Try to parse the value
            try:
                # Try as JSON
                value = json.loads(value_str)
                result[key] = value
            except:
                # Try as string
                if value_str.startswith('"') and value_str.endswith('"'):
                    result[key] = value_str[1:-1]
                # Try as number
                elif value_str.replace('.', '').replace('-', '').isdigit():
                    try:
                        if '.' in value_str:
                            result[key] = float(value_str)
                        else:
                            result[key] = int(value_str)
                    except:
                        result[key] = value_str
                # Try as boolean
                elif value_str.lower() in ('true', 'false'):
                    result[key] = value_str.lower() == 'true'
                # Try as null
                elif value_str.lower() == 'null':
                    result[key] = None
                else:
                    result[key] = value_str
        
        if result:
            logger.info(f"Extracted {len(result)} key-value pairs from malformed JSON")
            return result
        
        return None
    
    @staticmethod
    def recursive_clean_values(obj):

        if isinstance(obj, dict):
            return {
                key: RobustJSONParser.recursive_clean_values(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [RobustJSONParser.recursive_clean_values(item) for item in obj]
        elif isinstance(obj, str):
            # Remove control characters from string
            cleaned = ''.join(
                char for char in obj
                if ord(char) >= 32 or char in '\n\r\t'
            )
            return cleaned
        else:
            return obj
