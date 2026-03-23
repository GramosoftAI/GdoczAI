# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import json
import logging
from typing import Dict, List, Optional, Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from fastapi import HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

# ============================================================================
# PYDANTIC MODELS FOR NESTED SCHEMA SUPPORT
# ============================================================================

class FieldDefinition(BaseModel):
    """
    Recursive schema for field definitions with full nested support
    
    Supports:
    - String, Number, Boolean (primitives)
    - Array with items schema
    - Object with properties schema
    - Array<Object> with nested property definitions
    """
    field_name: str = Field(..., description="Name of the field to extract")
    type: Literal["String", "Number", "Boolean", "Array", "Object"] = Field(
        ..., 
        description="Data type: String, Number, Boolean, Array, or Object"
    )
    description: str = Field(..., description="Description to guide extraction")
    required: bool = Field(default=True, description="Whether this field is required")
    
    # Nested schema support
    items: Optional['FieldDefinition'] = Field(
        None, 
        description="Schema for Array elements (only valid when type=Array)"
    )
    properties: Optional[List['FieldDefinition']] = Field(
        None, 
        description="Schema for Object properties (only valid when type=Object)"
    )
    
    @model_validator(mode='after')
    def validate_nested_fields(self):
        """
        Validate that items/properties are only used with correct types
        """
        # items can only be used with Array
        if self.items is not None and self.type != 'Array':
            raise ValueError(f'items field can only be used with type=Array, got type={self.type}')
        
        # properties can only be used with Object
        if self.properties is not None and self.type != 'Object':
            raise ValueError(f'properties field can only be used with type=Object, got type={self.type}')
        
        return self

# Enable forward references for recursive model
FieldDefinition.model_rebuild()

class MarkdownExtractionRequest(BaseModel):
    """Request model for markdown extraction with nested schema support"""
    markdown_content: str = Field(..., description="Markdown content to extract from")
    fields: List[FieldDefinition] = Field(..., description="List of fields to extract (supports nesting)")

# ============================================================================
# RECURSIVE SCHEMA SERIALIZER FOR PROMPTS
# ============================================================================

class RecursiveSchemaSerializer:
    """Serializes nested schemas into human-readable prompt format"""
    
    @staticmethod
    def serialize_field(field: FieldDefinition, indent_level: int = 0) -> str:
        """
        Recursively serialize a field definition into prompt text
        
        Args:
            field: Field definition to serialize
            indent_level: Current indentation level for nested fields
            
        Returns:
            Formatted string describing the field schema
        """
        indent = "  " * indent_level
        required_marker = "REQUIRED" if field.required else "OPTIONAL"
        
        lines = []
        
        # Base field description
        if field.type == "Array":
            if field.items:
                # Array with typed elements
                lines.append(f'{indent}- "{field.field_name}" (Array, {required_marker}): {field.description}')
                lines.append(f'{indent}  Each element is a {field.items.type}:')
                
                if field.items.type == "Object" and field.items.properties:
                    # Array<Object> - describe object properties
                    lines.append(f'{indent}    Properties:')
                    for prop in field.items.properties:
                        prop_lines = RecursiveSchemaSerializer.serialize_field(prop, indent_level + 3)
                        lines.append(prop_lines)
                elif field.items.type == "Array":
                    # Nested Array - recurse
                    nested_lines = RecursiveSchemaSerializer.serialize_field(field.items, indent_level + 2)
                    lines.append(nested_lines)
                else:
                    # Primitive array element
                    req_marker = "REQUIRED" if field.items.required else "OPTIONAL"
                    lines.append(f'{indent}    - {field.items.type} ({req_marker}): {field.items.description}')
            else:
                # Untyped array (backward compatibility)
                lines.append(f'{indent}- "{field.field_name}" (Array, {required_marker}): {field.description}')
                lines.append(f'{indent}  Elements: Any type')
        
        elif field.type == "Object":
            lines.append(f'{indent}- "{field.field_name}" (Object, {required_marker}): {field.description}')
            if field.properties:
                lines.append(f'{indent}  Properties:')
                for prop in field.properties:
                    prop_lines = RecursiveSchemaSerializer.serialize_field(prop, indent_level + 2)
                    lines.append(prop_lines)
            else:
                lines.append(f'{indent}  Properties: Any')
        
        else:
            # Primitive types
            lines.append(f'{indent}- "{field.field_name}" ({field.type}, {required_marker}): {field.description}')
        
        return "\n".join(lines)
    
    @staticmethod
    def serialize_schema(fields: List[FieldDefinition]) -> str:

        lines = []
        for field in fields:
            lines.append(RecursiveSchemaSerializer.serialize_field(field, indent_level=0))
        
        return "\n".join(lines)


# ============================================================================
# RECURSIVE JSON EXAMPLE GENERATOR
# ============================================================================

class RecursiveExampleGenerator:
    """Generates example JSON structures from schema"""
    
    @staticmethod
    def generate_example_value(field: FieldDefinition) -> Any:

        if field.type == "String":
            return "example text"
        elif field.type == "Number":
            return 123
        elif field.type == "Boolean":
            return True
        elif field.type == "Array":
            if field.items:
                # Generate example array with one element
                example_element = RecursiveExampleGenerator.generate_example_value(field.items)
                return [example_element]
            else:
                return ["item1", "item2"]
        elif field.type == "Object":
            if field.properties:
                # Generate nested object
                obj = {}
                for prop in field.properties:
                    obj[prop.field_name] = RecursiveExampleGenerator.generate_example_value(prop)
                return obj
            else:
                return {"key": "value"}
        else:
            return None
    
    @staticmethod
    def generate_example_output(fields: List[FieldDefinition]) -> Dict:

        output = {}
        for field in fields:
            output[field.field_name] = RecursiveExampleGenerator.generate_example_value(field)
        
        return output


# ============================================================================
# EXTRACTION PROMPT BUILDER WITH NESTED SCHEMA SUPPORT
# ============================================================================

class ExtractionPromptBuilder:
    """Builds extraction prompts for Gemini with full nested schema support"""
    
    @staticmethod
    def build_extraction_prompt(markdown_content: str, fields: List[FieldDefinition]) -> str:

        # Serialize schema recursively
        schema_description = RecursiveSchemaSerializer.serialize_schema(fields)
        
        # Generate example output
        example_output = RecursiveExampleGenerator.generate_example_output(fields)
        example_json = json.dumps(example_output, indent=2)
        
        prompt = f"""You are an expert data extraction AI. Your task is to extract structured information from markdown content following a PRECISE SCHEMA.

EXTRACTION SCHEMA:
{schema_description}

CRITICAL EXTRACTION RULES:
1. Read the entire markdown content carefully
2. Extract ONLY the fields defined in the schema above
3. Follow the EXACT schema structure for nested objects and arrays
4. For Arrays with typed elements:
   - Each array element MUST match the defined element schema
   - Do NOT add elements that don't match the schema
5. For Objects with properties:
   - Include ONLY the properties defined in the schema
   - Do NOT add extra properties not in the schema
6. For Array<Object>:
   - Each array element is an object with the defined properties
   - All objects in the array must have the SAME structure
7. Data type accuracy:
   - String: Extract as text, remove extra whitespace, no quotes in the value
   - Number: Extract as integer or float (no quotes, just the number)
   - Boolean: Extract as true/false (no quotes)
   - Array: Extract as JSON array [ ]
   - Object: Extract as JSON object {{ }}
8. For REQUIRED fields:
   - Extract the value or use null if not found
   - Do NOT omit required fields
9. For OPTIONAL fields:
   - Only include if the information is present in the markdown
   - Omit the field entirely if not found
10. If a value cannot be found, use null for that field
11. Do NOT invent or hallucinate data
12. Do NOT add fields not defined in the schema
13. Do NOT change the structure of nested objects/arrays

OUTPUT FORMAT:
Return ONLY valid JSON. No explanations, no markdown fences, no extra text.
Start with {{ and end with }}.

EXAMPLE OUTPUT STRUCTURE:
{example_json}

MARKDOWN CONTENT TO EXTRACT FROM:
---START MARKDOWN---
{markdown_content}
---END MARKDOWN---

FINAL REMINDERS:
- Return ONLY the JSON object
- Match the schema structure EXACTLY
- No extra keys beyond the schema
- For nested structures, maintain the exact hierarchy
- Arrays must contain only elements matching the item schema
- Objects must contain only the defined properties
- Ensure all strings are properly escaped
- No control characters in string values
- No trailing commas before }} or ]]
- Start with {{ and end with }}
- No markdown code fences (```)
- No explanatory text before or after JSON

OUTPUT (JSON only):"""
        
        return prompt


# ============================================================================
# RECURSIVE JSON VALIDATOR
# ============================================================================

class RecursiveJSONValidator:
    """Validates and cleans JSON output against nested schema"""
    
    @staticmethod
    def validate_and_clean_value(value: Any, field: FieldDefinition) -> Any:

        if value is None:
            return None
        
        if field.type == "String":
            return str(value) if value is not None else None
        
        elif field.type == "Number":
            try:
                # Try int first, then float
                if isinstance(value, (int, float)):
                    return value
                return float(value) if '.' in str(value) else int(value)
            except (ValueError, TypeError):
                return None
        
        elif field.type == "Boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes')
            return bool(value)
        
        elif field.type == "Array":
            if not isinstance(value, list):
                return None
            
            if field.items:
                # Validate each array element against items schema
                cleaned_array = []
                for item in value:
                    cleaned_item = RecursiveJSONValidator.validate_and_clean_value(item, field.items)
                    if cleaned_item is not None:
                        cleaned_array.append(cleaned_item)
                return cleaned_array
            else:
                # Untyped array - return as is
                return value
        
        elif field.type == "Object":
            if not isinstance(value, dict):
                return None
            
            if field.properties:
                # Validate and clean object properties
                cleaned_obj = {}
                for prop in field.properties:
                    if prop.field_name in value:
                        cleaned_value = RecursiveJSONValidator.validate_and_clean_value(
                            value[prop.field_name], 
                            prop
                        )
                        if cleaned_value is not None or prop.required:
                            cleaned_obj[prop.field_name] = cleaned_value
                
                return cleaned_obj
            else:
                # Untyped object - return as is
                return value
        
        return value
    
    @staticmethod
    def validate_and_clean_output(output: Dict, fields: List[FieldDefinition]) -> Dict:

        cleaned_output = {}
        
        for field in fields:
            if field.field_name in output:
                cleaned_value = RecursiveJSONValidator.validate_and_clean_value(
                    output[field.field_name],
                    field
                )
                
                # Include value if it's not None or if field is required
                if cleaned_value is not None or field.required:
                    cleaned_output[field.field_name] = cleaned_value
            elif field.required:
                # Required field missing - set to null
                cleaned_output[field.field_name] = None
        
        return cleaned_output


# ============================================================================
# MARKDOWN EXTRACTOR CLASS (UPDATED)
# ============================================================================

class MarkdownExtractor:
    """Handles markdown-to-JSON extraction with full nested schema support"""
    
    def __init__(self, gemini_generator):

        self.gemini_generator = gemini_generator
        self.prompt_builder = ExtractionPromptBuilder()
        self.validator = RecursiveJSONValidator()
    
    async def extract_from_markdown(
        self, 
        markdown_content: str, 
        fields: List[FieldDefinition],
    ) -> Dict:

        if not self.gemini_generator.enabled:
            raise HTTPException(
                status_code=503,
                detail="Gemini API is not available. Cannot perform extraction."
            )
        
        try:
            # Reset token counters
            self.gemini_generator.reset_token_counters()
            
            logger.info("=" * 80)
            logger.info("?? MARKDOWN EXTRACTION REQUEST (NESTED SCHEMA)")
            logger.info("=" * 80)
            logger.info(f"?? Markdown Length: {len(markdown_content)} characters")
            logger.info(f"?? Fields to Extract: {len(fields)}")
            
            # Log field structure
            for field in fields:
                self._log_field_structure(field, indent_level=0)
            
            logger.info("=" * 80)
            
            # Build extraction prompt with nested schema
            prompt = self.prompt_builder.build_extraction_prompt(
                markdown_content=markdown_content,
                fields=fields
            )
            
            logger.info("?? Calling Gemini API for nested extraction...")
            
            # Call Gemini API
            response_text, prompt_tokens, response_tokens = await self.gemini_generator._call_gemini_api_async(prompt)
            
            if not response_text:
                raise HTTPException(
                    status_code=500,
                    detail="No response received from Gemini API"
                )
            
            logger.info("? Received response from Gemini")
            logger.info(f"?? Token Usage - Prompt: {prompt_tokens}, Response: {response_tokens}, Total: {prompt_tokens + response_tokens}")
            
            # Validate and parse JSON
            extracted_data = self.gemini_generator._validate_json_response(response_text)
            
            if extracted_data.get("status") == "error":
                logger.error(f"? JSON validation failed: {extracted_data.get('message')}")
                raise HTTPException(
                    status_code=500,
                    detail=f"JSON extraction failed: {extracted_data.get('message')}"
                )
            
            # Apply recursive validation and cleaning
            logger.info("?? Applying recursive schema validation...")
            cleaned_data = self.validator.validate_and_clean_output(extracted_data, fields)
            
            logger.info("=" * 80)
            logger.info("? EXTRACTION SUCCESSFUL (NESTED SCHEMA)")
            logger.info("=" * 80)
            logger.info(f"?? Extracted Fields: {len(cleaned_data)} fields")
            
            # Log extracted field names
            for key in cleaned_data.keys():
                value_preview = str(cleaned_data[key])[:100]
                logger.info(f"   ? {key}: {value_preview}{'...' if len(str(cleaned_data[key])) > 100 else ''}")
            
            logger.info("=" * 80)
            
            return {
                "success": True,
                "extracted_data": cleaned_data,
                "metadata": {
                    "fields_requested": len(fields),
                    "fields_extracted": len(cleaned_data),
                    "markdown_length": len(markdown_content),
                    "schema_type": "nested",
                    "token_usage": {
                        "prompt_tokens": prompt_tokens,
                        "response_tokens": response_tokens,
                        "total_tokens": prompt_tokens + response_tokens
                    }
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"? Extraction error: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Extraction failed: {str(e)}"
            )
    
    def _log_field_structure(self, field: FieldDefinition, indent_level: int = 0):
        """Log field structure recursively for debugging"""
        indent = "  " * indent_level
        req_str = "? REQUIRED" if field.required else "? OPTIONAL"
        
        if field.type == "Array" and field.items:
            logger.info(f"{indent}{req_str} | {field.field_name} (Array<{field.items.type}>)")
            if field.items.type == "Object" and field.items.properties:
                logger.info(f"{indent}  +- Object properties:")
                for prop in field.items.properties:
                    self._log_field_structure(prop, indent_level + 2)
        elif field.type == "Object" and field.properties:
            logger.info(f"{indent}{req_str} | {field.field_name} (Object)")
            logger.info(f"{indent}  +- Properties:")
            for prop in field.properties:
                self._log_field_structure(prop, indent_level + 2)
        else:
            logger.info(f"{indent}{req_str} | {field.field_name} ({field.type}): {field.description[:60]}...")


# ============================================================================
# API ENDPOINT (Updated)
# ============================================================================

async def extract_from_markdown_endpoint(
    request: MarkdownExtractionRequest,
    gemini_generator,
    user_id: int
) -> Dict:
    
    extractor = MarkdownExtractor(gemini_generator)
    
    result = await extractor.extract_from_markdown(
        markdown_content=request.markdown_content,
        fields=request.fields
    )
    result["metadata"]["user_id"] = user_id
    return JSONResponse(content=result)