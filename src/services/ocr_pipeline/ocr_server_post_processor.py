# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
logger = logging.getLogger(__name__)
# ============================================================================
# HELPER FUNCTIONS FOR NULL DETECTION
# ============================================================================
def is_all_null(data: Any) -> bool:

    if data is None:
        return True
    
    if isinstance(data, str):
        return data.strip().lower() in ["null", ""]
    
    if isinstance(data, dict):
        if not data:  # Empty dict
            return True
        return all(is_all_null(v) for v in data.values())
    
    if isinstance(data, list):
        if not data:  # Empty list
            return True
        return all(is_all_null(item) for item in data)
    
    return False

def has_meaningful_data(data: Any) -> bool:
    return not is_all_null(data)

def filter_null_items(items: List[Any]) -> List[Any]:
    return [item for item in items if has_meaningful_data(item)]

def merge_with_null_handling(existing: Any, new: Any) -> Any:

    existing_has_data = has_meaningful_data(existing)
    new_has_data = has_meaningful_data(new)
    
    if not new_has_data:
        return existing
    
    if not existing_has_data:
        return new
    
    if isinstance(existing, dict) and isinstance(new, dict):
        merged = existing.copy()
        for key, value in new.items():
            if key in merged:
                merged[key] = merge_with_null_handling(merged[key], value)
            else:
                if has_meaningful_data(value):
                    merged[key] = value
        return merged
    
    if isinstance(existing, list) and isinstance(new, list):
        # Extend list but filter out null items
        combined = existing + new
        return filter_null_items(combined)
    
    return new if new_has_data else existing

# ============================================================================
# ?? CONFLICT RESOLUTION FUNCTIONS (FOR UNSTRUCTURED CHUNKS)
# ============================================================================
def resolve_field_conflict(field_name: str, values: List[Any]) -> Any:

    non_null_values = [v for v in values if has_meaningful_data(v)]
    
    if not non_null_values:
        return None
    
    if len(non_null_values) == 1:
        return non_null_values[0]
    
    logger.info(f"?? Resolving conflict for field '{field_name}' with {len(non_null_values)} different values")
    
    # Rule 1: For numeric fields, take the maximum (likely most complete)
    if all(isinstance(v, (int, float)) for v in non_null_values):
        resolved = max(non_null_values)
        logger.info(f"   ? Numeric conflict resolved: {resolved} (max of {non_null_values})")
        return resolved
    
    # Rule 2: For string fields, take the longest (likely most complete)
    if all(isinstance(v, str) for v in non_null_values):
        resolved = max(non_null_values, key=len)
        logger.info(f"   ? String conflict resolved: '{resolved[:50]}...' (longest)")
        return resolved
    
    # Rule 3: For lists, merge and deduplicate
    if all(isinstance(v, list) for v in non_null_values):
        merged_list = []
        for lst in non_null_values:
            merged_list.extend(lst)
        deduplicated = deduplicate_list(merged_list)
        logger.info(f"   ? List conflict resolved: merged {len(non_null_values)} lists into {len(deduplicated)} items")
        return deduplicated
    
    # Rule 4: For dicts, deep merge
    if all(isinstance(v, dict) for v in non_null_values):
        merged_dict = {}
        for d in non_null_values:
            for key, value in d.items():
                if key in merged_dict:
                    merged_dict[key] = merge_with_null_handling(merged_dict[key], value)
                else:
                    merged_dict[key] = value
        logger.info(f"   ? Dict conflict resolved: merged {len(non_null_values)} dicts")
        return merged_dict
    
    # Rule 5: Default - take first non-null value
    resolved = non_null_values[0]
    logger.info(f"   ?? Mixed types conflict: using first value")
    return resolved


def deduplicate_list(items: List[Any]) -> List[Any]:
    """
    Safely deduplicate a list, handling both hashable and unhashable items.
    """
    if not items:
        return []
    
    # Filter null items first
    items = filter_null_items(items)
    
    if not items:
        return []
    
    try:
        return list(dict.fromkeys(items))
    except TypeError:
        deduplicated = []
        seen_json = set()
        
        for item in items:
            try:
                item_json = json.dumps(item, sort_keys=True)
                if item_json not in seen_json:
                    seen_json.add(item_json)
                    deduplicated.append(item)
            except (TypeError, ValueError):
                deduplicated.append(item)        
        return deduplicated

# ============================================================================
# GENERIC POST-PROCESSOR CLASS
# ============================================================================
class GenericPostProcessor:
    
    def __init__(self):
        logger.info("? Enhanced Generic Post-Processor initialized")
        logger.info("   ?? Hardened for Unstructured chunk boundaries")
        logger.info("   ? Conflict resolution enabled")
        logger.info("   ?? Array deduplication enabled")
    
    def process_chunks(self, chunk_jsons: List[Dict], section_keys: List[str]) -> Dict:

        try:
            logger.info("=" * 80)
            logger.info("?? GENERIC POST-PROCESSING: Starting enhanced merge...")
            logger.info("=" * 80)
            logger.info(f"?? Processing {len(chunk_jsons)} chunks")
            logger.info(f"?? Section keys: {', '.join(section_keys) if section_keys else 'None (Unstructured chunks)'}")
            
            if not chunk_jsons or len(chunk_jsons) == 0:
                logger.error("? No chunks to process")
                return {
                    "status": "error",
                    "message": "No chunks provided for post-processing"
                }
            
            # Analyze chunk structure and determine processing strategy
            strategy = self._determine_strategy(chunk_jsons, section_keys)
            logger.info(f"?? Processing strategy: {strategy['type']}")
            
            if strategy['type'] == 'section_based':
                result = self._merge_sections(chunk_jsons, section_keys, strategy)
            elif strategy['type'] == 'table_based':
                result = self._merge_tables(chunk_jsons, section_keys)
            else:
                result = self._simple_merge(chunk_jsons)
            
            # ?? Apply conflict resolution for overlapping fields
            result = self._resolve_conflicts(result, chunk_jsons)
            
            # Clean up: Remove metadata and null-only sections
            result = self._cleanup_final_output(result)
            
            logger.info("=" * 80)
            logger.info("? GENERIC POST-PROCESSING COMPLETE")
            logger.info("=" * 80)
            
            return result
            
        except Exception as e:
            logger.error(f"? Generic post-processing error: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Post-processing failed: {str(e)}"
            }
    
    def _determine_strategy(self, chunk_jsons: List[Dict], section_keys: List[str]) -> Dict:
        has_header = False
        has_tables = False
        has_summary = False
        table_count = 0
        
        for chunk in chunk_jsons:
            if "Header" in chunk or "header" in chunk:
                has_header = True
            if "tables" in chunk or "Tables" in chunk:
                has_tables = True
                tables = chunk.get("tables", chunk.get("Tables", []))
                if isinstance(tables, list):
                    table_count += len(tables)
            if "InvoiceSummary" in chunk or "Summary" in chunk or "summary" in chunk:
                has_summary = True
        
        logger.info(f"?? Chunk analysis:")
        logger.info(f"   Header found: {has_header}")
        logger.info(f"   Tables found: {has_tables} (count: {table_count})")
        logger.info(f"   Summary found: {has_summary}")
        
        if has_header and has_tables and has_summary:
            return {
                'type': 'section_based',
                'has_header': True,
                'has_tables': True,
                'has_summary': True,
                'table_count': table_count
            }
        elif has_tables:
            return {
                'type': 'table_based',
                'table_count': table_count
            }
        else:
            return {
                'type': 'simple_merge'
            }
    
    def _merge_sections(self, chunk_jsons: List[Dict], section_keys: List[str], strategy: Dict) -> Dict:
    
        logger.info("?? Using section-based merging strategy with null filtering")
        
        result = {
            "Header": [],
            "tables": [],
            "Summary": {}
        }
        
        # Process each chunk
        for idx, chunk in enumerate(chunk_jsons, 1):
            logger.info(f"?? Processing chunk {idx}/{len(chunk_jsons)}...")
            
            # Extract and merge Header (with null filtering)
            header_data = self._extract_field(chunk, ["Header", "header", "HEADER"])
            if header_data and has_meaningful_data(header_data):
                if isinstance(header_data, list):
                    # Filter out null items
                    filtered_headers = filter_null_items(header_data)
                    if filtered_headers:
                        result["Header"].extend(filtered_headers)
                        logger.info(f"   ? Added {len(filtered_headers)} header items (filtered from {len(header_data)})")
                    else:
                        logger.info(f"   ?? Skipped {len(header_data)} null header items")
                elif isinstance(header_data, dict):
                    result["Header"].append(header_data)
                    logger.info(f"   ? Added 1 header item")
            else:
                logger.info(f"   ?? Skipped null header data")
            
            # Extract and merge tables (with null filtering)
            tables_data = self._extract_field(chunk, ["tables", "Tables", "TABLES"])
            if tables_data and has_meaningful_data(tables_data):
                if isinstance(tables_data, list):
                    # Filter out null tables
                    filtered_tables = filter_null_items(tables_data)
                    if filtered_tables:
                        # Tag each table with section metadata
                        for table in filtered_tables:
                            if isinstance(table, dict):
                                section_name = self._identify_table_section(table, section_keys, idx)
                                if section_name:
                                    table['_section'] = section_name
                        
                        result["tables"].extend(filtered_tables)
                        logger.info(f"   ? Added {len(filtered_tables)} table(s) (filtered from {len(tables_data)})")
                    else:
                        logger.info(f"   ?? Skipped {len(tables_data)} null tables")
                elif isinstance(tables_data, dict):
                    result["tables"].append(tables_data)
                    logger.info(f"   ? Added 1 table")
            else:
                logger.info(f"   ?? Skipped null table data")
            
            # Extract and merge Summary (with intelligent merging)
            summary_data = self._extract_field(chunk, [
                "InvoiceSummary", "Invoice Summary", "Summary", 
                "summary", "SUMMARY"
            ])
            if summary_data and has_meaningful_data(summary_data):
                if isinstance(summary_data, dict):
                    for key, value in summary_data.items():
                        if has_meaningful_data(value):
                            result["Summary"][key] = value
                    logger.info(f"   ? Merged summary fields (non-null only)")
                elif isinstance(summary_data, list) and len(summary_data) > 0:
                    if isinstance(summary_data[0], dict):
                        for key, value in summary_data[0].items():
                            if has_meaningful_data(value):
                                result["Summary"][key] = value
                        logger.info(f"   ? Merged summary from list (non-null only)")
            else:
                logger.info(f"   ?? Skipped null summary data")
        
        if result["Header"]:
            result["Header"] = self._deduplicate_headers(result["Header"])
        
        logger.info(f"? Section merge complete:")
        logger.info(f"   Header items: {len(result['Header'])}")
        logger.info(f"   Tables: {len(result['tables'])}")
        logger.info(f"   Summary fields: {len(result['Summary'])}")
        
        return result
    
    def _merge_tables(self, chunk_jsons: List[Dict], section_keys: List[str]) -> Dict:
        logger.info("?? Using table-based merging strategy with null filtering")
        
        result = {
            "tables": [],
            "metadata": {}
        }
        for idx, chunk in enumerate(chunk_jsons, 1):
            tables_data = self._extract_field(chunk, ["tables", "Tables", "TABLES"])
            if tables_data and has_meaningful_data(tables_data):
                if isinstance(tables_data, list):
                    filtered_tables = filter_null_items(tables_data)
                    for table in filtered_tables:
                        if isinstance(table, dict):
                            section_name = self._identify_table_section(table, section_keys, idx)
                            if section_name:
                                table['_section'] = section_name
                    result["tables"].extend(filtered_tables)
                elif isinstance(tables_data, dict):
                    result["tables"].append(tables_data)
            
            for key, value in chunk.items():
                if key not in ["tables", "Tables", "TABLES", "_metadata", "chunk_metadata"]:
                    if has_meaningful_data(value):
                        if key not in result["metadata"]:
                            result["metadata"][key] = value
        
        logger.info(f"? Table merge complete: {len(result['tables'])} tables")
        
        return result
    
    def _simple_merge(self, chunk_jsons: List[Dict]) -> Dict:

        logger.info("?? Using simple merge strategy with null filtering and conflict resolution")
        
        result = {}
        field_occurrences = {}  # Track how many times each field appears
        
        for idx, chunk in enumerate(chunk_jsons, 1):
            for key, value in chunk.items():
                if key in ["_metadata", "chunk_metadata"]:
                    continue
                
                if not has_meaningful_data(value):
                    logger.debug(f"   ?? Skipped null value for key '{key}'")
                    continue
                
                if key not in field_occurrences:
                    field_occurrences[key] = []
                field_occurrences[key].append(value)
                
                if key not in result:
                    result[key] = value
                else:
                    result[key] = merge_with_null_handling(result[key], value)
        
        conflicts_found = 0
        for key, values in field_occurrences.items():
            if len(values) > 1:
                unique_values = []
                for v in values:
                    if not any(self._values_equal(v, uv) for uv in unique_values):
                        unique_values.append(v)
                
                if len(unique_values) > 1:
                    conflicts_found += 1
                    logger.info(f"?? Conflict detected for field '{key}': {len(unique_values)} different values across {len(values)} chunks")
        
        if conflicts_found > 0:
            logger.info(f"?? Resolved {conflicts_found} field conflicts using precedence rules")
        
        logger.info(f"? Simple merge complete: {len(result)} top-level keys")
        
        return result
    
    def _values_equal(self, val1: Any, val2: Any) -> bool:
        try:
            if isinstance(val1, dict) and isinstance(val2, dict):
                return json.dumps(val1, sort_keys=True) == json.dumps(val2, sort_keys=True)
            elif isinstance(val1, list) and isinstance(val2, list):
                return json.dumps(val1, sort_keys=True) == json.dumps(val2, sort_keys=True)
            else:
                return val1 == val2
        except:
            return False
    
    def _resolve_conflicts(self, result: Dict, chunk_jsons: List[Dict]) -> Dict:

        logger.info("?? Applying final conflict resolution pass...")
        field_values = {}
        for chunk in chunk_jsons:
            for key, value in chunk.items():
                if key in ["_metadata", "chunk_metadata"]:
                    continue
                if not has_meaningful_data(value):
                    continue
                
                if key not in field_values:
                    field_values[key] = []
                field_values[key].append(value)
        
        conflicts_resolved = 0
        for key, values in field_values.items():
            if len(values) > 1:
                unique_values = []
                for v in values:
                    if not any(self._values_equal(v, uv) for uv in unique_values):
                        unique_values.append(v)
                
                if len(unique_values) > 1:
                    resolved_value = resolve_field_conflict(key, unique_values)
                    result[key] = resolved_value
                    conflicts_resolved += 1
        
        if conflicts_resolved > 0:
            logger.info(f"? Resolved {conflicts_resolved} additional conflicts in final pass")
        else:
            logger.info(f"? No additional conflicts found")
        
        return result
    
    def _deduplicate_headers(self, headers: List[Dict]) -> List[Dict]:

        if not headers or len(headers) <= 1:
            return headers
        
        logger.info(f"?? Deduplicating {len(headers)} header entries...")
        
        def score_header(header: Dict) -> int:
            if not isinstance(header, dict):
                return 0
            score = 0
            for value in header.values():
                if has_meaningful_data(value):
                    score += 1
            return score
        
        max_score = max(score_header(h) for h in headers)
        deduplicated = [h for h in headers if score_header(h) == max_score]
        
        if len(deduplicated) > 1:
            deduplicated = [deduplicated[0]]
        
        logger.info(f"   ? Reduced to {len(deduplicated)} header(s)")
        
        return deduplicated
    
    def _cleanup_final_output(self, result: Dict) -> Dict:

        logger.info("?? Cleaning up final output...")        
        metadata_keys = ["_metadata", "metadata", "chunk_metadata"]
        for key in metadata_keys:
            if key in result:
                del result[key]
                logger.info(f"   ? Removed '{key}' from final output")
        
        empty_keys = []
        for key, value in result.items():
            if not has_meaningful_data(value):
                empty_keys.append(key)
        
        for key in empty_keys:
            del result[key]
            logger.info(f"   ? Removed empty section '{key}'")
        
        return result
    
    def _extract_field(self, chunk: Dict, field_names: List[str]) -> Any:
        for field_name in field_names:
            if field_name in chunk:
                return chunk[field_name]
        return None
    
    def _identify_table_section(self, table: Dict, section_keys: List[str], chunk_idx: int) -> Optional[str]:
        if '_section' in table:
            return table['_section']
        if 'section' in table:
            return table['section']
        
        table_name = table.get('table_name', '').lower()
        for section_key in section_keys:
            if section_key.lower() in table_name:
                return section_key.lower()
        
        table_str = json.dumps(table).lower()
        for section_key in section_keys:
            if section_key.lower() in table_str:
                return section_key.lower()
        
        return None
    
    def validate_structure(self, merged_json: Dict) -> Tuple[bool, List[str]]:
        warnings = []
        expected_keys = ["Header", "tables", "Summary"]
        for key in expected_keys:
            if key not in merged_json:
                warnings.append(f"Missing expected key: '{key}'")
            else:
                if key == "Header" and not isinstance(merged_json[key], list):
                    warnings.append(f"'{key}' should be a list, got {type(merged_json[key])}")
                elif key == "tables" and not isinstance(merged_json[key], list):
                    warnings.append(f"'{key}' should be a list, got {type(merged_json[key])}")
                elif key == "Summary" and not isinstance(merged_json[key], dict):
                    warnings.append(f"'{key}' should be a dict, got {type(merged_json[key])}")
                
                if not has_meaningful_data(merged_json[key]):
                    warnings.append(f"'{key}' contains no meaningful data")
        is_valid = len(warnings) == 0
        if warnings:
            logger.warning(f"?? Validation warnings: {', '.join(warnings)}")
        else:
            logger.info("? Structure validation passed")
        
        return is_valid, warnings


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def clean_table_data(table: Dict) -> Dict:
    internal_fields = ["_section", "_chunk_index", "_original_format", "chunk_metadata"]
    cleaned = {k: v for k, v in table.items() if k not in internal_fields}
    return cleaned

def extract_section_name_from_table(table: Dict) -> Optional[str]:
    return table.get("_section") or table.get("section") or None

def group_tables_by_section(tables: List[Dict]) -> Dict[str, List[Dict]]:
    grouped = {}
    for table in tables:
        section = extract_section_name_from_table(table)
        if section:
            if section not in grouped:
                grouped[section] = []
            grouped[section].append(table)
        else:
            if "unknown" not in grouped:
                grouped["unknown"] = []
            grouped["unknown"].append(table)
    
    return grouped


def merge_duplicate_entries(items: List[Any], key_field: Optional[str] = None) -> List[Any]:
    if not items:
        return []
    items = filter_null_items(items)    
    if not items:
        return []
    if not isinstance(items[0], dict):
        return list(set(items))
    seen = set()
    unique = []
    for item in items:
        if key_field and key_field in item:
            key = item[key_field]
            if key not in seen:
                seen.add(key)
                unique.append(item)
        else:
            item_str = json.dumps(item, sort_keys=True)
            if item_str not in seen:
                seen.add(item_str)
                unique.append(item)
    return unique

def create_post_processor() -> GenericPostProcessor:
    return GenericPostProcessor()