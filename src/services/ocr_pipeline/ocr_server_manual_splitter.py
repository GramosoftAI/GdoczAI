# -*- coding: utf-8 -*-

#!/usr/bin/env python3

"""
Manual Markdown Splitter for Large LangChain Chunks

This module provides table-aware splitting for LangChain chunks that exceed 7,000 characters.
It preserves table structure by splitting based on rows while keeping headers intact.

Key Features:
- Detects HTML table structures in markdown
- Preserves table headers in each sub-chunk
- Splits tables into manageable sub-chunks (1 header + max 10 data rows)
- Only applied to LangChain chunks (NOT Unstructured chunks)
- Configurable thresholds and row limits
"""

import re
import logging
from typing import List, Dict, Tuple, Optional

logger = logging.getLogger(__name__)


class ManualMarkdownSplitter:

    def __init__(self, 
                 threshold_chars: int = 7000,
                 max_rows_per_chunk: int = 10):

        self.threshold_chars = threshold_chars
        self.max_rows_per_chunk = max_rows_per_chunk
        
        logger.info(f"?? Manual Splitter initialized:")
        logger.info(f"   ?? Threshold: {threshold_chars} characters")
        logger.info(f"   ?? Max rows per chunk: {max_rows_per_chunk}")
    
    def needs_manual_splitting(self, chunk_text: str) -> bool:

        return len(chunk_text) > self.threshold_chars
    
    def split_chunk(self, chunk_text: str, chunk_metadata: Dict) -> List[Dict]:

        chunk_size = len(chunk_text)
        logger.info("=" * 80)
        logger.info(f"?? MANUAL SPLITTING TRIGGERED")
        logger.info(f"   ?? Chunk size: {chunk_size} characters")
        logger.info(f"   ?? Threshold: {self.threshold_chars} characters")
        logger.info(f"   ?? Exceeds by: {chunk_size - self.threshold_chars} characters")
        logger.info("=" * 80)
        
        # Check if chunk contains HTML table
        if '<table>' in chunk_text and '</table>' in chunk_text:
            logger.info("?? Table structure detected - applying table-aware splitting")
            return self._split_table_chunk(chunk_text, chunk_metadata)
        else:
            logger.info("?? No table structure - applying text-based splitting")
            return self._split_text_chunk(chunk_text, chunk_metadata)
    
    def _split_table_chunk(self, chunk_text: str, chunk_metadata: Dict) -> List[Dict]:

        try:
            # Extract table and surrounding content
            table_pattern = r'(<table>.*?</table>)'
            match = re.search(table_pattern, chunk_text, re.DOTALL)
            
            if not match:
                logger.warning("?? Table tags found but regex failed - falling back to text split")
                return self._split_text_chunk(chunk_text, chunk_metadata)
            
            table_html = match.group(1)
            prefix_content = chunk_text[:match.start()].strip()
            suffix_content = chunk_text[match.end():].strip()
            
            logger.info(f"?? Table extraction:")
            logger.info(f"   ?? Prefix content: {len(prefix_content)} chars")
            logger.info(f"   ?? Table content: {len(table_html)} chars")
            logger.info(f"   ?? Suffix content: {len(suffix_content)} chars")
            
            # Parse table structure
            header_rows, data_rows = self._parse_table_structure(table_html)
            
            logger.info(f"?? Table structure:")
            logger.info(f"   ?? Header rows: {len(header_rows)}")
            logger.info(f"   ?? Data rows: {len(data_rows)}")
            
            if len(data_rows) == 0:
                logger.warning("?? No data rows found - returning original chunk")
                return [{
                    'text': chunk_text,
                    'token_count': len(chunk_text) // 4,
                    'metadata': {**chunk_metadata, 'manually_split': False}
                }]
            
            # Create sub-chunks
            sub_chunks = []
            total_data_rows = len(data_rows)
            num_sub_chunks = (total_data_rows + self.max_rows_per_chunk - 1) // self.max_rows_per_chunk
            
            logger.info(f"?? Splitting into {num_sub_chunks} sub-chunks:")
            logger.info(f"   ?? Max rows per sub-chunk: {self.max_rows_per_chunk}")
            
            for i in range(num_sub_chunks):
                start_idx = i * self.max_rows_per_chunk
                end_idx = min((i + 1) * self.max_rows_per_chunk, total_data_rows)
                
                # Get data rows for this sub-chunk
                sub_data_rows = data_rows[start_idx:end_idx]
                
                # Reconstruct table with header + sub-data
                sub_table = self._reconstruct_table(header_rows, sub_data_rows)
                
                # Build sub-chunk content
                sub_chunk_parts = []
                
                # Add prefix only to first sub-chunk
                if i == 0 and prefix_content:
                    sub_chunk_parts.append(prefix_content)
                
                # Add table
                sub_chunk_parts.append(sub_table)
                
                # Add suffix only to last sub-chunk
                if i == num_sub_chunks - 1 and suffix_content:
                    sub_chunk_parts.append(suffix_content)
                
                sub_chunk_text = '\n\n'.join(sub_chunk_parts)
                
                # Create sub-chunk metadata
                sub_metadata = {
                    **chunk_metadata,
                    'manually_split': True,
                    'sub_chunk_index': i + 1,
                    'total_sub_chunks': num_sub_chunks,
                    'rows_in_sub_chunk': len(sub_data_rows),
                    'row_range': f"{start_idx + 1}-{end_idx}"
                }
                
                sub_chunks.append({
                    'text': sub_chunk_text,
                    'token_count': len(sub_chunk_text) // 4,
                    'metadata': sub_metadata
                })
                
                logger.info(f"   ? Sub-chunk {i + 1}/{num_sub_chunks}:")
                logger.info(f"      ?? Size: {len(sub_chunk_text)} chars")
                logger.info(f"      ?? Rows: {start_idx + 1}-{end_idx} ({len(sub_data_rows)} rows)")
            
            logger.info(f"? Manual splitting complete: {len(sub_chunks)} sub-chunks created")
            return sub_chunks
            
        except Exception as e:
            logger.error(f"? Table splitting failed: {e}", exc_info=True)
            logger.warning("?? Falling back to text-based splitting")
            return self._split_text_chunk(chunk_text, chunk_metadata)
    
    def _parse_table_structure(self, table_html: str) -> Tuple[List[str], List[str]]:

        # Extract all rows
        row_pattern = r'<tr>(.*?)</tr>'
        all_rows = re.findall(row_pattern, table_html, re.DOTALL)
        
        if not all_rows:
            logger.warning("?? No rows found in table")
            return [], []
        
        # Detect header rows (rows with colspan, rowspan, or all <th> tags)
        header_rows = []
        data_rows = []
        
        for row in all_rows:
            # Check if row is a header row
            is_header = (
                'colspan' in row.lower() or 
                'rowspan' in row.lower() or
                '<th>' in row.lower()
            )
            
            if is_header and len(data_rows) == 0:
                # Header rows come before data rows
                header_rows.append(f'<tr>{row}</tr>')
            else:
                # This is a data row
                data_rows.append(f'<tr>{row}</tr>')
        
        # If no explicit headers detected, treat first row as header
        if len(header_rows) == 0 and len(data_rows) > 0:
            logger.info("?? No explicit headers detected - using first row as header")
            header_rows.append(data_rows.pop(0))
        
        return header_rows, data_rows
    
    def _reconstruct_table(self, header_rows: List[str], data_rows: List[str]) -> str:

        rows = header_rows + data_rows
        table_content = ''.join(rows)
        return f'<table>{table_content}</table>'
    
    def _split_text_chunk(self, chunk_text: str, chunk_metadata: Dict) -> List[Dict]:

        logger.info("?? Applying text-based splitting")
        
        # Split by paragraphs
        paragraphs = chunk_text.split('\n\n')
        logger.info(f"   ?? Found {len(paragraphs)} paragraphs")
        
        sub_chunks = []
        current_chunk = []
        current_size = 0
        
        for para in paragraphs:
            para_size = len(para)
            
            if current_size + para_size > self.threshold_chars and current_chunk:
                # Save current chunk
                sub_chunk_text = '\n\n'.join(current_chunk)
                sub_chunks.append({
                    'text': sub_chunk_text,
                    'token_count': len(sub_chunk_text) // 4,
                    'metadata': {
                        **chunk_metadata,
                        'manually_split': True,
                        'sub_chunk_index': len(sub_chunks) + 1,
                        'split_type': 'text_based'
                    }
                })
                
                # Start new chunk
                current_chunk = [para]
                current_size = para_size
            else:
                current_chunk.append(para)
                current_size += para_size + 2  # +2 for '\n\n'
        
        # Save remaining content
        if current_chunk:
            sub_chunk_text = '\n\n'.join(current_chunk)
            sub_chunks.append({
                'text': sub_chunk_text,
                'token_count': len(sub_chunk_text) // 4,
                'metadata': {
                    **chunk_metadata,
                    'manually_split': True,
                    'sub_chunk_index': len(sub_chunks) + 1,
                    'split_type': 'text_based'
                }
            })
        
        # Update total_sub_chunks in metadata
        for chunk in sub_chunks:
            chunk['metadata']['total_sub_chunks'] = len(sub_chunks)
        
        logger.info(f"? Text splitting complete: {len(sub_chunks)} sub-chunks created")
        return sub_chunks


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def process_oversized_chunks(chunks: List[Dict], config) -> List[Dict]:

    # Initialize splitter
    threshold = getattr(config, 'manual_split_threshold', 7000)
    max_rows = getattr(config, 'manual_split_max_rows', 10)
    
    splitter = ManualMarkdownSplitter(
        threshold_chars=threshold,
        max_rows_per_chunk=max_rows
    )
    
    logger.info("=" * 80)
    logger.info("?? CHECKING FOR OVERSIZED CHUNKS")
    logger.info(f"   ?? Total chunks to check: {len(chunks)}")
    logger.info(f"   ?? Size threshold: {threshold} characters")
    logger.info("=" * 80)
    
    final_chunks = []
    oversized_count = 0
    
    for i, chunk in enumerate(chunks, 1):
        chunk_text = chunk.get('text', '')
        chunk_size = len(chunk_text)
        chunk_metadata = chunk.get('metadata', {})
        
        if splitter.needs_manual_splitting(chunk_text):
            oversized_count += 1
            logger.info(f"?? Chunk {i}/{len(chunks)} is OVERSIZED ({chunk_size} chars)")
            
            # Split this chunk
            sub_chunks = splitter.split_chunk(chunk_text, chunk_metadata)
            final_chunks.extend(sub_chunks)
            
            logger.info(f"   ? Split into {len(sub_chunks)} sub-chunks")
        else:
            # Keep chunk as-is
            final_chunks.append(chunk)
            logger.info(f"? Chunk {i}/{len(chunks)} OK ({chunk_size} chars)")
    
    logger.info("=" * 80)
    logger.info(f"?? MANUAL SPLITTING SUMMARY:")
    logger.info(f"   ?? Input chunks: {len(chunks)}")
    logger.info(f"   ?? Oversized chunks: {oversized_count}")
    logger.info(f"   ?? Output chunks: {len(final_chunks)}")
    logger.info(f"   ? Additional chunks created: {len(final_chunks) - len(chunks)}")
    logger.info("=" * 80)
    
    return final_chunks


# ============================================================================
# TESTING AND VALIDATION
# ============================================================================

def test_splitter_with_sample():
    """
    Test the splitter with a sample table
    """
    sample_table = """<table><tr><td rowspan="2">Work Order Type</td><td rowspan="2">Parts #</td><td rowspan="2">Description of Goods</td><td rowspan="2">HSN Code</td><td rowspan="2">Qty.</td><td rowspan="2">Rate</td><td rowspan="2">Insurance Amount</td><td rowspan="2">Amount</td><td rowspan="2">Customer Discount %</td><td rowspan="2">Customer Contribution %</td><td rowspan="2">Taxable Value</td><td colspan="2">CGST</td><td colspan="2">SGST</td><td colspan="2">IGST</td><td colspan="2">UTGST</td><td rowspan="2">Total Amount</td></tr><tr><td>Rate</td><td>Amount</td><td>Rate</td><td>Amount</td><td>Rate</td><td>Amount</td><td>Rate</td><td>Amount</td></tr><tr><td></td><td>A24788096009999</td><td>TRIM, BUMPER</td><td>87089900</td><td>1</td><td>80839</td><td>0</td><td>80839</td><td>0</td><td>0</td><td>0</td><td>14</td><td>0</td><td>14</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td></tr><tr><td></td><td>A2436302001</td><td>SIDE WALL</td><td>87089900</td><td>1</td><td>166167</td><td>0</td><td>166167</td><td>0</td><td>0</td><td>0</td><td>14</td><td>0</td><td>14</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td></tr></table>"""
    
    splitter = ManualMarkdownSplitter(threshold_chars=500, max_rows_per_chunk=2)
    
    metadata = {'section': 'parts'}
    sub_chunks = splitter.split_chunk(sample_table, metadata)
    
    print(f"\nTest Results:")
    print(f"Original size: {len(sample_table)} chars")
    print(f"Sub-chunks created: {len(sub_chunks)}")
    
    for i, chunk in enumerate(sub_chunks, 1):
        print(f"\nSub-chunk {i}:")
        print(f"  Size: {len(chunk['text'])} chars")
        print(f"  Metadata: {chunk['metadata']}")


if __name__ == "__main__":
    # Run test
    logging.basicConfig(level=logging.INFO)
    test_splitter_with_sample()