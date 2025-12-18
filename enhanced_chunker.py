"""
Enhanced Chunker for Azure Document Intelligence

Implements production-grade chunking strategy:
1. Dedicated table chunks (KV-table and markdown formats)
2. Page-based text splitting using <!-- PageBreak -->
3. Header prefixes for context
4. Deduplication via content hashing
5. Quality filters for noise removal

Based on production patterns from enterprise document processing.
"""

import json
import re
import hashlib
from typing import List, Dict, Optional
from dataclasses import dataclass

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION - Change these for different documents
# ═══════════════════════════════════════════════════════════════════════════════
PDF_NAME = "invoice2.PDF"                    # Source PDF name (for metadata)
RAW_OCR_PATH = "RAW_OCR.json"               # Input: Raw OCR from azure_ocr.py
ENHANCED_CHUNKS_PATH = "ENHANCED_CHUNKS.json"  # Output: Enhanced chunks


@dataclass
class EnhancedChunk:
    """Represents a processed chunk with metadata."""
    content: str
    content_type: str  # "table", "table_kv", "text"
    page_number: Optional[int]
    section: Optional[str]
    metadata: Dict


class EnhancedChunker:
    """
    Production-grade chunker for Azure DI markdown output.
    
    Features:
    - Dedicated table extraction with KV-table detection
    - Page-based text splitting (one-shot mode)
    - Content deduplication
    - Noise filtering
    - Contextual headers
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.min_chunk_length = self.config.get("min_chunk_length", 50)
        self.max_chunk_length = self.config.get("max_chunk_length", 4000)
        self.content_hash_length = self.config.get("content_hash_length", 700)  # Increased for better dedup accuracy
        
        # Noise patterns to filter out
        self.noise_patterns = [
            r'^Page \d+ of \d+$',
            r'^:selected:$',
            r'^\s*$',
            r'^F\d{3,4}\s+\d+\s+\d+$',  # Footer codes like "F014 11 16"
        ]
        
        # Track seen content for deduplication
        self.seen_hashes = set()
    
    def load_raw_ocr(self, file_path: str) -> Dict:
        """Load the raw Azure DI JSON output."""
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def extract_chunks(self, raw_ocr: Dict, filename: str = "document") -> List[EnhancedChunk]:
        """
        Main entry point: Extract enhanced chunks from Azure DI output.
        
        Args:
            raw_ocr: The raw JSON from Azure DI
            filename: Source filename for metadata
            
        Returns:
            List of EnhancedChunk objects
        """
        analyze_result = raw_ocr.get("analyzeResult", raw_ocr)
        
        content = analyze_result.get("content", "")
        content_format = analyze_result.get("contentFormat", "text")
        tables = analyze_result.get("tables", [])
        pages = analyze_result.get("pages", [])
        
        print(f"\n{'='*60}")
        print(f"📄 ENHANCED CHUNKER")
        print(f"{'='*60}")
        print(f"Content format: {content_format}")
        print(f"Content length: {len(content):,} chars")
        print(f"Tables: {len(tables)}")
        print(f"Pages: {len(pages)}")
        
        chunks = []
        self.seen_hashes.clear()
        
        # === STEP 1: Extract dedicated table chunks ===
        print(f"\n📊 Extracting table chunks...")
        table_chunks = self._extract_table_chunks(tables, filename)
        chunks.extend(table_chunks)
        print(f"   Created {len(table_chunks)} table chunks")
        
        # === STEP 2: Process text content (one-shot page-based) ===
        print(f"\n📝 Processing text content (page-based)...")
        text_chunks = self._process_one_shot(content, filename, pages)
        chunks.extend(text_chunks)
        print(f"   Created {len(text_chunks)} text chunks")
        
        # === STEP 3: Final deduplication and quality check ===
        print(f"\n🔍 Running quality filters...")
        final_chunks = self._filter_chunks(chunks)
        print(f"   Final chunks after filtering: {len(final_chunks)}")
        
        print(f"\n✅ Total chunks created: {len(final_chunks)}")
        
        return final_chunks
    
    def _should_collapse_rows(self, grid: List[List], header_rows: set) -> bool:
        """
        Determine if a table has continuation rows that should be collapsed.
        
        A table needs collapsing if:
        - There are data rows (not headers) with empty column 0
        - These empty column 0 rows appear after a row with non-empty column 0
        
        This handles tables where multi-line cells are split into separate rows
        by Azure DI (e.g., address fields, item descriptions with sub-items).
        """
        if not grid or len(grid) < 2:
            return False
        
        has_parent_row = False
        has_continuation_row = False
        
        for row_idx, row in enumerate(grid):
            # Skip header rows
            if row_idx in header_rows:
                continue
            
            col0_value = (row[0] or "").strip() if row else ""
            
            if col0_value:
                has_parent_row = True
            else:
                # Empty column 0 in a data row = potential continuation
                if has_parent_row:
                    has_continuation_row = True
        
        return has_parent_row and has_continuation_row
    
    def _collapse_rows(self, grid: List[List], header_rows: set, col_count: int) -> List[List]:
        """
        Collapse continuation rows into their parent rows.
        
        Logic:
        - A "parent row" has a non-empty value in column 0
        - A "continuation row" has an empty column 0 and follows a parent
        - For each column independently: collect non-empty values and join with "; "
        
        This handles Azure DI's flattening of multi-line cells into separate rows,
        ensuring data like multi-line addresses stay associated with their record.
        """
        if not grid:
            return grid
        
        collapsed = []
        current_parent = None
        current_continuations = []
        
        for row_idx, row in enumerate(grid):
            # Header rows pass through unchanged
            if row_idx in header_rows:
                # First, flush any pending parent + continuations
                if current_parent is not None:
                    merged = self._merge_row_group(current_parent, current_continuations, col_count)
                    collapsed.append(merged)
                    current_parent = None
                    current_continuations = []
                collapsed.append(row)
                continue
            
            col0_value = (row[0] or "").strip() if row else ""
            
            if col0_value:
                # This is a parent row (has value in column 0)
                # First, flush any previous parent + its continuations
                if current_parent is not None:
                    merged = self._merge_row_group(current_parent, current_continuations, col_count)
                    collapsed.append(merged)
                
                # Start new parent group
                current_parent = row
                current_continuations = []
            else:
                # This is a continuation row (empty column 0)
                if current_parent is not None:
                    current_continuations.append(row)
                else:
                    # Orphan row (no parent yet) - keep as-is
                    collapsed.append(row)
        
        # Don't forget to flush the last parent group
        if current_parent is not None:
            merged = self._merge_row_group(current_parent, current_continuations, col_count)
            collapsed.append(merged)
        
        return collapsed
    
    def _merge_row_group(self, parent: List, continuations: List[List], col_count: int) -> List:
        """
        Merge a parent row with its continuation rows, column by column.
        
        For each column:
        - Collect all non-empty values (parent first, then continuations in order)
        - Join with "; " separator
        
        This ensures multi-line data (addresses, descriptions) stays in the correct column.
        """
        merged = [None] * col_count
        
        for col_idx in range(col_count):
            values = []
            
            # Get parent's value for this column
            parent_val = (parent[col_idx] or "").strip() if col_idx < len(parent) else ""
            if parent_val:
                values.append(parent_val)
            
            # Get each continuation's value for this column
            for cont_row in continuations:
                cont_val = (cont_row[col_idx] or "").strip() if col_idx < len(cont_row) else ""
                if cont_val:
                    values.append(cont_val)
            
            # Join collected values
            if len(values) == 0:
                merged[col_idx] = ""
            elif len(values) == 1:
                merged[col_idx] = values[0]
            else:
                merged[col_idx] = "; ".join(values)
        
        return merged
    
    def _is_kv_table(self, grid: List[List], col_count: int, row_count: int) -> bool:
        """
        Detect if table is a Key-Value table (2 columns with label-value pattern).
        
        KV tables look like:
        | Invoice No | F250335786 |
        | Date       | 16-Jun-25  |
        """
        if col_count != 2 or row_count < 2:
            return False
        
        label_like_count = 0
        non_empty_rows = 0
        
        for row in grid:
            if row[0]:
                non_empty_rows += 1
                left_cell = row[0].strip()
                if len(left_cell) < 40 and len(left_cell.split()) <= 5:
                    alpha_count = sum(1 for c in left_cell if c.isalpha())
                    if alpha_count > len(left_cell) * 0.3:
                        label_like_count += 1
        
        if non_empty_rows < 2:
            return False
        return label_like_count >= non_empty_rows * 0.6
    
    def _extract_table_chunks(self, tables: List[Dict], filename: str) -> List[EnhancedChunk]:
        """
        Extract dedicated chunks for each table with smart formatting.
        
        - Detects KV tables (2-col label-value) and formats as "Label: Value"
        - Only treats headers when DI explicitly marks them (kind == "columnHeader")
        - No fallback heuristics - prevents values being promoted to headers
        """
        chunks = []
        
        for table_idx, table in enumerate(tables):
            row_count = table.get("rowCount", 0)
            col_count = table.get("columnCount", 0)
            cells = table.get("cells", [])
            
            if not cells:
                continue
            
            # Get page number
            bounding_regions = table.get("boundingRegions", [])
            page_num = bounding_regions[0].get("pageNumber", 1) if bounding_regions else 1
            
            # Build grid and detect explicit headers from Azure DI
            grid = [[None for _ in range(col_count)] for _ in range(row_count)]
            headers = [None] * col_count
            header_rows = set()
            has_explicit_headers = False
            
            for cell in cells:
                row_idx = cell.get("rowIndex", 0)
                col_idx = cell.get("columnIndex", 0)
                content = cell.get("content", "").strip()
                kind = cell.get("kind", "")
                
                if row_idx < row_count and col_idx < col_count:
                    grid[row_idx][col_idx] = content
                    
                    if kind == "columnHeader":
                        headers[col_idx] = content
                        header_rows.add(row_idx)
                        has_explicit_headers = True
            
            # === ROW COLLAPSING: Merge continuation rows into parent rows ===
            # This handles Azure DI's flattening of multi-line cells (addresses, descriptions)
            # into separate rows with empty column 0
            if self._should_collapse_rows(grid, header_rows):
                grid = self._collapse_rows(grid, header_rows, col_count)
                # Update row count after collapsing for metadata
                row_count = len(grid)
            
            # Detect if this is a KV table (check AFTER collapsing)
            is_kv_table = self._is_kv_table(grid, col_count, row_count)
            
            # === KV TABLE MODE ===
            if is_kv_table:
                kv_pairs = []
                for row in grid:
                    label = (row[0] or "").strip().rstrip(':')
                    value = (row[1] or "").strip()
                    if label and value:
                        kv_pairs.append(f"{label}: {value}")
                
                if kv_pairs:
                    kv_content = "\n".join(kv_pairs)
                    header = self._build_header(
                        filename=filename,
                        section=f"Table {table_idx + 1} (Key-Value)",
                        page=page_num
                    )
                    chunks.append(EnhancedChunk(
                        content=header + kv_content,
                        content_type="table_kv",
                        page_number=page_num,
                        section=f"Table {table_idx + 1}",
                        metadata={
                            "table_index": table_idx,
                            "table_type": "key_value",
                            "row_count": row_count,
                            "column_count": col_count
                        }
                    ))
                continue
            
            # === REGULAR TABLE ===
            table_markdown = self._table_to_markdown(grid, headers, header_rows, has_explicit_headers, col_count)
            if table_markdown and len(table_markdown) > 20:
                header = self._build_header(
                    filename=filename,
                    section=f"Table {table_idx + 1}",
                    page=page_num
                )
                chunks.append(EnhancedChunk(
                    content=header + table_markdown,
                    content_type="table",
                    page_number=page_num,
                    section=f"Table {table_idx + 1}",
                    metadata={
                        "table_index": table_idx,
                        "row_count": row_count,
                        "column_count": col_count,
                        "headers": [h for h in headers if h] if has_explicit_headers else []
                    }
                ))
        
        return chunks
    
    def _table_to_markdown(self, grid: List[List], headers: List, header_rows: set, 
                           has_explicit_headers: bool, col_count: int) -> str:
        """
        Convert table grid to markdown format.
        
        - If has_explicit_headers: use DI headers, skip header_rows in body
        - If no explicit headers: use synthetic "Column 1", "Column 2", etc.
        """
        if not grid or not grid[0]:
            return ""
        
        lines = []
        
        if has_explicit_headers:
            header_row = " | ".join(h or "" for h in headers)
            lines.append(f"| {header_row} |")
            lines.append("|" + "|".join(["---"] * len(headers)) + "|")
            
            for row_idx, row in enumerate(grid):
                if row_idx in header_rows:
                    continue
                row_content = " | ".join(cell or "" for cell in row)
                lines.append(f"| {row_content} |")
        else:
            synthetic_headers = [f"Column {i+1}" for i in range(col_count)]
            header_row = " | ".join(synthetic_headers)
            lines.append(f"| {header_row} |")
            lines.append("|" + "|".join(["---"] * col_count) + "|")
            
            for row in grid:
                row_content = " | ".join(cell or "" for cell in row)
                lines.append(f"| {row_content} |")
        
        return "\n".join(lines)
    
    def _process_one_shot(self, content: str, filename: str, pages: List[Dict]) -> List[EnhancedChunk]:
        """
        One-shot mode: Keep markdown as minimal chunks, split only by page breaks.
        """
        chunks = []
        
        clean_content = self._remove_tables_from_content(content)
        
        if not clean_content or len(clean_content) < self.min_chunk_length:
            return chunks
        
        # For small documents, create single chunk
        if len(clean_content) <= self.max_chunk_length:
            header = self._build_header(
                filename=filename,
                section="Document",
                page=1
            )
            chunks.append(EnhancedChunk(
                content=header + clean_content,
                content_type="text",
                page_number=1,
                section="Document",
                metadata={"total_pages": len(pages)}
            ))
            return chunks
        
        # Split by page breaks
        parts = clean_content.split("<!-- PageBreak -->") if "<!-- PageBreak -->" in clean_content else [clean_content]
        
        for idx, part in enumerate(parts):
            trimmed = part.strip()
            if len(trimmed) >= self.min_chunk_length:
                header = self._build_header(
                    filename=filename,
                    section=f"Page {idx + 1}",
                    page=idx + 1
                )
                chunks.append(EnhancedChunk(
                    content=header + trimmed,
                    content_type="text",
                    page_number=idx + 1,
                    section=f"Page {idx + 1}",
                    metadata={}
                ))
        
        return chunks
    
    def _remove_tables_from_content(self, content: str) -> str:
        """Remove both HTML and Markdown table blocks from content."""
        # Remove <table>...</table> HTML blocks
        clean = re.sub(r'<table>.*?</table>', '', content, flags=re.DOTALL)
        
        # Remove Markdown tables (lines starting with |)
        lines = clean.split('\n')
        filtered_lines = []
        in_table = False
        
        for line in lines:
            stripped = line.strip()
            is_table_line = (
                stripped.startswith('|') and '|' in stripped[1:] or
                re.match(r'^\|[\s\-:|]+\|$', stripped)
            )
            
            if is_table_line:
                in_table = True
                continue
            else:
                if in_table and stripped == '':
                    in_table = False
                    continue
                in_table = False
                filtered_lines.append(line)
        
        return '\n'.join(filtered_lines)
    
    def _build_header(self, filename: str, section: str, page: int) -> str:
        """Build a contextual header prefix for chunks."""
        return f"[Source: {filename} | {section} | Page {page}]\n\n"
    
    def _is_noise(self, text: str) -> bool:
        """Check if text matches noise patterns."""
        for pattern in self.noise_patterns:
            if re.match(pattern, text.strip(), re.IGNORECASE):
                return True
        return False
    
    def _get_content_hash(self, text: str) -> str:
        """
        Generate hash for deduplication.
        
        - Hashes BODY only (strips [Source: ...] header) so identical content
          with different headers is still detected as duplicate.
        - Caps hash input to content_hash_length chars to avoid expensive hashing
          on very long chunks while still capturing enough to distinguish them.
        """
        # Strip the [Source: ...]\n\n header — dedup is based on body content only
        body = text
        if text.startswith("[Source:"):
            header_end = text.find("]\n\n")
            if header_end != -1:
                body = text[header_end + 3:]
        
        # Use full body if short, otherwise cap at content_hash_length
        # This ensures small chunks hash fully, large chunks hash first N chars
        hash_input = body if len(body) <= self.content_hash_length else body[:self.content_hash_length]
        
        # Normalize: lowercase, collapse whitespace
        normalized = hash_input.lower().strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _filter_chunks(self, chunks: List[EnhancedChunk]) -> List[EnhancedChunk]:
        """Apply final quality filters and deduplication."""
        filtered = []
        
        for chunk in chunks:
            if len(chunk.content) < self.min_chunk_length:
                continue
            
            if self._is_noise(chunk.content):
                continue
            
            content_hash = self._get_content_hash(chunk.content)
            if content_hash in self.seen_hashes:
                continue
            self.seen_hashes.add(content_hash)
            
            filtered.append(chunk)
        
        return filtered
    
    def to_vectordb_format(self, chunks: List[EnhancedChunk]) -> List[Dict]:
        """Convert chunks to format ready for vector DB storage."""
        return [
            {
                "text": chunk.content,
                "metadata": {
                    "content_type": chunk.content_type,
                    "page_number": chunk.page_number,
                    "section": chunk.section,
                    **chunk.metadata
                }
            }
            for chunk in chunks
        ]
    
    def save_chunks(self, chunks: List[EnhancedChunk], output_path: str):
        """Save chunks to JSON file."""
        data = {
            "total_chunks": len(chunks),
            "chunks": self.to_vectordb_format(chunks)
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Saved {len(chunks)} chunks to {output_path}")


def main():
    """Main function to run the enhanced chunker."""
    print(f"\n{'='*60}")
    print("📄 ENHANCED CHUNKER")
    print(f"{'='*60}")
    print(f"   PDF Name:     {PDF_NAME}")
    print(f"   Input:        {RAW_OCR_PATH}")
    print(f"   Output:       {ENHANCED_CHUNKS_PATH}")
    print(f"{'='*60}")
    
    chunker = EnhancedChunker(config={
        "min_chunk_length": 50,
        "max_chunk_length": 4000,
    })
    
    raw_ocr = chunker.load_raw_ocr(RAW_OCR_PATH)
    chunks = chunker.extract_chunks(raw_ocr, filename=PDF_NAME)
    chunker.save_chunks(chunks, ENHANCED_CHUNKS_PATH)
    
    print(f"\n{'='*60}")
    print(f"✅ Chunking complete! Output saved to {ENHANCED_CHUNKS_PATH}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
