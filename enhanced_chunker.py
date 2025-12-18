"""
Enhanced Chunker for Azure Document Intelligence

Implements production-grade chunking strategy:
1. Dedicated table chunks (KV-table and markdown formats)
2. Page-based text splitting using <!-- PageBreak -->
3. Header prefixes for context
4. Deduplication via content hashing
5. Quality filters for noise removal
"""

import re
import hashlib
from typing import List, Dict, Optional
from dataclasses import dataclass


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
        self.content_hash_length = self.config.get("content_hash_length", 700)
        
        # Noise patterns to filter out
        self.noise_patterns = [
            r'^Page \d+ of \d+$',
            r'^:selected:$',
            r'^\s*$',
            r'^F\d{3,4}\s+\d+\s+\d+$',
        ]
        
        self.seen_hashes = set()
    
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
        
        # Extract table chunks
        print(f"\n📊 Extracting table chunks...")
        table_chunks = self._extract_table_chunks(tables, filename)
        chunks.extend(table_chunks)
        print(f"   Created {len(table_chunks)} table chunks")
        
        # Process text content
        print(f"\n📝 Processing text content (page-based)...")
        text_chunks = self._process_one_shot(content, filename, pages)
        chunks.extend(text_chunks)
        print(f"   Created {len(text_chunks)} text chunks")
        
        # Apply quality filters
        print(f"\n🔍 Running quality filters...")
        final_chunks = self._filter_chunks(chunks)
        print(f"   Final chunks after filtering: {len(final_chunks)}")
        
        print(f"\n✅ Total chunks created: {len(final_chunks)}")
        
        return final_chunks
    
    def _should_collapse_rows(self, grid: List[List], header_rows: set) -> bool:
        """Check if table has continuation rows that should be collapsed."""
        if not grid or len(grid) < 2:
            return False
        
        has_parent_row = False
        has_continuation_row = False
        
        for row_idx, row in enumerate(grid):
            if row_idx in header_rows:
                continue
            
            col0_value = (row[0] or "").strip() if row else ""
            
            if col0_value:
                has_parent_row = True
            else:
                if has_parent_row:
                    has_continuation_row = True
        
        return has_parent_row and has_continuation_row
    
    def _collapse_rows(self, grid: List[List], header_rows: set, col_count: int) -> List[List]:
        """Collapse continuation rows into their parent rows."""
        if not grid:
            return grid
        
        collapsed = []
        current_parent = None
        current_continuations = []
        
        for row_idx, row in enumerate(grid):
            if row_idx in header_rows:
                if current_parent is not None:
                    merged = self._merge_row_group(current_parent, current_continuations, col_count)
                    collapsed.append(merged)
                    current_parent = None
                    current_continuations = []
                collapsed.append(row)
                continue
            
            col0_value = (row[0] or "").strip() if row else ""
            
            if col0_value:
                if current_parent is not None:
                    merged = self._merge_row_group(current_parent, current_continuations, col_count)
                    collapsed.append(merged)
                
                current_parent = row
                current_continuations = []
            else:
                if current_parent is not None:
                    current_continuations.append(row)
                else:
                    collapsed.append(row)
        
        if current_parent is not None:
            merged = self._merge_row_group(current_parent, current_continuations, col_count)
            collapsed.append(merged)
        
        return collapsed
    
    def _merge_row_group(self, parent: List, continuations: List[List], col_count: int) -> List:
        """Merge a parent row with its continuation rows, column by column."""
        merged = [None] * col_count
        
        for col_idx in range(col_count):
            values = []
            
            parent_val = (parent[col_idx] or "").strip() if col_idx < len(parent) else ""
            if parent_val:
                values.append(parent_val)
            
            for cont_row in continuations:
                cont_val = (cont_row[col_idx] or "").strip() if col_idx < len(cont_row) else ""
                if cont_val:
                    values.append(cont_val)
            
            if len(values) == 0:
                merged[col_idx] = ""
            elif len(values) == 1:
                merged[col_idx] = values[0]
            else:
                merged[col_idx] = "; ".join(values)
        
        return merged
    
    def _is_kv_table(self, grid: List[List], col_count: int, row_count: int) -> bool:
        """Detect if table is a Key-Value table (2 columns with label-value pattern)."""
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
        """Extract dedicated chunks for each table with smart formatting."""
        chunks = []
        
        for table_idx, table in enumerate(tables):
            row_count = table.get("rowCount", 0)
            col_count = table.get("columnCount", 0)
            cells = table.get("cells", [])
            
            if not cells:
                continue
            
            bounding_regions = table.get("boundingRegions", [])
            page_num = bounding_regions[0].get("pageNumber", 1) if bounding_regions else 1
            
            # Build grid and detect headers
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
            
            # Collapse continuation rows
            if self._should_collapse_rows(grid, header_rows):
                grid = self._collapse_rows(grid, header_rows, col_count)
                row_count = len(grid)
            
            is_kv_table = self._is_kv_table(grid, col_count, row_count)
            
            # KV Table format
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
            
            # Regular table format
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
        """Convert table grid to markdown format."""
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
        """Process text content with page-based splitting."""
        chunks = []
        
        clean_content = self._remove_tables_from_content(content)
        
        if not clean_content or len(clean_content) < self.min_chunk_length:
            return chunks
        
        # Small documents: single chunk
        if len(clean_content) <= self.max_chunk_length:
            header = self._build_header(filename=filename, section="Document", page=1)
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
                header = self._build_header(filename=filename, section=f"Page {idx + 1}", page=idx + 1)
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
        clean = re.sub(r'<table>.*?</table>', '', content, flags=re.DOTALL)
        
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
        """Generate hash for deduplication (based on body content only)."""
        body = text
        if text.startswith("[Source:"):
            header_end = text.find("]\n\n")
            if header_end != -1:
                body = text[header_end + 3:]
        
        hash_input = body if len(body) <= self.content_hash_length else body[:self.content_hash_length]
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
        """Convert chunks to format ready for vector DB storage or display."""
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
