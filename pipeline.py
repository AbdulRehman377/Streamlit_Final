"""
Document Processing Pipeline

Orchestrates the full document processing flow:
1. Azure OCR extraction (azure_ocr.py)
2. Enhanced chunking (enhanced_chunker.py)
3. Store in ChromaDB (store_enhanced_chunks.py)

Usage:
    python pipeline.py                    # Uses default PDF from config
    python pipeline.py invoice.pdf        # Process specific PDF
    python pipeline.py invoice.pdf myDoc  # Process with custom doc ID
"""

import sys
import os
import json
import time

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
DEFAULT_PDF = "invoice2.PDF"
RAW_OCR_PATH = "RAW_OCR.json"
ENHANCED_CHUNKS_PATH = "ENHANCED_CHUNKS.json"
DEFAULT_DOC_ID = "document"


def run_pipeline(pdf_path: str, doc_id: str = None):
    """
    Run the full document processing pipeline.
    
    Args:
        pdf_path: Path to the PDF file
        doc_id: Document identifier for ChromaDB (defaults to filename without extension)
    """
    start_time = time.time()
    
    # Validate PDF exists
    if not os.path.exists(pdf_path):
        print(f"❌ Error: PDF file not found: {pdf_path}")
        return False
    
    # Use filename as doc_id if not provided
    if not doc_id:
        doc_id = os.path.splitext(os.path.basename(pdf_path))[0]
    
    print("\n" + "═" * 60)
    print("🚀 DOCUMENT PROCESSING PIPELINE")
    print("═" * 60)
    print(f"   PDF:      {pdf_path}")
    print(f"   Doc ID:   {doc_id}")
    print("═" * 60)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Azure OCR Extraction
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 60)
    print("📄 STEP 1: Azure OCR Extraction")
    print("─" * 60)
    
    try:
        from azure_ocr import analyze_layout_rest
        
        result = analyze_layout_rest(pdf_path)
        
        # Save raw OCR output
        with open(RAW_OCR_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"✅ OCR complete → {RAW_OCR_PATH}")
        
    except Exception as e:
        print(f"❌ OCR failed: {e}")
        return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Enhanced Chunking
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 60)
    print("📦 STEP 2: Enhanced Chunking")
    print("─" * 60)
    
    try:
        from enhanced_chunker import EnhancedChunker
        
        chunker = EnhancedChunker(config={
            "min_chunk_length": 50,
            "max_chunk_length": 4000,
        })
        
        raw_ocr = chunker.load_raw_ocr(RAW_OCR_PATH)
        chunks = chunker.extract_chunks(raw_ocr, filename=os.path.basename(pdf_path))
        chunker.save_chunks(chunks, ENHANCED_CHUNKS_PATH)
        
        print(f"✅ Chunking complete → {ENHANCED_CHUNKS_PATH}")
        
    except Exception as e:
        print(f"❌ Chunking failed: {e}")
        return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Store in ChromaDB
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 60)
    print("🗄️  STEP 3: Store in ChromaDB")
    print("─" * 60)
    
    try:
        from store_enhanced_chunks import load_enhanced_chunks, store_chunks
        
        chunks = load_enhanced_chunks(ENHANCED_CHUNKS_PATH)
        print(f"   Loaded {len(chunks)} chunks")
        
        vectorstore = store_chunks(chunks, doc_id=doc_id)
        
        print(f"✅ Storage complete → ChromaDB")
        
    except Exception as e:
        print(f"❌ Storage failed: {e}")
        return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DONE
    # ═══════════════════════════════════════════════════════════════════════════
    elapsed = time.time() - start_time
    
    print("\n" + "═" * 60)
    print("✅ PIPELINE COMPLETE")
    print("═" * 60)
    print(f"   Total time: {elapsed:.1f}s")
    print(f"   Output files:")
    print(f"     • {RAW_OCR_PATH}")
    print(f"     • {ENHANCED_CHUNKS_PATH}")
    print(f"     • ./chroma_db_enhanced/")
    print("═" * 60)
    print("\n💡 Run 'python rag_chat.py' to query your document!\n")
    
    return True


def main():
    """Main entry point with CLI argument handling."""
    # Parse arguments
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PDF
    doc_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Run pipeline
    success = run_pipeline(pdf_path, doc_id)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
