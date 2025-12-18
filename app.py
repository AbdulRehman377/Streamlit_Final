"""
Document Processing App - Streamlit Frontend

Simple interface to:
1. Upload PDF documents
2. Process with Azure OCR
3. View enhanced chunks
"""

import streamlit as st
import tempfile
import os
import json

from azure_ocr import analyze_layout_rest
from enhanced_chunker import EnhancedChunker


# Page configuration
st.set_page_config(
    page_title="Document Chunker",
    page_icon="📄",
    layout="wide"
)

# Custom CSS for cleaner look
st.markdown("""
<style>
    .chunk-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
        border-left: 4px solid #4CAF50;
    }
    .chunk-header {
        font-weight: 600;
        color: #1a73e8;
        margin-bottom: 8px;
    }
    .chunk-meta {
        font-size: 0.85em;
        color: #666;
        margin-bottom: 8px;
    }
    .chunk-content {
        font-size: 0.9em;
        line-height: 1.5;
        white-space: pre-wrap;
    }
    .kv-chunk {
        border-left-color: #2196F3;
    }
    .table-chunk {
        border-left-color: #FF9800;
    }
    .stats-box {
        background-color: #e3f2fd;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


def process_pdf(pdf_file) -> dict:
    """
    Process uploaded PDF through OCR and chunking pipeline.
    
    Returns dict with chunks and metadata.
    """
    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(pdf_file.read())
        tmp_path = tmp.name
    
    try:
        # Step 1: Azure OCR
        with st.spinner("🔍 Extracting text with Azure OCR..."):
            raw_ocr = analyze_layout_rest(tmp_path)
        
        # Step 2: Enhanced Chunking
        with st.spinner("📦 Creating enhanced chunks..."):
            chunker = EnhancedChunker(config={
                "min_chunk_length": 50,
                "max_chunk_length": 4000,
            })
            chunks = chunker.extract_chunks(raw_ocr, filename=pdf_file.name)
            chunks_data = chunker.to_vectordb_format(chunks)
        
        return {
            "success": True,
            "filename": pdf_file.name,
            "total_chunks": len(chunks_data),
            "chunks": chunks_data
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
    
    finally:
        # Cleanup temp file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def display_chunk(chunk: dict, index: int):
    """Display a single chunk in a nice card format."""
    content_type = chunk["metadata"].get("content_type", "text")
    page = chunk["metadata"].get("page_number", "?")
    section = chunk["metadata"].get("section", "")
    
    # Determine card style based on content type
    if content_type == "table_kv":
        card_class = "chunk-card kv-chunk"
        type_emoji = "🔑"
        type_label = "Key-Value Table"
    elif content_type == "table":
        card_class = "chunk-card table-chunk"
        type_emoji = "📊"
        type_label = "Table"
    else:
        card_class = "chunk-card"
        type_emoji = "📝"
        type_label = "Text"
    
    # Get content without source header
    content = chunk["text"]
    if content.startswith("[Source:"):
        content = content.split("]\n\n", 1)[-1] if "]\n\n" in content else content
    
    st.markdown(f"""
    <div class="{card_class}">
        <div class="chunk-header">{type_emoji} Chunk {index + 1}: {type_label}</div>
        <div class="chunk-meta">📄 Page {page} | 📁 {section}</div>
        <div class="chunk-content">{content}</div>
    </div>
    """, unsafe_allow_html=True)


def main():
    # Header
    st.title("📄 Document Chunker")
    st.markdown("Upload a PDF to extract and view enhanced chunks.")
    st.markdown("---")
    
    # Sidebar for upload
    with st.sidebar:
        st.header("📤 Upload PDF")
        uploaded_file = st.file_uploader(
            "Choose a PDF file",
            type=["pdf"],
            help="Upload a PDF document to process"
        )
        
        if uploaded_file:
            st.success(f"📎 {uploaded_file.name}")
            process_btn = st.button("🚀 Process Document", type="primary", use_container_width=True)
        else:
            process_btn = False
            st.info("👆 Upload a PDF to get started")
    
    # Main content area
    if "chunks_data" not in st.session_state:
        st.session_state.chunks_data = None
    
    # Process when button clicked
    if process_btn and uploaded_file:
        result = process_pdf(uploaded_file)
        
        if result["success"]:
            st.session_state.chunks_data = result
            st.success(f"✅ Successfully processed **{result['filename']}**")
        else:
            st.error(f"❌ Error: {result['error']}")
            st.session_state.chunks_data = None
    
    # Display chunks if available
    if st.session_state.chunks_data:
        data = st.session_state.chunks_data
        
        # Stats row
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="stats-box">
                <h3>📄 {data['filename']}</h3>
                <p>Document</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="stats-box">
                <h3>{data['total_chunks']}</h3>
                <p>Total Chunks</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Count chunk types
        type_counts = {}
        for chunk in data["chunks"]:
            ct = chunk["metadata"].get("content_type", "text")
            type_counts[ct] = type_counts.get(ct, 0) + 1
        
        with col3:
            types_str = " | ".join([f"{k}: {v}" for k, v in type_counts.items()])
            st.markdown(f"""
            <div class="stats-box">
                <h3>📊</h3>
                <p>{types_str}</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Filter options
        st.subheader("🔍 View Chunks")
        
        col1, col2 = st.columns([1, 3])
        with col1:
            filter_type = st.selectbox(
                "Filter by type",
                ["All"] + list(type_counts.keys())
            )
        
        # Filter chunks
        chunks_to_show = data["chunks"]
        if filter_type != "All":
            chunks_to_show = [c for c in chunks_to_show if c["metadata"].get("content_type") == filter_type]
        
        st.caption(f"Showing {len(chunks_to_show)} chunks")
        
        # Display chunks
        for i, chunk in enumerate(chunks_to_show):
            with st.expander(f"Chunk {i + 1}: {chunk['metadata'].get('content_type', 'text').upper()} - Page {chunk['metadata'].get('page_number', '?')}", expanded=False):
                display_chunk(chunk, i)
        
        # Download option
        st.markdown("---")
        st.download_button(
            label="📥 Download Chunks as JSON",
            data=json.dumps(data, indent=2, ensure_ascii=False),
            file_name=f"{data['filename'].replace('.pdf', '').replace('.PDF', '')}_chunks.json",
            mime="application/json"
        )
    
    else:
        # Empty state
        st.markdown("""
        <div style="text-align: center; padding: 60px; color: #666;">
            <h2>👈 Upload a PDF to get started</h2>
            <p>Your document will be processed using Azure Document Intelligence OCR 
            and split into intelligent chunks.</p>
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

