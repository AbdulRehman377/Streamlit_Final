# 📄 Document Chunker

A Streamlit-based application that processes PDF documents using Azure Document Intelligence OCR and creates intelligent, structured chunks for downstream processing.

---

## 🎯 Overview

This application provides a simple web interface to:
1. **Upload PDF documents**
2. **Extract text and tables** using Azure Document Intelligence OCR
3. **Create enhanced chunks** with smart table detection and page-based splitting
4. **View and download** the processed chunks

---

## ✨ Features

- **Azure Document Intelligence Integration** - Uses Azure's prebuilt-layout model for accurate OCR
- **Smart Table Detection** - Automatically detects and formats:
  - Key-Value tables (2-column label-value pairs)
  - Regular tables (converted to markdown format)
- **Page-Based Splitting** - Respects document page boundaries
- **Deduplication** - Removes duplicate content via content hashing
- **Noise Filtering** - Filters out page numbers, empty content, and footer codes
- **Download Support** - Export chunks as JSON for further processing

---

## 📁 Project Structure

```
├── app.py                 # Streamlit frontend application
├── azure_ocr.py           # Azure Document Intelligence OCR module
├── enhanced_chunker.py    # Chunking logic and table processing
├── requirements.txt       # Python dependencies
├── .env.example           # Environment variables template
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

### File Descriptions

| File | Purpose |
|------|---------|
| `app.py` | Main Streamlit application. Handles PDF upload, orchestrates processing, and displays results. |
| `azure_ocr.py` | Sends PDFs to Azure Document Intelligence and handles polling for results with exponential backoff. |
| `enhanced_chunker.py` | Processes raw OCR output into structured chunks. Handles table extraction, page splitting, and quality filtering. |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Azure Document Intelligence resource (with endpoint and key)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` and add your Azure credentials:
   ```
   AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT=https://your-resource.cognitiveservices.azure.com
   AZURE_DOCUMENT_INTELLIGENCE_KEY=your-key-here
   ```

### Running the Application

```bash
streamlit run app.py
```

The application will open in your browser at `http://localhost:8501`

---

## 📊 Chunk Schema

When a PDF is processed, it generates chunks with the following schema:

### Chunk Object Structure

```json
{
  "text": "string",
  "metadata": {
    "content_type": "string",
    "page_number": "number | null",
    "section": "string | null",
    ...additional_metadata
  }
}
```

### Schema Details

| Field | Type | Description |
|-------|------|-------------|
| `text` | `string` | The chunk content with a source header prefix |
| `metadata` | `object` | Metadata about the chunk |
| `metadata.content_type` | `string` | Type of content: `"text"`, `"table"`, or `"table_kv"` |
| `metadata.page_number` | `number \| null` | Page number where the chunk originates |
| `metadata.section` | `string \| null` | Section identifier (e.g., "Page 1", "Table 2") |

### Content Types

| Type | Description | Additional Metadata |
|------|-------------|---------------------|
| `text` | Regular text content from document pages | `total_pages` (for single-page documents) |
| `table` | Regular table converted to markdown format | `table_index`, `row_count`, `column_count`, `headers` |
| `table_kv` | Key-Value table (2-column label-value pairs) | `table_index`, `table_type`, `row_count`, `column_count` |

### Example Chunks

#### Text Chunk
```json
{
  "text": "[Source: invoice.pdf | Page 1 | Page 1]\n\nINVOICE\n\nBill To:\nJohn Doe\n123 Main Street...",
  "metadata": {
    "content_type": "text",
    "page_number": 1,
    "section": "Page 1"
  }
}
```

#### Key-Value Table Chunk
```json
{
  "text": "[Source: invoice.pdf | Table 1 (Key-Value) | Page 1]\n\nInvoice No: F250335786\nDate: 16-Jun-2025\nDue Date: 30-Jun-2025",
  "metadata": {
    "content_type": "table_kv",
    "page_number": 1,
    "section": "Table 1",
    "table_index": 0,
    "table_type": "key_value",
    "row_count": 3,
    "column_count": 2
  }
}
```

#### Regular Table Chunk
```json
{
  "text": "[Source: invoice.pdf | Table 2 | Page 1]\n\n| Item | Qty | Price |\n|---|---|---|\n| Widget A | 10 | $5.00 |\n| Widget B | 5 | $10.00 |",
  "metadata": {
    "content_type": "table",
    "page_number": 1,
    "section": "Table 2",
    "table_index": 1,
    "row_count": 3,
    "column_count": 3,
    "headers": ["Item", "Qty", "Price"]
  }
}
```

### Full Output Schema

The complete JSON output when downloading chunks:

```json
{
  "success": true,
  "filename": "document.pdf",
  "total_chunks": 5,
  "chunks": [
    {
      "text": "...",
      "metadata": { ... }
    }
  ]
}
```

---

## 🧪 Testing

### Manual Testing Steps

1. **Start the application**
   ```bash
   streamlit run app.py
   ```

2. **Upload a test PDF**
   - Click "Browse files" in the sidebar
   - Select a PDF document

3. **Process the document**
   - Click "🚀 Process Document" button
   - Wait for OCR and chunking to complete

4. **Verify the output**
   - Check the statistics (total chunks, chunk types)
   - Expand individual chunks to view content
   - Use the filter dropdown to view specific chunk types
   - Download the JSON to verify the schema

### Test Cases

| Test Case | Expected Result |
|-----------|-----------------|
| Upload single-page PDF | Creates text chunks and any table chunks |
| Upload multi-page PDF | Creates separate chunks per page |
| Upload PDF with tables | Detects tables and creates table/table_kv chunks |
| Upload PDF with forms | Key-value pairs extracted as table_kv chunks |
| Download JSON | Valid JSON matching the schema above |

### Verifying Chunk Quality

When reviewing chunks, check:
- ✅ Source header is present: `[Source: filename | Section | Page X]`
- ✅ Content type is correctly identified
- ✅ Tables are properly formatted (markdown or key-value)
- ✅ No duplicate chunks
- ✅ No noise content (page numbers, empty text)

---

## ⚙️ Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT` | Yes | Azure DI resource endpoint URL |
| `AZURE_DOCUMENT_INTELLIGENCE_KEY` | Yes | Azure DI subscription key |

### Chunker Configuration

The chunker can be configured with the following parameters (in `app.py`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_chunk_length` | 50 | Minimum characters for a valid chunk |
| `max_chunk_length` | 4000 | Maximum characters before splitting |

---

## 🔧 Technical Details

### Azure OCR Settings

- **API Version**: 2024-11-30
- **Model**: prebuilt-layout
- **Output Format**: Markdown with tables

### Polling Behavior

The OCR module implements robust polling:
- Maximum 60 attempts
- Exponential backoff (1.2x multiplier)
- Handles 429 (rate limit) and 503 (service unavailable) gracefully
- Handles 404 (not ready) without counting as failure

### Table Processing

1. **Detection**: Tables are identified from Azure DI's structured output
2. **Classification**: 2-column tables with label-like content → Key-Value format
3. **Row Collapsing**: Multi-line cells split by Azure DI are merged back
4. **Formatting**: Regular tables → Markdown, KV tables → "Label: Value" format

---

## 📝 Notes

- PDFs are processed in-memory and temporary files are cleaned up after processing
- Large documents may take longer due to Azure OCR processing time
- The application maintains state between interactions using Streamlit session state

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 📄 License

[Add your license here]

