"""
Azure Document Intelligence OCR Module

Sends PDF to Azure DI Layout Model and returns structured JSON.
Features:
- Updated API version (2024-11-30) matching production
- Robust polling with exponential backoff
- Handles 429/503 throttling and 404 "not ready" responses
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

# Azure DI Configuration
ENDPOINT = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT", "").rstrip("/")
KEY = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")
API_VERSION = "2024-11-30"
MODEL_ID = "prebuilt-layout"

# Polling settings
MAX_POLL_ATTEMPTS = 60
INITIAL_WAIT_MS = 2000
MAX_WAIT_MS = 10000
BACKOFF_MULTIPLIER = 1.2


def analyze_layout_rest(file_path: str, max_attempts: int = MAX_POLL_ATTEMPTS) -> dict:
    """
    Send file to Azure Layout model & return RAW JSON response.
    
    Args:
        file_path: Path to the PDF file
        max_attempts: Maximum polling attempts before timeout
        
    Returns:
        dict: Raw Azure DI response with analyzeResult
        
    Raises:
        RuntimeError: If credentials missing or analysis fails
    """
    if not ENDPOINT or not KEY:
        raise RuntimeError("Missing Azure credentials. Check .env file.")

    # Submit analysis request
    analyze_url = (
        f"{ENDPOINT}/documentintelligence/documentModels/"
        f"{MODEL_ID}:analyze?api-version={API_VERSION}&outputContentFormat=markdown"
    )

    headers = {
        "Ocp-Apim-Subscription-Key": KEY,
        "Content-Type": "application/pdf",
    }

    print(f"\n{'='*60}")
    print("📡 AZURE DOCUMENT INTELLIGENCE")
    print(f"{'='*60}")
    print(f"   API Version: {API_VERSION}")
    print(f"   Model: {MODEL_ID}")
    print(f"   File: {file_path}")
    print(f"{'='*60}")
    
    print("\n📤 Submitting analysis request...")
    
    with open(file_path, "rb") as f:
        resp = requests.post(analyze_url, headers=headers, data=f)

    if resp.status_code != 202:
        print(f"❌ Submission failed with status: {resp.status_code}")
        print(resp.text)
        resp.raise_for_status()

    operation_url = resp.headers.get("operation-location")
    if not operation_url:
        raise RuntimeError("No operation-location header in response")
    
    operation_id = operation_url.split('/')[-1].split('?')[0]
    print(f"✅ Analysis submitted. Operation ID: {operation_id}")

    # Poll for results with robust handling
    print("\n⏳ Polling for results...")
    
    poll_headers = {"Ocp-Apim-Subscription-Key": KEY}
    attempt = 0
    
    while attempt < max_attempts:
        try:
            poll = requests.get(operation_url, headers=poll_headers)
            
            # Handle throttling - don't count as attempt
            if poll.status_code in [429, 503]:
                retry_after = int(poll.headers.get('retry-after', 3)) * 1000
                print(f"   ⚠️  Rate limited ({poll.status_code}), waiting {retry_after}ms...")
                time.sleep(retry_after / 1000)
                continue
            
            # Handle not ready - don't count as attempt
            if poll.status_code == 404:
                print(f"   ⏳ Operation not ready yet, waiting...")
                time.sleep(INITIAL_WAIT_MS / 1000)
                continue
            
            poll.raise_for_status()
            result = poll.json()
            status = result.get("status")
            
            if status == "succeeded":
                print(f"\n✅ Layout extraction complete! (attempt {attempt + 1})")
                
                analysis = result.get("analyzeResult", {})
                print(f"   Content length: {len(analysis.get('content', '')):,} chars")
                print(f"   Pages: {len(analysis.get('pages', []))}")
                print(f"   Tables: {len(analysis.get('tables', []))}")
                print(f"   Paragraphs: {len(analysis.get('paragraphs', []))}")
                
                return result
            
            elif status == "failed":
                error_msg = result.get("error", {}).get("message", "Unknown error")
                raise RuntimeError(f"❌ Document analysis failed: {error_msg}")
            
            # Still running - wait with exponential backoff
            wait_time = min(INITIAL_WAIT_MS * (BACKOFF_MULTIPLIER ** attempt), MAX_WAIT_MS)
            print(f"   Attempt {attempt + 1}/{max_attempts}: Status = {status}, waiting {int(wait_time)}ms...")
            time.sleep(wait_time / 1000)
            attempt += 1
            
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Poll attempt {attempt + 1} error: {e}")
            attempt += 1
            if attempt < max_attempts:
                time.sleep(3)
    
    raise RuntimeError(f"❌ Polling timeout after {max_attempts} attempts")
