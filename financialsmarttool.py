# ============================================================
# DATAIKU SMART FINANCIAL STATEMENT EXTRACTOR
# PDF + DOCX + TXT | Managed Folder Compatible
# ============================================================

import dataiku
from dataiku import Folder

import pdfplumber
import pandas as pd
import numpy as np
import re
import tempfile
import os
import io
from typing import List, Dict, Tuple
from docx import Document

# ============================================================
# CONFIGURATION
# ============================================================

INPUT_FOLDER_ID = "input_docs_folder"
OUTPUT_FOLDER_ID = "output_excel_folder"

# ============================================================
# FINANCIAL CONFIG
# ============================================================

FINANCIAL_TERMS = [
    'operating income', 'operating expenses', 'credit impairment',
    'profit before tax', 'profit after tax',
    'net interest income', 'fee income',
    'earnings per share', 'eps',
    'total assets', 'total equity',
    'risk weighted assets',
    'common equity tier', 'cet1',
    'tier 1 capital', 'tier 2 capital'
]

EXCLUSION_PHRASES = {
    'tier 1': ['1'],
    'tier 2': ['2'],
    'tier 3': ['3'],
    'cet1': ['1'],
    'q1': ['1'], 'q2': ['2'], 'q3': ['3'], 'q4': ['4']
}

LINE_ITEM_PATTERN = re.compile(r'^([^:\d]+?):\s*(.+)$', re.IGNORECASE)

# ============================================================
# TEXT HELPERS
# ============================================================

def clean_text(text: str) -> str:
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[–—]', '-', text)
    return text.strip()

def extract_numbers_smart(text: str) -> Tuple[List[str], List[str], List[str]]:
    text_lower = text.lower()
    exclusions = []

    for phrase, nums in EXCLUSION_PHRASES.items():
        pos = text_lower.find(phrase)
        if pos != -1:
            exclusions.append((pos, pos + len(phrase), nums))

    number_pattern = re.compile(
        r'\(?-?\$?\d[\d,]*(?:\.\d+)?\)?(?:\s*(?:bps|%|[kmb]))?',
        re.IGNORECASE
    )

    regular, excluded, all_nums = [], [], []

    for m in number_pattern.finditer(text):
        num = m.group().strip()
        digits = re.sub(r'[^\d]', '', num)
        all_nums.append(num)

        is_excluded = False
        for start, end, nums in exclusions:
            if m.start() >= start and m.end() <= end and digits in nums:
                is_excluded = True
                break

        if is_excluded:
            excluded.append(num)
        else:
            regular.append(num)

    return regular, excluded, all_nums

def count_consecutive_regular_numbers(text: str) -> int:
    regular, _, _ = extract_numbers_smart(text)
    if len(regular) <= 1:
        return len(regular)

    temp = text
    markers = []

    for i, num in enumerate(regular):
        marker = f"__NUM{i}__"
        temp = temp.replace(num, marker, 1)
        markers.append(marker)

    pattern = r'\s*'.join(map(re.escape, markers))
    return len(markers) if re.search(pattern, temp) else 1

def is_financial_line(line: str) -> bool:
    if LINE_ITEM_PATTERN.match(line):
        return True
    ll = line.lower()
    if any(term in ll for term in FINANCIAL_TERMS):
        return True
    nums, _, _ = extract_numbers_smart(line)
    return len(nums) >= 2

# ============================================================
# DOCUMENT READERS
# ============================================================

def read_pdf_lines(file_path: str) -> List[Tuple[int, str]]:
    lines = []
    with pdfplumber.open(file_path) as pdf:
        for page_no, page in enumerate(pdf.pages, 1):
            text = page.extract_text(layout=True)
            if text:
                for line in text.split("\n"):
                    lines.append((page_no, line))
    return lines

def read_docx_lines(file_path: str) -> List[Tuple[int, str]]:
    doc = Document(file_path)
    lines = []
    for i, para in enumerate(doc.paragraphs, 1):
        if para.text.strip():
            lines.append((i, para.text))
    return lines

def read_txt_lines(file_path: str) -> List[Tuple[int, str]]:
    lines = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f, 1):
            if line.strip():
                lines.append((i, line.strip()))
    return lines

# ============================================================
# CORE EXTRACTION
# ============================================================

def extract_financial_data(lines: List[Tuple[int, str]]) -> List[Dict]:
    results = []

    for page, raw in lines:
        line = clean_text(raw)
        if not line or not is_financial_line(line):
            continue

        match = LINE_ITEM_PATTERN.match(line)
        if match:
            label = match.group(1).strip()
            nums, ex, alln = extract_numbers_smart(match.group(2))
        else:
            nums, ex, alln = extract_numbers_smart(line)
            label = line.split(nums[0])[0].strip() if nums else line

        results.append({
            "Page": page,
            "Label": label,
            "Raw_Line": line,
            "Regular_Numbers": nums,
            "Excluded_Numbers": ex,
            "All_Numbers": alln,
            "Regular_Count": len(nums),
            "Excluded_Count": len(ex),
            "Total_Count": len(alln),
            "Consecutive_Count": count_consecutive_regular_numbers(line)
        })

    return results

# ============================================================
# DATAIKU I/O
# ============================================================

def get_local_file(folder: Folder, filename: str) -> str:
    suffix = os.path.splitext(filename)[1]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    with folder.get_download_stream(filename) as stream:
        tmp.write(stream.read())
    tmp.close()
    return tmp.name

def write_excel(folder: Folder, filename: str, df: pd.DataFrame):
    # Create an in-memory bytes buffer for the Excel file
    output = io.BytesIO()
    
    # Write DataFrame to Excel using pandas ExcelWriter
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Raw_Extraction', index=False)
    
    # Get the byte content and write to Dataiku folder
    output.seek(0)
    excel_content = output.getvalue()
    
    # Write to Dataiku folder
    with folder.get_writer(filename) as stream:
        stream.write(excel_content)

# ============================================================
# DATAIKU ENTRY POINT
# ============================================================

def run():
    input_folder = Folder(INPUT_FOLDER_ID)
    output_folder = Folder(OUTPUT_FOLDER_ID)

    files = input_folder.list_paths_in_partition()
    files = [f for f in files if f.lower().endswith((".pdf", ".docx", ".txt"))]

    if not files:
        raise Exception("❌ No PDF, DOCX, or TXT files found in input folder")

    filename = files[0]
    print(f"📄 Processing file: {filename}")

    local_path = get_local_file(input_folder, filename)

    # Route to appropriate reader based on file extension
    if filename.lower().endswith(".pdf"):
        lines = read_pdf_lines(local_path)
    elif filename.lower().endswith(".docx"):
        lines = read_docx_lines(local_path)
    elif filename.lower().endswith(".txt"):
        lines = read_txt_lines(local_path)
    else:
        raise Exception(f"Unsupported file type: {filename}")

    data = extract_financial_data(lines)

    if not data:
        raise Exception("❌ No financial data extracted")

    df = pd.DataFrame(data)

    output_name = filename.rsplit(".", 1)[0] + "_raw_extraction.xlsx"
    write_excel(output_folder, output_name, df)

    print(f"✅ Extraction complete → {output_name}")
    print(f"📊 Extracted {len(data)} financial line items")
    
    # Clean up temporary file
    try:
        os.unlink(local_path)
    except:
        pass

# Main execution
if __name__ == "__main__":
    run()
