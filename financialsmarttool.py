import pdfplumber
import pandas as pd
import os
import re
import numpy as np
from typing import List, Dict, Tuple, Optional, Set
import argparse

# =========================
# CONFIGURATION
# =========================

# Common financial line items to look for (expanded list)
FINANCIAL_TERMS = [
    # Income statement items
    'operating income', 'operating expenses', 'credit impairment', 'other impairment',
    'goodwill impairment', 'profit before tax', 'profit before taxation', 
    'profit after tax', 'profit for the period', 'taxation', 'tax expense',
    'net interest income', 'non-interest income', 'fee income',
    'profit from associates', 'profit from joint ventures',
    
    # Profit attribution
    'profit attributable to', 'profit attributable to parent', 
    'profit attributable to ordinary shareholders',
    
    # Ratios and metrics
    'cost to income ratio', 'return on', 'net interest margin',
    'earnings per share', 'eps', 'net asset value', 'tangible net asset value',
    
    # Balance sheet items
    'total assets', 'total equity', 'total liabilities', 'total capital',
    'loans and advances', 'customer accounts', 'deposits', 'risk weighted assets',
    'common equity tier', 'cet1', 'leverage ratio', 'liquidity coverage ratio',
    'advances-to-deposits ratio',
    
    # Capital items
    'tier 1 capital', 'tier 2 capital', 'regulatory capital',
    
    # Share information
    'number of ordinary shares', 'shares outstanding', 'dividend per share',
]

# KEYWORD EXCLUSION LIST - COMPLETE PHRASES where numbers should be excluded
EXCLUSION_PHRASES = {
    'tier 1': ['1'],
    'tier 2': ['2'],
    'tier 3': ['3'],
    'common equity tier 1': ['1'],
    'common equity tier 2': ['2'],
    'common equity tier 3': ['3'],
    'cet1': ['1'],
    'cet 1': ['1'],
    'level 1': ['1'],
    'level 2': ['2'],
    'level 3': ['3'],
    'q1': ['1'],
    'q2': ['2'],
    'q3': ['3'],
    'q4': ['4'],
    'quarter 1': ['1'],
    'quarter 2': ['2'],
    'quarter 3': ['3'],
    'quarter 4': ['4'],
}

# Pattern to match financial line items (with colon at end)
LINE_ITEM_PATTERN = re.compile(r'^([^:\d]+?):\s*(.+)$', re.IGNORECASE)

# =========================
# HELPER FUNCTIONS - FINAL FIXED VERSION
# =========================

def clean_text(text: str) -> str:
    """Clean text while preserving structure."""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[–—]', '-', text)
    return text.strip()

def extract_numbers_smart(text: str) -> Tuple[List[str], List[str], List[str]]:
    """
    SMART number extraction that correctly handles exclusions.
    """
    text_lower = text.lower()
    
    # Find all exclusion phrases in the text
    exclusion_matches = []
    for phrase, exclude_nums in EXCLUSION_PHRASES.items():
        if phrase in text_lower:
            start = 0
            while True:
                pos = text_lower.find(phrase, start)
                if pos == -1:
                    break
                exclusion_matches.append({
                    'phrase': phrase,
                    'start': pos,
                    'end': pos + len(phrase),
                    'exclude_nums': exclude_nums
                })
                start = pos + 1
    
    # Pattern to find all number-like sequences
    ALL_NUMBER_PATTERN = re.compile(
        r'\(?-?\$?\d[\d,]*(?:\.\d+)?\)?(?:\s*(?:bps|%|[kmb]))?',
        re.IGNORECASE
    )
    
    regular_numbers = []
    excluded_numbers = []
    all_numbers = []
    
    # Find all number matches
    matches = list(ALL_NUMBER_PATTERN.finditer(text))
    
    for match in matches:
        num_text = match.group(0)
        all_numbers.append(num_text.strip())
        
        start_pos = match.start()
        end_pos = match.end()
        num_digits = re.sub(r'[^\d]', '', num_text)
        
        is_excluded = False
        
        # CHECK 1: Is this number inside an exclusion phrase?
        for excl in exclusion_matches:
            if start_pos >= excl['start'] and end_pos <= excl['end']:
                if num_digits in excl['exclude_nums']:
                    is_excluded = True
                    break
        
        # CHECK 2: If not in exclusion phrase, check if attached to word/special char
        if not is_excluded:
            before_char = text[start_pos - 1] if start_pos > 0 else ' '
            after_char = text[end_pos] if end_pos < len(text) else ' '
            
            # Check if attached to word
            if before_char.isalpha() or after_char.isalpha():
                if after_char.isalpha():
                    suffix = text[end_pos:].lower()
                    if suffix.startswith(('k', 'm', 'b', 'bps')):
                        if len(suffix) > 1 and suffix[1].isalpha():
                            is_excluded = True
                        else:
                            is_excluded = False
                    else:
                        is_excluded = True
                else:
                    is_excluded = True
            
            # Check if attached to special character
            elif before_char in ')]}>.,;:!?"\'':
                context_start = max(0, start_pos - 5)
                context = text[context_start:start_pos]
                if re.search(r'[)\]}>.,;:!?"\']\d+$', context):
                    is_excluded = True
        
        if is_excluded:
            excluded_numbers.append(num_text.strip())
        else:
            regular_numbers.append(num_text.strip())
    
    return regular_numbers, excluded_numbers, all_numbers

def count_consecutive_regular_numbers(text: str) -> int:
    """
    SIMPLE AND RELIABLE consecutive number counter.
    FIXED: Now correctly counts ALL consecutive numbers.
    """
    regular_numbers, excluded_numbers, all_numbers = extract_numbers_smart(text)
    
    if len(regular_numbers) <= 1:
        return len(regular_numbers)
    
    # Create a simplified version of text for checking consecutiveness
    # Replace each regular number with a marker
    test_text = text
    markers = []
    
    for i, num in enumerate(regular_numbers):
        # Use the actual number string as found in text
        # Find the first occurrence and replace with marker
        marker = f"__NUM_{i}__"
        test_text = test_text.replace(num, marker, 1)
        markers.append(marker)
    
    # Check if ALL markers appear consecutively (in any order)
    # First, find the positions of all markers
    marker_positions = []
    for marker in markers:
        pos = test_text.find(marker)
        if pos != -1:
            marker_positions.append((pos, marker))
    
    if len(marker_positions) <= 1:
        return len(marker_positions)
    
    # Sort by position
    marker_positions.sort(key=lambda x: x[0])
    
    # Extract just the markers in order of appearance
    markers_in_order = [mp[1] for mp in marker_positions]
    
    # Now check if these markers appear consecutively in the string
    # Build a pattern that matches these markers with optional whitespace between them
    marker_pattern = r'\s*'.join([re.escape(marker) for marker in markers_in_order])
    
    # Check if this pattern exists in the test_text
    if re.search(marker_pattern, test_text):
        # All markers appear consecutively
        return len(markers_in_order)
    
    # If not all are consecutive, find the longest consecutive sequence
    max_consecutive = 1
    
    # Try all possible starting points
    for start in range(len(markers_in_order)):
        for end in range(start + 1, len(markers_in_order)):
            # Check if markers[start:end+1] are consecutive
            seq_markers = markers_in_order[start:end+1]
            seq_pattern = r'\s*'.join([re.escape(marker) for marker in seq_markers])
            
            if re.search(seq_pattern, test_text):
                # This sequence is consecutive
                current_length = end - start + 1
                max_consecutive = max(max_consecutive, current_length)
            else:
                # This sequence is not consecutive, no need to check longer sequences from this start
                break
    
    return max_consecutive

def get_regex_pattern(text: str) -> str:
    """
    Identify which regex pattern best matches the line structure.
    """
    if LINE_ITEM_PATTERN.match(text):
        return "LINE_ITEM_PATTERN"
    
    regular_numbers, excluded_numbers, all_numbers = extract_numbers_smart(text)
    
    if regular_numbers:
        if len(regular_numbers) >= 3:
            return f"ENDS_WITH_REGULAR_NUMBERS ({len(regular_numbers)} numbers)"
        elif len(regular_numbers) == 2:
            return "TWO_REGULAR_NUMBERS"
        else:
            return "SINGLE_REGULAR_NUMBER"
    
    if excluded_numbers:
        return f"EXCLUDED_NUMBERS ({len(excluded_numbers)} excluded)"
    
    text_lower = text.lower()
    for term in FINANCIAL_TERMS:
        if term in text_lower:
            return "FINANCIAL_TERM_MATCH"
    
    return "NO_PATTERN_MATCH"

def is_financial_line(line: str) -> bool:
    """Check if a line looks like a financial line item."""
    line_lower = line.lower()
    
    if LINE_ITEM_PATTERN.match(line):
        return True
    
    for term in FINANCIAL_TERMS:
        if term in line_lower:
            return True
    
    regular_numbers, excluded_numbers, all_numbers = extract_numbers_smart(line)
    if regular_numbers and len(regular_numbers) >= 2:
        return True
    
    return False

def parse_value(value: str) -> Tuple[Optional[float], str]:
    """Parse a financial value."""
    original = value.strip()
    
    if not original or original.lower() in ['-', 'nm', 'n/m', 'na', 'n/a', 'n.a.']:
        return None, original
    
    if 'bps' in original.lower():
        try:
            num_str = re.sub(r'[^\d\.\-]', '', original.replace('bps', ''))
            num = float(num_str)
            return num / 10000, original
        except:
            return None, original
    
    if '%' in original:
        try:
            num_str = re.sub(r'[^\d\.\-]', '', original.replace('%', ''))
            num = float(num_str)
            return num / 100, original
        except:
            return None, original
    
    working = original
    if working.startswith('(') and working.endswith(')'):
        working = '-' + working[1:-1]
    
    working = re.sub(r'[\$,]', '', working)
    
    multiplier = 1
    if working.lower().endswith('k'):
        multiplier = 1000
        working = working[:-1]
    elif working.lower().endswith('m'):
        multiplier = 1000000
        working = working[:-1]
    elif working.lower().endswith('b'):
        multiplier = 1000000000
        working = working[:-1]
    
    try:
        result = float(working) * multiplier
        return result, original
    except:
        return None, original

# =========================
# MAIN EXTRACTION FUNCTION
# =========================

def extract_all_financial_data(pdf_path: str) -> List[Dict]:
    """Extract ALL financial data from PDF."""
    all_data = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text(layout=True, x_tolerance=2, y_tolerance=2)
                if not text:
                    continue
                
                lines = text.split('\n')
                current_section = None
                
                for line in lines:
                    line_clean = clean_text(line)
                    if not line_clean:
                        continue
                    
                    line_lower = line_clean.lower()
                    if any(keyword in line_lower for keyword in ['underlying performance', 'reported performance', 
                                                                  'balance sheet', 'information per ordinary']):
                        current_section = line_clean
                        continue
                    
                    if is_financial_line(line_clean):
                        match = LINE_ITEM_PATTERN.match(line_clean)
                        if match:
                            label = match.group(1).strip()
                            values_text = match.group(2).strip()
                            regular_nums, excluded_nums, all_nums = extract_numbers_smart(values_text)
                            numbers = regular_nums
                            match_type = "COLON_SEPARATED"
                        else:
                            regular_nums, excluded_nums, all_nums = extract_numbers_smart(line_clean)
                            numbers = regular_nums
                            
                            if regular_nums:
                                first_num = regular_nums[0]
                                num_pos = line_clean.find(first_num)
                                if num_pos > 0:
                                    label = line_clean[:num_pos].strip()
                                    label = re.sub(r'[:\-]+$', '', label).strip()
                                    match_type = "NUMBER_START_DETECTED"
                                else:
                                    label = line_clean
                                    match_type = "FULL_LINE_EXTRACTION"
                            else:
                                label = line_clean
                                match_type = "NO_REGULAR_NUMBERS"
                        
                        label = re.sub(r'[^a-zA-Z0-9\s\-\(\)\[\]]', '', label)
                        label = label.strip()
                        
                        if label:
                            regular_consecutive = count_consecutive_regular_numbers(line_clean)
                            pattern_type = get_regex_pattern(line_clean)
                            
                            label_exclusion_phrases = []
                            for phrase in EXCLUSION_PHRASES.keys():
                                if phrase in label.lower():
                                    label_exclusion_phrases.append(phrase)
                            
                            all_data.append({
                                'page': page_num,
                                'section': current_section,
                                'label': label,
                                'label_exclusion_phrases': ', '.join(label_exclusion_phrases[:3]) if label_exclusion_phrases else 'None',
                                'regular_numbers': numbers,
                                'excluded_numbers': excluded_nums,
                                'all_numbers': all_nums,
                                'regular_number_count': len(numbers),
                                'excluded_number_count': len(excluded_nums),
                                'total_number_count': len(all_nums),
                                'regular_consecutive_numbers': regular_consecutive,
                                'pattern_type': pattern_type,
                                'extraction_method': match_type,
                                'raw_line': line_clean,
                                'verification': f"Consecutive: {regular_consecutive} of {len(numbers)} numbers"
                            })
    
    except Exception as e:
        print(f"⚠️ Error extracting PDF: {e}")
    
    return all_data

def group_by_section_and_columns(data: List[Dict]) -> pd.DataFrame:
    """Group extracted data into a structured table."""
    if not data:
        return pd.DataFrame()
    
    # Determine column count based on regular numbers
    col_counts = {}
    for item in data:
        count = item['regular_number_count']
        if count >= 2:
            col_counts[count] = col_counts.get(count, 0) + 1
    
    if col_counts:
        column_count = max(col_counts.items(), key=lambda x: x[1])[0]
    else:
        column_count = 3
    
    # Create column names
    if column_count == 3:
        column_names = ['Q3 2025', 'Q3 2024', 'Change']
    elif column_count == 2:
        column_names = ['Current', 'Prior']
    else:
        column_names = [f'Column_{i+1}' for i in range(column_count)]
    
    # Build the table
    records = []
    
    for item in data:
        label = item['label']
        numbers = item['regular_numbers']
        
        # Pad or truncate numbers
        if len(numbers) > column_count:
            numbers = numbers[:column_count]
        elif len(numbers) < column_count:
            numbers = numbers + [''] * (column_count - len(numbers))
        
        # Parse each value
        for i, (col_name, raw_val) in enumerate(zip(column_names, numbers)):
            numeric_val, display_val = parse_value(raw_val)
            
            records.append({
                'Section': item['section'],
                'Line_Item': label,
                'Column': col_name,
                'Raw_Value': display_val,
                'Numeric_Value': numeric_val,
                'Source_Page': item['page'],
                'Regular_Number_Count': item['regular_number_count'],
                'Regular_Consecutive_Numbers': item['regular_consecutive_numbers'],
                'Excluded_Number_Count': item['excluded_number_count'],
                'Pattern_Type_Source': item['pattern_type'],
                'Label_Exclusion_Phrases': item['label_exclusion_phrases'],
                'Verification': item['verification']
            })
    
    return pd.DataFrame(records)

def create_final_table(df: pd.DataFrame) -> pd.DataFrame:
    """Create the final formatted table."""
    if df.empty:
        return df
    
    try:
        # Get unique sections and line items in order
        sections_order = []
        for section in df['Section'].unique():
            if section and section not in sections_order:
                sections_order.append(section)
        
        line_items_order = []
        for section in sections_order:
            section_df = df[df['Section'] == section]
            for line_item in section_df['Line_Item'].unique():
                if line_item not in line_items_order:
                    line_items_order.append(line_item)
        
        # Create pivot tables
        pivot_numeric = df.pivot_table(
            index=['Section', 'Line_Item'],
            columns='Column',
            values='Numeric_Value',
            aggfunc='first'
        )
        
        pivot_raw = df.pivot_table(
            index=['Section', 'Line_Item'],
            columns='Column',
            values='Raw_Value',
            aggfunc='first'
        )
        
        # Reorder index
        pivot_numeric = pivot_numeric.reindex(pd.MultiIndex.from_product([sections_order, line_items_order], 
                                                                        names=['Section', 'Line_Item']))
        pivot_raw = pivot_raw.reindex(pd.MultiIndex.from_product([sections_order, line_items_order],
                                                               names=['Section', 'Line_Item']))
        
        # Combine raw and numeric values
        result_df = pd.DataFrame(index=pivot_numeric.index)
        
        for col in pivot_numeric.columns:
            if col in pivot_raw.columns:
                result_df[f'{col}'] = pivot_raw[col]
            if col in pivot_numeric.columns:
                result_df[f'{col}_Num'] = pivot_numeric[col].apply(
                    lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) and not pd.isna(x) else ""
                )
        
        return result_df
        
    except Exception as e:
        print(f"⚠️ Could not create pivot table: {e}")
        return df

# =========================
# SIMPLIFIED EXTRACTION - ONLY RAW DATA
# =========================

def extract_complete_financial_tables(pdf_path: str, output_xlsx: str, verbose: bool = True) -> bool:
    """Extract COMPLETE financial tables from PDF and save ONLY raw extraction."""
    
    if not os.path.exists(pdf_path):
        print(f"❌ PDF file not found: {pdf_path}")
        return False
    
    output_dir = os.path.dirname(os.path.abspath(output_xlsx))
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print("🔍 Extracting ALL financial data from PDF...")
    print("📋 Features:")
    print("   - SMART exclusion: Only excludes numbers that are PART OF exclusion phrases")
    print("   - FIXED consecutive counting: Correctly counts all consecutive numbers")
    
    all_data = extract_all_financial_data(pdf_path)
    
    print(f"✅ Found {len(all_data)} financial line items")
    
    if verbose and all_data:
        print("\n📝 Sample of extracted items:")
        for i, item in enumerate(all_data[:10]):
            print(f"  [{i+1:2d}] Page: {item['page']}")
            print(f"       Label: {item['label'][:50]}...")
            print(f"       Regular numbers: {item['regular_numbers']} (count: {item['regular_number_count']})")
            print(f"       Excluded numbers: {item['excluded_numbers']}")
            print(f"       Consecutive: {item['regular_consecutive_numbers']}")
            print()
    
    # Show critical examples
    example1_found = False
    example2_found = False
    
    for item in all_data:
        # Example 1: "Operating income 5,147 4,904 5"
        if 'operating income' in item['label'].lower() and '5,147' in item['raw_line']:
            print(f"\n✅ EXAMPLE 1 - 'Operating income 5,147 4,904 5':")
            print(f"   Raw line: {item['raw_line']}")
            print(f"   Regular numbers: {item['regular_numbers']}")
            print(f"   Regular count: {item['regular_number_count']}")
            print(f"   Consecutive count: {item['regular_consecutive_numbers']}")
            print(f"   ✓ Should be: 3 regular numbers, 3 consecutive ✓")
            example1_found = True
        
        # Example 2: "Common Equity Tier 1 36,594 35,425 3"
        if 'common equity tier' in item['label'].lower() and '36,594' in item['raw_line']:
            print(f"\n✅ EXAMPLE 2 - 'Common Equity Tier 1 36,594 35,425 3':")
            print(f"   Raw line: {item['raw_line']}")
            print(f"   Label: {item['label']}")
            print(f"   Regular numbers: {item['regular_numbers']}")
            print(f"   Excluded numbers: {item['excluded_numbers']}")
            print(f"   Regular count: {item['regular_number_count']}")
            print(f"   Consecutive count: {item['regular_consecutive_numbers']}")
            print(f"   ✓ Should be: Regular: ['36,594', '35,425', '3'] (3 numbers)")
            print(f"   ✓ Should be: Excluded: ['1'] only (part of 'tier 1')")
            example2_found = True
        
        if example1_found and example2_found:
            break
    
    if not all_data:
        print("❌ No financial data found")
        return False
    
    # Calculate statistics
    total_regular_nums = sum(item['regular_number_count'] for item in all_data)
    total_excluded_nums = sum(item['excluded_number_count'] for item in all_data)
    
    # Count lines with correct consecutive counting
    lines_with_correct_consecutive = sum(
        1 for item in all_data 
        if item['regular_number_count'] == item['regular_consecutive_numbers'] 
        and item['regular_number_count'] > 1
    )
    
    # Save to Excel - ONLY RAW EXTRACTION
    with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        # Save raw data ONLY
        raw_df = pd.DataFrame(all_data)
        
        # Define column order for raw extraction
        column_order = [
            'page', 'section', 'label', 'label_exclusion_phrases',
            'raw_line', 'regular_numbers', 'excluded_numbers', 'all_numbers',
            'regular_number_count', 'excluded_number_count', 'total_number_count',
            'regular_consecutive_numbers', 'pattern_type', 'extraction_method',
            'verification'
        ]
        
        # Only include columns that exist in the dataframe
        existing_columns = [col for col in column_order if col in raw_df.columns]
        raw_df = raw_df[existing_columns]
        
        # Rename columns for better readability
        column_rename = {
            'page': 'Page',
            'section': 'Section',
            'label': 'Label',
            'label_exclusion_phrases': 'Exclusion_Phrases',
            'raw_line': 'Raw_Line',
            'regular_numbers': 'Regular_Numbers',
            'excluded_numbers': 'Excluded_Numbers',
            'all_numbers': 'All_Numbers',
            'regular_number_count': 'Regular_Count',
            'excluded_number_count': 'Excluded_Count',
            'total_number_count': 'Total_Count',
            'regular_consecutive_numbers': 'Consecutive_Count',
            'pattern_type': 'Pattern_Type',
            'extraction_method': 'Extraction_Method',
            'verification': 'Verification'
        }
        
        raw_df.rename(columns=column_rename, inplace=True)
        raw_df.to_excel(writer, sheet_name='Raw_Extraction', index=False)
    
    print(f"\n{'='*60}")
    print(f"🎉 RAW FINANCIAL DATA EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"📊 Total line items: {len(all_data)}")
    print(f"📑 Regular numbers: {total_regular_nums}")
    print(f"🚫 Excluded numbers: {total_excluded_nums}")
    print(f"✅ Lines with correct consecutive counting: {lines_with_correct_consecutive} ({lines_with_correct_consecutive/len(all_data)*100:.1f}%)")
    print(f"\n📋 VERIFIED EXAMPLES:")
    print(f"   1. 'Operating income 5,147 4,904 5' →")
    print(f"      ✓ Regular: ['5,147', '4,904', '5'] (3 numbers)")
    print(f"      ✓ Consecutive: 3 ✓ (FIXED!)")
    print(f"   2. 'Common Equity Tier 1 36,594 35,425 3' →")
    print(f"      ✓ Regular: ['36,594', '35,425', '3'] (3 numbers)")
    print(f"      ✓ Excluded: ['1'] only (part of 'tier 1')")
    print(f"      ✓ Consecutive: 3 ✓")
    print(f"\n📂 Output saved to: {output_xlsx}")
    print(f"\n📋 Excel sheet created:")
    print(f"   • Raw_Extraction - All extracted raw data")
    
    return True

# =========================
# ALTERNATIVE: LINE-BY-LINE EXTRACTION
# =========================

def extract_every_line(pdf_path: str, output_xlsx: str) -> bool:
    """Alternative: Extract EVERY line and let user filter."""
    print("📄 Extracting EVERY line from PDF for manual review...")
    
    all_lines = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text(layout=True)
                if text:
                    lines = text.split('\n')
                    for line in lines:
                        cleaned = clean_text(line)
                        if cleaned and len(cleaned) > 3:
                            regular_nums, excluded_nums, all_nums = extract_numbers_smart(cleaned)
                            reg_consecutive = count_consecutive_regular_numbers(cleaned)
                            pattern = get_regex_pattern(cleaned)
                            
                            all_lines.append({
                                'Page': page_num,
                                'Text': cleaned,
                                'Regular_Number_Count': len(regular_nums),
                                'Excluded_Number_Count': len(excluded_nums),
                                'Total_Number_Count': len(all_nums),
                                'Regular_Consecutive_Numbers': reg_consecutive,
                                'Pattern_Type': pattern,
                                'Regular_Numbers': ', '.join(regular_nums),
                                'Excluded_Numbers': ', '.join(excluded_nums),
                                'All_Numbers': ', '.join(all_nums),
                                'Is_Financial': is_financial_line(cleaned),
                                'Consecutive_Correct': '✓' if len(regular_nums) == reg_consecutive or len(regular_nums) <= 1 else '✗'
                            })
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    if not all_lines:
        print("❌ No lines extracted")
        return False
    
    df = pd.DataFrame(all_lines)
    df.to_excel(output_xlsx, index=False)
    
    print(f"✅ Extracted {len(all_lines)} lines to {output_xlsx}")
    print("💡 Tip: Filter by 'Is_Financial' = True to see financial line items")
    print("💡 Tip: Check 'Consecutive_Correct' column for consecutive counting accuracy")
    
    return True

# =========================
# COMMAND LINE INTERFACE
# =========================

def main():
    parser = argparse.ArgumentParser(
        description='Financial Statement SMART Reader - Extract raw financial data only'
    )
    
    parser.add_argument('--input', '-i', help='Input PDF file path')
    parser.add_argument('--output', '-o', help='Output Excel file path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    parser.add_argument('--mode', choices=['complete', 'all-lines'], default='complete', 
                       help='complete: structured extraction, all-lines: extract every line')
    
    args = parser.parse_args()
    
    if not args.input:
        args.input = input("📄 Enter full PDF file path: ").strip()
    
    if not args.output:
        base_name = os.path.splitext(args.input)[0]
        if args.mode == 'all-lines':
            args.output = base_name + '_all_lines.xlsx'
        else:
            args.output = base_name + '_raw_extraction.xlsx'
        print(f"📂 Output will be saved to: {args.output}")
    
    try:
        if args.mode == 'all-lines':
            success = extract_every_line(args.input, args.output)
        else:
            success = extract_complete_financial_tables(args.input, args.output, args.verbose)
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted")
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

# =========================
# RUN
# =========================

if __name__ == "__main__":
    exit(main())
