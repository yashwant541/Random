# =============================================================================
# DATAIKU TABLE STRUCTURING TOOL
# =============================================================================
# Processes grouped tables into structured format:
# Line Item | Column1 | Column2 | Column3 | ...
# Reads Excel files from Dataiku folders and saves structured tables to Dataiku.
# =============================================================================

import dataiku
import pandas as pd
import io
import re
import json
from typing import List, Dict, Tuple, Optional
from datetime import datetime

# =============================================================================
# CONFIGURATION - SET THESE TO YOUR DATAIKU FOLDER IDs
# =============================================================================
INPUT_FOLDER_ID = "xFGhJtYE"          # Your Dataiku INPUT folder ID (grouped tables)
OUTPUT_FOLDER_ID = "output_folder_id" # Your Dataiku OUTPUT folder ID (structured tables)

# =============================================================================
# DATAIKU HELPER FUNCTIONS
# =============================================================================

def get_input_folder():
    """Get Dataiku input folder object"""
    return dataiku.Folder(INPUT_FOLDER_ID)

def get_output_folder():
    """Get Dataiku output folder object"""
    return dataiku.Folder(OUTPUT_FOLDER_ID)

def list_excel_files_in_folder() -> List[str]:
    """
    List all Excel files in the Dataiku input folder
    
    Returns:
        List of Excel filenames
    """
    folder = get_input_folder()
    all_files = folder.list_paths_in_partition()
    
    excel_files = [f for f in all_files if f.lower().endswith(('.xlsx', '.xls'))]
    return sorted(excel_files)

def read_excel_from_dataiku(filename: str) -> Dict[str, pd.DataFrame]:
    """
    Read Excel file from Dataiku folder and load all sheets
    
    Args:
        filename: Name of Excel file in Dataiku folder
        
    Returns:
        Dictionary with sheet names as keys and DataFrames as values
    """
    folder = get_input_folder()
    
    print(f"📥 Reading Excel file from Dataiku: {filename}")
    
    try:
        # Read file from Dataiku
        with folder.get_download_stream(filename) as stream:
            excel_bytes = stream.read()
        
        print(f"   ✅ Read {len(excel_bytes):,} bytes")
        
        # Convert bytes to Excel file object
        excel_file = io.BytesIO(excel_bytes)
        
        # Read all sheets
        xls = pd.ExcelFile(excel_file)
        sheet_names = xls.sheet_names
        
        tables = {}
        for sheet in sheet_names:
            try:
                # Reset file pointer for each sheet
                excel_file.seek(0)
                df = pd.read_excel(excel_file, sheet_name=sheet)
                tables[sheet] = df
                print(f"   📊 {sheet}: {len(df):,} rows")
            except Exception as e:
                print(f"   ⚠️ Error loading sheet {sheet}: {e}")
        
        print(f"   ✅ Loaded {len(tables)} sheets")
        return tables
        
    except Exception as e:
        print(f"❌ Error reading file {filename}: {e}")
        raise

def save_excel_to_dataiku(tables: Dict[str, pd.DataFrame], filename: str) -> None:
    """
    Save multiple tables (sheets) to Excel in Dataiku output folder
    
    Args:
        tables: Dictionary with sheet names as keys and DataFrames as values
        filename: Output filename
    """
    folder = get_output_folder()
    
    try:
        # Create Excel in memory
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Save each table
            for sheet_name, table_df in tables.items():
                if table_df.empty:
                    continue
                
                # Clean sheet name for Excel
                clean_sheet_name = re.sub(r'[\\/*?:[\]]', '', str(sheet_name))
                if len(clean_sheet_name) > 31:
                    clean_sheet_name = clean_sheet_name[:31]
                
                # Ensure unique sheet name
                base_name = clean_sheet_name
                counter = 1
                while clean_sheet_name in writer.sheets:
                    clean_sheet_name = f"{base_name}_{counter}"
                    counter += 1
                
                # Save to Excel
                table_df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
        
        # Get the bytes and save to Dataiku
        excel_bytes = output.getvalue()
        
        with folder.get_writer(filename) as writer:
            writer.write(excel_bytes)
        
        print(f"✅ Saved to Dataiku: {filename}")
        print(f"   Sheets: {len(tables)}")
        
    except Exception as e:
        print(f"❌ Error saving file {filename}: {e}")
        raise

# =============================================================================
# CORE PROCESSING FUNCTIONS
# =============================================================================

def parse_regular_numbers(regular_numbers_str: str) -> List[str]:
    """
    Parse Regular_Numbers string into a clean list.
    Handles different formats like "['5,147', '4,904', '5']"
    """
    if pd.isna(regular_numbers_str):
        return []
    
    # If it's already a list, return it
    if isinstance(regular_numbers_str, list):
        return regular_numbers_str
    
    # Clean the string
    cleaned = str(regular_numbers_str).strip()
    
    # Remove brackets and quotes
    if cleaned.startswith('[') and cleaned.endswith(']'):
        cleaned = cleaned[1:-1]
    
    # Split by comma, handling quotes properly
    numbers = []
    current = []
    in_quotes = False
    quote_char = None
    
    for i, char in enumerate(cleaned):
        if char in ["'", '"']:
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None
            current.append(char)
        elif char == ',' and not in_quotes:
            # Check if next character is space or end of string
            if i + 1 >= len(cleaned) or cleaned[i + 1] == ' ':
                numbers.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        else:
            current.append(char)
    
    # Add the last number
    if current:
        numbers.append(''.join(current).strip())
    
    # Clean each number
    cleaned_numbers = []
    for num in numbers:
        # Remove quotes
        num = num.strip()
        if (num.startswith("'") and num.endswith("'")) or (num.startswith('"') and num.endswith('"')):
            num = num[1:-1]
        cleaned_numbers.append(num)
    
    return cleaned_numbers

def clean_label(label: str) -> str:
    """
    Clean and format the label for better presentation.
    """
    if pd.isna(label):
        return ""
    
    label = str(label)
    
    # Remove trailing punctuation and spaces
    label = label.strip()
    
    # Remove trailing colons, dashes, etc.
    label = re.sub(r'[:;,\-\s]+$', '', label)
    
    # Capitalize first letter of each word (title case)
    words = label.split()
    cleaned_words = []
    
    for word in words:
        if word and not word[0].isdigit():
            # Keep acronyms in uppercase
            if word.isupper():
                cleaned_words.append(word)
            else:
                cleaned_words.append(word.title())
        else:
            cleaned_words.append(word)
    
    return ' '.join(cleaned_words)

def create_structured_table_from_raw(table_df: pd.DataFrame, table_num: int) -> pd.DataFrame:
    """
    Convert raw table data into structured format with Line Item and values.
    This function works directly with the raw extraction format.
    """
    if table_df.empty:
        return pd.DataFrame()
    
    # Make a copy
    df = table_df.copy()
    
    # Parse Regular_Numbers for each row
    df['Parsed_Numbers'] = df['Regular_Numbers'].apply(parse_regular_numbers)
    
    # Check if all rows have the same number of regular numbers
    num_counts = df['Parsed_Numbers'].apply(len).unique()
    if len(num_counts) != 1:
        print(f"  ⚠️ Table {table_num}: Rows have different numbers of regular numbers: {num_counts}")
        # Use the most common count
        from collections import Counter
        count_counter = Counter(df['Parsed_Numbers'].apply(len))
        most_common_count = count_counter.most_common(1)[0][0]
        print(f"     Using most common count: {most_common_count}")
    else:
        most_common_count = num_counts[0]
    
    # Create column names based on count
    if most_common_count == 3:
        column_names = ['Current', 'Prior', 'Change']
    elif most_common_count == 4:
        column_names = ['Q1', 'Q2', 'Q3', 'Q4']
    elif most_common_count == 2:
        column_names = ['Current', 'Prior']
    else:
        column_names = [f'Value{i+1}' for i in range(most_common_count)]
    
    # Create new structured DataFrame
    structured_data = []
    
    for idx, row in df.iterrows():
        # Get the label (use Label column if available)
        if 'Label' in row and not pd.isna(row['Label']):
            label = str(row['Label']).strip()
        else:
            # Try to extract from Raw_Line
            raw_line = row['Raw_Line'] if 'Raw_Line' in row else ''
            # Remove numbers from the end to get the label
            label = re.sub(r'[-\d,\$\(\)%\s]+$', '', str(raw_line)).strip()
        
        # Clean the label
        label = clean_label(label)
        
        # Get the parsed numbers
        parsed_numbers = row['Parsed_Numbers']
        
        # Pad or truncate to match column count
        if len(parsed_numbers) > most_common_count:
            parsed_numbers = parsed_numbers[:most_common_count]
        elif len(parsed_numbers) < most_common_count:
            parsed_numbers = parsed_numbers + [''] * (most_common_count - len(parsed_numbers))
        
        # Create row dictionary
        row_dict = {'Line Item': label}
        
        # Add value columns
        for i, (col_name, value) in enumerate(zip(column_names, parsed_numbers)):
            row_dict[col_name] = value
        
        # Add metadata
        metadata = {}
        if 'Page' in row:
            metadata['Page'] = int(row['Page']) if pd.notna(row['Page']) else ''
        if 'Section' in row:
            metadata['Section'] = str(row['Section']) if pd.notna(row['Section']) else ''
        
        # Add metadata as JSON
        if metadata:
            row_dict['_metadata'] = json.dumps(metadata)
        
        structured_data.append(row_dict)
    
    # Create the structured DataFrame
    structured_df = pd.DataFrame(structured_data)
    
    return structured_df

def parse_numeric_value(value_str: str) -> Tuple[Optional[float], str]:
    """
    Parse a string value into numeric format.
    Returns (numeric_value, cleaned_string)
    """
    if pd.isna(value_str) or value_str == '':
        return None, ''
    
    original = str(value_str).strip()
    working = original
    
    # Handle negative numbers in parentheses
    if working.startswith('(') and working.endswith(')'):
        working = '-' + working[1:-1]
    
    # Check for basis points
    if 'bps' in working.lower():
        try:
            num_str = re.sub(r'[^\d\.\-]', '', working.replace('bps', ''))
            num = float(num_str)
            return num / 10000, original  # Convert bps to decimal
        except:
            return None, original
    
    # Check for percentage
    if '%' in working:
        try:
            num_str = re.sub(r'[^\d\.\-]', '', working.replace('%', ''))
            num = float(num_str)
            return num / 100, original  # Convert percentage to decimal
        except:
            return None, original
    
    # Handle multipliers (k, m, b)
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
    
    # Remove commas, currency symbols, and other non-numeric characters (keep decimal point and minus)
    working = re.sub(r'[^\d\.\-]', '', working)
    
    try:
        if working == '' or working == '-':
            return None, original
        num = float(working) * multiplier
        return num, original
    except:
        return None, original

def add_numeric_columns(structured_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add numeric parsed columns alongside the original string columns.
    """
    if structured_df.empty:
        return structured_df
    
    df = structured_df.copy()
    
    # Identify value columns (exclude Line Item and metadata)
    value_columns = [col for col in df.columns if col not in ['Line Item', '_metadata']]
    
    # Add numeric versions of each value column
    for col in value_columns:
        numeric_values = []
        string_values = []
        
        for value in df[col]:
            numeric_val, string_val = parse_numeric_value(value)
            numeric_values.append(numeric_val)
            string_values.append(string_val)
        
        # Update the original column with cleaned string values
        df[col] = string_values
        
        # Add numeric column
        df[f'{col}_Num'] = numeric_values
    
    return df

def process_single_table(table_df: pd.DataFrame, table_num: int) -> pd.DataFrame:
    """
    Process a single table from raw format to structured format.
    """
    print(f"\n  📋 Processing Table {table_num}:")
    print(f"    Rows: {len(table_df):,}")
    
    # Check required columns
    required_cols = ['Label', 'Regular_Numbers']
    missing_cols = [col for col in required_cols if col not in table_df.columns]
    
    if missing_cols:
        print(f"    ⚠️ Missing columns: {missing_cols}")
        
        # Try to find alternative column names
        alt_mapping = {
            'Label': ['label', 'Label', 'line_item', 'Line_Item'],
            'Regular_Numbers': ['regular_numbers', 'Regular_Numbers', 'numbers', 'Numbers']
        }
        
        for missing in missing_cols:
            for alt in alt_mapping.get(missing, []):
                if alt in table_df.columns:
                    table_df = table_df.rename(columns={alt: missing})
                    print(f"    ✅ Found alternative: '{alt}' -> '{missing}'")
                    break
        
        # Check again
        missing_cols = [col for col in required_cols if col not in table_df.columns]
        if missing_cols:
            print(f"    ❌ Still missing: {missing_cols}")
            return pd.DataFrame()
    
    # Create structured table
    structured_df = create_structured_table_from_raw(table_df, table_num)
    
    if structured_df.empty:
        print(f"    ❌ Failed to create structured table")
        return pd.DataFrame()
    
    print(f"    ✅ Created structured table: {len(structured_df):,} rows")
    print(f"    Columns: {list(structured_df.columns)}")
    
    # Add numeric columns
    structured_df = add_numeric_columns(structured_df)
    
    # Show sample
    if len(structured_df) > 0:
        sample_label = structured_df['Line Item'].iloc[0]
        value_cols = [col for col in structured_df.columns if col not in ['Line Item', '_metadata'] and not col.endswith('_Num')]
        if value_cols:
            sample_values = structured_df[value_cols[0]].iloc[0]
            print(f"    Sample: '{sample_label}' → '{sample_values}'")
    
    return structured_df

def save_structured_tables_to_dataiku(structured_tables: Dict[str, pd.DataFrame], output_filename: str) -> None:
    """
    Save all structured tables to Dataiku output folder.
    """
    if not structured_tables:
        print("❌ No structured tables to save")
        return
    
    print(f"\n💾 Saving structured tables to Dataiku...")
    
    # Prepare all tables for Excel
    all_tables = {}
    
    # Add structured tables
    for sheet_name, table_df in structured_tables.items():
        if not table_df.empty:
            all_tables[sheet_name] = table_df
    
    # Create summary sheet
    summary_data = []
    for sheet_name, table_df in structured_tables.items():
        if table_df.empty:
            continue
        
        # Get value columns (excluding numeric and metadata)
        value_cols = [col for col in table_df.columns 
                     if col not in ['Line Item', '_metadata'] and not col.endswith('_Num')]
        
        summary_data.append({
            'Table_Name': sheet_name,
            'Rows': len(table_df),
            'Value_Columns': len(value_cols),
            'Column_Names': ', '.join(value_cols),
            'Sample_Line_Item': table_df['Line Item'].iloc[0] if len(table_df) > 0 else '',
            'Sample_Value': table_df[value_cols[0]].iloc[0] if value_cols else ''
        })
    
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        all_tables['Summary'] = summary_df
    
    # Create README sheet
    readme_content = [
        ["STRUCTURED FINANCIAL TABLES"],
        [""],
        ["This file contains structured financial tables extracted from raw data."],
        ["Each table is in a separate sheet."],
        [""],
        ["COLUMN FORMAT:"],
        ["- Line Item: Cleaned label/description"],
        ["- Column1, Column2, etc.: Original string values"],
        ["- Column1_Num, Column2_Num, etc.: Parsed numeric values"],
        ["- _metadata: JSON containing page and section information"],
        [""],
        ["GENERATED:", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        ["TOTAL TABLES:", len(structured_tables)]
    ]
    
    readme_df = pd.DataFrame(readme_content)
    all_tables['README'] = readme_df
    
    # Save to Dataiku
    save_excel_to_dataiku(all_tables, output_filename)
    
    print(f"\n🎉 Successfully saved {len(structured_tables)} structured tables")

def show_table_preview(structured_tables: Dict[str, pd.DataFrame]) -> None:
    """
    Show a preview of the structured tables.
    """
    if not structured_tables:
        return
    
    print(f"\n📊 PREVIEW OF STRUCTURED TABLES")
    print("="*80)
    
    # Show first 3 tables only
    tables_to_show = list(structured_tables.items())[:3]
    
    for sheet_name, table_df in tables_to_show:
        if table_df.empty:
            continue
        
        print(f"\n📋 {sheet_name} ({len(table_df):,} rows)")
        print("-" * 80)
        
        # Get value columns (excluding numeric and metadata)
        value_cols = [col for col in table_df.columns 
                     if col not in ['Line Item', '_metadata'] and not col.endswith('_Num')]
        
        # Show header
        header = f"{'Line Item':<30} | "
        header += " | ".join([f"{col:<15}" for col in value_cols[:3]])  # Show first 3 value columns
        print(header)
        print("-" * 80)
        
        # Show first 3 rows
        for idx, row in table_df.head(3).iterrows():
            label = str(row['Line Item'])[:28]
            values = " | ".join([str(row[col])[:13] for col in value_cols[:3]])
            print(f"{label:<30} | {values}")
        
        if len(tables_to_show) > 1 and sheet_name != tables_to_show[-1][0]:
            print()

# =============================================================================
# BATCH PROCESSING FUNCTIONS
# =============================================================================

def batch_process_all_files():
    """
    Process all grouped table files in the Dataiku input folder
    """
    print(f"\n{'='*60}")
    print("DATAIKU TABLE STRUCTURING TOOL - BATCH PROCESSING")
    print(f"{'='*60}")
    print(f"Input folder:  {INPUT_FOLDER_ID}")
    print(f"Output folder: {OUTPUT_FOLDER_ID}")
    print(f"{'='*60}")
    
    # List all Excel files
    excel_files = list_excel_files_in_folder()
    
    if not excel_files:
        print("❌ No Excel files found in input folder.")
        return []
    
    print(f"📁 Found {len(excel_files)} Excel file(s):")
    for i, filename in enumerate(excel_files, 1):
        print(f"   {i}. {filename}")
    
    print(f"\n🚀 Starting batch processing...")
    print(f"{'='*60}")
    
    # Process each file
    results = []
    for filename in excel_files:
        print(f"\n🎯 Processing: {filename}")
        
        try:
            # Read all sheets from Dataiku
            all_tables = read_excel_from_dataiku(filename)
            
            if not all_tables:
                print(f"   ⚠️ No tables found in file, skipping...")
                continue
            
            # Process each table sheet
            structured_tables = {}
            
            for sheet_name, table_df in all_tables.items():
                # Skip summary/README sheets
                if sheet_name.lower() in ['summary', 'readme', 'table_statistics', 'all_tables']:
                    print(f"  ⏩ Skipping {sheet_name} (summary sheet)")
                    continue
                
                # Extract table number from sheet name
                table_num_match = re.search(r'(\d+)', str(sheet_name))
                table_num = int(table_num_match.group(1)) if table_num_match else 1
                
                # Process the table
                structured_df = process_single_table(table_df, table_num)
                
                if not structured_df.empty:
                    structured_tables[sheet_name] = structured_df
            
            if not structured_tables:
                print(f"   ⚠️ No tables were successfully structured")
                results.append({
                    'input_file': filename,
                    'status': 'no_tables',
                    'error': 'No tables could be structured'
                })
                continue
            
            # Save to Dataiku
            output_filename = filename.replace('.xlsx', '_structured.xlsx').replace('.xls', '_structured.xlsx')
            save_structured_tables_to_dataiku(structured_tables, output_filename)
            
            # Show preview
            show_table_preview(structured_tables)
            
            results.append({
                'input_file': filename,
                'output_file': output_filename,
                'tables_processed': len(structured_tables),
                'status': 'success'
            })
            
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
            results.append({
                'input_file': filename,
                'error': str(e),
                'status': 'error'
            })
        
        if filename != excel_files[-1]:
            print(f"\n{'-'*60}")
    
    # Generate summary
    print(f"\n{'='*60}")
    print("BATCH PROCESSING SUMMARY")
    print(f"{'='*60}")
    
    successful = [r for r in results if r['status'] == 'success']
    errors = [r for r in results if r['status'] == 'error']
    no_tables = [r for r in results if r['status'] == 'no_tables']
    
    print(f"📊 Results:")
    print(f"   Total files processed: {len(results)}")
    print(f"   Successfully structured: {len(successful)}")
    print(f"   No tables found: {len(no_tables)}")
    print(f"   Errors: {len(errors)}")
    
    if successful:
        total_tables = sum(r.get('tables_processed', 0) for r in successful)
        print(f"\n✅ Created {total_tables} total structured tables")
        
        print(f"\n📁 Output files created:")
        for result in successful:
            print(f"   • {result['output_file']} ({result['tables_processed']} tables)")
    
    if errors:
        print(f"\n❌ Files with errors:")
        for result in errors:
            print(f"   • {result['input_file']}: {result['error']}")
    
    return results

# =============================================================================
# INTERACTIVE MODE (Optional)
# =============================================================================

def interactive_select_file():
    """
    Interactive mode for selecting a file from Dataiku folder
    """
    print(f"\n{'='*60}")
    print("DATAIKU TABLE STRUCTURING - INTERACTIVE MODE")
    print(f"{'='*60}")
    
    # List available files
    excel_files = list_excel_files_in_folder()
    
    if not excel_files:
        print("❌ No Excel files found in input folder.")
        return
    
    print(f"\n📁 Available files in '{INPUT_FOLDER_ID}' folder:")
    print("-" * 50)
    
    for i, filename in enumerate(excel_files, 1):
        print(f"{i:2d}. {filename}")
    
    print("-" * 50)
    print("\nOptions:")
    print("  [number] - Process a single file")
    print("  'all'    - Process all files")
    print("  'q'      - Quit")
    
    while True:
        choice = input("\n👉 Your choice: ").strip().lower()
        
        if choice == 'q':
            print("👋 Goodbye!")
            return
        
        elif choice == 'all':
            print(f"\n🎯 Processing ALL {len(excel_files)} files...")
            batch_process_all_files()
            break
        
        elif choice.isdigit():
            index = int(choice) - 1
            if 0 <= index < len(excel_files):
                selected_file = excel_files[index]
                print(f"\n🎯 Selected file: {selected_file}")
                
                try:
                    # Read all sheets from Dataiku
                    all_tables = read_excel_from_dataiku(selected_file)
                    
                    if not all_tables:
                        print(f"❌ No tables found in file")
                        break
                    
                    # Process each table sheet
                    structured_tables = {}
                    
                    for sheet_name, table_df in all_tables.items():
                        # Skip summary/README sheets
                        if sheet_name.lower() in ['summary', 'readme', 'table_statistics', 'all_tables']:
                            print(f"  ⏩ Skipping {sheet_name} (summary sheet)")
                            continue
                        
                        # Extract table number from sheet name
                        table_num_match = re.search(r'(\d+)', str(sheet_name))
                        table_num = int(table_num_match.group(1)) if table_num_match else 1
                        
                        # Process the table
                        structured_df = process_single_table(table_df, table_num)
                        
                        if not structured_df.empty:
                            structured_tables[sheet_name] = structured_df
                    
                    if not structured_tables:
                        print(f"❌ No tables were successfully structured")
                        break
                    
                    # Ask for output filename
                    default_output = selected_file.replace('.xlsx', '_structured.xlsx').replace('.xls', '_structured.xlsx')
                    output_choice = input(f"\n💾 Output filename [Enter for '{default_output}']: ").strip()
                    
                    if not output_choice:
                        output_choice = default_output
                    
                    # Save to Dataiku
                    save_structured_tables_to_dataiku(structured_tables, output_choice)
                    
                    # Show preview
                    show_table_preview(structured_tables)
                    
                    print(f"\n✅ Processing complete!")
                    print(f"   Created {len(structured_tables)} structured tables")
                    print(f"   Saved to: {output_choice}")
                    
                except Exception as e:
                    print(f"❌ Error: {e}")
                
                break
            else:
                print(f"❌ Please enter a number between 1 and {len(excel_files)}")
        
        else:
            print("❌ Invalid choice. Please enter a number, 'all', or 'q'")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main function to run the Dataiku table structuring tool
    """
    print(f"\n{'='*60}")
    print("DATAIKU TABLE STRUCTURING TOOL")
    print(f"{'='*60}")
    print("Converts grouped tables into structured format:")
    print("  Line Item | Column1 | Column2 | Column3 | ...")
    print()
    print(f"Configuration:")
    print(f"  Input folder:  {INPUT_FOLDER_ID}")
    print(f"  Output folder: {OUTPUT_FOLDER_ID}")
    print(f"{'='*60}")
    
    try:
        # Check required packages
        try:
            import pandas as pd
            import openpyxl
        except ImportError as e:
            print(f"❌ Missing required package: {e}")
            print("   Please add 'pandas' and 'openpyxl' to your Dataiku environment")
            return
        
        # Run in batch mode (automatically processes all files)
        results = batch_process_all_files()
        
        if results:
            print(f"\n{'='*60}")
            print("✅ PROCESSING COMPLETE!")
            print(f"{'='*60}")
            
            # Check if any processing was successful
            successful = any(r.get('status') == 'success' for r in results)
            if successful:
                print(f"📁 Check the output folder '{OUTPUT_FOLDER_ID}' for results.")
            else:
                print(f"⚠️ No files were successfully processed.")
        
        else:
            print(f"\n⚠️ No files were processed.")
            print(f"   Please check that Excel files exist in folder '{INPUT_FOLDER_ID}'")
    
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

# =============================================================================
# SIMPLE VERSION - MINIMAL CODE
# =============================================================================

def simple_dataiku_structure():
    """
    Simple version for basic table structuring
    """
    # CONFIGURATION
    INPUT_FOLDER = dataiku.Folder("xFGhJtYE")      # Your input folder
    OUTPUT_FOLDER = dataiku.Folder("output_folder_id")  # Your output folder
    
    print("Starting Dataiku table structuring...")
    
    # List Excel files
    all_files = INPUT_FOLDER.list_paths_in_partition()
    excel_files = [f for f in all_files if f.lower().endswith('.xlsx') and 'grouped' in f.lower()]
    
    if not excel_files:
        print("No grouped table files found!")
        return
    
    for filename in excel_files:
        print(f"\nProcessing: {filename}")
        
        try:
            # 1. READ FROM DATAIKU
            with INPUT_FOLDER.get_download_stream(filename) as stream:
                excel_bytes = stream.read()
            
            # 2. LOAD MAIN SHEET (Table_1)
            df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='Table_1')
            print(f"  Read {len(df)} rows from Table_1")
            
            if df.empty:
                print("  ⚠️ Table is empty, skipping...")
                continue
            
            # 3. CHECK COLUMNS
            if 'Label' not in df.columns or 'Regular_Numbers' not in df.columns:
                print("  ❌ Required columns not found. Skipping...")
                continue
            
            # 4. PARSE REGULAR NUMBERS
            def parse_numbers(x):
                if pd.isna(x): return []
                if isinstance(x, list): return x
                s = str(x).strip('[]\'"').replace(', ', ',').split(',')
                return [n.strip('\'"') for n in s if n.strip()]
            
            df['Parsed_Numbers'] = df['Regular_Numbers'].apply(parse_numbers)
            
            # 5. CREATE STRUCTURED TABLE
            structured_data = []
            for _, row in df.iterrows():
                label = str(row['Label']).strip()
                numbers = row['Parsed_Numbers']
                
                if numbers:
                    row_dict = {'Line Item': label}
                    for i, num in enumerate(numbers[:3]):  # Max 3 columns
                        row_dict[f'Value{i+1}'] = num
                    structured_data.append(row_dict)
            
            if not structured_data:
                print("  ⚠️ No structured data created")
                continue
            
            structured_df = pd.DataFrame(structured_data)
            print(f"  ✅ Created structured table: {len(structured_df)} rows")
            
            # 6. SAVE TO DATAIKU
            output_file = filename.replace('.xlsx', '_structured.xlsx')
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                structured_df.to_excel(writer, sheet_name='Structured_Table', index=False)
            
            with OUTPUT_FOLDER.get_writer(output_file) as writer:
                writer.write(output.getvalue())
            
            print(f"  💾 Saved to Dataiku: {output_file}")
        
        except Exception as e:
            print(f"  ❌ Error: {e}")

# =============================================================================
# RUN THE CODE
# =============================================================================

if __name__ == "__main__":
    # Run the main function (recommended)
    main()
    
    # Alternatively, run the simple version:
    # simple_dataiku_structure()
