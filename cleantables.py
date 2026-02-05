import pandas as pd
import os
import sys
import glob
import re
import json
from typing import List, Dict, Tuple, Optional
from datetime import datetime

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
    print(f"    Rows: {len(table_df)}")
    
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
    
    print(f"    ✅ Created structured table: {len(structured_df)} rows")
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

def load_grouped_tables_file(input_file: str) -> Dict[str, pd.DataFrame]:
    """
    Load all tables from grouped tables Excel file.
    Returns dictionary with sheet names as keys and DataFrames as values.
    """
    print(f"\n📄 Loading grouped tables from: {input_file}")
    
    try:
        xls = pd.ExcelFile(input_file)
        sheet_names = xls.sheet_names
        
        print(f"✅ Found {len(sheet_names)} sheets:")
        
        tables = {}
        for sheet in sheet_names:
            try:
                df = pd.read_excel(input_file, sheet_name=sheet)
                tables[sheet] = df
                print(f"   📊 {sheet}: {len(df)} rows")
            except Exception as e:
                print(f"   ❌ Error loading sheet {sheet}: {e}")
        
        return tables
        
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        import traceback
        traceback.print_exc()
        return {}

def save_structured_tables(structured_tables: Dict[str, pd.DataFrame], output_file: str) -> None:
    """
    Save all structured tables to a new Excel file.
    """
    if not structured_tables:
        print("❌ No structured tables to save")
        return
    
    print(f"\n💾 Saving structured tables to: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Save each structured table
        for sheet_name, table_df in structured_tables.items():
            if table_df.empty:
                continue
            
            # Clean sheet name for Excel (max 31 characters, no invalid chars)
            clean_sheet_name = re.sub(r'[\\/*?:[\]]', '', sheet_name)
            if len(clean_sheet_name) > 31:
                clean_sheet_name = clean_sheet_name[:31]
            
            # Ensure sheet name is unique
            base_name = clean_sheet_name
            counter = 1
            while clean_sheet_name in writer.sheets:
                clean_sheet_name = f"{base_name}_{counter}"
                counter += 1
            
            # Save to Excel
            table_df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
            print(f"   ✅ Saved {clean_sheet_name}: {len(table_df)} rows")
        
        # Create a summary sheet
        summary_data = []
        for sheet_name, table_df in structured_tables.items():
            if table_df.empty:
                continue
            
            # Get value columns (excluding Line Item and metadata)
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
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            print(f"   ✅ Created Summary sheet")
        
        # Add a README sheet
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
        readme_df.to_excel(writer, sheet_name='README', index=False, header=False)
        print(f"   ✅ Created README sheet")
    
    print(f"\n🎉 Successfully saved {len(structured_tables)} structured tables")

def get_input_file() -> str:
    """
    Ask user for input file path (grouped tables file).
    """
    print("\n" + "="*60)
    print("📊 TABLE PROCESSING TOOL")
    print("="*60)
    print("Processes grouped tables into structured format:")
    print("Line Item | Column1 | Column2 | Column3 | ...")
    print()
    
    # Look for grouped tables files
    print("📁 Searching for grouped table files...")
    
    patterns = [
        '*grouped*.xlsx',
        '*tables*.xlsx',
        '*_grouped.xlsx',
        '*.xlsx'
    ]
    
    found_files = []
    for pattern in patterns:
        files = glob.glob(pattern)
        for file in files:
            if file not in found_files and os.path.isfile(file):
                found_files.append(file)
    
    if found_files:
        print(f"✅ Found {len(found_files)} Excel file(s):")
        print("-" * 60)
        
        for i, file in enumerate(found_files, 1):
            file_size = os.path.getsize(file)
            size_str = f"{file_size/1024:.1f} KB"
            file_name = os.path.basename(file)
            print(f"{i:2d}. {file_name} ({size_str})")
            print(f"    Path: {file}")
        
        print("-" * 60)
        
        while True:
            choice = input("\n👉 Select file (number) or enter path (or 'q' to quit): ").strip()
            
            if choice.lower() == 'q':
                print("👋 Goodbye!")
                sys.exit(0)
            
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(found_files):
                    selected = found_files[idx]
                    print(f"✅ Selected: {selected}")
                    return selected
                else:
                    print(f"❌ Please enter 1-{len(found_files)}")
            elif choice:
                if os.path.exists(choice):
                    print(f"✅ Selected: {choice}")
                    return choice
                else:
                    print(f"❌ File not found: {choice}")
            else:
                print("❌ Please enter a selection")
    
    else:
        print("❌ No Excel files found in current directory.")
        
        while True:
            file_path = input("\n👉 Enter full file path (or 'q' to quit): ").strip()
            
            if file_path.lower() == 'q':
                print("👋 Goodbye!")
                sys.exit(0)
            
            if not file_path:
                print("❌ Please enter a file path")
                continue
            
            if os.path.exists(file_path):
                print(f"✅ File found: {file_path}")
                return file_path
            else:
                print(f"❌ File not found: {file_path}")

def get_output_file(input_file: str) -> str:
    """
    Generate output file path based on input file.
    """
    base_name = os.path.splitext(input_file)[0]
    
    # Remove any existing suffixes
    for suffix in ['_grouped', '_tables', '_filtered']:
        if base_name.endswith(suffix):
            base_name = base_name[:-len(suffix)]
    
    default_output = base_name + '_structured.xlsx'
    
    print(f"\n💾 Default output file: {default_output}")
    
    while True:
        output_path = input("👉 Enter output path (press Enter for default): ").strip()
        
        if not output_path:
            output_path = default_output
            print(f"✅ Using default: {output_path}")
            return output_path
        
        # Check directory
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            print(f"⚠️ Directory doesn't exist: {output_dir}")
            create = input("   Create it? (y/n): ").strip().lower()
            if create == 'y':
                try:
                    os.makedirs(output_dir, exist_ok=True)
                    print(f"✅ Created directory")
                except Exception as e:
                    print(f"❌ Failed to create directory: {e}")
                    continue
        
        # Check if file exists
        if os.path.exists(output_path):
            print(f"⚠️ File already exists: {output_path}")
            overwrite = input("   Overwrite? (y/n): ").strip().lower()
            if overwrite != 'y':
                print("   Please enter a different path.")
                continue
        
        return output_path

def show_table_preview(structured_tables: Dict[str, pd.DataFrame]) -> None:
    """
    Show a preview of the structured tables.
    """
    if not structured_tables:
        return
    
    print(f"\n📊 PREVIEW OF STRUCTURED TABLES")
    print("="*80)
    
    for sheet_name, table_df in list(structured_tables.items())[:3]:  # Show first 3 tables
        if table_df.empty:
            continue
        
        print(f"\n📋 {sheet_name} ({len(table_df)} rows)")
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
        
        if len(structured_tables) > 1 and sheet_name != list(structured_tables.keys())[-1]:
            print()

def main():
    """
    Main function to process grouped tables.
    """
    try:
        print("\n" + "="*60)
        print("📊 FINANCIAL TABLE STRUCTURING TOOL")
        print("="*60)
        
        # Get input file
        input_file = get_input_file()
        
        # Get output file
        output_file = get_output_file(input_file)
        
        print("\n" + "="*60)
        print("🚀 PROCESSING TABLES")
        print("="*60)
        
        # Load all tables from the grouped file
        all_tables = load_grouped_tables_file(input_file)
        
        if not all_tables:
            print("❌ No tables found in the file")
            return
        
        print(f"\n🔍 Found {len(all_tables)} sheets to process")
        
        # Process each table
        structured_tables = {}
        
        for sheet_name, table_df in all_tables.items():
            # Skip summary/README sheets
            if sheet_name.lower() in ['summary', 'readme', 'table_statistics', 'all_tables']:
                print(f"\n  ⏩ Skipping {sheet_name} (summary sheet)")
                continue
            
            # Extract table number from sheet name
            table_num_match = re.search(r'(\d+)', sheet_name)
            table_num = int(table_num_match.group(1)) if table_num_match else 1
            
            # Process the table
            structured_df = process_single_table(table_df, table_num)
            
            if not structured_df.empty:
                structured_tables[sheet_name] = structured_df
        
        if not structured_tables:
            print("\n❌ No tables were successfully structured")
            return
        
        # Save the structured tables
        save_structured_tables(structured_tables, output_file)
        
        # Show preview
        show_table_preview(structured_tables)
        
        print("\n" + "="*60)
        print("🎉 PROCESSING COMPLETE!")
        print("="*60)
        print(f"✅ Processed {len(structured_tables)} tables")
        print(f"📂 Output saved to: {output_file}")
        
        # Offer to open the file
        if sys.platform == 'win32':
            open_file = input("\n📂 Open output file? (y/n): ").strip().lower()
            if open_file == 'y':
                os.startfile(output_file)
        elif sys.platform == 'darwin':
            open_file = input("\n📂 Open output file? (y/n): ").strip().lower()
            if open_file == 'y':
                os.system(f'open "{output_file}"')
        elif sys.platform.startswith('linux'):
            open_file = input("\n📂 Open output file? (y/n): ").strip().lower()
            if open_file == 'y':
                os.system(f'xdg-open "{output_file}"')
        
        # Process another file?
        print("\n" + "-"*40)
        another = input("🔄 Process another file? (y/n): ").strip().lower()
        if another == 'y':
            main()
        else:
            print("👋 Goodbye!")
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user")
        print("👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

# =========================
# RUN THE SCRIPT
# =========================

if __name__ == "__main__":
    main()
