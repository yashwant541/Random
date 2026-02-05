import pandas as pd
import os
import sys
import glob
from typing import List, Dict, Tuple, Optional
import json

def group_into_tables(df: pd.DataFrame) -> List[pd.DataFrame]:
    """
    Group rows into tables based on consecutive rows having the same 
    Regular_Count and Consecutive_Count values.
    
    Args:
        df: Input DataFrame with filtered data
    
    Returns:
        List of DataFrames, each representing a table
    """
    if df.empty:
        print("❌ Input DataFrame is empty")
        return []
    
    # Find column names for Regular_Count and Consecutive_Count
    regular_col = None
    consecutive_col = None
    
    for col in df.columns:
        col_lower = col.lower()
        if 'regular' in col_lower and 'count' in col_lower:
            regular_col = col
        elif 'consecutive' in col_lower and 'count' in col_lower:
            consecutive_col = col
    
    if not regular_col or not consecutive_col:
        print(f"❌ Could not find Regular_Count and Consecutive_Count columns")
        print(f"   Available columns: {list(df.columns)}")
        return []
    
    print(f"🔍 Using columns:")
    print(f"   Regular_Count: '{regular_col}'")
    print(f"   Consecutive_Count: '{consecutive_col}'")
    
    # Reset index for clean iteration
    df = df.reset_index(drop=True)
    tables = []
    current_table = []
    current_counts = None
    table_number = 1
    
    print(f"\n📊 Starting table grouping...")
    print(f"   Total rows to process: {len(df)}")
    
    for idx, row in df.iterrows():
        current_regular = row[regular_col]
        current_consecutive = row[consecutive_col]
        current_pair = (current_regular, current_consecutive)
        
        if current_counts is None:
            # First row starts a new table
            current_counts = current_pair
            current_table.append(row)
            print(f"   📋 Table {table_number} started at row {idx+1} (Counts: {current_regular}/{current_consecutive})")
        
        elif current_pair == current_counts:
            # Same counts, continue current table
            current_table.append(row)
        
        else:
            # Different counts, save current table and start new one
            if current_table:
                table_df = pd.DataFrame(current_table)
                table_df = table_df.reset_index(drop=True)
                tables.append(table_df)
                print(f"   📋 Table {table_number}: {len(table_df)} rows (Counts: {current_counts[0]}/{current_counts[1]})")
                table_number += 1
            
            # Start new table
            current_counts = current_pair
            current_table = [row]
            print(f"   📋 Table {table_number} started at row {idx+1} (Counts: {current_regular}/{current_consecutive})")
    
    # Don't forget the last table
    if current_table:
        table_df = pd.DataFrame(current_table)
        table_df = table_df.reset_index(drop=True)
        tables.append(table_df)
        print(f"   📋 Table {table_number}: {len(table_df)} rows (Counts: {current_counts[0]}/{current_counts[1]})")
    
    print(f"\n✅ Grouped into {len(tables)} tables")
    
    # Show table statistics
    for i, table in enumerate(tables, 1):
        reg_count = table[regular_col].iloc[0] if len(table) > 0 else 'N/A'
        cons_count = table[consecutive_col].iloc[0] if len(table) > 0 else 'N/A'
        
        # Get section information if available
        section_info = ''
        if 'Section' in table.columns:
            sections = table['Section'].dropna().unique()
            if len(sections) > 0:
                main_section = sections[0]
                section_info = f" | Section: {main_section}"
                if len(sections) > 1:
                    section_info += f" (+{len(sections)-1} more)"
        
        print(f"   Table {i}: {len(table)} rows | Counts: {reg_count}/{reg_count}{section_info}")
    
    return tables

def extract_table_structure(table_df: pd.DataFrame, table_num: int) -> Dict:
    """
    Extract the structure of a table for analysis.
    
    Args:
        table_df: DataFrame representing a table
        table_num: Table number
    
    Returns:
        Dictionary with table structure information
    """
    structure = {
        'table_number': table_num,
        'row_count': len(table_df),
        'columns': list(table_df.columns),
        'column_count': len(table_df.columns)
    }
    
    # Find Regular_Count and Consecutive_Count columns
    regular_col = None
    consecutive_col = None
    
    for col in table_df.columns:
        col_lower = col.lower()
        if 'regular' in col_lower and 'count' in col_lower:
            regular_col = col
        elif 'consecutive' in col_lower and 'count' in col_lower:
            consecutive_col = col
    
    if regular_col and consecutive_col:
        structure['regular_count'] = table_df[regular_col].iloc[0] if len(table_df) > 0 else None
        structure['consecutive_count'] = table_df[consecutive_col].iloc[0] if len(table_df) > 0 else None
        structure['counts_match'] = structure['regular_count'] == structure['consecutive_count']
    
    # Extract sample data
    if 'Label' in table_df.columns:
        structure['sample_labels'] = table_df['Label'].head(5).tolist()
    
    if 'Section' in table_df.columns:
        sections = table_df['Section'].dropna().unique()
        structure['sections'] = sections.tolist()
        structure['section_count'] = len(sections)
    
    if 'Page' in table_df.columns:
        pages = table_df['Page'].unique()
        structure['pages'] = pages.tolist()
        structure['page_count'] = len(pages)
    
    # Check if Regular_Numbers column exists and extract sample
    if 'Regular_Numbers' in table_df.columns:
        try:
            # Take first row's regular numbers as sample
            sample_numbers = table_df['Regular_Numbers'].iloc[0]
            structure['sample_numbers'] = sample_numbers
            structure['number_count'] = len(sample_numbers) if isinstance(sample_numbers, list) else 'N/A'
        except:
            structure['sample_numbers'] = 'N/A'
            structure['number_count'] = 'N/A'
    
    return structure

def save_tables_to_excel(tables: List[pd.DataFrame], output_file: str, 
                         include_individual_sheets: bool = True) -> None:
    """
    Save all tables to an Excel file.
    
    Args:
        tables: List of table DataFrames
        output_file: Output Excel file path
        include_individual_sheets: Whether to create individual sheets for each table
    """
    if not tables:
        print("❌ No tables to save")
        return
    
    print(f"\n💾 Saving tables to: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Save combined view
        combined_data = []
        for i, table in enumerate(tables, 1):
            table_copy = table.copy()
            table_copy.insert(0, 'Table_Number', i)
            combined_data.append(table_copy)
        
        combined_df = pd.concat(combined_data, ignore_index=True)
        combined_df.to_excel(writer, sheet_name='All_Tables', index=False)
        print(f"   ✅ Saved combined view: {len(combined_df)} rows")
        
        if include_individual_sheets:
            # Save each table to individual sheet
            for i, table in enumerate(tables, 1):
                sheet_name = f"Table_{i}"
                # Excel sheet names max 31 characters
                if len(sheet_name) > 31:
                    sheet_name = sheet_name[:31]
                
                # Add table number as first column
                table_copy = table.copy()
                table_copy.insert(0, 'Row_In_Table', range(1, len(table_copy) + 1))
                
                table_copy.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"   ✅ Saved {len(tables)} individual table sheets")
        
        # Save table statistics
        stats_data = []
        for i, table in enumerate(tables, 1):
            structure = extract_table_structure(table, i)
            
            # Create summary row
            summary = {
                'Table_Number': i,
                'Row_Count': structure['row_count'],
                'Regular_Count': structure.get('regular_count', 'N/A'),
                'Consecutive_Count': structure.get('consecutive_count', 'N/A'),
                'Counts_Match': structure.get('counts_match', 'N/A'),
                'Section_Count': structure.get('section_count', 0),
                'Page_Count': structure.get('page_count', 0),
                'Sections': ', '.join(map(str, structure.get('sections', [])))[:100],
                'Sample_Labels': ', '.join(map(str, structure.get('sample_labels', [])))[:100]
            }
            stats_data.append(summary)
        
        stats_df = pd.DataFrame(stats_data)
        stats_df.to_excel(writer, sheet_name='Table_Statistics', index=False)
        print(f"   ✅ Saved table statistics")
        
        # Save table structures in JSON format (as a string)
        structures = []
        for i, table in enumerate(tables, 1):
            structure = extract_table_structure(table, i)
            structures.append(structure)
        
        # Convert to JSON string for Excel
        json_str = json.dumps(structures, indent=2)
        json_df = pd.DataFrame({'Table_Structures': [json_str]})
        json_df.to_excel(writer, sheet_name='Table_Structures', index=False)
        print(f"   ✅ Saved table structures (JSON)")
    
    print(f"\n🎉 Successfully saved {len(tables)} tables to {output_file}")

def analyze_table_columns(tables: List[pd.DataFrame]) -> None:
    """
    Analyze the columns in each table to identify potential table structures.
    
    Args:
        tables: List of table DataFrames
    """
    print(f"\n🔍 Analyzing table column structures...")
    
    column_analysis = {}
    
    for i, table in enumerate(tables, 1):
        if 'Regular_Numbers' not in table.columns:
            continue
        
        # Analyze first row to infer column count from Regular_Numbers
        first_row = table.iloc[0]
        regular_numbers = first_row['Regular_Numbers']
        
        if isinstance(regular_numbers, list):
            num_columns = len(regular_numbers)
            
            if num_columns not in column_analysis:
                column_analysis[num_columns] = {
                    'count': 0,
                    'tables': [],
                    'sample_labels': []
                }
            
            column_analysis[num_columns]['count'] += 1
            column_analysis[num_columns]['tables'].append(i)
            
            # Add sample label
            if 'Label' in table.columns:
                label = first_row['Label'][:50]
                column_analysis[num_columns]['sample_labels'].append(label)
    
    # Display analysis
    print(f"\n📊 Table Column Analysis:")
    print("-" * 60)
    
    if not column_analysis:
        print("   No Regular_Numbers column found for analysis")
        return
    
    for num_cols in sorted(column_analysis.keys()):
        analysis = column_analysis[num_cols]
        print(f"\n   📋 {num_cols}-column tables: {analysis['count']} table(s)")
        print(f"      Tables: {', '.join(map(str, analysis['tables']))}")
        
        if analysis['sample_labels']:
            print(f"      Sample labels:")
            for label in analysis['sample_labels'][:3]:  # Show first 3
                print(f"        - {label}")
            if len(analysis['sample_labels']) > 3:
                print(f"        ... and {len(analysis['sample_labels']) - 3} more")

def get_input_file() -> str:
    """
    Ask user for input file path with autocomplete suggestions.
    
    Returns:
        Selected file path
    """
    print("\n" + "="*60)
    print("📊 TABLE GROUPING TOOL")
    print("="*60)
    print("Groups rows into tables based on consecutive rows")
    print("having the same Regular_Count and Consecutive_Count.")
    print()
    
    # Look for filtered files in current directory
    print("📁 Searching for filtered extraction files in current directory...")
    current_dir = os.getcwd()
    
    # Common patterns for filtered files
    patterns = [
        '*filtered*.xlsx',
        '*_no2.xlsx',
        '*_filtered_no2.xlsx',
        '*raw*.xlsx',
        '*.xlsx'  # All Excel files as last resort
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
        print("\nOptions:")
        print("  - Enter a number (1-{}) to select from list".format(len(found_files)))
        print("  - Enter a custom file path")
        print("  - Enter 'q' to quit")
        
        while True:
            choice = input("\n👉 Your choice: ").strip()
            
            if choice.lower() == 'q':
                print("👋 Goodbye!")
                sys.exit(0)
            
            # Check if choice is a number
            if choice.isdigit():
                index = int(choice) - 1
                if 0 <= index < len(found_files):
                    selected_file = found_files[index]
                    if os.path.exists(selected_file):
                        print(f"✅ Selected: {selected_file}")
                        return selected_file
                    else:
                        print(f"❌ File no longer exists: {selected_file}")
                else:
                    print(f"❌ Please enter a number between 1 and {len(found_files)}")
            
            # Check if it's a file path
            elif choice:
                if os.path.exists(choice):
                    print(f"✅ File found: {choice}")
                    return choice
                else:
                    print(f"❌ File not found: {choice}")
                    print("   Please check the path and try again.")
            
            else:
                print("❌ Please enter a valid choice")
    else:
        print("❌ No Excel files found in current directory.")
        print(f"   Current directory: {current_dir}")
        print("\nPlease enter the full path to your filtered Excel file.")
        
        while True:
            file_path = input("\n👉 Enter file path (or 'q' to quit): ").strip()
            
            if file_path.lower() == 'q':
                print("👋 Goodbye!")
                sys.exit(0)
            
            if not file_path:
                print("❌ Please enter a file path")
                continue
            
            if os.path.exists(file_path):
                if file_path.lower().endswith(('.xlsx', '.xls')):
                    print(f"✅ Excel file found: {file_path}")
                    return file_path
                else:
                    print(f"⚠️ Warning: '{file_path}' is not an Excel file (.xlsx or .xls)")
                    confirm = input("   Do you want to use it anyway? (y/n): ").strip().lower()
                    if confirm == 'y':
                        return file_path
            else:
                print(f"❌ File not found: {file_path}")
                print("   Please check the path and try again.")

def get_output_file(input_file: str) -> str:
    """
    Ask user for output file path with auto-suggestion.
    
    Args:
        input_file: Input file path
    
    Returns:
        Output file path
    """
    # Generate default output filename
    base_name = os.path.splitext(input_file)[0]
    default_output = base_name + '_grouped.xlsx'
    
    print(f"\n💾 Output file suggestion: {default_output}")
    
    while True:
        output_path = input("👉 Enter output file path (press Enter for default): ").strip()
        
        if not output_path:
            output_path = default_output
            print(f"✅ Using default: {output_path}")
            return output_path
        
        # Check if directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            print(f"⚠️ Directory does not exist: {output_dir}")
            create_dir = input("   Create directory? (y/n): ").strip().lower()
            if create_dir == 'y':
                try:
                    os.makedirs(output_dir, exist_ok=True)
                    print(f"✅ Created directory: {output_dir}")
                except Exception as e:
                    print(f"❌ Failed to create directory: {e}")
                    continue
        
        # Check if file already exists
        if os.path.exists(output_path):
            print(f"⚠️ File already exists: {output_path}")
            overwrite = input("   Overwrite? (y/n): ").strip().lower()
            if overwrite != 'y':
                print("   Please enter a different output file path.")
                continue
        
        return output_path

def load_data(input_file: str) -> Optional[pd.DataFrame]:
    """
    Load data from Excel file.
    
    Args:
        input_file: Path to Excel file
    
    Returns:
        DataFrame with loaded data, or None if error
    """
    print(f"\n📄 Loading data from: {input_file}")
    
    try:
        # Read the Excel file
        xls = pd.ExcelFile(input_file)
        
        # Try to find the right sheet
        sheet_name = None
        preferred_sheets = ['Raw_Extraction', 'Sheet1', 'Filtered', 'All_Tables']
        
        for preferred in preferred_sheets:
            if preferred in xls.sheet_names:
                sheet_name = preferred
                break
        
        if sheet_name is None:
            # Use first sheet
            sheet_name = xls.sheet_names[0]
            print(f"⚠️ Using first sheet: {sheet_name}")
        else:
            print(f"✅ Using sheet: {sheet_name}")
        
        # Read the data
        df = pd.read_excel(input_file, sheet_name=sheet_name)
        print(f"✅ Loaded {len(df)} rows")
        print(f"📋 Columns: {len(df.columns)}")
        print(f"   {list(df.columns)}")
        
        # Check for required columns
        has_regular = any('regular' in col.lower() and 'count' in col.lower() for col in df.columns)
        has_consecutive = any('consecutive' in col.lower() and 'count' in col.lower() for col in df.columns)
        
        if not has_regular or not has_consecutive:
            print(f"⚠️ Warning: Could not find Regular_Count and/or Consecutive_Count columns")
            print(f"   Will try to proceed with available columns")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """
    Main function to run the table grouping process.
    """
    try:
        # Get input file from user
        input_file = get_input_file()
        
        # Get output file from user
        output_file = get_output_file(input_file)
        
        print("\n" + "="*60)
        print("🚀 STARTING TABLE GROUPING PROCESS")
        print("="*60)
        
        # Load the data
        df = load_data(input_file)
        
        if df is None or df.empty:
            print("❌ Could not load data or data is empty")
            return
        
        # Group into tables
        tables = group_into_tables(df)
        
        if not tables:
            print("❌ No tables were created")
            return
        
        # Analyze table columns
        analyze_table_columns(tables)
        
        # Save tables to Excel
        save_tables_to_excel(tables, output_file)
        
        print("\n" + "="*60)
        print("🎉 TABLE GROUPING COMPLETE!")
        print("="*60)
        print(f"✅ Created {len(tables)} tables")
        print(f"📂 Output saved to: {output_file}")
        print("\n📋 Output sheets:")
        print("   1. All_Tables - Combined view of all tables")
        print("   2. Table_1, Table_2, ... - Individual table sheets")
        print("   3. Table_Statistics - Summary statistics")
        print("   4. Table_Structures - JSON structure details")
        
        # Offer to open the file
        if sys.platform == 'win32':
            open_file = input("\n📂 Open the output file? (y/n): ").strip().lower()
            if open_file == 'y':
                os.startfile(output_file)
        elif sys.platform == 'darwin':  # macOS
            open_file = input("\n📂 Open the output file? (y/n): ").strip().lower()
            if open_file == 'y':
                os.system(f'open "{output_file}"')
        else:  # Linux
            open_file = input("\n📂 Open the output file? (y/n): ").strip().lower()
            if open_file == 'y':
                os.system(f'xdg-open "{output_file}"')
        
        # Ask if user wants to process another file
        print("\n" + "-"*40)
        another = input("🔄 Process another file? (y/n): ").strip().lower()
        if another == 'y':
            main()  # Recursively call main for another file
        else:
            print("👋 Goodbye!")
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user")
        print("👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

# =========================
# RUN THE SCRIPT
# =========================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("📊 TABLE GROUPING TOOL")
    print("="*60)
    print("Groups consecutive rows into tables where:")
    print("  - Regular_Count == Consecutive_Count")
    print("  - Consecutive rows have SAME count values")
    print()
    print("This identifies structured financial tables in the data.")
    print("="*60)
    
    main()
