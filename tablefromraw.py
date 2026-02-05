# =============================================================================
# DATAIKU TABLE GROUPING TOOL
# =============================================================================
# Groups rows into tables based on consecutive rows having the same 
# Regular_Count and Consecutive_Count values.
# Reads Excel files from Dataiku folders and saves results to Dataiku folders.
# =============================================================================

import dataiku
import pandas as pd
import io
import json
from typing import List, Dict, Tuple, Optional

# =============================================================================
# CONFIGURATION - SET THESE TO YOUR DATAIKU FOLDER IDs
# =============================================================================
INPUT_FOLDER_ID = "xFGhJtYE"          # Your Dataiku INPUT folder ID
OUTPUT_FOLDER_ID = "output_folder_id" # Your Dataiku OUTPUT folder ID

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

def read_excel_from_dataiku(filename: str) -> pd.DataFrame:
    """
    Read Excel file from Dataiku folder
    
    Args:
        filename: Name of Excel file in Dataiku folder
        
    Returns:
        DataFrame with loaded data
    """
    folder = get_input_folder()
    
    print(f"📥 Reading Excel file from Dataiku: {filename}")
    
    try:
        # Read file from Dataiku
        with folder.get_download_stream(filename) as stream:
            excel_bytes = stream.read()
        
        print(f"   ✅ Read {len(excel_bytes):,} bytes")
        
        # Convert bytes to DataFrame
        excel_file = io.BytesIO(excel_bytes)
        
        # Read Excel file
        df = pd.read_excel(excel_file)
        
        print(f"   ✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
        
        return df
        
    except Exception as e:
        print(f"❌ Error reading file {filename}: {e}")
        raise

def save_excel_to_dataiku(df: pd.DataFrame, filename: str) -> None:
    """
    Save DataFrame to Excel in Dataiku output folder
    
    Args:
        df: DataFrame to save
        filename: Output filename
    """
    folder = get_output_folder()
    
    try:
        # Create Excel in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        
        # Get the bytes
        excel_bytes = output.getvalue()
        
        # Save to Dataiku folder
        with folder.get_writer(filename) as writer:
            writer.write(excel_bytes)
        
        print(f"✅ Saved to Dataiku: {filename}")
        print(f"   Rows: {len(df):,}, Columns: {len(df.columns)}")
        
    except Exception as e:
        print(f"❌ Error saving file {filename}: {e}")
        raise

def save_tables_to_dataiku(tables: List[pd.DataFrame], output_filename: str, 
                           include_individual_sheets: bool = True) -> None:
    """
    Save all tables to an Excel file in Dataiku folder
    
    Args:
        tables: List of table DataFrames
        output_filename: Output Excel filename
        include_individual_sheets: Whether to create individual sheets for each table
    """
    if not tables:
        print("❌ No tables to save")
        return
    
    print(f"\n💾 Saving tables to Dataiku folder: {output_filename}")
    
    try:
        # Create Excel in memory
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Save combined view
            combined_data = []
            for i, table in enumerate(tables, 1):
                table_copy = table.copy()
                table_copy.insert(0, 'Table_Number', i)
                combined_data.append(table_copy)
            
            combined_df = pd.concat(combined_data, ignore_index=True)
            combined_df.to_excel(writer, sheet_name='All_Tables', index=False)
            print(f"   ✅ Saved combined view: {len(combined_df):,} rows")
            
            if include_individual_sheets:
                # Save each table to individual sheet
                for i, table in enumerate(tables, 1):
                    sheet_name = f"Table_{i}"
                    # Excel sheet names max 31 characters
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                    
                    # Add row numbers within table
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
        
        # Save to Dataiku folder
        excel_bytes = output.getvalue()
        folder = get_output_folder()
        
        with folder.get_writer(output_filename) as writer:
            writer.write(excel_bytes)
        
        print(f"\n🎉 Successfully saved {len(tables)} tables to Dataiku")
        print(f"   File: {output_filename}")
        
    except Exception as e:
        print(f"❌ Error saving tables: {e}")
        raise

# =============================================================================
# CORE GROUPING FUNCTIONS
# =============================================================================

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
        col_lower = str(col).lower()
        if 'regular' in col_lower and 'count' in col_lower:
            regular_col = col
        elif 'consecutive' in col_lower and 'count' in col_lower:
            consecutive_col = col
    
    if not regular_col or not consecutive_col:
        print(f"❌ Could not find Regular_Count and Consecutive_Count columns")
        print(f"   Available columns: {list(df.columns)}")
        return []
    
    print(f"\n🔍 Using columns:")
    print(f"   Regular_Count: '{regular_col}'")
    print(f"   Consecutive_Count: '{consecutive_col}'")
    
    # Reset index for clean iteration
    df = df.reset_index(drop=True)
    tables = []
    current_table = []
    current_counts = None
    table_number = 1
    
    print(f"\n📊 Starting table grouping...")
    print(f"   Total rows to process: {len(df):,}")
    
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
    print(f"\n📋 Table Summary:")
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
        
        print(f"   Table {i}: {len(table):,} rows | Counts: {reg_count}/{cons_count}{section_info}")
    
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
        col_lower = str(col).lower()
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
                label = str(first_row['Label'])[:50]
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

# =============================================================================
# BATCH PROCESSING FUNCTIONS
# =============================================================================

def batch_process_all_files():
    """
    Process all Excel files in the Dataiku input folder
    """
    print(f"\n{'='*60}")
    print("DATAIKU TABLE GROUPING TOOL - BATCH PROCESSING")
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
            # Read Excel from Dataiku
            df = read_excel_from_dataiku(filename)
            
            if df.empty:
                print(f"   ⚠️ File is empty, skipping...")
                continue
            
            # Group into tables
            tables = group_into_tables(df)
            
            if not tables:
                print(f"   ⚠️ No tables created, skipping...")
                continue
            
            # Analyze table columns
            analyze_table_columns(tables)
            
            # Save to Dataiku
            output_filename = filename.replace('.xlsx', '_grouped.xlsx').replace('.xls', '_grouped.xlsx')
            save_tables_to_dataiku(tables, output_filename)
            
            results.append({
                'input_file': filename,
                'output_file': output_filename,
                'table_count': len(tables),
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
    
    print(f"📊 Results:")
    print(f"   Total files processed: {len(results)}")
    print(f"   Successfully grouped: {len(successful)}")
    print(f"   Errors: {len(errors)}")
    
    if successful:
        total_tables = sum(r['table_count'] for r in successful)
        print(f"\n✅ Created {total_tables} total tables")
        
        print(f"\n📁 Output files created:")
        for result in successful:
            print(f"   • {result['output_file']} ({result['table_count']} tables)")
    
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
    print("DATAIKU TABLE GROUPING - INTERACTIVE MODE")
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
                    # Read Excel from Dataiku
                    df = read_excel_from_dataiku(selected_file)
                    
                    if df.empty:
                        print(f"❌ File is empty")
                        break
                    
                    # Group into tables
                    tables = group_into_tables(df)
                    
                    if not tables:
                        print(f"❌ No tables created")
                        break
                    
                    # Analyze table columns
                    analyze_table_columns(tables)
                    
                    # Ask for output filename
                    default_output = selected_file.replace('.xlsx', '_grouped.xlsx').replace('.xls', '_grouped.xlsx')
                    output_choice = input(f"\n💾 Output filename [Enter for '{default_output}']: ").strip()
                    
                    if not output_choice:
                        output_choice = default_output
                    
                    # Save to Dataiku
                    save_tables_to_dataiku(tables, output_choice)
                    
                    print(f"\n✅ Processing complete!")
                    print(f"   Created {len(tables)} tables")
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
    Main function to run the Dataiku table grouping tool
    """
    print(f"\n{'='*60}")
    print("DATAIKU TABLE GROUPING TOOL")
    print(f"{'='*60}")
    print("Groups consecutive rows into tables where:")
    print("  • Regular_Count == Consecutive_Count")
    print("  • Consecutive rows have SAME count values")
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

def simple_dataiku_table_group():
    """
    Simple version for basic table grouping
    """
    # CONFIGURATION
    INPUT_FOLDER = dataiku.Folder("xFGhJtYE")      # Your input folder
    OUTPUT_FOLDER = dataiku.Folder("output_folder_id")  # Your output folder
    
    print("Starting Dataiku table grouping...")
    
    # List Excel files
    all_files = INPUT_FOLDER.list_paths_in_partition()
    excel_files = [f for f in all_files if f.lower().endswith('.xlsx')]
    
    if not excel_files:
        print("No Excel files found!")
        return
    
    for filename in excel_files:
        print(f"\nProcessing: {filename}")
        
        try:
            # 1. READ FROM DATAIKU
            with INPUT_FOLDER.get_download_stream(filename) as stream:
                excel_bytes = stream.read()
            
            # 2. LOAD DATAFRAME
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(f"  Read {len(df)} rows")
            
            # 3. FIND COLUMNS
            reg_col = None
            cons_col = None
            
            for col in df.columns:
                col_str = str(col).lower()
                if 'regular' in col_str and 'count' in col_str:
                    reg_col = col
                if 'consecutive' in col_str and 'count' in col_str:
                    cons_col = col
            
            if not reg_col or not cons_col:
                print(f"  ❌ Columns not found. Skipping...")
                continue
            
            # 4. GROUP INTO TABLES
            tables = []
            current_table = []
            current_counts = None
            
            for idx, row in df.iterrows():
                current_pair = (row[reg_col], row[cons_col])
                
                if current_counts is None:
                    current_counts = current_pair
                    current_table.append(row)
                elif current_pair == current_counts:
                    current_table.append(row)
                else:
                    if current_table:
                        tables.append(pd.DataFrame(current_table))
                    current_counts = current_pair
                    current_table = [row]
            
            if current_table:
                tables.append(pd.DataFrame(current_table))
            
            print(f"  ✅ Created {len(tables)} tables")
            
            # 5. SAVE TO DATAIKU
            if tables:
                output_file = filename.replace('.xlsx', '_grouped.xlsx')
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    # Combined view
                    combined_data = []
                    for i, table in enumerate(tables, 1):
                        table_copy = table.copy()
                        table_copy.insert(0, 'Table_Number', i)
                        combined_data.append(table_copy)
                    
                    pd.concat(combined_data).to_excel(writer, sheet_name='All_Tables', index=False)
                
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
    # simple_dataiku_table_group()
