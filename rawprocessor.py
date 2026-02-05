# =============================================================================
# DATAIKU RAW EXTRACTION FILTER
# =============================================================================
# This recipe filters raw extraction data from Excel files in Dataiku folders
# Filter criteria:
#   1. Regular_Count == Consecutive_Count
#   2. Regular_Count > 2 (excludes counts ≤ 2)
# =============================================================================

import dataiku
import pandas as pd
import io
from typing import Tuple, Optional, Dict, List
import re
from datetime import datetime

# =============================================================================
# CONFIGURATION - SET THESE TO YOUR DATAIKU FOLDER IDs
# =============================================================================
INPUT_FOLDER_ID = "xFGhJtYE"          # Replace with your input folder ID
OUTPUT_FOLDER_ID = "output_folder_id" # Replace with your output folder ID

# Optional: File patterns to process (comma-separated)
FILE_PATTERNS = ["*raw*.xlsx", "*extraction*.xlsx", "*.xlsx"]

# Optional: Output filename suffix
OUTPUT_SUFFIX = "_filtered_no2"

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
    List all Excel files in the input folder matching patterns
    
    Returns:
        List of Excel filenames
    """
    folder = get_input_folder()
    all_files = folder.list_paths_in_partition()
    
    excel_files = []
    patterns = [p.strip() for p in FILE_PATTERNS]
    
    import fnmatch
    for pattern in patterns:
        for file in all_files:
            if fnmatch.fnmatch(file.lower(), pattern.lower()):
                if file not in excel_files:
                    excel_files.append(file)
    
    return sorted(excel_files)

def read_excel_from_dataiku(filename: str) -> pd.DataFrame:
    """
    Read Excel file from Dataiku folder and detect sheet names
    
    Returns:
        Tuple of (DataFrame, sheet_name, sheet_names_list)
    """
    folder = get_input_folder()
    
    try:
        print(f"📄 Reading Excel file from Dataiku: {filename}")
        
        # Read the entire file as bytes
        with folder.get_download_stream(filename) as stream:
            excel_bytes = stream.read()
        
        print(f"   Read {len(excel_bytes):,} bytes")
        
        # Use BytesIO to work with pandas
        excel_file = io.BytesIO(excel_bytes)
        
        # Get sheet names
        xls = pd.ExcelFile(excel_file)
        sheet_names = xls.sheet_names
        
        print(f"   Found {len(sheet_names)} sheet(s): {sheet_names}")
        
        # Reset file pointer
        excel_file.seek(0)
        
        return excel_file, sheet_names
        
    except Exception as e:
        print(f"❌ Error reading Excel file {filename}: {e}")
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
            df.to_excel(writer, index=False, sheet_name='Filtered_Data')
        
        # Get the bytes
        excel_bytes = output.getvalue()
        
        # Save to Dataiku folder
        with folder.get_writer(filename) as writer:
            writer.write(excel_bytes)
        
        print(f"✅ Saved filtered data to: {filename}")
        print(f"   Rows: {len(df):,}, Columns: {len(df.columns)}")
        
    except Exception as e:
        print(f"❌ Error saving Excel file {filename}: {e}")
        raise

# =============================================================================
# CORE FILTERING FUNCTION
# =============================================================================

def filter_raw_extraction(input_filename: str, output_filename: Optional[str] = None) -> Tuple[pd.DataFrame, int]:
    """
    Filter raw extraction data where:
    1. Regular_Count == Consecutive_Count
    2. Regular_Count > 2 (excludes counts ≤ 2)
    
    Args:
        input_filename: Name of Excel file in Dataiku input folder
        output_filename: Name for output file (if None, auto-generates)
    
    Returns:
        Tuple of (filtered DataFrame, number of filtered rows)
    """
    
    print(f"\n🎯 Processing file: {input_filename}")
    print("-" * 50)
    
    try:
        # Read Excel from Dataiku
        excel_file, sheet_names = read_excel_from_dataiku(input_filename)
        
        # Find the raw extraction sheet
        sheet_name = None
        for sheet in sheet_names:
            if 'raw' in sheet.lower() or 'Raw_Extraction' in sheet:
                sheet_name = sheet
                break
        
        if sheet_name is None:
            # Try to read the first sheet
            sheet_name = sheet_names[0]
            print(f"⚠️ No raw extraction sheet found, using first sheet: {sheet_name}")
        
        # Read the data
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        print(f"✅ Loaded {len(df):,} rows from sheet: '{sheet_name}'")
        
        # Display available columns
        print(f"\n📋 Available columns ({len(df.columns)} total):")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i:2d}. {col}")
        
        # Check for required columns (handle case-insensitive)
        column_map = {}
        required_columns = ['regular_count', 'consecutive_count']
        
        for col in df.columns:
            col_lower = col.lower().replace('_', '').replace(' ', '')
            if 'regular' in col_lower and 'count' in col_lower:
                column_map['regular_count'] = col
            elif 'consecutive' in col_lower and 'count' in col_lower:
                column_map['consecutive_count'] = col
        
        # If not found with pattern matching, try exact matches
        if 'regular_count' not in column_map:
            for col in df.columns:
                col_clean = col.lower().replace('_', '').replace(' ', '')
                if col_clean == 'regularcount':
                    column_map['regular_count'] = col
                    break
        
        if 'consecutive_count' not in column_map:
            for col in df.columns:
                col_clean = col.lower().replace('_', '').replace(' ', '')
                if col_clean == 'consecutivecount':
                    column_map['consecutive_count'] = col
                    break
        
        if 'regular_count' not in column_map or 'consecutive_count' not in column_map:
            print(f"\n❌ Required columns not found!")
            print(f"   Looking for columns containing 'regular count' and 'consecutive count'")
            print(f"   Column mapping attempted: {column_map}")
            
            # Show what we found
            print(f"\n💡 Similar columns found:")
            for col in df.columns:
                col_lower = col.lower()
                if 'regular' in col_lower or 'consecutive' in col_lower:
                    print(f"   - '{col}'")
            
            print("\n❓ Possible solutions:")
            print("   1. Check column names in your Excel file")
            print("   2. Rename columns to include 'Regular_Count' and 'Consecutive_Count'")
            print("   3. Modify the code to match your column names")
            
            return pd.DataFrame(), 0
        
        # Extract the actual column names
        regular_col = column_map['regular_count']
        consecutive_col = column_map['consecutive_count']
        
        print(f"\n🔍 Using columns:")
        print(f"   Regular count: '{regular_col}'")
        print(f"   Consecutive count: '{consecutive_col}'")
        
        # Ensure columns are numeric
        df[regular_col] = pd.to_numeric(df[regular_col], errors='coerce')
        df[consecutive_col] = pd.to_numeric(df[consecutive_col], errors='coerce')
        
        # Display sample values
        print(f"\n📊 Data statistics before filtering:")
        print(f"   Regular count range: {df[regular_col].min():.0f} to {df[regular_col].max():.0f}")
        print(f"   Consecutive count range: {df[consecutive_col].min():.0f} to {df[consecutive_col].max():.0f}")
        print(f"   Null values in Regular count: {df[regular_col].isna().sum()}")
        print(f"   Null values in Consecutive count: {df[consecutive_col].isna().sum()}")
        
        # Count rows by Regular_Count
        reg_counts = df[regular_col].value_counts().sort_index()
        print(f"\n📈 Distribution by Regular_Count:")
        for count, freq in reg_counts.items():
            if pd.notna(count):
                print(f"   Regular_Count = {int(count):>3}: {freq:>6} rows")
        
        # Apply filters
        print(f"\n🎯 Applying filters:")
        print(f"   1. Regular_Count == Consecutive_Count")
        print(f"   2. Regular_Count > 2 (excludes counts ≤ 2)")
        
        # Filter 1: Regular_Count == Consecutive_Count
        filter1_mask = df[regular_col] == df[consecutive_col]
        df_filter1 = df[filter1_mask].copy()
        print(f"   ✓ Filter 1 matched: {len(df_filter1):,} rows")
        
        # Filter 2: Regular_Count > 2
        filter2_mask = df_filter1[regular_col] > 2
        df_filtered = df_filter1[filter2_mask].copy()
        print(f"   ✓ Filter 2 matched: {len(df_filtered):,} rows")
        
        # Show filtering breakdown
        print(f"\n📊 Detailed filtering breakdown:")
        print(f"   Total rows: {len(df):,}")
        print(f"   After Regular_Count == Consecutive_Count: {len(df_filter1):,}")
        print(f"   After Regular_Count > 2: {len(df_filtered):,}")
        
        # Show what was filtered out by count
        print(f"\n📈 Rows filtered out by count value:")
        for count in [0, 1, 2]:
            count_rows = len(df[df[regular_col] == count])
            if count_rows > 0:
                # Count how many of these had Regular_Count == Consecutive_Count
                equal_rows = len(df[(df[regular_col] == count) & (df[regular_col] == df[consecutive_col])])
                print(f"   Count = {count}: {count_rows:,} total, {equal_rows:,} had Regular==Consecutive")
        
        if len(df_filtered) == 0:
            print(f"\n⚠️ No rows matched all filters!")
            
            # Show diagnostic information
            print(f"\n🔍 Diagnostic information:")
            
            # Check if any rows have Regular_Count > 2
            gt2_rows = len(df[df[regular_col] > 2])
            print(f"   Rows with Regular_Count > 2: {gt2_rows:,}")
            
            # Check if any rows have Regular_Count == Consecutive_Count
            equal_rows = len(df[df[regular_col] == df[consecutive_col]])
            print(f"   Rows with Regular_Count == Consecutive_Count: {equal_rows:,}")
            
            # Check overlap
            overlap = len(df[(df[regular_col] > 2) & (df[regular_col] == df[consecutive_col])])
            print(f"   Rows satisfying BOTH conditions: {overlap:,}")
            
            return pd.DataFrame(), 0
        
        # Show statistics
        print(f"\n📊 Filter statistics:")
        print(f"   Total rows: {len(df):,}")
        print(f"   Rows filtered out: {len(df) - len(df_filtered):,}")
        print(f"   Rows kept: {len(df_filtered):,} ({len(df_filtered)/len(df)*100:.1f}%)")
        
        # Show distribution of kept rows
        print(f"\n📈 Kept rows by Regular_Count:")
        kept_counts = df_filtered[regular_col].value_counts().sort_index()
        for count, freq in kept_counts.items():
            print(f"   Regular_Count = {int(count):>3}: {freq:>6} rows")
        
        # Auto-generate output filename if not provided
        if output_filename is None:
            base_name = input_filename.rsplit('.', 1)[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{base_name}{OUTPUT_SUFFIX}_{timestamp}.xlsx"
        
        # Save to Dataiku output folder
        save_excel_to_dataiku(df_filtered, output_filename)
        
        # Show sample of filtered data
        print(f"\n📝 Sample of filtered data (first 3 rows):")
        
        # Select columns to display
        display_cols = [regular_col, consecutive_col]
        
        # Add other interesting columns if available
        interesting_cols = ['Page', 'Label', 'Raw_Line', 'Regular_Numbers', 'Text', 'Table']
        for col in interesting_cols:
            for df_col in df_filtered.columns:
                if col.lower() in df_col.lower():
                    if df_col not in display_cols:
                        display_cols.append(df_col)
        
        # Limit to 5 columns for display
        display_cols = display_cols[:5]
        
        sample_df = df_filtered[display_cols].head(3)
        for idx, row in sample_df.iterrows():
            print(f"\n   Row {idx}:")
            for col in display_cols:
                value = row[col]
                if pd.isna(value):
                    display_value = "[NaN]"
                elif isinstance(value, float):
                    display_value = f"{value:.2f}"
                else:
                    display_value = str(value)
                print(f"     {col}: {display_value[:50]}")
        
        return df_filtered, len(df_filtered)
        
    except Exception as e:
        print(f"\n❌ Error processing file {input_filename}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), 0

# =============================================================================
# BATCH PROCESSING FUNCTIONS
# =============================================================================

def process_single_file(input_filename: str, output_filename: Optional[str] = None) -> Dict:
    """
    Process a single file and return results summary
    
    Returns:
        Dictionary with processing results
    """
    print(f"\n{'='*60}")
    print(f"🚀 PROCESSING: {input_filename}")
    print(f"{'='*60}")
    
    result = {
        'input_file': input_filename,
        'output_file': '',
        'total_rows': 0,
        'filtered_rows': 0,
        'status': 'pending',
        'error': None
    }
    
    try:
        df_filtered, filtered_count = filter_raw_extraction(input_filename, output_filename)
        
        if filtered_count > 0:
            result['status'] = 'success'
            result['filtered_rows'] = filtered_count
            result['total_rows'] = len(df_filtered)
            
            if output_filename:
                result['output_file'] = output_filename
            else:
                # Generate output filename
                base_name = input_filename.rsplit('.', 1)[0]
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                result['output_file'] = f"{base_name}{OUTPUT_SUFFIX}_{timestamp}.xlsx"
        else:
            result['status'] = 'no_match'
            result['error'] = 'No rows matched filter criteria'
    
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
    
    return result

def batch_process_all_files() -> List[Dict]:
    """
    Process all Excel files in the input folder
    
    Returns:
        List of processing results
    """
    print(f"\n{'='*60}")
    print("DATAIKU RAW EXTRACTION FILTER - BATCH PROCESSING")
    print(f"{'='*60}")
    print(f"Input folder:  {INPUT_FOLDER_ID}")
    print(f"Output folder: {OUTPUT_FOLDER_ID}")
    print(f"{'='*60}")
    
    # List all Excel files
    excel_files = list_excel_files_in_folder()
    
    if not excel_files:
        print("❌ No Excel files found in input folder.")
        print(f"   Folder ID: {INPUT_FOLDER_ID}")
        print(f"   Patterns: {FILE_PATTERNS}")
        return []
    
    print(f"📁 Found {len(excel_files)} Excel file(s):")
    for i, filename in enumerate(excel_files, 1):
        print(f"   {i}. {filename}")
    
    print(f"\n🎯 Starting batch processing...")
    
    # Process each file
    results = []
    for filename in excel_files:
        result = process_single_file(filename)
        results.append(result)
        
        # Add separator between files
        if filename != excel_files[-1]:
            print(f"\n{'-'*60}")
    
    # Generate summary
    print(f"\n{'='*60}")
    print("BATCH PROCESSING SUMMARY")
    print(f"{'='*60}")
    
    successful = [r for r in results if r['status'] == 'success']
    no_match = [r for r in results if r['status'] == 'no_match']
    errors = [r for r in results if r['status'] == 'error']
    
    print(f"📊 Results:")
    print(f"   Total files processed: {len(results)}")
    print(f"   Successfully filtered: {len(successful)}")
    print(f"   No matches found: {len(no_match)}")
    print(f"   Errors: {len(errors)}")
    
    if successful:
        total_filtered = sum(r['filtered_rows'] for r in successful)
        print(f"\n✅ Successfully filtered {total_filtered:,} total rows")
        
        print(f"\n📁 Output files created:")
        for result in successful:
            print(f"   • {result['output_file']} ({result['filtered_rows']:,} rows)")
    
    if no_match:
        print(f"\n⚠️ Files with no matching rows:")
        for result in no_match:
            print(f"   • {result['input_file']}")
    
    if errors:
        print(f"\n❌ Files with errors:")
        for result in errors:
            print(f"   • {result['input_file']}: {result['error']}")
    
    # Save summary to CSV
    if results:
        summary_df = pd.DataFrame([
            {
                'input_file': r['input_file'],
                'output_file': r.get('output_file', ''),
                'status': r['status'],
                'filtered_rows': r.get('filtered_rows', 0),
                'error': r.get('error', '')
            }
            for r in results
        ])
        
        # Save summary to Dataiku output folder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_filename = f"batch_processing_summary_{timestamp}.csv"
        
        folder = get_output_folder()
        with folder.get_writer(summary_filename) as writer:
            summary_df.to_csv(writer, index=False)
        
        print(f"\n📋 Summary saved to: {summary_filename}")
    
    return results

# =============================================================================
# INTERACTIVE MODE FUNCTIONS
# =============================================================================

def interactive_mode():
    """
    Interactive mode for selecting and processing files
    """
    print(f"\n{'='*60}")
    print("DATAIKU RAW EXTRACTION FILTER - INTERACTIVE MODE")
    print(f"{'='*60}")
    print("Filter criteria:")
    print("  1. Regular_Count == Consecutive_Count")
    print("  2. Regular_Count > 2 (excludes counts ≤ 2)")
    print(f"{'='*60}")
    
    # List available files
    excel_files = list_excel_files_in_folder()
    
    if not excel_files:
        print("❌ No Excel files found in input folder.")
        print(f"   Please upload files to folder: {INPUT_FOLDER_ID}")
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
                
                # Ask for output filename
                base_name = selected_file.rsplit('.', 1)[0]
                default_output = f"{base_name}{OUTPUT_SUFFIX}.xlsx"
                
                output_choice = input(f"Output filename [Enter for '{default_output}']: ").strip()
                if not output_choice:
                    output_choice = default_output
                
                # Process the file
                process_single_file(selected_file, output_choice)
                break
            else:
                print(f"❌ Please enter a number between 1 and {len(excel_files)}")
        
        else:
            print("❌ Invalid choice. Please enter a number, 'all', or 'q'")

# =============================================================================
# MAIN EXECUTION FUNCTION
# =============================================================================

def main():
    """
    Main function to run the Dataiku raw extraction filter
    """
    print(f"\n{'='*60}")
    print("DATAIKU RAW EXTRACTION FILTER")
    print(f"{'='*60}")
    print("Purpose: Filter raw extraction Excel files where:")
    print("  • Regular_Count == Consecutive_Count")
    print("  • Regular_Count > 2 (excludes counts ≤ 2)")
    print(f"\nConfiguration:")
    print(f"  Input folder:  {INPUT_FOLDER_ID}")
    print(f"  Output folder: {OUTPUT_FOLDER_ID}")
    print(f"{'='*60}")
    
    try:
        # Check if pandas is available
        try:
            import pandas as pd
        except ImportError:
            print("❌ ERROR: pandas is not installed.")
            print("   Please add 'pandas' to your Dataiku code environment packages.")
            return
        
        # Check if openpyxl is available (needed for Excel writing)
        try:
            import openpyxl
        except ImportError:
            print("❌ ERROR: openpyxl is not installed.")
            print("   Please add 'openpyxl' to your Dataiku code environment packages.")
            return
        
        # Run in batch mode by default
        # Change this to interactive_mode() if you want interactive selection
        results = batch_process_all_files()
        
        if results:
            print(f"\n{'='*60}")
            print("✅ PROCESSING COMPLETE!")
            print(f"{'='*60}")
            
            # Check if any processing was successful
            successful = any(r['status'] == 'success' for r in results)
            if successful:
                print(f"📁 Check the output folder '{OUTPUT_FOLDER_ID}' for results.")
            else:
                print(f"⚠️ No files were successfully processed.")
                print(f"   Please check your input data and filter criteria.")
        
        else:
            print(f"\n⚠️ No files were processed.")
            print(f"   Please check that Excel files exist in folder '{INPUT_FOLDER_ID}'")
    
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

# =============================================================================
# SIMPLE VERSION (Minimal Code)
# =============================================================================

def simple_filter():
    """
    Simple version for basic filtering - minimal dependencies
    """
    import dataiku
    import pandas as pd
    import io
    
    # Configuration - CHANGE THESE!
    INPUT_FOLDER = dataiku.Folder("xFGhJtYE")  # Your input folder ID
    OUTPUT_FOLDER = dataiku.Folder("output_folder_id")  # Your output folder ID
    
    print("Starting simple raw extraction filter...")
    
    # List Excel files
    all_files = INPUT_FOLDER.list_paths_in_partition()
    excel_files = [f for f in all_files if f.lower().endswith(('.xlsx', '.xls'))]
    
    if not excel_files:
        print("No Excel files found!")
        return
    
    print(f"Found {len(excel_files)} Excel file(s)")
    
    for input_file in excel_files:
        print(f"\nProcessing: {input_file}")
        
        try:
            # Read from Dataiku
            with INPUT_FOLDER.get_download_stream(input_file) as stream:
                excel_bytes = stream.read()
            
            # Read Excel
            df = pd.read_excel(io.BytesIO(excel_bytes))
            
            # Find regular_count and consecutive_count columns (case-insensitive)
            reg_col = None
            cons_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if 'regular' in col_lower and 'count' in col_lower:
                    reg_col = col
                elif 'consecutive' in col_lower and 'count' in col_lower:
                    cons_col = col
            
            if not reg_col or not cons_col:
                print(f"  ❌ Required columns not found in {input_file}")
                continue
            
            # Apply filters
            mask = (df[reg_col] == df[cons_col]) & (df[reg_col] > 2)
            df_filtered = df[mask].copy()
            
            print(f"  ✓ Filtered: {len(df_filtered)}/{len(df)} rows kept")
            
            if len(df_filtered) > 0:
                # Save to Dataiku
                output_file = input_file.replace('.xlsx', '_filtered.xlsx').replace('.xls', '_filtered.xlsx')
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_filtered.to_excel(writer, index=False)
                
                with OUTPUT_FOLDER.get_writer(output_file) as writer:
                    writer.write(output.getvalue())
                
                print(f"  ✅ Saved: {output_file}")
        
        except Exception as e:
            print(f"  ❌ Error processing {input_file}: {e}")

# =============================================================================
# EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Run the main conversion (recommended)
    main()
    
    # Alternatively, run the simple version:
    # simple_filter()
