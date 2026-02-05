import pandas as pd
import os
import sys
import glob
from typing import Tuple, Optional

def filter_raw_extraction(input_file: str, output_file: Optional[str] = None) -> Tuple[pd.DataFrame, int]:
    """
    Filter raw extraction data where:
    1. Regular_Count == Consecutive_Count
    2. Regular_Count > 2 (excludes counts ≤ 2)
    
    Args:
        input_file: Path to the Excel file containing raw extraction
        output_file: Path to save filtered output (if None, auto-generates)
    
    Returns:
        Tuple of (filtered DataFrame, number of filtered rows)
    """
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return pd.DataFrame(), 0
    
    # Auto-generate output filename if not provided
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        output_file = base_name + '_filtered.xlsx'
    
    print(f"📄 Reading input file: {input_file}")
    
    try:
        # Read the Excel file
        xls = pd.ExcelFile(input_file)
        
        # Find the raw extraction sheet
        sheet_name = None
        for sheet in xls.sheet_names:
            if 'raw' in sheet.lower() or 'Raw_Extraction' in sheet:
                sheet_name = sheet
                break
        
        if sheet_name is None:
            # Try to read the first sheet
            sheet_name = xls.sheet_names[0]
            print(f"⚠️ No raw extraction sheet found, using first sheet: {sheet_name}")
        
        # Read the data
        df = pd.read_excel(input_file, sheet_name=sheet_name)
        print(f"✅ Loaded {len(df)} rows from sheet: {sheet_name}")
        
        # Display available columns
        print(f"\n📋 Available columns: {list(df.columns)}")
        
        # Check for required columns (handle case-insensitive)
        column_map = {}
        required_columns = ['regular_count', 'consecutive_count']
        
        for col in df.columns:
            col_lower = col.lower()
            if 'regular' in col_lower and 'count' in col_lower:
                column_map['regular_count'] = col
            elif 'consecutive' in col_lower and 'count' in col_lower:
                column_map['consecutive_count'] = col
        
        # If not found with pattern matching, try exact matches
        if 'regular_count' not in column_map:
            for col in df.columns:
                if col.lower() == 'regular_count' or col == 'Regular_Count':
                    column_map['regular_count'] = col
                    break
        
        if 'consecutive_count' not in column_map:
            for col in df.columns:
                if col.lower() == 'consecutive_count' or col == 'Consecutive_Count':
                    column_map['consecutive_count'] = col
                    break
        
        if 'regular_count' not in column_map or 'consecutive_count' not in column_map:
            print(f"\n❌ Required columns not found!")
            print(f"   Looking for columns containing 'regular count' and 'consecutive count'")
            print(f"   Found columns: {list(df.columns)}")
            print(f"   Column mapping: {column_map}")
            
            # Try to find similar columns
            similar_cols = {}
            for col in df.columns:
                col_lower = col.lower()
                if 'regular' in col_lower:
                    similar_cols['regular'] = col
                if 'consecutive' in col_lower:
                    similar_cols['consecutive'] = col
            
            if similar_cols:
                print(f"\n💡 Similar columns found: {similar_cols}")
                print("   Please check the column names in your Excel file.")
            
            return pd.DataFrame(), 0
        
        # Extract the actual column names
        regular_col = column_map['regular_count']
        consecutive_col = column_map['consecutive_count']
        
        print(f"\n🔍 Using columns:")
        print(f"   Regular count: '{regular_col}'")
        print(f"   Consecutive count: '{consecutive_col}'")
        
        # Display sample values
        print(f"\n📊 Sample values before filtering:")
        print(f"   Regular count range: {df[regular_col].min()} to {df[regular_col].max()}")
        print(f"   Consecutive count range: {df[consecutive_col].min()} to {df[consecutive_col].max()}")
        
        # Count rows by Regular_Count
        reg_counts = df[regular_col].value_counts().sort_index()
        print(f"\n📈 Distribution by Regular_Count:")
        for count, freq in reg_counts.items():
            print(f"   Regular_Count = {count}: {freq} rows")
        
        # Apply filters
        print(f"\n🎯 Applying filters:")
        print(f"   1. Regular_Count == Consecutive_Count")
        print(f"   2. Regular_Count > 2 (excludes counts ≤ 2)")
        
        # Filter 1: Regular_Count == Consecutive_Count
        filter1_mask = df[regular_col] == df[consecutive_col]
        df_filter1 = df[filter1_mask].copy()
        print(f"   ✓ Filter 1 matched: {len(df_filter1)} rows")
        
        # Filter 2: Regular_Count > 2 (changed from > 1)
        filter2_mask = df_filter1[regular_col] > 2
        df_filtered = df_filter1[filter2_mask].copy()
        print(f"   ✓ Filter 2 matched: {len(df_filtered)} rows")
        
        # Show what was filtered out by each condition
        print(f"\n📊 Detailed filtering breakdown:")
        print(f"   Total rows: {len(df)}")
        
        # Count rows with Regular_Count == Consecutive_Count
        equal_count = len(df_filter1)
        print(f"   Rows with Regular_Count == Consecutive_Count: {equal_count}")
        
        # Count rows by specific counts that are being filtered out
        print(f"\n📈 Rows being filtered out by count:")
        
        # Rows with count = 0
        count_0 = len(df[df[regular_col] == 0])
        if count_0 > 0:
            print(f"   Count = 0: {count_0} rows")
        
        # Rows with count = 1
        count_1 = len(df[df[regular_col] == 1])
        if count_1 > 0:
            print(f"   Count = 1: {count_1} rows")
        
        # Rows with count = 2
        count_2 = len(df[df[regular_col] == 2])
        if count_2 > 0:
            print(f"   Count = 2: {count_2} rows")
            
            # Show if any count=2 rows have Regular_Count == Consecutive_Count
            count_2_equal = len(df[(df[regular_col] == 2) & (df[regular_col] == df[consecutive_col])])
            print(f"     - Among these, {count_2_equal} have Regular_Count == Consecutive_Count")
        
        # Rows with count > 2
        count_gt2 = len(df[df[regular_col] > 2])
        if count_gt2 > 0:
            print(f"   Count > 2: {count_gt2} rows")
            
            # Show how many of these have Regular_Count == Consecutive_Count
            count_gt2_equal = len(df[(df[regular_col] > 2) & (df[regular_col] == df[consecutive_col])])
            print(f"     - Among these, {count_gt2_equal} have Regular_Count == Consecutive_Count")
        
        if len(df_filtered) == 0:
            print(f"\n⚠️ No rows matched all filters!")
            print(f"   Consider checking if columns contain the expected data types.")
            
            # Show some examples that might be close
            print(f"\n🔍 Examples of data that didn't match:")
            
            # Show rows where Regular_Count != Consecutive_Count
            diff_mask = df[regular_col] != df[consecutive_col]
            if diff_mask.any():
                diff_examples = df[diff_mask].head(3)
                print(f"\n   Rows where Regular_Count != Consecutive_Count:")
                for idx, row in diff_examples.iterrows():
                    print(f"     - Row {idx}: Regular={row[regular_col]}, Consecutive={row[consecutive_col]}")
            
            # Show rows where Regular_Count <= 2
            low_mask = df[regular_col] <= 2
            if low_mask.any():
                low_examples = df[low_mask].head(3)
                print(f"\n   Rows where Regular_Count <= 2:")
                for idx, row in low_examples.iterrows():
                    print(f"     - Row {idx}: Regular={row[regular_col]}, Consecutive={row[consecutive_col]}")
            
            return pd.DataFrame(), 0
        
        # Show what was filtered out
        print(f"\n📊 Filter statistics:")
        print(f"   Total rows: {len(df)}")
        print(f"   Rows filtered out: {len(df) - len(df_filtered)}")
        print(f"   Rows kept: {len(df_filtered)} ({len(df_filtered)/len(df)*100:.1f}%)")
        
        # Show distribution of kept rows by Regular_Count
        print(f"\n📈 Kept rows by Regular_Count:")
        kept_counts = df_filtered[regular_col].value_counts().sort_index()
        for count, freq in kept_counts.items():
            print(f"   Regular_Count = {count}: {freq} rows")
        
        # Save to Excel
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save the filtered data
        df_filtered.to_excel(output_file, index=False)
        print(f"\n✅ Filtered data saved to: {output_file}")
        print(f"   Rows: {len(df_filtered)}")
        print(f"   Columns: {len(df_filtered.columns)}")
        
        # Show sample of filtered data
        print(f"\n📝 Sample of filtered data (first 5 rows):")
        sample_cols = [regular_col, consecutive_col]
        
        # Add other interesting columns if available
        other_cols = ['Page', 'Label', 'Raw_Line', 'Regular_Numbers']
        for col in other_cols:
            for df_col in df_filtered.columns:
                if col.lower() in df_col.lower():
                    sample_cols.append(df_col)
                    break
        
        # Take unique columns
        sample_cols = list(dict.fromkeys(sample_cols))
        
        sample_df = df_filtered[sample_cols].head(5)
        for idx, row in sample_df.iterrows():
            print(f"\n   [{idx}]")
            for col in sample_cols:
                value = row[col]
                if isinstance(value, list):
                    print(f"     {col}: {str(value)[:50]}...")
                else:
                    print(f"     {col}: {str(value)[:50]}")
        
        return df_filtered, len(df_filtered)
        
    except Exception as e:
        print(f"\n❌ Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), 0

def get_input_file() -> str:
    """
    Ask user for input file path with autocomplete suggestions.
    
    Returns:
        Selected file path
    """
    print("\n" + "="*60)
    print("🔍 RAW EXTRACTION FILTER TOOL")
    print("="*60)
    print("Filters:")
    print("  1. Regular_Count == Consecutive_Count")
    print("  2. Regular_Count > 2 (excludes counts ≤ 2)")
    print()
    
    # Look for raw extraction files in current directory
    print("📁 Searching for raw extraction files in current directory...")
    current_dir = os.getcwd()
    
    # Common patterns for raw extraction files
    patterns = [
        '*raw*.xlsx',
        '*raw*.xls',
        '*extraction*.xlsx',
        '*financial*.xlsx',
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
        print("-" * 40)
        
        for i, file in enumerate(found_files, 1):
            file_size = os.path.getsize(file)
            size_str = f"{file_size/1024:.1f} KB"
            print(f"{i:2d}. {file} ({size_str})")
        
        print("-" * 40)
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
        print("\nPlease enter the full path to your raw extraction Excel file.")
        
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
    default_output = base_name + '_filtered_no2.xlsx'
    
    print(f"\n💾 Output file suggestion: {default_output}")
    print("   (Note: '_no2' indicates count=2 rows are filtered out)")
    
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

def main():
    """
    Main function to run the filtering process.
    """
    try:
        # Get input file from user
        input_file = get_input_file()
        
        # Get output file from user
        output_file = get_output_file(input_file)
        
        print("\n" + "="*60)
        print("🚀 STARTING FILTERING PROCESS")
        print("="*60)
        print("Filter criteria:")
        print("  1. Regular_Count == Consecutive_Count")
        print("  2. Regular_Count > 2 (excludes counts of 0, 1, and 2)")
        print("="*60)
        
        # Run the filtering
        df_filtered, count = filter_raw_extraction(input_file, output_file)
        
        if count > 0:
            print("\n" + "="*60)
            print("🎉 FILTERING COMPLETE!")
            print("="*60)
            print(f"✅ Successfully filtered {count} rows")
            print(f"📂 Output saved to: {output_file}")
            print("\n📊 Summary:")
            print(f"   - Only rows with Regular_Count > 2 are kept")
            print(f"   - Rows with count = 2 or less are filtered out")
            print(f"   - Only consecutive regular numbers are included")
            
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
        else:
            print("\n" + "="*60)
            print("⚠️ FILTERING COMPLETE - NO DATA MATCHED")
            print("="*60)
            print("No rows matched the filter criteria.")
            print("\nPossible reasons:")
            print("  1. No rows have Regular_Count == Consecutive_Count")
            print("  2. All rows have Regular_Count ≤ 2")
            print("  3. Column names don't match expected patterns")
        
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
    print("🔍 RAW EXTRACTION FILTER TOOL")
    print("="*60)
    print("This tool filters raw extraction data where:")
    print("  1. Regular_Count == Consecutive_Count")
    print("  2. Regular_Count > 2 (excludes counts ≤ 2)")
    print()
    print("It helps identify financial table rows where:")
    print("  - All regular numbers appear consecutively")
    print("  - There are MORE THAN 2 regular numbers")
    print("  - (Counts of 0, 1, or 2 are excluded)")
    print("="*60)
    
    main()
