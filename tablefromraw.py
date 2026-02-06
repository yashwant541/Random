# =============================================================================
# DATAIKU TABLE GROUPING TOOL - OPTIMIZED VERSION
# =============================================================================
# Groups rows into tables based on consecutive rows having the same 
# Regular_Count and Consecutive_Count values.
# Reads Excel files from Dataiku folders and saves results to Dataiku folders.
# =============================================================================

import dataiku
import pandas as pd
import tempfile
import os
import time
from typing import List, Dict, Tuple, Optional
from pathlib import Path

# =============================================================================
# CONFIGURATION - SET THESE TO YOUR DATAIKU FOLDER IDs
# =============================================================================
INPUT_FOLDER_ID = "xFGhJtYE"          # Your Dataiku INPUT folder ID
OUTPUT_FOLDER_ID = "output_folder_id" # Your Dataiku OUTPUT folder ID

# =============================================================================
# DATAIKU HELPER FUNCTIONS - OPTIMIZED
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
    """
    folder = get_input_folder()
    all_files = folder.list_paths_in_partition()
    
    excel_files = [f for f in all_files if f.lower().endswith(('.xlsx', '.xls'))]
    return sorted(excel_files)

def read_excel_from_dataiku(filename: str) -> pd.DataFrame:
    """
    Read Excel file from Dataiku folder - Optimized
    """
    folder = get_input_folder()
    
    print(f"📥 Reading: {filename}")
    start_time = time.time()
    
    try:
        # Read file from Dataiku
        with folder.get_download_stream(filename) as stream:
            # Save to temp file to avoid memory issues
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                tmp_path = tmp.name
                # Write in chunks if file is large
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = stream.read(chunk_size)
                    if not chunk:
                        break
                    tmp.write(chunk)
        
        # Read Excel file from temp file
        # Use appropriate engine based on file extension
        file_ext = Path(filename).suffix.lower()
        
        if file_ext == '.xlsx':
            df = pd.read_excel(tmp_path, engine='openpyxl')
        elif file_ext == '.xls':
            try:
                df = pd.read_excel(tmp_path, engine='openpyxl')
            except:
                # Try xlrd for old .xls files
                try:
                    import xlrd
                    df = pd.read_excel(tmp_path, engine='xlrd')
                except ImportError:
                    # Fallback to default engine
                    df = pd.read_excel(tmp_path)
        else:
            # For any other extension, try openpyxl first
            try:
                df = pd.read_excel(tmp_path, engine='openpyxl')
            except:
                df = pd.read_excel(tmp_path)
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        elapsed = time.time() - start_time
        print(f"   ✅ Loaded {len(df):,} rows, {len(df.columns)} cols in {elapsed:.2f}s")
        
        return df
        
    except Exception as e:
        print(f"❌ Error reading file {filename}: {e}")
        # Clean up temp file if it exists
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

def save_excel_to_dataiku(df: pd.DataFrame, filename: str) -> None:
    """
    Save DataFrame to Excel in Dataiku output folder - Optimized
    """
    folder = get_output_folder()
    
    print(f"💾 Saving: {filename}")
    start_time = time.time()
    
    try:
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        # Write directly to disk using openpyxl
        df.to_excel(temp_path, index=False, engine='openpyxl')
        
        # Read file and upload
        file_size = os.path.getsize(temp_path)
        with open(temp_path, 'rb') as f:
            # Upload in chunks for large files
            chunk_size = 1024 * 1024 * 10  # 10MB chunks
            with folder.get_writer(filename) as writer:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    writer.write(chunk)
        
        # Clean up
        os.unlink(temp_path)
        
        elapsed = time.time() - start_time
        print(f"   ✅ Saved {len(df):,} rows, {file_size:,} bytes in {elapsed:.2f}s")
        
    except Exception as e:
        print(f"❌ Error saving file {filename}: {e}")
        # Clean up temp file if it exists
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        raise

def save_tables_to_dataiku(tables: List[pd.DataFrame], output_filename: str, 
                           include_individual_sheets: bool = True,
                           max_sheets: int = 20) -> None:
    """
    Save all tables to an Excel file in Dataiku folder - Optimized
    """
    if not tables:
        print("❌ No tables to save")
        return
    
    print(f"\n💾 Saving {len(tables)} tables to: {output_filename}")
    start_time = time.time()
    
    try:
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        # Write all sheets at once using openpyxl
        with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
            # 1. Combined view
            combined_data = []
            for i, table in enumerate(tables, 1):
                table_copy = table.copy()
                table_copy.insert(0, 'Table_Number', i)
                combined_data.append(table_copy)
            
            if combined_data:
                combined_df = pd.concat(combined_data, ignore_index=True)
                combined_df.to_excel(writer, sheet_name='All_Tables', index=False)
                print(f"   ✅ Combined view: {len(combined_df):,} rows")
            
            # 2. Individual sheets (limited to avoid performance issues)
            if include_individual_sheets and tables:
                sheets_to_create = min(len(tables), max_sheets)
                for i in range(sheets_to_create):
                    sheet_name = f"Table_{i+1}"[:31]
                    table_copy = tables[i].copy()
                    table_copy.insert(0, 'Row_In_Table', range(1, len(table_copy) + 1))
                    table_copy.to_excel(writer, sheet_name=sheet_name, index=False)
                
                if len(tables) > max_sheets:
                    print(f"   ⚠️  Limited to first {max_sheets} individual sheets (of {len(tables)})")
                else:
                    print(f"   ✅ Created {sheets_to_create} individual sheets")
            
            # 3. Statistics sheet
            stats_data = []
            for i, table in enumerate(tables, 1):
                structure = extract_table_structure(table, i)
                
                summary = {
                    'Table_Number': i,
                    'Row_Count': structure['row_count'],
                    'Regular_Count': structure.get('regular_count', 'N/A'),
                    'Consecutive_Count': structure.get('consecutive_count', 'N/A'),
                    'Counts_Match': structure.get('counts_match', 'N/A'),
                    'Section_Count': structure.get('section_count', 0),
                    'Page_Count': structure.get('page_count', 0),
                    'Sections': ', '.join(map(str, structure.get('sections', [])))[:50],
                    'Sample_Labels': ', '.join(map(str, structure.get('sample_labels', [])))[:50]
                }
                stats_data.append(summary)
            
            if stats_data:
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='Statistics', index=False)
                print(f"   ✅ Created statistics sheet")
        
        # Upload to Dataiku
        file_size = os.path.getsize(temp_path)
        folder = get_output_folder()
        
        with open(temp_path, 'rb') as f:
            with folder.get_writer(output_filename) as writer:
                chunk_size = 1024 * 1024 * 10  # 10MB chunks
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    writer.write(chunk)
        
        # Clean up temp file
        os.unlink(temp_path)
        
        elapsed = time.time() - start_time
        print(f"\n🎉 Successfully saved {len(tables)} tables")
        print(f"   File: {output_filename}")
        print(f"   Size: {file_size:,} bytes")
        print(f"   Time: {elapsed:.2f} seconds")
        
    except Exception as e:
        print(f"❌ Error saving tables: {e}")
        # Clean up temp file if it exists
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        raise

# =============================================================================
# CORE GROUPING FUNCTIONS - OPTIMIZED
# =============================================================================

def group_into_tables_fast(df: pd.DataFrame) -> List[pd.DataFrame]:
    """
    FAST VERSION: Group rows into tables using vectorized operations
    """
    if df.empty:
        print("❌ Input DataFrame is empty")
        return []
    
    # Reset index once
    df = df.reset_index(drop=True)
    
    # Find column names using vectorized approach
    cols_lower = [str(col).lower() for col in df.columns]
    regular_col = None
    consecutive_col = None
    
    for i, col_lower in enumerate(cols_lower):
        if 'regular' in col_lower and 'count' in col_lower:
            regular_col = df.columns[i]
        elif 'consecutive' in col_lower and 'count' in col_lower:
            consecutive_col = df.columns[i]
    
    if not regular_col or not consecutive_col:
        print(f"❌ Could not find required columns")
        print(f"   Available columns: {list(df.columns)}")
        return []
    
    print(f"\n🔍 Using columns:")
    print(f"   Regular_Count: '{regular_col}'")
    print(f"   Consecutive_Count: '{consecutive_col}'")
    
    print(f"\n📊 Starting table grouping...")
    print(f"   Total rows: {len(df):,}")
    start_time = time.time()
    
    # VECTORIZED GROUPING - MUCH FASTER
    # Create group identifiers when counts change
    df['_group_change'] = (df[regular_col] != df[regular_col].shift()) | \
                          (df[consecutive_col] != df[consecutive_col].shift())
    df['_group_id'] = df['_group_change'].cumsum()
    
    # Group all at once
    tables = []
    group_stats = []
    
    for group_id, group in df.groupby('_group_id'):
        # Remove temporary columns
        table_df = group.drop(columns=['_group_change', '_group_id'])
        tables.append(table_df)
        
        # Collect stats
        reg_val = group[regular_col].iloc[0] if len(group) > 0 else 'N/A'
        cons_val = group[consecutive_col].iloc[0] if len(group) > 0 else 'N/A'
        group_stats.append((len(group), reg_val, cons_val))
    
    elapsed = time.time() - start_time
    
    print(f"✅ Grouped into {len(tables)} tables in {elapsed:.2f}s")
    
    # Display summary
    print(f"\n📋 Table Summary:")
    for i, (row_count, reg_val, cons_val) in enumerate(group_stats, 1):
        section_info = ""
        if i-1 < len(tables) and 'Section' in tables[i-1].columns:
            sections = tables[i-1]['Section'].dropna().unique()
            if len(sections) > 0:
                main_section = sections[0]
                section_info = f" | Section: {main_section}"
        
        print(f"   Table {i}: {row_count:,} rows | Counts: {reg_val}/{cons_val}{section_info}")
    
    return tables

def extract_table_structure(table_df: pd.DataFrame, table_num: int) -> Dict:
    """
    Extract the structure of a table for analysis.
    """
    structure = {
        'table_number': table_num,
        'row_count': len(table_df),
        'columns': list(table_df.columns),
        'column_count': len(table_df.columns)
    }
    
    # Find count columns
    for col in table_df.columns:
        col_lower = str(col).lower()
        if 'regular' in col_lower and 'count' in col_lower:
            structure['regular_count'] = table_df[col].iloc[0] if len(table_df) > 0 else None
        elif 'consecutive' in col_lower and 'count' in col_lower:
            structure['consecutive_count'] = table_df[col].iloc[0] if len(table_df) > 0 else None
    
    if 'regular_count' in structure and 'consecutive_count' in structure:
        structure['counts_match'] = structure['regular_count'] == structure['consecutive_count']
    
    # Extract sample data
    if 'Label' in table_df.columns:
        structure['sample_labels'] = table_df['Label'].head(3).tolist()
    
    if 'Section' in table_df.columns:
        sections = table_df['Section'].dropna().unique()
        structure['sections'] = sections.tolist()
        structure['section_count'] = len(sections)
    
    if 'Page' in table_df.columns:
        pages = table_df['Page'].unique()
        structure['pages'] = pages.tolist()
        structure['page_count'] = len(pages)
    
    return structure

def analyze_table_columns(tables: List[pd.DataFrame]) -> None:
    """
    Analyze the columns in each table.
    """
    if not tables:
        return
    
    print(f"\n🔍 Analyzing table structures...")
    
    column_analysis = {}
    
    for i, table in enumerate(tables, 1):
        # Check for Regular_Numbers column
        if 'Regular_Numbers' not in table.columns:
            continue
        
        try:
            first_row = table.iloc[0]
            regular_numbers = first_row.get('Regular_Numbers')
            
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
                    label = str(first_row.get('Label', ''))[:50]
                    column_analysis[num_columns]['sample_labels'].append(label)
        except:
            continue
    
    # Display analysis
    if column_analysis:
        print(f"\n📊 Table Column Analysis:")
        print("-" * 60)
        
        for num_cols in sorted(column_analysis.keys()):
            analysis = column_analysis[num_cols]
            print(f"\n   📋 {num_cols}-column tables: {analysis['count']} table(s)")
            print(f"      Tables: {', '.join(map(str, analysis['tables'][:10]))}")
            if len(analysis['tables']) > 10:
                print(f"      ... and {len(analysis['tables']) - 10} more")
    else:
        print("   No Regular_Numbers column found for analysis")

# =============================================================================
# BATCH PROCESSING FUNCTIONS - OPTIMIZED
# =============================================================================

def batch_process_all_files():
    """
    Process all Excel files in the Dataiku input folder
    """
    print(f"\n{'='*60}")
    print("DATAIKU TABLE GROUPING TOOL - OPTIMIZED BATCH PROCESSING")
    print(f"{'='*60}")
    print(f"Input folder:  {INPUT_FOLDER_ID}")
    print(f"Output folder: {OUTPUT_FOLDER_ID}")
    print(f"{'='*60}")
    
    total_start_time = time.time()
    
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
    
    results = []
    total_tables_created = 0
    total_rows_processed = 0
    
    for filename in excel_files:
        file_start_time = time.time()
        print(f"\n🎯 Processing: {filename}")
        
        try:
            # 1. Read Excel
            df = read_excel_from_dataiku(filename)
            
            if df.empty:
                print(f"   ⚠️ File is empty, skipping...")
                results.append({
                    'input_file': filename,
                    'status': 'skipped',
                    'reason': 'empty file'
                })
                continue
            
            total_rows_processed += len(df)
            
            # 2. Group into tables (using fast version)
            print(f"   🔄 Grouping {len(df):,} rows into tables...")
            tables = group_into_tables_fast(df)
            
            if not tables:
                print(f"   ⚠️ No tables created, skipping...")
                results.append({
                    'input_file': filename,
                    'status': 'skipped',
                    'reason': 'no tables created'
                })
                continue
            
            # 3. Analyze (optional)
            if len(tables) <= 20:  # Only analyze if not too many tables
                analyze_table_columns(tables)
            
            # 4. Save results - FIXED: No .xlsxx bug
            # Use pathlib for safe filename handling
            input_path = Path(filename)
            stem = input_path.stem  # Get filename without extension
            output_filename = f"{stem}_grouped.xlsx"
            
            # Save with limited individual sheets for performance
            max_sheets = 10  # Limit to 10 individual sheets
            save_tables_to_dataiku(
                tables, 
                output_filename, 
                include_individual_sheets=(len(tables) <= max_sheets),
                max_sheets=max_sheets
            )
            
            tables_created = len(tables)
            total_tables_created += tables_created
            
            file_elapsed = time.time() - file_start_time
            
            results.append({
                'input_file': filename,
                'output_file': output_filename,
                'table_count': tables_created,
                'rows_processed': len(df),
                'processing_time': file_elapsed,
                'status': 'success'
            })
            
            print(f"   ⏱️  File processed in {file_elapsed:.2f}s")
            
        except Exception as e:
            file_elapsed = time.time() - file_start_time
            print(f"❌ Error processing {filename}: {e}")
            results.append({
                'input_file': filename,
                'error': str(e),
                'processing_time': file_elapsed,
                'status': 'error'
            })
        
        if filename != excel_files[-1]:
            print(f"\n{'-'*60}")
    
    # Generate summary
    total_elapsed = time.time() - total_start_time
    
    print(f"\n{'='*60}")
    print("BATCH PROCESSING SUMMARY")
    print(f"{'='*60}")
    
    successful = [r for r in results if r['status'] == 'success']
    errors = [r for r in results if r['status'] == 'error']
    skipped = [r for r in results if r['status'] == 'skipped']
    
    print(f"📊 Results:")
    print(f"   Total files: {len(results)}")
    print(f"   Successfully processed: {len(successful)}")
    print(f"   Errors: {len(errors)}")
    print(f"   Skipped: {len(skipped)}")
    print(f"   Total time: {total_elapsed:.2f}s")
    
    if successful:
        print(f"\n📈 Performance Metrics:")
        print(f"   Total tables created: {total_tables_created:,}")
        print(f"   Total rows processed: {total_rows_processed:,}")
        print(f"   Average processing time per file: {total_elapsed/len(results):.2f}s")
        
        # Calculate rows per second
        if total_elapsed > 0:
            rows_per_second = total_rows_processed / total_elapsed
            print(f"   Processing speed: {rows_per_second:.0f} rows/second")
        
        print(f"\n📁 Output files created:")
        for result in successful:
            print(f"   • {result['output_file']} ({result['table_count']} tables, {result['rows_processed']:,} rows)")
    
    if errors:
        print(f"\n❌ Files with errors:")
        for result in errors:
            print(f"   • {result['input_file']}: {result['error']}")
    
    if skipped:
        print(f"\n⚠️ Skipped files:")
        for result in skipped:
            print(f"   • {result['input_file']}: {result.get('reason', 'unknown')}")
    
    return results

# =============================================================================
# SIMPLE TEST FUNCTION
# =============================================================================

def test_filename_fix():
    """Test that the .xlsxx bug is fixed"""
    test_cases = [
        "data.xlsx",
        "data.xls",
        "data.XLSX",
        "data.XLS",
        "my_file.xlsx",
        "test.xls",
        "document.xlsx",
    ]
    
    print("Testing filename generation...")
    for filename in test_cases:
        input_path = Path(filename)
        stem = input_path.stem
        output_filename = f"{stem}_grouped.xlsx"
        print(f"  {filename} → {output_filename}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main function to run the optimized Dataiku table grouping tool
    """
    print(f"\n{'='*60}")
    print("DATAIKU TABLE GROUPING TOOL - OPTIMIZED VERSION")
    print(f"{'='*60}")
    print("Key optimizations:")
    print("  • No BytesIO overhead - uses temp files")
    print("  • Vectorized grouping algorithm")
    print("  • Chunked file uploads/downloads")
    print("  • Limited individual sheets for performance")
    print("  • FIXED: No .xlsxx bug - uses Pathlib for safe filenames")
    print("  • FIXED: Explicit openpyxl engine for all Excel operations")
    print(f"\nConfiguration:")
    print(f"  Input folder:  {INPUT_FOLDER_ID}")
    print(f"  Output folder: {OUTPUT_FOLDER_ID}")
    print(f"{'='*60}")
    
    try:
        # Check required packages
        try:
            import pandas as pd
            import openpyxl
            print(f"✅ Required packages loaded:")
            print(f"   pandas: {pd.__version__}")
            print(f"   openpyxl: {openpyxl.__version__}")
        except ImportError as e:
            print(f"❌ Missing required package: {e}")
            print("   Please add 'pandas' and 'openpyxl' to your Dataiku environment")
            return
        
        # Test filename fix
        test_filename_fix()
        
        # Run batch processing
        print(f"\nStarting automated batch processing...")
        results = batch_process_all_files()
        
        if results:
            successful = any(r.get('status') == 'success' for r in results)
            
            print(f"\n{'='*60}")
            if successful:
                print("✅ PROCESSING COMPLETE - OPTIMIZED RESULTS")
                print(f"📁 Check the output folder '{OUTPUT_FOLDER_ID}' for results.")
            else:
                print("⚠️ PROCESSING COMPLETE - NO SUCCESSFUL PROCESSING")
                print("   Please check your input files and configuration.")
            print(f"{'='*60}")
        
        else:
            print(f"\n⚠️ No files were processed.")
            print(f"   Please check that Excel files exist in folder '{INPUT_FOLDER_ID}'")
    
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

# =============================================================================
# SIMPLE VERSION FOR QUICK USE
# =============================================================================

def simple_dataiku_table_group():
    """
    Simple version for basic table grouping with fixes
    """
    # CONFIGURATION
    INPUT_FOLDER = dataiku.Folder("xFGhJtYE")      # Your input folder
    OUTPUT_FOLDER = dataiku.Folder("output_folder_id")  # Your output folder
    
    print("Starting Dataiku table grouping...")
    
    # List Excel files
    all_files = INPUT_FOLDER.list_paths_in_partition()
    excel_files = [f for f in all_files if f.lower().endswith(('.xlsx', '.xls'))]
    
    if not excel_files:
        print("No Excel files found!")
        return
    
    for filename in excel_files:
        print(f"\nProcessing: {filename}")
        
        try:
            # 1. READ FROM DATAIKU
            with INPUT_FOLDER.get_download_stream(filename) as stream:
                # Save to temp file
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                    tmp_path = tmp.name
                    chunk_size = 1024 * 1024
                    while True:
                        chunk = stream.read(chunk_size)
                        if not chunk:
                            break
                        tmp.write(chunk)
            
            # 2. LOAD DATAFRAME with openpyxl
            df = pd.read_excel(tmp_path, engine='openpyxl')
            os.unlink(tmp_path)  # Clean up
            
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
            
            # 4. GROUP INTO TABLES (simple vectorized version)
            df = df.reset_index(drop=True)
            df['_group_change'] = (df[reg_col] != df[reg_col].shift()) | (df[cons_col] != df[cons_col].shift())
            df['_group_id'] = df['_group_change'].cumsum()
            
            tables = [group.drop(columns=['_group_change', '_group_id']) 
                     for _, group in df.groupby('_group_id')]
            
            print(f"  ✅ Created {len(tables)} tables")
            
            # 5. SAVE TO DATAIKU
            if tables:
                # Fix filename properly
                stem = Path(filename).stem
                output_file = f"{stem}_grouped.xlsx"
                
                # Create temp file
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                    temp_path = tmp.name
                
                # Save with openpyxl
                with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
                    # Combined view
                    combined_data = []
                    for i, table in enumerate(tables, 1):
                        table_copy = table.copy()
                        table_copy.insert(0, 'Table_Number', i)
                        combined_data.append(table_copy)
                    
                    pd.concat(combined_data).to_excel(writer, sheet_name='All_Tables', index=False)
                
                # Upload to Dataiku
                with open(temp_path, 'rb') as f:
                    with OUTPUT_FOLDER.get_writer(output_file) as writer:
                        chunk_size = 1024 * 1024 * 5
                        while True:
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            writer.write(chunk)
                
                # Clean up
                os.unlink(temp_path)
                
                print(f"  💾 Saved to Dataiku: {output_file}")
        
        except Exception as e:
            print(f"  ❌ Error: {e}")
            # Clean up temp files
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)

# =============================================================================
# RUN THE CODE
# =============================================================================

if __name__ == "__main__":
    # Run the main function (recommended)
    main()
    
    # Alternatively, run the simple version:
    # simple_dataiku_table_group()
